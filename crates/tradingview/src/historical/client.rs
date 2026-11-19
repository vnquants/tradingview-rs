use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;
use tracing::{debug, error, instrument, trace, warn};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    DataPoint, DataServer, Error, OHLCV, Result, SymbolInfo,
    chart::options::Range,
    historical::{
        HistoricalRequest, HistoricalResult, PaginationPlan, PaginationStop, state::HistoricalState,
    },
    live::handler::{CommandTx, Handler, HandlerFactory},
    live::models::TradingViewDataEvent,
    live::websocket::{ShutdownGuard, WebSocketClient},
    utils::symbol_init,
};

/// Series identifier used in `create_series` and `request_more_data`.
const SERIES_IDENTIFIER: &str = "sds_1";
/// Series id (turnaround id) used in `create_series`.
const SERIES_ID: &str = "s1";

/// High-level client for fetching historical TradingView chart data.
pub struct HistoricalClient {
    pub(crate) auth_token: String,
    pub(crate) server: DataServer,
}

impl HistoricalClient {
    pub fn new(auth_token: impl Into<String>, server: DataServer) -> Self {
        Self {
            auth_token: auth_token.into(),
            server,
        }
    }

    /// Fetch historical bars for one symbol over a new WebSocket connection.
    ///
    /// Sends one `create_series` request and, when the request enables
    /// pagination, `request_more_data` pages until a stop condition holds; see
    /// [`HistoricalRequest`]. The connection is closed before returning, also
    /// when this future is dropped.
    #[instrument(skip(self), fields(symbol, exchange))]
    pub async fn retrieve(&self, request: HistoricalRequest) -> Result<HistoricalResult> {
        // Reject an incomplete request before opening a connection.
        request.resolve_symbol_exchange()?;
        let connection = HistoricalConnection::open(&self.auth_token, self.server).await?;
        let result = connection.fetch(&request).await;
        // Cleanup problems must not discard data that was already retrieved.
        connection.close().await;
        result
    }

    pub(crate) async fn wait_for_completion(state: &Arc<Mutex<HistoricalState>>) {
        wait_until(state, |state| state.completed || state.errored).await;
    }
}

/// Wait until `predicate` holds for the shared state. Lost-wakeup safe: the
/// waiter registers before the predicate is checked.
async fn wait_until(
    state: &Arc<Mutex<HistoricalState>>,
    predicate: impl Fn(&HistoricalState) -> bool,
) {
    let notify = {
        let guard = state.lock().unwrap();
        guard.notify.clone()
    };
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if predicate(&state.lock().unwrap()) {
            break;
        }

        notified.await;
    }
}

/// Return the recorded protocol error, if any.
fn take_error(state: &Mutex<HistoricalState>) -> Result<()> {
    let mut guard = state.lock().unwrap();
    if guard.errored {
        let message = guard
            .error_message
            .take()
            .unwrap_or_else(|| "Historical data retrieval failed".to_string());
        return Err(Error::Internal(message.into()));
    }
    Ok(())
}

// =============================================================================
// HistoricalConnection
// =============================================================================

/// One WebSocket connection that runs historical fetches one after another,
/// each in its own chart session. Batch workers reuse it across symbols.
pub(crate) struct HistoricalConnection {
    ws: Arc<WebSocketClient<SessionRouter>>,
    router: SessionRouter,
    reader: JoinHandle<()>,
    // Stops background tasks even if the owning future is dropped.
    shutdown_guard: ShutdownGuard<SessionRouter>,
}

impl HistoricalConnection {
    pub(crate) async fn open(auth_token: &str, server: DataServer) -> Result<Self> {
        let router = SessionRouter::default();
        let ws = WebSocketClient::builder()
            .auth_token(auth_token)
            .server(server)
            .handler(router.clone())
            .build()
            .await?;
        let shutdown_guard = ws.shutdown_guard();
        let reader = Arc::clone(&ws).spawn_reader_task_with_handle();
        Ok(Self {
            ws,
            router,
            reader,
            shutdown_guard,
        })
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.ws.is_closed()
    }

    pub(crate) async fn close(self) {
        let Self {
            ws,
            reader,
            shutdown_guard,
            ..
        } = self;
        if let Err(error) = ws.close_reader_task(reader).await {
            warn!(?error, "Failed to close WebSocket after retrieval");
        }
        drop(shutdown_guard);
    }

    /// Fetch one symbol in a new chart session and delete the session after.
    pub(crate) async fn fetch(&self, request: &HistoricalRequest) -> Result<HistoricalResult> {
        let started = Instant::now();
        let (symbol, exchange) = request.resolve_symbol_exchange()?;
        debug!(symbol = %symbol, exchange = %exchange, "Historical retrieval started");

        let plan = request.pagination_plan();
        let initial_bars = initial_bar_count(request, plan.as_ref());
        let capacity = plan
            .and_then(|plan| plan.max_bars)
            .or(initial_bars)
            .unwrap_or_default()
            .min(100_000) as usize;
        let state = Arc::new(Mutex::new(HistoricalState::with_capacity(capacity)));
        let chart_session = format!("cs_{}", crate::utils::gen_id());
        self.router.register(&chart_session, Arc::clone(&state));

        let outcome = self
            .run(
                request,
                plan.as_ref(),
                initial_bars,
                &symbol,
                &exchange,
                &chart_session,
                &state,
            )
            .await;

        if !self.ws.is_closed()
            && let Err(error) = self.ws.delete_chart_session(&chart_session).await
        {
            debug!(?error, "Failed to delete chart session");
        }
        self.router.unregister(&chart_session);
        let outcome = outcome?;

        let mut guard = state.lock().unwrap();
        let total_bars = guard.total_bars;
        let data = trim_to_request(guard.finalize(), request.from_timestamp, request.max_bars);
        let symbol_info = guard
            .symbol_info
            .take()
            .ok_or_else(|| Error::Internal("No symbol info received".into()))?;
        Ok(HistoricalResult {
            symbol_info,
            data,
            series_info: guard.series_info.take(),
            total_bars_received: total_bars,
            replay_used: request.with_replay,
            elapsed: started.elapsed(),
            pages_fetched: outcome.map_or(0, |outcome| outcome.pages),
            pagination_stop: outcome.map(|outcome| outcome.stop),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn run(
        &self,
        request: &HistoricalRequest,
        plan: Option<&PaginationPlan>,
        initial_bars: Option<u64>,
        symbol: &str,
        exchange: &str,
        chart_session: &str,
        state: &Arc<Mutex<HistoricalState>>,
    ) -> Result<Option<PaginationOutcome>> {
        // ── Protocol sequence ──────────────────────────────────────────
        //   1. chart_create_session
        //   2. resolve_symbol       → symbol_resolved (SymbolInfo)
        //   3. create_series        → timescale_update ... series_completed
        //   4. request_more_data    → older bars ... series_completed (repeat)
        let instrument = format!("{exchange}:{symbol}");
        let symbol_series_id = format!("sds_sym_{}", crate::utils::gen_id());

        self.ws.create_chart_session(chart_session).await?;
        debug!(session = %chart_session, "Chart session created");

        let symbol_init_str = symbol_init()
            .instrument(&instrument)
            .maybe_adjustment(request.adjustment)
            .maybe_currency(request.currency)
            .maybe_session_type(request.session_type)
            .call()?;
        self.ws
            .send(
                "resolve_symbol",
                &[
                    Value::from(chart_session),
                    Value::from(symbol_series_id.as_str()),
                    Value::from(symbol_init_str),
                ],
            )
            .await?;
        debug!(instrument = %instrument, "Symbol resolution requested");

        // In count mode, exactly 6 arguments are sent (no range).
        // In range mode, exactly 7 arguments are sent with a positive bar count.
        let create_series_args = build_create_series_args(
            chart_session,
            SERIES_IDENTIFIER,
            SERIES_ID,
            &symbol_series_id,
            request.interval,
            initial_bars,
            request.range,
        );
        self.ws.send("create_series", &create_series_args).await?;
        debug!(
            interval = ?request.interval,
            bars = initial_bars,
            range = ?request.range,
            "Data series created"
        );

        let loaded = tokio::time::timeout(
            request.timeout,
            HistoricalClient::wait_for_completion(state),
        )
        .await;
        if loaded.is_err() {
            return Err(Error::Timeout("Historical data retrieval timed out".into()));
        }
        take_error(state)?;

        let Some(plan) = plan else {
            return Ok(None);
        };
        let ws = &self.ws;
        let outcome = paginate(state, plan, |count| async move {
            ws.request_more_data(chart_session, SERIES_IDENTIFIER, count)
                .await
        })
        .await?;
        Ok(Some(outcome))
    }
}

/// Bars for the initial `create_series`: the requested `num_bars`, or with
/// pagination and no `num_bars` one page (capped at `max_bars`).
fn initial_bar_count(request: &HistoricalRequest, plan: Option<&PaginationPlan>) -> Option<u64> {
    match (request.num_bars, plan) {
        (Some(num_bars), _) => Some(num_bars),
        (None, Some(plan)) if request.range.is_none() => Some(
            plan.max_bars
                .map_or(plan.page_size, |max_bars| max_bars.min(plan.page_size))
                .max(1),
        ),
        (None, _) => None,
    }
}

/// Drop bars older than `from_timestamp` and keep only the newest `max_bars`
/// of the sorted, de-duplicated `data`.
fn trim_to_request(
    mut data: Vec<DataPoint>,
    from_timestamp: Option<i64>,
    max_bars: Option<u64>,
) -> Vec<DataPoint> {
    if let Some(from) = from_timestamp {
        data.retain(|point| point.timestamp() >= from);
    }
    if let Some(max_bars) = max_bars {
        let max_bars = usize::try_from(max_bars).unwrap_or(usize::MAX);
        if data.len() > max_bars {
            data.drain(..data.len() - max_bars);
        }
    }
    data
}

// =============================================================================
// Pagination
// =============================================================================

/// What the pagination loop did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PaginationOutcome {
    pub(crate) pages: u32,
    pub(crate) stop: PaginationStop,
}

/// Walk back with `request_more_data` until a stop condition of `plan` holds.
///
/// Must be called after the initial series completed. `request_more` sends
/// one `request_more_data` for the given number of bars; the handler records
/// the response in `state`. A page that does not complete within
/// `plan.page_timeout` ends the walk with [`PaginationStop::Timeout`]; a
/// protocol error or a failed send is returned as an error.
pub(crate) async fn paginate<F, Fut>(
    state: &Arc<Mutex<HistoricalState>>,
    plan: &PaginationPlan,
    mut request_more: F,
) -> Result<PaginationOutcome>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut pages = 0u32;
    let mut previous_earliest = None;
    loop {
        let (bars, earliest, data_completed, completions) = {
            let mut guard = state.lock().unwrap();
            let (bars, earliest) = guard.bar_count_and_earliest();
            (
                bars,
                earliest,
                guard.data_completed.clone(),
                guard.completions,
            )
        };
        if let Some(stop) = stop_reason(
            plan,
            pages,
            bars,
            earliest,
            previous_earliest,
            data_completed.as_deref(),
        ) {
            debug!(pages, bars, ?earliest, %stop, "Pagination stopped");
            return Ok(PaginationOutcome { pages, stop });
        }
        previous_earliest = earliest;

        if !plan.page_delay.is_zero() {
            tokio::time::sleep(plan.page_delay).await;
        }
        request_more(plan.page_size).await?;
        pages += 1;
        trace!(page = pages, bars, ?earliest, "Requested more data");

        let page_done = tokio::time::timeout(
            plan.page_timeout,
            wait_until(state, |state| {
                state.errored || state.completions > completions
            }),
        )
        .await;
        if page_done.is_err() {
            warn!(
                page = pages,
                bars, "Timed out waiting for a page; returning the bars received so far"
            );
            return Ok(PaginationOutcome {
                pages,
                stop: PaginationStop::Timeout,
            });
        }
        take_error(state)?;
    }
}

/// First stop condition that holds after `pages` pages, if any.
fn stop_reason(
    plan: &PaginationPlan,
    pages: u32,
    bars: usize,
    earliest: Option<i64>,
    previous_earliest: Option<i64>,
    data_completed: Option<&str>,
) -> Option<PaginationStop> {
    if let (Some(from), Some(earliest)) = (plan.from_timestamp, earliest)
        && earliest <= from
    {
        return Some(PaginationStop::ReachedFrom);
    }
    if plan
        .max_bars
        .is_some_and(|max_bars| bars as u64 >= max_bars)
    {
        return Some(PaginationStop::ReachedMaxBars);
    }
    match data_completed {
        Some("end") => return Some(PaginationStop::EndOfHistory),
        Some("limit") => return Some(PaginationStop::BarLimit),
        Some(other) => {
            warn!(
                data_completed = other,
                "Unknown data_completed value; treating it as no more data"
            );
            return Some(PaginationStop::NoMoreData);
        }
        None => {}
    }
    let no_older_bar = match (earliest, previous_earliest) {
        (None, _) => true,
        (Some(earliest), Some(previous)) => pages > 0 && earliest >= previous,
        (Some(_), None) => false,
    };
    if no_older_bar {
        return Some(PaginationStop::NoMoreData);
    }
    (pages >= plan.max_pages).then_some(PaginationStop::MaxPages)
}

/// Builds the argument list for TradingView's `create_series` WebSocket message.
///
/// In count mode (`range` is `None`), the message must have exactly 6 arguments:
/// `[chart_session, series_identifier, series_id, symbol_series_id, interval, bar_count]`.
///
/// In range mode (`range` is `Some`), the message must have exactly 7 arguments:
/// `[chart_session, series_identifier, series_id, symbol_series_id, interval, bar_count, range]`.
/// Range requests require a positive bar count: preserve the requested value,
/// default to 512 when omitted, and normalize an explicit zero to 1.
/// Providing 7 arguments with an empty range causes the server to fail with
/// `critical_error: "unsupported method: du"`.
pub(crate) fn build_create_series_args(
    chart_session: &str,
    series_identifier: &str,
    series_id: &str,
    symbol_series_id: &str,
    interval: crate::Interval,
    num_bars: Option<u64>,
    range: Option<Range>,
) -> Vec<Value> {
    if let Some(r) = range {
        vec![
            Value::from(chart_session),
            Value::from(series_identifier),
            Value::from(series_id),
            Value::from(symbol_series_id),
            Value::from(interval.to_string()),
            Value::from(num_bars.unwrap_or(512).max(1)),
            Value::from(r.to_string()),
        ]
    } else {
        let bar_count = num_bars.unwrap_or(100);
        vec![
            Value::from(chart_session),
            Value::from(series_identifier),
            Value::from(series_id),
            Value::from(symbol_series_id),
            Value::from(interval.to_string()),
            Value::from(bar_count),
        ]
    }
}

// =============================================================================
// Event handling
// =============================================================================

/// `data_completed` flag of a `series_completed` message, e.g.
/// `["cs_x", "sds_1", "streaming", "s1", {"data_completed": "limit"}]`.
fn series_completed_flag(message: &[Value]) -> Option<String> {
    message
        .iter()
        .rev()
        .find_map(|value| value.get("data_completed"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Apply one chart event to the state of the chart session it belongs to.
fn apply_event(state: &Mutex<HistoricalState>, event: TradingViewDataEvent, message: &[Value]) {
    match event {
        TradingViewDataEvent::OnSymbolResolved => {
            // resolve_symbol response: [session, symbol_series_id, SymbolInfo]
            if let Some(sym_info) = message.get(2) {
                match SymbolInfo::deserialize(sym_info) {
                    Ok(info) => {
                        debug!(name = %info.name, "Symbol resolved");
                        state.lock().unwrap().record_symbol_info(info);
                    }
                    Err(error) => warn!(%error, "Failed to parse symbol_resolved payload"),
                }
            }
        }
        TradingViewDataEvent::OnChartData | TradingViewDataEvent::OnChartDataUpdate => {
            let Some(obj) = message.get(1).and_then(Value::as_object) else {
                return;
            };
            for series_val in obj.values() {
                if let Some(s_arr) = series_val.get("s").and_then(|v| v.as_array()) {
                    let points: Vec<DataPoint> = s_arr
                        .iter()
                        .filter_map(|v| DataPoint::deserialize(v).ok())
                        .collect();
                    if !points.is_empty() {
                        state.lock().unwrap().record_points(points, s_arr.len());
                    }
                }
            }
        }
        TradingViewDataEvent::OnSeriesCompleted => {
            let data_completed = series_completed_flag(message);
            debug!(?data_completed, "Series completed");
            state.lock().unwrap().complete_series(data_completed);
        }
        TradingViewDataEvent::OnError(tv_error) => {
            error!(?tv_error, ?message, "TradingView protocol error");
            state
                .lock()
                .unwrap()
                .fail(format!("TradingView error: {tv_error:?}"));
        }
        _ => {}
    }
}

fn record_transport_error(state: &Mutex<HistoricalState>, error: &Error) {
    let mut state = state.lock().unwrap();
    if state.record_error() {
        state.fail(format!("Too many errors: {error:?}"));
    }
}

// =============================================================================
// SessionRouter
// =============================================================================

/// Handler that routes chart events to the state of their chart session, so
/// one connection can serve several fetches in turn. Events of unknown or
/// already deleted chart sessions are ignored.
#[derive(Clone, Default)]
pub(crate) struct SessionRouter {
    sessions: Arc<Mutex<HashMap<String, Arc<Mutex<HistoricalState>>>>>,
}

impl SessionRouter {
    fn register(&self, chart_session: &str, state: Arc<Mutex<HistoricalState>>) {
        self.sessions
            .lock()
            .unwrap()
            .insert(chart_session.to_string(), state);
    }

    fn unregister(&self, chart_session: &str) {
        self.sessions.lock().unwrap().remove(chart_session);
    }

    fn all(&self) -> Vec<Arc<Mutex<HistoricalState>>> {
        self.sessions.lock().unwrap().values().cloned().collect()
    }
}

impl Handler for SessionRouter {
    fn handle_events(&self, event: TradingViewDataEvent, message: &[Value]) {
        match message.first().and_then(Value::as_str) {
            Some(session) if session.starts_with("cs_") => {
                let state = self.sessions.lock().unwrap().get(session).cloned();
                match state {
                    Some(state) => apply_event(&state, event, message),
                    None => trace!(
                        ?event,
                        session, "Ignoring event of an unknown chart session"
                    ),
                }
            }
            // Errors such as `protocol_error` do not name a chart session.
            _ if matches!(event, TradingViewDataEvent::OnError(_)) => {
                for state in self.all() {
                    apply_event(&state, event, message);
                }
            }
            _ => {}
        }
    }

    fn handle_quote_data(&self, _message: &[Value]) {}
    fn handle_series_data(&self, _event: TradingViewDataEvent, _messages: &[Value]) {}

    fn notify_error(&self, error: Error, _message: &[Value]) {
        warn!(?error, "Historical handler error");
        for state in self.all() {
            record_transport_error(&state, &error);
        }
    }
}

// =============================================================================
// HistoricalDataHandler
// =============================================================================

/// Event handler that accumulates chart data points into shared
/// [`HistoricalState`].  Implements the [`Handler`] trait for use with
/// [`WebSocketClient`].
#[derive(Clone)]
pub struct HistoricalDataHandler {
    state: Arc<Mutex<HistoricalState>>,
    #[allow(dead_code)]
    cmd_tx: CommandTx,
}
impl Handler for HistoricalDataHandler {
    fn handle_events(&self, event: TradingViewDataEvent, message: &[Value]) {
        apply_event(&self.state, event, message);
    }

    fn handle_quote_data(&self, _message: &[Value]) {}
    fn handle_series_data(&self, _event: TradingViewDataEvent, _messages: &[Value]) {}

    fn notify_error(&self, error: Error, _message: &[Value]) {
        warn!(?error, "Historical handler error");
        record_transport_error(&self.state, &error);
    }
}

// =============================================================================
// HistoricalDataHandlerFactory
// =============================================================================

/// Factory for creating [`HistoricalDataHandler`] instances that share a
/// common [`HistoricalState`].
pub struct HistoricalDataHandlerFactory {
    state: Arc<Mutex<HistoricalState>>,
}

impl HistoricalDataHandlerFactory {
    /// Create a new factory wrapping the given shared state.
    pub fn new(state: Arc<Mutex<HistoricalState>>) -> Self {
        Self { state }
    }
}

impl HandlerFactory for HistoricalDataHandlerFactory {
    type Handler = HistoricalDataHandler;

    fn create(&self, command_tx: CommandTx) -> Self::Handler {
        HistoricalDataHandler {
            state: Arc::clone(&self.state),
            cmd_tx: command_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Interval;
    use crate::chart::options::Range;
    use crate::error::TradingViewError;
    use crate::live::models::SocketMessageDe;
    use crate::live::websocket::dispatch_message_data;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::time::Duration;

    // ------------------------------------------------------------------
    // Pagination helpers
    // ------------------------------------------------------------------

    const CS: &str = "cs_test";

    fn handler_for(state: &Arc<Mutex<HistoricalState>>) -> HistoricalDataHandler {
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel(4);
        HistoricalDataHandlerFactory::new(Arc::clone(state)).create(cmd_tx)
    }

    /// Route one raw `{"m": ..., "p": [...]}` message through the real dispatch path.
    fn feed<H: Handler>(handler: &H, message: &Value) {
        let message: SocketMessageDe = serde_json::from_value(message.clone()).unwrap();
        dispatch_message_data(handler, message);
    }

    fn bars(timestamps: impl IntoIterator<Item = i64>) -> Value {
        let rows: Vec<Value> = timestamps
            .into_iter()
            .map(|ts| json!({"i": 0, "v": [ts as f64, 1.0, 2.0, 0.5, 1.5, 10.0]}))
            .collect();
        json!({
            "m": "timescale_update",
            "p": [CS, {"sds_1": {"s": rows, "ns": {"d": "", "indexes": []}, "t": "s1"}}, {}]
        })
    }

    fn completed(data_completed: Option<&str>) -> Value {
        let mut flags = json!({"rt_update_period": 1});
        if let Some(flag) = data_completed {
            flags["data_completed"] = json!(flag);
        }
        json!({"m": "series_completed", "p": [CS, "sds_1", "streaming", "s1", flags]})
    }

    fn plan() -> PaginationPlan {
        PaginationPlan {
            from_timestamp: None,
            max_bars: None,
            max_pages: 100,
            page_size: 50,
            page_delay: Duration::ZERO,
            page_timeout: Duration::from_secs(2),
        }
    }

    /// State after the initial load of `initial` and a scripted server that
    /// answers each `request_more_data` with the next page of messages.
    struct ScriptedServer {
        state: Arc<Mutex<HistoricalState>>,
        handler: HistoricalDataHandler,
        pages: Arc<Mutex<VecDeque<Vec<Value>>>>,
        requested: Arc<Mutex<Vec<u64>>>,
        response_delay: Option<Duration>,
    }

    impl ScriptedServer {
        fn new(initial: &[Value], pages: Vec<Vec<Value>>) -> Self {
            let state = Arc::new(Mutex::new(HistoricalState::new()));
            let handler = handler_for(&state);
            for message in initial {
                feed(&handler, message);
            }
            Self {
                state,
                handler,
                pages: Arc::new(Mutex::new(pages.into())),
                requested: Arc::new(Mutex::new(Vec::new())),
                response_delay: None,
            }
        }

        /// Answer from a spawned task after `delay` instead of inline.
        fn with_response_delay(mut self, delay: Duration) -> Self {
            self.response_delay = Some(delay);
            self
        }

        async fn paginate(&self, plan: &PaginationPlan) -> Result<PaginationOutcome> {
            paginate(&self.state, plan, |count| {
                self.requested.lock().unwrap().push(count);
                let page = self.pages.lock().unwrap().pop_front().unwrap_or_default();
                let handler = self.handler.clone();
                let delay = self.response_delay;
                async move {
                    match delay {
                        Some(delay) => {
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                for message in &page {
                                    feed(&handler, message);
                                }
                            });
                        }
                        None => {
                            for message in &page {
                                feed(&handler, message);
                            }
                        }
                    }
                    Ok(())
                }
            })
            .await
        }

        fn timestamps(&self) -> Vec<i64> {
            self.state
                .lock()
                .unwrap()
                .finalize()
                .iter()
                .map(|point| point.timestamp())
                .collect()
        }

        fn requested(&self) -> Vec<u64> {
            self.requested.lock().unwrap().clone()
        }
    }

    /// Split a recorded session into the initial load and one segment per
    /// `request_more_data` page, each ending with `series_completed`.
    fn recorded_segments() -> Vec<Vec<Value>> {
        let messages: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/data/request_more_data.json")).unwrap();
        let mut segments = vec![Vec::new()];
        for message in messages {
            let is_completed = message["m"] == "series_completed";
            segments.last_mut().unwrap().push(message);
            if is_completed {
                segments.push(Vec::new());
            }
        }
        segments.retain(|segment| !segment.is_empty());
        segments
    }

    // ------------------------------------------------------------------
    // Pagination loop
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_paginate_replays_recorded_session_to_end_of_history() {
        // BINANCE:BTCUSDT 1M, anonymous: 10 initial bars, then pages of 50,
        // 50 and 2 bars; the last series_completed has data_completed "end".
        let mut segments = recorded_segments();
        assert_eq!(segments.len(), 4);
        let initial = segments.remove(0);
        let server =
            ScriptedServer::new(&initial, segments).with_response_delay(Duration::from_millis(5));
        assert_eq!(server.state.lock().unwrap().completions, 1);
        assert!(server.state.lock().unwrap().symbol_info.is_some());

        let outcome = server.paginate(&plan()).await.unwrap();

        assert_eq!(
            outcome,
            PaginationOutcome {
                pages: 3,
                stop: PaginationStop::EndOfHistory
            }
        );
        assert_eq!(server.requested(), vec![50, 50, 50]);
        let timestamps = server.timestamps();
        assert_eq!(timestamps.len(), 112);
        assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(timestamps.first(), Some(&1_501_545_600)); // 2017-08-01
        assert_eq!(timestamps.last(), Some(&1_793_491_200)); // 2026-11-01
    }

    #[tokio::test]
    async fn test_paginate_dedups_overlapping_pages_and_stops_at_from() {
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![
                vec![bars(995..1_003), completed(None)],
                vec![bars(990..997), completed(None)],
                vec![bars(980..990), completed(None)],
            ],
        );
        let plan = PaginationPlan {
            from_timestamp: Some(992),
            ..plan()
        };

        let outcome = server.paginate(&plan).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::ReachedFrom);
        assert_eq!(outcome.pages, 2);
        assert_eq!(server.timestamps(), (990..1_010).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_paginate_stops_on_empty_page() {
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![vec![completed(None)], vec![bars(900..950), completed(None)]],
        );

        let outcome = server.paginate(&plan()).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::NoMoreData);
        assert_eq!(outcome.pages, 1);
        assert_eq!(server.timestamps().len(), 10);
    }

    #[tokio::test]
    async fn test_paginate_stops_when_page_adds_no_older_bar() {
        // The page only repeats existing bars (e.g. a real-time update).
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![vec![bars(1_005..1_010), completed(None)]],
        );

        let outcome = server.paginate(&plan()).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::NoMoreData);
        assert_eq!(outcome.pages, 1);
    }

    #[tokio::test]
    async fn test_paginate_stops_at_max_pages() {
        let pages = (1..=10)
            .map(|page: i64| {
                vec![
                    bars(1_000 - page * 10..1_000 - (page - 1) * 10),
                    completed(None),
                ]
            })
            .collect();
        let server = ScriptedServer::new(&[bars(1_000..1_010), completed(None)], pages);
        let plan = PaginationPlan {
            max_pages: 3,
            page_size: 10,
            ..plan()
        };

        let outcome = server.paginate(&plan).await.unwrap();

        assert_eq!(
            outcome,
            PaginationOutcome {
                pages: 3,
                stop: PaginationStop::MaxPages
            }
        );
        assert_eq!(server.requested(), vec![10, 10, 10]);
        assert_eq!(server.timestamps(), (970..1_010).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_paginate_zero_max_pages_sends_nothing() {
        let server = ScriptedServer::new(&[bars(1_000..1_010), completed(None)], vec![]);
        let plan = PaginationPlan {
            max_pages: 0,
            ..plan()
        };

        let outcome = server.paginate(&plan).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::MaxPages);
        assert_eq!(outcome.pages, 0);
        assert!(server.requested().is_empty());
    }

    #[tokio::test]
    async fn test_paginate_stops_at_max_bars() {
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![
                vec![bars(990..1_000), completed(None)],
                vec![bars(980..990), completed(None)],
            ],
        );
        let plan = PaginationPlan {
            max_bars: Some(15),
            ..plan()
        };

        let outcome = server.paginate(&plan).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::ReachedMaxBars);
        assert_eq!(outcome.pages, 1);
        let kept = trim_to_request(server.state.lock().unwrap().finalize(), None, Some(15));
        let kept: Vec<i64> = kept.iter().map(|p| p.timestamp()).collect();
        assert_eq!(kept, (995..1_010).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_paginate_reports_server_bar_limit() {
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![vec![bars(995..1_000), completed(Some("limit"))]],
        );

        let outcome = server.paginate(&plan()).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::BarLimit);
        assert_eq!(outcome.pages, 1);
        assert_eq!(server.timestamps().len(), 15);
    }

    #[tokio::test]
    async fn test_paginate_sends_nothing_when_initial_load_hit_limit() {
        let server = ScriptedServer::new(&[bars(1_000..1_010), completed(Some("limit"))], vec![]);

        let outcome = server.paginate(&plan()).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::BarLimit);
        assert_eq!(outcome.pages, 0);
        assert!(server.requested().is_empty());
    }

    #[tokio::test]
    async fn test_paginate_does_not_request_more_without_bars() {
        let server = ScriptedServer::new(&[completed(None)], vec![]);

        let outcome = server.paginate(&plan()).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::NoMoreData);
        assert!(server.requested().is_empty());
    }

    #[tokio::test]
    async fn test_paginate_timeout_keeps_received_bars() {
        // The server never answers the second page.
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![vec![bars(990..1_000), completed(None)], vec![]],
        );
        let plan = PaginationPlan {
            page_timeout: Duration::from_millis(50),
            ..plan()
        };

        let started = Instant::now();
        let outcome = server.paginate(&plan).await.unwrap();

        assert_eq!(
            outcome,
            PaginationOutcome {
                pages: 2,
                stop: PaginationStop::Timeout
            }
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert_eq!(server.timestamps(), (990..1_010).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_paginate_waits_page_delay_before_each_page() {
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![
                vec![bars(990..1_000), completed(None)],
                vec![bars(980..990), completed(Some("end"))],
            ],
        );
        let plan = PaginationPlan {
            page_delay: Duration::from_millis(40),
            ..plan()
        };

        let started = Instant::now();
        let outcome = server.paginate(&plan).await.unwrap();

        assert_eq!(outcome.stop, PaginationStop::EndOfHistory);
        assert_eq!(outcome.pages, 2);
        assert!(started.elapsed() >= Duration::from_millis(80));
    }

    #[tokio::test]
    async fn test_paginate_returns_protocol_error() {
        let series_error = json!({"m": "series_error", "p": [CS, "sds_1", "s1", "boom"]});
        let server = ScriptedServer::new(
            &[bars(1_000..1_010), completed(None)],
            vec![vec![series_error]],
        );

        let error = server.paginate(&plan()).await.unwrap_err();

        assert!(error.to_string().contains("SeriesError"), "{error}");
    }

    #[tokio::test]
    async fn test_paginate_returns_send_error() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        let handler = handler_for(&state);
        feed(&handler, &bars(1_000..1_010));
        feed(&handler, &completed(None));

        let result = paginate(&state, &plan(), |_| async {
            Err(Error::WebSocket("closed".into()))
        })
        .await;

        assert!(matches!(result, Err(Error::WebSocket(_))));
    }

    #[test]
    fn test_trim_to_request() {
        let data: Vec<DataPoint> = (100..110)
            .map(|ts| DataPoint {
                index: 0,
                value: vec![ts as f64, 1.0, 1.0, 1.0, 1.0, 1.0],
            })
            .collect();
        let timestamps = |data: Vec<DataPoint>| -> Vec<i64> {
            data.iter().map(|point| point.timestamp()).collect()
        };
        assert_eq!(
            timestamps(trim_to_request(data.clone(), None, None)).len(),
            10
        );
        assert_eq!(
            timestamps(trim_to_request(data.clone(), Some(105), None)),
            vec![105, 106, 107, 108, 109]
        );
        assert_eq!(
            timestamps(trim_to_request(data.clone(), None, Some(3))),
            vec![107, 108, 109]
        );
        assert_eq!(
            timestamps(trim_to_request(data, Some(108), Some(5))),
            vec![108, 109]
        );
    }

    #[test]
    fn test_initial_bar_count() {
        let base = || {
            HistoricalRequest::builder()
                .symbol("VCB".to_string())
                .exchange("HOSE".to_string())
        };
        let plain = base().build();
        assert_eq!(initial_bar_count(&plain, None), None);

        let explicit = base().num_bars(300).max_bars(20_000).build();
        assert_eq!(
            initial_bar_count(&explicit, explicit.pagination_plan().as_ref()),
            Some(300)
        );

        let small = base().max_bars(42).build();
        assert_eq!(
            initial_bar_count(&small, small.pagination_plan().as_ref()),
            Some(42)
        );

        let by_time = base().from_timestamp(0).page_size(2_000).build();
        assert_eq!(
            initial_bar_count(&by_time, by_time.pagination_plan().as_ref()),
            Some(2_000)
        );
    }

    #[test]
    fn test_series_completed_flag() {
        let limit = completed(Some("limit"));
        let plain = completed(None);
        assert_eq!(
            series_completed_flag(limit["p"].as_array().unwrap()),
            Some("limit".to_string())
        );
        assert_eq!(series_completed_flag(plain["p"].as_array().unwrap()), None);
        assert_eq!(series_completed_flag(&[]), None);
    }

    // ------------------------------------------------------------------
    // SessionRouter
    // ------------------------------------------------------------------

    #[test]
    fn test_session_router_routes_by_chart_session() {
        let router = SessionRouter::default();
        let first = Arc::new(Mutex::new(HistoricalState::new()));
        let second = Arc::new(Mutex::new(HistoricalState::new()));
        router.register("cs_first", Arc::clone(&first));
        router.register("cs_second", Arc::clone(&second));

        let mut message = bars(1_000..1_003);
        message["p"][0] = json!("cs_second");
        feed(&router, &message);
        let mut done = completed(Some("end"));
        done["p"][0] = json!("cs_second");
        feed(&router, &done);
        // A late message of a deleted session is ignored.
        let mut stale = bars(1..5);
        stale["p"][0] = json!("cs_deleted");
        feed(&router, &stale);

        assert!(first.lock().unwrap().data.is_empty());
        assert_eq!(first.lock().unwrap().completions, 0);
        let second = second.lock().unwrap();
        assert_eq!(second.data.len(), 3);
        assert_eq!(second.data_completed.as_deref(), Some("end"));
    }

    #[test]
    fn test_session_router_unregister_and_session_less_errors() {
        let router = SessionRouter::default();
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        router.register("cs_a", Arc::clone(&state));
        router.unregister("cs_a");
        let mut message = bars(1_000..1_003);
        message["p"][0] = json!("cs_a");
        feed(&router, &message);
        assert!(state.lock().unwrap().data.is_empty());

        // `protocol_error` names no chart session: every active fetch fails.
        let active = Arc::new(Mutex::new(HistoricalState::new()));
        router.register("cs_b", Arc::clone(&active));
        feed(
            &router,
            &json!({"m": "protocol_error", "p": ["wrong data"]}),
        );
        assert!(active.lock().unwrap().errored);
        assert!(!state.lock().unwrap().errored);
    }

    #[test]
    fn test_create_series_args_count_mode_default_bars() {
        let args = build_create_series_args(
            "cs_test",
            "sds_1",
            "s1",
            "sds_sym_1",
            Interval::OneDay,
            None,
            None,
        );
        assert_eq!(
            args.len(),
            6,
            "Count mode without range must have exactly 6 arguments"
        );
        assert_eq!(args[0], Value::from("cs_test"));
        assert_eq!(args[1], Value::from("sds_1"));
        assert_eq!(args[2], Value::from("s1"));
        assert_eq!(args[3], Value::from("sds_sym_1"));
        assert_eq!(args[4], Value::from("1D"));
        assert_eq!(
            args[5],
            Value::from(100u64),
            "Default bar count must be 100"
        );
    }

    #[test]
    fn test_create_series_args_count_mode_custom_bars() {
        let args = build_create_series_args(
            "cs_test",
            "sds_1",
            "s1",
            "sds_sym_1",
            Interval::FiveMinutes,
            Some(500),
            None,
        );
        assert_eq!(
            args.len(),
            6,
            "Count mode without range must have exactly 6 arguments"
        );
        assert_eq!(args[4], Value::from("5"));
        assert_eq!(args[5], Value::from(500u64));
    }

    #[test]
    fn test_create_series_args_range_mode_from_to() {
        let range = Range::FromTo(1626220800, 1628640000);
        let args = build_create_series_args(
            "cs_test",
            "sds_1",
            "s1",
            "sds_sym_1",
            Interval::OneDay,
            Some(500),
            Some(range),
        );
        assert_eq!(args.len(), 7, "Range mode must have exactly 7 arguments");
        assert_eq!(args[0], Value::from("cs_test"));
        assert_eq!(args[1], Value::from("sds_1"));
        assert_eq!(args[2], Value::from("s1"));
        assert_eq!(args[3], Value::from("sds_sym_1"));
        assert_eq!(args[4], Value::from("1D"));
        assert_eq!(
            args[5],
            Value::from(500u64),
            "Range mode preserves the requested positive bar count"
        );
        assert_eq!(args[6], Value::from(range.to_string()));
    }

    #[test]
    fn test_create_series_args_range_mode_preset() {
        let range = Range::OneDay;
        let args = build_create_series_args(
            "cs_test",
            "sds_1",
            "s1",
            "sds_sym_1",
            Interval::OneMinute,
            None,
            Some(range),
        );
        assert_eq!(args.len(), 7, "Range mode must have exactly 7 arguments");
        assert_eq!(
            args[5],
            Value::from(512u64),
            "Range mode defaults to a positive bar count"
        );
        assert_eq!(args[6], Value::from(range.to_string()));
    }

    #[test]
    fn test_create_series_args_range_mode_explicit_zero() {
        let range = Range::FromTo(1626220800, 1628640000);
        let args = build_create_series_args(
            "cs_test",
            "sds_1",
            "s1",
            "sds_sym_1",
            Interval::OneDay,
            Some(0),
            Some(range),
        );
        assert_eq!(args.len(), 7);
        assert_eq!(args[5], Value::from(1u64));
        assert_eq!(args[6], Value::from(range.to_string()));
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_immediately_on_complete() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        let state_clone = Arc::clone(&state);

        let wait_handle = tokio::spawn(async move {
            HistoricalClient::wait_for_completion(&state_clone).await;
        });

        tokio::task::yield_now().await;

        let start = std::time::Instant::now();
        state.lock().unwrap().complete();

        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), wait_handle).await;
        assert!(
            timeout_result.is_ok(),
            "wait_for_completion must wake without polling"
        );
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
        assert!(state.lock().unwrap().completed);
    }

    #[tokio::test]
    async fn test_wait_for_completion_wakes_immediately_on_fail() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        let state_clone = Arc::clone(&state);

        let wait_handle = tokio::spawn(async move {
            HistoricalClient::wait_for_completion(&state_clone).await;
        });

        tokio::task::yield_now().await;

        let start = std::time::Instant::now();
        state.lock().unwrap().fail("simulated error".into());

        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), wait_handle).await;
        assert!(
            timeout_result.is_ok(),
            "wait_for_completion must wake immediately on failure"
        );
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
        let guard = state.lock().unwrap();
        assert!(guard.errored);
        assert_eq!(guard.error_message.as_deref(), Some("simulated error"));
    }

    #[tokio::test]
    async fn test_wait_for_completion_lost_wakeup_safe_pre_completed() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));

        // Complete state BEFORE calling wait_for_completion
        state.lock().unwrap().complete();

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            HistoricalClient::wait_for_completion(&state),
        )
        .await;

        assert!(
            result.is_ok(),
            "wait_for_completion must return immediately if already completed"
        );
    }

    #[tokio::test]
    async fn test_wait_for_completion_lost_wakeup_safe_pre_errored() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));

        // Fail state BEFORE calling wait_for_completion
        state.lock().unwrap().fail("early failure".into());

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            HistoricalClient::wait_for_completion(&state),
        )
        .await;

        assert!(
            result.is_ok(),
            "wait_for_completion must return immediately if already errored"
        );
    }

    #[tokio::test]
    async fn test_handler_signals_series_completed() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        let factory = HistoricalDataHandlerFactory::new(Arc::clone(&state));
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel(4);
        let handler = factory.create(cmd_tx);

        let state_clone = Arc::clone(&state);
        let wait_handle = tokio::spawn(async move {
            HistoricalClient::wait_for_completion(&state_clone).await;
        });

        tokio::task::yield_now().await;

        handler.handle_events(TradingViewDataEvent::OnSeriesCompleted, &[]);

        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), wait_handle).await;
        assert!(
            timeout_result.is_ok(),
            "handler must signal completion immediately to waiters"
        );
        assert!(state.lock().unwrap().completed);
    }

    #[tokio::test]
    async fn test_handler_signals_protocol_error() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        let factory = HistoricalDataHandlerFactory::new(Arc::clone(&state));
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel(4);
        let handler = factory.create(cmd_tx);

        let state_clone = Arc::clone(&state);
        let wait_handle = tokio::spawn(async move {
            HistoricalClient::wait_for_completion(&state_clone).await;
        });

        tokio::task::yield_now().await;

        handler.handle_events(
            TradingViewDataEvent::OnError(TradingViewError::SeriesError),
            &[],
        );

        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), wait_handle).await;
        assert!(
            timeout_result.is_ok(),
            "handler must signal error immediately to waiters"
        );
        assert!(state.lock().unwrap().errored);
    }

    #[test]
    fn test_handler_parses_chart_data_without_cloning() {
        let state = Arc::new(Mutex::new(HistoricalState::new()));
        let factory = HistoricalDataHandlerFactory::new(Arc::clone(&state));
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel(4);
        let handler = factory.create(cmd_tx);

        let chart_data_payload = serde_json::json!([
            "session_id",
            {
                "s1": {
                    "s": [
                        { "i": 100, "v": [100.0, 105.0, 99.0, 104.0, 1000.0] },
                        { "i": 101, "v": [104.0, 106.0, 103.0, 105.5, 1200.0] }
                    ]
                }
            }
        ]);

        let msg_slice = chart_data_payload.as_array().unwrap();
        handler.handle_events(TradingViewDataEvent::OnChartData, msg_slice);

        let guard = state.lock().unwrap();
        assert_eq!(guard.data.len(), 2);
        assert_eq!(guard.total_bars, 2);
        assert_eq!(guard.data[0].index, 100);
        assert_eq!(guard.data[1].index, 101);
    }
}
