use std::sync::Arc;
use std::time::Instant;

use crate::error::TradingViewError;
use crate::websocket::SeriesInfo;
use crate::{DataPoint, OHLCV, SymbolInfo};

#[derive(Debug)]
pub struct HistoricalState {
    pub data: Vec<DataPoint>,
    pub symbol_info: Option<SymbolInfo>,
    pub series_info: Option<SeriesInfo>,
    pub replay: ReplayState,
    pub error_count: u32,
    pub completed: bool,
    pub errored: bool,
    pub error_message: Option<String>,
    pub first_data_at: Option<Instant>,
    pub total_bars: usize,
    pub notify: Arc<tokio::sync::Notify>,
    /// Number of `series_completed` messages received: one for the initial
    /// load and one per `request_more_data` page.
    pub completions: u64,
    /// `data_completed` value of the latest `series_completed` message:
    /// `"end"` at the start of the symbol's history, `"limit"` at the data
    /// server's bar limit, `None` while more data can be requested.
    pub data_completed: Option<String>,
    /// Bar rows that could not be parsed (malformed or without OHLC values).
    pub dropped_rows: usize,
    /// `lbs.bar_close_time` of the newest bar, as reported by the server.
    pub last_bar_close_time: Option<i64>,
    /// Why the connection closed, once it has; waiters wake up when it is set.
    pub disconnected: Option<String>,
    /// First TradingView protocol error (`symbol_error`, `series_error`, ...).
    pub protocol_error: Option<TradingViewError>,
    /// Latest server timestamp (`t_ms`) seen on the connection and when it was
    /// received, to judge bar completeness by the server's clock.
    pub server_time: Option<(u64, Instant)>,
}

impl HistoricalState {
    pub fn new() -> Self {
        Self::with_notify(Arc::new(tokio::sync::Notify::new()))
    }
    pub fn with_notify(notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            data: Vec::new(),
            symbol_info: None,
            series_info: None,
            replay: ReplayState::default(),
            error_count: 0,
            completed: false,
            errored: false,
            error_message: None,
            first_data_at: None,
            total_bars: 0,
            notify,
            completions: 0,
            data_completed: None,
            dropped_rows: 0,
            last_bar_close_time: None,
            disconnected: None,
            protocol_error: None,
            server_time: None,
        }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            ..Self::new()
        }
    }
    pub fn with_capacity_and_notify(capacity: usize, notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            ..Self::with_notify(notify)
        }
    }
    pub fn record_chart_data(&mut self, series_info: SeriesInfo, points: Vec<DataPoint>) {
        self.series_info = Some(series_info);
        let received = points.len();
        if self.first_data_at.is_none() {
            self.first_data_at = Some(Instant::now());
        }
        self.data.extend(points);
        self.total_bars += received;
    }
    pub fn record_points(&mut self, points: Vec<DataPoint>, total_in_batch: usize) {
        if self.first_data_at.is_none() {
            self.first_data_at = Some(Instant::now());
        }
        self.data.extend(points);
        self.total_bars += total_in_batch;
    }
    /// Count bar rows that could not be parsed.
    pub fn record_dropped_rows(&mut self, dropped: usize) {
        self.dropped_rows += dropped;
    }
    /// Remember the newest `lbs.bar_close_time` the server reported.
    pub fn record_last_bar_close_time(&mut self, close_time: i64) {
        self.last_bar_close_time = Some(
            self.last_bar_close_time
                .map_or(close_time, |known| known.max(close_time)),
        );
    }
    /// Remember the newest server timestamp (milliseconds) and when it arrived.
    pub fn record_server_time(&mut self, server_time_ms: u64) {
        if self
            .server_time
            .is_none_or(|(known, _)| server_time_ms >= known)
        {
            self.server_time = Some((server_time_ms, Instant::now()));
        }
    }
    /// The server's current time in Unix seconds, extrapolated from the newest
    /// server timestamp; `None` when no message carried one.
    pub fn server_now(&self) -> Option<i64> {
        let (server_ms, received) = self.server_time?;
        let elapsed_ms = u64::try_from(received.elapsed().as_millis()).unwrap_or(u64::MAX);
        i64::try_from(server_ms.saturating_add(elapsed_ms) / 1_000).ok()
    }
    /// Record that the connection closed and wake waiters. The first reason wins.
    pub fn disconnect(&mut self, reason: &str) {
        if self.disconnected.is_none() {
            self.disconnected = Some(reason.to_string());
        }
        self.notify.notify_waiters();
    }
    pub fn record_symbol_info(&mut self, info: SymbolInfo) {
        self.symbol_info = Some(info);
    }
    pub fn record_error(&mut self) -> bool {
        self.error_count += 1;
        self.error_count > 5
    }
    pub fn complete(&mut self) {
        self.complete_series(None);
    }
    /// Record a `series_completed` message and wake waiters.
    pub fn complete_series(&mut self, data_completed: Option<String>) {
        self.completed = true;
        self.completions += 1;
        self.data_completed = data_completed;
        self.notify.notify_waiters();
    }
    /// Record a failure and wake waiters. The first message is kept, since it
    /// names the root cause (`symbol_error` arrives before `series_error`).
    pub fn fail(&mut self, msg: String) {
        self.errored = true;
        self.error_message.get_or_insert(msg);
        self.notify.notify_waiters();
    }
    /// Record a TradingView protocol error; the first one is kept.
    pub fn fail_with(&mut self, error: TradingViewError, msg: String) {
        self.protocol_error.get_or_insert(error);
        self.fail(msg);
    }
    pub fn is_done(&self) -> bool {
        self.completed || self.errored || self.disconnected.is_some()
    }
    /// Sort the bars by timestamp and keep one bar per timestamp, the one
    /// received last (a later real-time update replaces an earlier value).
    pub fn normalize(&mut self) {
        // Stable sort keeps arrival order among equal timestamps.
        self.data.sort_by_key(|point| point.timestamp());
        let mut deduped: Vec<DataPoint> = Vec::with_capacity(self.data.len());
        for point in self.data.drain(..) {
            match deduped.last_mut() {
                Some(last) if last.timestamp() == point.timestamp() => *last = point,
                _ => deduped.push(point),
            }
        }
        self.data = deduped;
    }
    /// Number of distinct bars and the oldest timestamp, after [`Self::normalize`].
    pub fn bar_count_and_earliest(&mut self) -> (usize, Option<i64>) {
        self.normalize();
        (
            self.data.len(),
            self.data.first().map(|point| point.timestamp()),
        )
    }
    pub fn finalize(&mut self) -> Vec<DataPoint> {
        self.normalize();
        std::mem::take(&mut self.data)
    }
}
impl Default for HistoricalState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct ReplayState {
    pub enabled: bool,
    pub configured: bool,
    pub earliest_ts: Option<i64>,
    pub data_received: bool,
}
impl ReplayState {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Default::default()
        }
    }
    pub fn update_earliest(&mut self, ts: i64) {
        self.earliest_ts = Some(self.earliest_ts.map(|e| e.min(ts)).unwrap_or(ts));
    }
    pub fn needs_setup(&self) -> bool {
        self.enabled && !self.configured && self.data_received
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(ts: i64, close: f64) -> DataPoint {
        DataPoint {
            index: 0,
            value: vec![ts as f64, close, close, close, close, 1.0],
        }
    }

    #[test]
    fn test_finalize_sorts_and_keeps_latest_value_per_timestamp() {
        let mut state = HistoricalState::new();
        state.record_points(vec![bar(300, 3.0), bar(100, 1.0), bar(200, 2.0)], 3);
        // A real-time update for the newest bar, then an overlapping older page.
        state.record_points(vec![bar(300, 3.5)], 1);
        state.record_points(vec![bar(50, 0.5), bar(100, 1.0)], 2);

        let data = state.finalize();
        let timestamps: Vec<i64> = data.iter().map(|p| p.timestamp()).collect();
        assert_eq!(timestamps, vec![50, 100, 200, 300]);
        assert_eq!(data[3].close(), 3.5, "latest update must win");
        assert_eq!(state.total_bars, 6);
        assert!(state.data.is_empty());
    }

    #[test]
    fn test_bar_count_and_earliest() {
        let mut state = HistoricalState::new();
        assert_eq!(state.bar_count_and_earliest(), (0, None));
        state.record_points(vec![bar(200, 2.0), bar(100, 1.0), bar(200, 2.1)], 3);
        assert_eq!(state.bar_count_and_earliest(), (2, Some(100)));
    }

    #[test]
    fn test_disconnect_keeps_first_reason_and_marks_done() {
        let mut state = HistoricalState::new();
        assert!(!state.is_done());
        state.disconnect("server closed");
        state.disconnect("later");
        assert_eq!(state.disconnected.as_deref(), Some("server closed"));
        assert!(state.is_done());
    }

    #[test]
    fn test_server_now_extrapolates_newest_server_time() {
        let mut state = HistoricalState::new();
        assert_eq!(state.server_now(), None);
        state.record_server_time(1_700_000_000_900);
        state.record_server_time(1_600_000_000_000); // older: ignored
        assert_eq!(state.server_now(), Some(1_700_000_000));
    }

    #[test]
    fn test_last_bar_close_time_keeps_newest() {
        let mut state = HistoricalState::new();
        state.record_last_bar_close_time(200);
        state.record_last_bar_close_time(100);
        assert_eq!(state.last_bar_close_time, Some(200));
        state.record_dropped_rows(2);
        state.record_dropped_rows(1);
        assert_eq!(state.dropped_rows, 3);
    }

    #[test]
    fn test_complete_series_counts_completions() {
        let mut state = HistoricalState::new();
        state.complete();
        assert_eq!(state.completions, 1);
        assert_eq!(state.data_completed, None);
        state.complete_series(Some("limit".to_string()));
        assert_eq!(state.completions, 2);
        assert!(state.completed);
        assert_eq!(state.data_completed.as_deref(), Some("limit"));
    }
}
