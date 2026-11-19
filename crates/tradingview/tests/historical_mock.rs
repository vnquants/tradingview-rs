//! End-to-end historical retrieval against a local mock TradingView server.
//!
//! Each test starts its own WebSocket server that speaks enough of the chart
//! protocol (`resolve_symbol`, `create_series`, `request_more_data`,
//! `chart_delete_session`) to exercise pagination, batch connection reuse and
//! failure handling. Symbol names select a misbehaviour:
//!
//! | Prefix | Behaviour |
//! | --- | --- |
//! | `BAD` | `symbol_error` |
//! | `SLOWINIT` | never answers `create_series` |
//! | `DROPINIT` / `CLOSEINIT` | drops the TCP connection / sends a close frame on `create_series` |
//! | `SLOWPAGE` | never answers `request_more_data` |
//! | `DROPPAGE` | drops the TCP connection on `request_more_data` |
//! | `LATEPAGE` | answers `request_more_data` 2.6 s late |
//! | `DIEAFTER` / `CLOSEAFTER` | drops the connection / sends a close frame on `chart_delete_session` |
//! | `DONECLOSE` / `DONEDROP` | completes the initial load, then sends a close frame / drops the connection |
//! | `FLAKY` | drops the connection on the first `create_series` for that name, then works |
//! | `FORMING` | newest bar closes 30 min from now (`lbs.bar_close_time`) |
//! | `DELAYED` | `delay: 900`; newest bar's nominal close was 60 s ago |
//! | `SKEWAHEAD` | messages carry server time `t`/`t_ms` 60 s behind the local clock; the bar closes 5 s after server now |
//! | `SKEWBEHIND` | server time 60 s ahead of the local clock; the bar closed 10 s before server now |
//!
//! Bars are hourly; every bar of a symbol has the same close, derived from
//! the symbol name, so mixed-up results are detectable.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tradingview::error::TradingViewError;
use tradingview::historical::{
    BatchConfig, BatchResult, HistoricalClient, HistoricalRequest, PaginationStop,
};
use tradingview::{DataServer, Error, Interval, OHLCV};

fn code(name: &str) -> f64 {
    (name.bytes().map(u64::from).sum::<u64>() % 9000 + 1000) as f64
}

fn frame(value: &Value) -> Message {
    let text = value.to_string();
    Message::Text(format!("~m~{}~m~{}", text.len(), text).into())
}

fn parse_frames(text: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(stripped) = rest.strip_prefix("~m~") {
        let Some(end) = stripped.find("~m~") else {
            break;
        };
        let Ok(len) = stripped[..end].parse::<usize>() else {
            break;
        };
        let body = &stripped[end + 3..];
        let (payload, tail) = body.split_at(len.min(body.len()));
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            out.push(value);
        }
        rest = tail;
    }
    out
}

fn local_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Offset of the mock's server clock from the local clock, in seconds.
fn server_skew(name: &str) -> Option<i64> {
    if name.starts_with("SKEWAHEAD") {
        Some(-60) // the local clock is 60 s ahead of the server
    } else if name.starts_with("SKEWBEHIND") {
        Some(60) // the local clock is 60 s behind the server
    } else {
        None
    }
}

/// `lbs.bar_close_time` of the newest bar for symbols that send one.
fn last_bar_close_time(name: &str) -> Option<i64> {
    let server_now = local_now() + server_skew(name).unwrap_or(0);
    if name.starts_with("FORMING") {
        Some(server_now + 1800)
    } else if name.starts_with("DELAYED") {
        Some(server_now - 60)
    } else if name.starts_with("SKEWAHEAD") {
        Some(server_now + 5)
    } else if name.starts_with("SKEWBEHIND") {
        Some(server_now - 10)
    } else {
        None
    }
}

/// `count` hourly bars with indices `from_index..from_index + count`; index 0
/// is the newest bar of the initial load.
fn bars(name: &str, from_index: i64, count: i64) -> Value {
    let rows: Vec<Value> = (from_index..from_index + count)
        .map(|i| {
            let ts = 1_700_000_000i64 + i * 3600;
            let c = code(name);
            json!({"i": i, "v": [ts as f64, c, c + 1.0, c - 1.0, c, 10.0]})
        })
        .collect();
    let mut series = json!({"s": rows, "ns": {"d": "", "indexes": []}, "t": "s1"});
    if let Some(close) = last_bar_close_time(name) {
        series["lbs"] = json!({"bar_close_time": close});
    }
    json!({"sds_1": series})
}

/// Add the server timestamps `t`/`t_ms` for symbols with a skewed server clock.
fn stamp(name: &str, mut message: Value) -> Value {
    if let Some(skew) = server_skew(name) {
        let server_ms = (local_now() + skew) * 1000;
        message["t"] = json!(server_ms / 1000);
        message["t_ms"] = json!(server_ms);
    }
    message
}

struct Session {
    name: String,
    earliest: i64,
}

#[derive(Default)]
struct Counters {
    connections: AtomicUsize,
    open_now: AtomicUsize,
    max_open: AtomicUsize,
    /// `create_series` calls per symbol name.
    attempts: Mutex<HashMap<String, usize>>,
}

struct MockServer {
    url: String,
    counters: Arc<Counters>,
}

impl MockServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let counters = Arc::new(Counters::default());
        let accept_counters = Arc::clone(&counters);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(serve(stream, Arc::clone(&accept_counters)));
            }
        });
        Self {
            url: format!("ws://127.0.0.1:{port}/socket.io/websocket"),
            counters,
        }
    }

    fn client(&self) -> HistoricalClient {
        HistoricalClient::new("unauthorized_user_token", DataServer::Data)
            .with_endpoint(&self.url)
            .unwrap()
    }

    fn connections(&self) -> usize {
        self.counters.connections.load(Ordering::SeqCst)
    }

    fn max_open(&self) -> usize {
        self.counters.max_open.load(Ordering::SeqCst)
    }
}

async fn serve(stream: tokio::net::TcpStream, counters: Arc<Counters>) {
    counters.connections.fetch_add(1, Ordering::SeqCst);
    let open = counters.open_now.fetch_add(1, Ordering::SeqCst) + 1;
    counters.max_open.fetch_max(open, Ordering::SeqCst);
    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
        session_loop(ws, &counters).await;
    }
    counters.open_now.fetch_sub(1, Ordering::SeqCst);
}

async fn session_loop(
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    counters: &Counters,
) {
    let (mut tx, mut rx) = ws.split();
    let mut sessions: HashMap<String, Session> = HashMap::new();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
    'conn: loop {
        tokio::select! {
            Some(value) = out_rx.recv() => {
                if tx.send(frame(&value)).await.is_err() { break 'conn; }
            }
            incoming = rx.next() => {
                let Some(Ok(message)) = incoming else { break 'conn };
                let Message::Text(text) = message else { continue };
                for packet in parse_frames(&text) {
                    let m = packet["m"].as_str().unwrap_or_default().to_string();
                    let p = packet["p"].as_array().cloned().unwrap_or_default();
                    let cs = p.first().and_then(Value::as_str).unwrap_or_default().to_string();
                    let mut replies = Vec::new();
                    let mut then_close = false;
                    let mut then_drop = false;
                    match m.as_str() {
                        "resolve_symbol" => {
                            let raw = p[2].as_str().unwrap().trim_start_matches('=');
                            let init: Value = serde_json::from_str(raw).unwrap();
                            let full = init["symbol"].as_str().unwrap().to_string();
                            let (exchange, name) = full.split_once(':').unwrap();
                            sessions.insert(cs.clone(), Session { name: name.to_string(), earliest: 0 });
                            replies.push(if name.starts_with("BAD") {
                                json!({"m": "symbol_error", "p": [cs, p[1], "invalid symbol", "0"]})
                            } else {
                                json!({"m": "symbol_resolved", "p": [cs, p[1], {
                                    "pro_name": full, "name": name, "exchange": exchange,
                                    "pricescale": 100, "minmov": 1,
                                    "delay": if name.starts_with("DELAYED") { 900 } else { 0 }
                                }]})
                            });
                        }
                        "create_series" => {
                            let name = sessions[&cs].name.clone();
                            if name.starts_with("BAD") || name.starts_with("SLOWINIT") {
                                continue;
                            }
                            if name.starts_with("DROPINIT") {
                                break 'conn;
                            }
                            if name.starts_with("CLOSEINIT") {
                                let _ = tx.send(Message::Close(None)).await;
                                break 'conn;
                            }
                            let attempt = {
                                let mut attempts = counters.attempts.lock().unwrap();
                                let n = attempts.entry(name.clone()).or_insert(0);
                                *n += 1;
                                *n
                            };
                            if name.starts_with("FLAKY") && attempt == 1 {
                                break 'conn;
                            }
                            then_close = name.starts_with("DONECLOSE");
                            then_drop = name.starts_with("DONEDROP");
                            let count = p[5].as_i64().unwrap();
                            sessions.get_mut(&cs).unwrap().earliest = -(count - 1);
                            replies.extend([
                                json!({"m": "series_loading", "p": [cs, "sds_1", "s1"]}),
                                json!({"m": "timescale_update", "p": [cs, bars(&name, -(count - 1), count), {}]}),
                                json!({"m": "series_completed", "p": [cs, "sds_1", "streaming", "s1", {"rt_update_period": 1}]}),
                            ]);
                        }
                        "request_more_data" => {
                            let name = sessions[&cs].name.clone();
                            if name.starts_with("SLOWPAGE") {
                                continue;
                            }
                            if name.starts_with("DROPPAGE") {
                                break 'conn;
                            }
                            let count = p[2].as_i64().unwrap();
                            let session = sessions.get_mut(&cs).unwrap();
                            let from = session.earliest - count;
                            session.earliest = from;
                            let page = vec![
                                json!({"m": "series_loading", "p": [cs, "sds_1", "s1"]}),
                                json!({"m": "timescale_update", "p": [cs, bars(&name, from, count), {}]}),
                                json!({"m": "series_completed", "p": [cs, "sds_1", "streaming", "s1", {"rt_update_period": 1}]}),
                            ];
                            if name.starts_with("LATEPAGE") {
                                // Answer well after the client's page timeout.
                                let out_tx = out_tx.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(Duration::from_millis(2600)).await;
                                    for message in page {
                                        let _ = out_tx.send(message);
                                    }
                                });
                                continue;
                            }
                            replies.extend(page);
                        }
                        "chart_delete_session" => {
                            let name = sessions.get(&cs).map(|s| s.name.clone()).unwrap_or_default();
                            if name.starts_with("DIEAFTER") {
                                break 'conn;
                            }
                            if name.starts_with("CLOSEAFTER") {
                                let _ = tx.send(Message::Close(None)).await;
                                break 'conn;
                            }
                        }
                        _ => {}
                    }
                    let name = sessions.get(&cs).map(|s| s.name.clone()).unwrap_or_default();
                    for reply in replies {
                        if tx.send(frame(&stamp(&name, reply))).await.is_err() {
                            break 'conn;
                        }
                    }
                    if then_close {
                        let _ = tx.send(Message::Close(None)).await;
                        break 'conn;
                    }
                    if then_drop {
                        break 'conn;
                    }
                }
            }
        }
    }
}

fn symbols(names: &[&str]) -> Vec<(String, String)> {
    names
        .iter()
        .map(|n| (n.to_string(), "MOCK".to_string()))
        .collect()
}

/// 10 initial bars, then pages of 10 until 35 bars.
fn template(page_delay_ms: u64) -> HistoricalRequest {
    HistoricalRequest::builder()
        .interval(Interval::OneHour)
        .num_bars(10)
        .max_bars(35)
        .page_size(10)
        .page_delay(Duration::from_millis(page_delay_ms))
        .build()
}

fn config(max_concurrency: usize, timeout_ms: u64) -> BatchConfig {
    BatchConfig {
        max_concurrency,
        per_symbol_timeout: Duration::from_millis(timeout_ms),
    }
}

async fn run_batch(
    server: &MockServer,
    names: &[&str],
    max_concurrency: usize,
    timeout_ms: u64,
    page_delay_ms: u64,
) -> (BatchResult, Duration) {
    let client = server.client();
    let started = Instant::now();
    let batch = tokio::time::timeout(
        Duration::from_secs(60),
        client.retrieve_batch_with(
            &symbols(names),
            &template(page_delay_ms),
            config(max_concurrency, timeout_ms),
        ),
    )
    .await
    .expect("batch must not hang");
    (batch, started.elapsed())
}

/// Assert that `symbol` succeeded with its own 35 contiguous bars.
fn assert_ok(batch: &BatchResult, symbol: &str) {
    let done = batch
        .successful
        .iter()
        .find(|done| done.symbol == symbol)
        .unwrap_or_else(|| panic!("{symbol} must succeed: {:?}", failure(batch, symbol)));
    let result = done.result.as_ref().unwrap();
    assert_eq!(result.symbol_info.name.as_str(), symbol);
    assert_eq!(result.len(), 35, "{symbol}");
    assert_eq!(result.pagination_stop, Some(PaginationStop::ReachedMaxBars));
    assert!(result.is_complete());
    assert!(
        result.data.iter().all(|p| p.close() == code(symbol)),
        "{symbol} got another symbol's bars"
    );
    let ts: Vec<i64> = result.data.iter().map(|p| p.timestamp()).collect();
    assert!(
        ts.windows(2).all(|w| w[1] - w[0] == 3600),
        "{symbol} bars not contiguous"
    );
}

fn failure<'a>(batch: &'a BatchResult, symbol: &str) -> Option<&'a Error> {
    batch
        .failed
        .iter()
        .find(|done| done.symbol == symbol)
        .and_then(|done| done.result.as_ref().err())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_symbols_share_bounded_connections_with_correct_attribution() {
    let server = MockServer::start().await;
    let names: Vec<String> = (0..24).map(|i| format!("S{i:02}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();

    let (batch, _) = run_batch(&server, &refs, 3, 2000, 0).await;

    assert_eq!(batch.success_count(), 24);
    assert_eq!(server.connections(), 3);
    assert!(server.max_open() <= 3);
    for name in &refs {
        assert_ok(&batch, name);
    }
    let mut seen: Vec<&str> = batch.successful.iter().map(|d| d.symbol.as_str()).collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 24, "every symbol exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_max_concurrency_uses_one_connection() {
    let server = MockServer::start().await;
    let (batch, _) = run_batch(&server, &["A", "B", "C"], 0, 2000, 0).await;
    assert_eq!(batch.success_count(), 3);
    assert_eq!(server.connections(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn symbol_errors_fail_only_their_symbol_and_keep_the_connection() {
    let server = MockServer::start().await;
    let (batch, _) = run_batch(&server, &["BAD1", "G1", "BAD2", "G2"], 1, 2000, 0).await;
    assert_ok(&batch, "G1");
    assert_ok(&batch, "G2");
    assert_eq!(batch.failure_count(), 2);
    assert!(
        matches!(
            failure(&batch, "BAD1"),
            Some(Error::TradingView {
                source: TradingViewError::SymbolError
            })
        ),
        "{:?}",
        failure(&batch, "BAD1")
    );
    assert_eq!(
        server.connections(),
        1,
        "a symbol error is not a connection failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_lost_during_initial_load_is_retried_once_then_fails_fast() {
    for broken in ["DROPINIT", "CLOSEINIT"] {
        let server = MockServer::start().await;
        let (batch, elapsed) = run_batch(&server, &[broken, "G1"], 1, 5000, 0).await;
        let error = failure(&batch, broken).unwrap_or_else(|| panic!("{broken} must fail"));
        assert!(matches!(error, Error::WebSocket(_)), "{broken}: {error}");
        assert_ok(&batch, "G1");
        // The broken symbol was tried twice, then G1 got a fresh connection.
        assert_eq!(server.connections(), 3, "{broken}");
        assert!(
            elapsed < Duration::from_secs(3),
            "{broken} waited for a timeout: {elapsed:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_lost_during_a_page_fails_fast_without_allow_partial() {
    let server = MockServer::start().await;
    let (batch, elapsed) = run_batch(&server, &["DROPPAGE", "G1"], 1, 5000, 0).await;
    let error = failure(&batch, "DROPPAGE").expect("truncated history must not be a success");
    assert!(matches!(error, Error::WebSocket(_)), "{error}");
    assert_ok(&batch, "G1");
    assert!(
        elapsed < Duration::from_secs(3),
        "waited for a timeout: {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unanswered_page_is_a_timeout_error_and_the_connection_is_replaced() {
    let server = MockServer::start().await;
    let (batch, _) = run_batch(&server, &["SLOWPAGE", "G1"], 1, 1000, 0).await;
    let error = failure(&batch, "SLOWPAGE").expect("truncated history must not be a success");
    assert!(matches!(error, Error::Timeout(_)), "{error}");
    assert_ok(&batch, "G1");
    assert_eq!(server.connections(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_page_never_leaks_into_the_next_symbol() {
    // LATEPAGE's page arrives 2.6 s after it was requested; the page timeout
    // is 2 s, so the next symbol is mid-fetch when the late page arrives.
    let server = MockServer::start().await;
    let (batch, _) = run_batch(&server, &["LATEPAGE", "G1"], 1, 2000, 1000).await;
    assert!(matches!(
        failure(&batch, "LATEPAGE"),
        Some(Error::Timeout(_))
    ));
    assert_ok(&batch, "G1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_closed_between_symbols_is_detected_without_a_timeout() {
    for dying in ["DIEAFTER", "CLOSEAFTER"] {
        let server = MockServer::start().await;
        let (batch, elapsed) = run_batch(&server, &[dying, "G1", "G2"], 1, 5000, 0).await;
        assert_eq!(batch.success_count(), 3, "{dying}: {:?}", batch.failed);
        for name in [dying, "G1", "G2"] {
            assert_ok(&batch, name);
        }
        assert!(
            elapsed < Duration::from_secs(3),
            "{dying} waited for a timeout: {elapsed:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_retrieve_initial_load_timeout_is_an_error() {
    let server = MockServer::start().await;
    let request = HistoricalRequest::builder()
        .symbol("SLOWINIT".to_string())
        .exchange("MOCK".to_string())
        .timeout(Duration::from_millis(500))
        .build();
    let result = server.client().retrieve(request).await;
    assert!(matches!(result, Err(Error::Timeout(_))), "{result:?}");
}

fn dropped_page_request(allow_partial: bool) -> HistoricalRequest {
    HistoricalRequest::builder()
        .symbol("DROPPAGE".to_string())
        .exchange("MOCK".to_string())
        .num_bars(10)
        .from_timestamp(0)
        .page_size(10)
        .page_delay(Duration::ZERO)
        .timeout(Duration::from_secs(5))
        .allow_partial(allow_partial)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_retrieve_connection_lost_during_page() {
    let server = MockServer::start().await;
    let started = Instant::now();

    let strict = server.client().retrieve(dropped_page_request(false)).await;
    assert!(matches!(strict, Err(Error::WebSocket(_))), "{strict:?}");

    let partial = server
        .client()
        .retrieve(dropped_page_request(true))
        .await
        .unwrap();
    assert_eq!(partial.len(), 10);
    assert_eq!(
        partial.pagination_stop,
        Some(PaginationStop::ConnectionClosed)
    );
    assert!(!partial.is_complete());
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_that_never_answers_is_bounded_by_the_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            held.push(stream); // keep open, never reply
        }
    });
    let client = HistoricalClient::new("unauthorized_user_token", DataServer::Data)
        .with_endpoint(&format!("ws://127.0.0.1:{port}/socket.io/websocket"))
        .unwrap();
    let started = Instant::now();
    let batch = tokio::time::timeout(
        Duration::from_secs(10),
        client.retrieve_batch_with(
            &symbols(&["A"]),
            &HistoricalRequest::builder().build(),
            config(1, 500),
        ),
    )
    .await
    .expect("connecting must be bounded");
    assert!(matches!(failure(&batch, "A"), Some(Error::WebSocket(_))));
    // One attempt plus one retry, each bounded by the 500 ms timeout.
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn with_endpoint_requires_tls_except_on_loopback() {
    let client = || HistoricalClient::new("t", DataServer::Data);
    assert!(
        client()
            .with_endpoint("https://example.com/socket")
            .is_err()
    );
    assert!(client().with_endpoint("not a url").is_err());
    assert!(
        client()
            .with_endpoint("ws://proxy.example.com/socket")
            .is_err()
    );
    assert!(
        client()
            .with_endpoint("ws://192.168.1.10:9000/socket")
            .is_err()
    );
    assert!(
        client()
            .with_endpoint("wss://proxy.example.com/socket")
            .is_ok()
    );
    assert!(client().with_endpoint("ws://127.0.0.1:9000/socket").is_ok());
    assert!(client().with_endpoint("ws://localhost:9000/socket").is_ok());
    assert!(client().with_endpoint("ws://[::1]:9000/socket").is_ok());
}

fn single(name: &str) -> HistoricalRequest {
    HistoricalRequest::builder()
        .symbol(name.to_string())
        .exchange("MOCK".to_string())
        .interval(Interval::OneHour)
        .num_bars(10)
        .timeout(Duration::from_secs(5))
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_load_survives_a_close_right_after_series_completed() {
    for kind in ["DONECLOSE", "DONEDROP"] {
        let server = MockServer::start().await;
        for run in 0..40 {
            let result = server
                .client()
                .retrieve(single(&format!("{kind}{run}")))
                .await;
            let result =
                result.unwrap_or_else(|e| panic!("{kind} run {run}: all bars arrived, got {e}"));
            assert_eq!(result.len(), 10, "{kind} run {run}");
            assert!(result.is_complete());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flaky_symbol_succeeds_on_the_retry() {
    let server = MockServer::start().await;
    let (batch, _) = run_batch(&server, &["FLAKY1", "G1"], 1, 2000, 0).await;
    assert_ok(&batch, "FLAKY1");
    assert_ok(&batch, "G1");
    // The dropped first connection, then one connection for the retry and G1.
    assert_eq!(server.connections(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_incomplete_last_bar_with_max_bars_returns_max_bars_closed_bars() {
    let server = MockServer::start().await;
    for max_bars in [25u64, 30] {
        let request = HistoricalRequest::builder()
            .symbol("FORMING".to_string())
            .exchange("MOCK".to_string())
            .interval(Interval::OneHour)
            .num_bars(10)
            .max_bars(max_bars)
            .page_size(10)
            .page_delay(Duration::ZERO)
            .drop_incomplete_last_bar(true)
            .timeout(Duration::from_secs(5))
            .build();
        let result = server.client().retrieve(request).await.unwrap();
        assert_eq!(result.len() as u64, max_bars);
        assert_eq!(result.pagination_stop, Some(PaginationStop::ReachedMaxBars));
        assert!(result.is_complete());
        assert_eq!(result.last_bar_complete, Some(true));
        // The forming bar (index 0, ts 1_700_000_000) is gone.
        assert_eq!(result.last_timestamp(), Some(1_700_000_000 - 3600));
    }
    let kept = server.client().retrieve(single("FORMING")).await.unwrap();
    assert_eq!(kept.len(), 10);
    assert_eq!(kept.last_bar_complete, Some(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn last_bar_complete_accounts_for_the_feed_delay() {
    let server = MockServer::start().await;
    // Nominal close 60 s ago, but the feed is 900 s delayed: still forming.
    let delayed = server.client().retrieve(single("DELAYED")).await.unwrap();
    assert_eq!(delayed.symbol_info.delay, 900);
    assert_eq!(delayed.last_bar_complete, Some(false));
    let mut request = single("DELAYED");
    request.drop_incomplete_last_bar = true;
    let dropped = server.client().retrieve(request).await.unwrap();
    assert_eq!(dropped.len(), 9);
    assert_eq!(dropped.last_bar_complete, Some(true));
    // No lbs at all: unknown.
    let plain = server.client().retrieve(single("PLAIN")).await.unwrap();
    assert_eq!(plain.last_bar_complete, None);
    assert_eq!(plain.last_bar_close_time, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn last_bar_complete_uses_the_server_clock_not_the_local_clock() {
    let server = MockServer::start().await;
    // Local clock 60 s ahead: by the local clock the bar closed 55 s ago, but
    // by the server clock it closes in 5 s.
    let ahead = server.client().retrieve(single("SKEWAHEAD")).await.unwrap();
    assert_eq!(ahead.last_bar_complete, Some(false));
    // Local clock 60 s behind: by the local clock the bar closes in 50 s, but
    // by the server clock it closed 10 s ago.
    let behind = server
        .client()
        .retrieve(single("SKEWBEHIND"))
        .await
        .unwrap();
    assert_eq!(behind.last_bar_complete, Some(true));

    let mut request = single("SKEWAHEAD");
    request.drop_incomplete_last_bar = true;
    let dropped = server.client().retrieve(request).await.unwrap();
    assert_eq!(dropped.len(), 9);
}
