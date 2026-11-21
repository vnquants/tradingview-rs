//! The auth token is sent to the server but never written to logs at the
//! recommended filter, even with `TRACE` enabled for everything else.
//!
//! `log` records (tungstenite logs through `log`) are bridged into `tracing`
//! the way `tracing_subscriber::fmt().init()` does it, so the test sees the
//! transport's own records too. This binary holds a single test and installs
//! a global subscriber, so no other test can change which events are enabled.

use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use serde_json::Value;
use tracing_subscriber::{filter::Targets, prelude::*, reload};
use tradingview::{
    DataServer, Error, RECOMMENDED_LOG_DIRECTIVES, TradingViewDataEvent,
    live::{handler::Handler, models::SocketMessageSer, websocket::WebSocketClient},
    payload,
};
use url::Url;

/// A JWT-shaped token like the ones `/quote_token/` returns.
const TOKEN: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
eyJ1c2VyX2lkIjo0MiwiZXhwIjoxNzk1MTE4MDAyLCJwbGFuIjoicHJvIn0.\
c2lnbmF0dXJlLXNlY3JldC12YWx1ZS10aGF0LW11c3Qtbm90LWxlYWs";

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn take(&self) -> String {
        String::from_utf8(std::mem::take(&mut *self.0.lock().unwrap())).unwrap()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct SilentHandler;

impl Handler for SilentHandler {
    fn handle_events(&self, _: TradingViewDataEvent, _: &[Value]) {}
    fn handle_quote_data(&self, _: &[Value]) {}
    fn handle_series_data(&self, _: TradingViewDataEvent, _: &[Value]) {}
    fn notify_error(&self, _: Error, _: &[Value]) {}
}

/// Connect to a local server, authenticate with `TOKEN` and send one session
/// command; returns the first two frames the server received.
async fn authenticate_once() -> (String, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let first = ws.next().await.unwrap().unwrap();
        let second = ws.next().await.unwrap().unwrap();
        (first.to_string(), second.to_string())
    });
    let client = WebSocketClient::builder()
        .auth_token(TOKEN)
        .server(DataServer::Data)
        .handler(SilentHandler)
        .endpoint(Url::parse(&format!("ws://{server_addr}/socket.io/websocket")).unwrap())
        .build()
        .await
        .unwrap();
    client.create_chart_session("cs_logged").await.unwrap();
    let frames = server.await.unwrap();
    client.close().await.unwrap();
    frames
}

fn leaks(logs: &str) -> Vec<&str> {
    logs.lines()
        .filter(|line| TOKEN.split('.').any(|part| line.contains(part)))
        .collect()
}

#[tokio::test]
async fn auth_token_never_appears_in_logs_at_the_recommended_filter() {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let recommended: Targets = format!("trace,{RECOMMENDED_LOG_DIRECTIVES}")
        .parse()
        .unwrap();
    let (filter, filter_handle) = reload::Layer::new(recommended);
    // `init` also installs `tracing_log::LogTracer`, bridging `log` records.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone()),
        )
        .with(filter)
        .init();

    // 1. Formatting the packet directly.
    let packet = SocketMessageSer::new("set_auth_token", payload!(TOKEN))
        .to_message()
        .unwrap();
    assert!(packet.to_text().unwrap().contains(TOKEN));

    // 2. A real connection at TRACE, with the transport capped at DEBUG.
    let (first, second) = authenticate_once().await;
    assert!(first.contains(TOKEN), "the server receives the token");
    assert!(second.contains("cs_logged"));
    let capped = logs.take();
    assert!(capped.contains("set_auth_token"), "{capped}");
    assert!(capped.contains("<redacted>"), "{capped}");
    assert!(capped.contains("cs_logged"), "{capped}");
    assert!(
        capped.contains("tungstenite") && capped.contains("handshake done"),
        "tungstenite's DEBUG `log` records must be bridged:\n{capped}"
    );
    assert!(
        leaks(&capped).is_empty(),
        "token leaked at the recommended filter:\n{:#?}",
        leaks(&capped)
    );

    // 3. Why the cap matters: at TRACE tungstenite logs the raw frame.
    filter_handle
        .reload("trace".parse::<Targets>().unwrap())
        .unwrap();
    authenticate_once().await;
    let uncapped = logs.take();
    let leaked = leaks(&uncapped);
    assert!(
        !leaked.is_empty() && leaked.iter().all(|line| line.contains("tungstenite")),
        "expected only tungstenite TRACE records to carry the token; got {leaked:#?}"
    );
}
