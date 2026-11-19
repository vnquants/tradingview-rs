//! The auth token is sent to the server but never written to logs, even at
//! TRACE level (`.cargo/config.toml` sets `RUST_LOG=debug` for local runs).
//!
//! This binary holds a single test and installs a global subscriber, so no
//! other test can change which log events are enabled.

use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use serde_json::Value;
use tradingview::{
    DataServer, Error, TradingViewDataEvent,
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

#[tokio::test]
async fn auth_token_never_appears_in_logs() {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .unwrap();

    // 1. Formatting the packet directly.
    let packet = SocketMessageSer::new("set_auth_token", payload!(TOKEN))
        .to_message()
        .unwrap();
    assert!(packet.to_text().unwrap().contains(TOKEN));

    // 2. A real connection that authenticates and sends a session command.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let first = ws.next().await.unwrap().unwrap();
        let second = ws.next().await.unwrap().unwrap();
        (first, second)
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
    let (first, second) = server.await.unwrap();
    client.close().await.unwrap();

    assert!(
        first.to_text().unwrap().contains(TOKEN),
        "the server receives the token"
    );
    assert!(second.to_text().unwrap().contains("cs_logged"));

    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("set_auth_token"), "{logs}");
    assert!(logs.contains("<redacted>"), "{logs}");
    assert!(logs.contains("cs_logged"), "{logs}");
    for part in TOKEN.split('.') {
        assert!(!logs.contains(part), "token part leaked into logs:\n{logs}");
    }
}
