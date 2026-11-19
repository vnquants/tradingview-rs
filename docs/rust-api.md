# Rust Core API Reference (`tradingview-rs`)

`tradingview-rs` is an asynchronous data source library for algorithmic trading written in Rust.

---

## Workspace Layout

- Root: Virtual Cargo workspace managing members.
- `crates/tradingview`: Core Rust library crate (`tradingview-rs`).
- `crates/tradingview-py`: PyO3 0.29 native Python extension (`tradingview-py`).

---

## Two-Tier Architecture

1. **High-Level Event Pipeline (`DataLoader`)**:
   - Assembles a `DataSource` into multiple `EventSink`s (Channel, Callback, Kafka).
   - Handles fan-out, buffer bounds, backpressure, and graceful task lifecycle.
2. **Low-Level Protocol Primitives**:
   - `WebSocketClient`: Direct WebSocket connection with automatic exponential-backoff reconnect and circuit breaking.
   - `HistoricalClient`: Single and batch historical OHLCV chart retrieval.
   - `StudyClient`: Pine indicator execution over WebSocket.
   - `fin_calendar`: REST client for global macroeconomic events.
   - `fundamental`: Built-in Pine study catalog and date-versioned registry.

---

## Historical Data Retrieval

```rust
use tradingview::historical::{BatchConfig, HistoricalClient, HistoricalRequest};
use tradingview::live::models::DataServer;
use tradingview::models::Interval;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HistoricalClient::new("unauthorized_user_token", DataServer::Data);

    // Single request builder
    let request = HistoricalRequest::builder()
        .symbol("AAPL")
        .exchange("NASDAQ")
        .interval(Interval::OneDay)
        .num_bars(100)
        .build();

    let result = client.retrieve(request).await?;
    println!("Bars received: {}", result.len());

    // Batch retrieval with semaphore-bounded concurrency
    let symbols = vec![
        ("AAPL".to_string(), "NASDAQ".to_string()),
        ("MSFT".to_string(), "NASDAQ".to_string()),
    ];

    let batch = client
        .retrieve_batch(
            &symbols,
            Interval::OneDay,
            Some(50),
            BatchConfig {
                max_concurrency: 4,
                per_symbol_timeout: std::time::Duration::from_secs(30),
            },
        )
        .await;

    println!("Batch successful: {}", batch.successful.len());
    Ok(())
}
```

### Pagination, adjustment, session and currency

`HistoricalRequest` pages back with `request_more_data` when `from_timestamp`,
`max_bars` or `max_pages` is set. `page_size` (default 5,000 bars) and
`page_delay` (default 500 ms) shape each page; `timeout` applies to the initial
load and to each page. `HistoricalResult::pages_fetched` and
`HistoricalResult::pagination_stop` (`PaginationStop::{ReachedFrom,
ReachedMaxBars, MaxPages, EndOfHistory, BarLimit, NoMoreData, Timeout,
ConnectionClosed}`, `#[non_exhaustive]`) report the walk, and
`HistoricalResult::is_complete()` is `true` only without pagination or after
`ReachedFrom`, `ReachedMaxBars` or `EndOfHistory`. The data server's bar limit
caps the depth (see [`history-depth.md`](history-depth.md)).

Truncation is an error by default: an initial or page timeout returns
`Error::Timeout`, and a closed connection returns `Error::WebSocket` as soon as
the reader notices it (no waiting for the timeout). With
`.allow_partial(true)`, a page timeout or closed connection during pagination
returns the bars received so far with `PaginationStop::Timeout` or
`PaginationStop::ConnectionClosed`. Connecting, including the WebSocket
handshake, is bounded by `timeout` too.

`retrieve` validates the request first: symbol and exchange are required and
`from_timestamp` must be Unix seconds no later than now (values above 10^11
look like milliseconds and are rejected).

Each result also carries `dropped_rows` (unparseable bar rows, logged as a
warning), `last_bar_close_time` (the server's `lbs.bar_close_time`) and
`last_bar_complete` (whether the newest bar had closed when the fetch
finished); `.drop_incomplete_last_bar(true)` removes a still-forming newest bar.

`HistoricalClient::with_endpoint("ws://...")` points the client at another
WebSocket URL, for example a proxy or a local test server
(`crates/tradingview/tests/historical_mock.rs` runs the client against one).
The auth token is sent to that endpoint.

```rust
use std::time::Duration;
use tradingview::historical::{BatchConfig, HistoricalClient, HistoricalRequest};
use tradingview::{Currency, DataServer, Interval, MarketAdjustment, SessionType};

# async fn run() -> tradingview::Result<()> {
let client = HistoricalClient::new("unauthorized_user_token", DataServer::ProData);
let request = HistoricalRequest::builder()
    .symbol("VCB".to_string())
    .exchange("HOSE".to_string())
    .interval(Interval::OneHour)
    .from_timestamp(1_735_689_600) // 2025-01-01T00:00:00Z
    .page_delay(Duration::from_secs(1))
    .adjustment(MarketAdjustment::Unadjusted) // Splits is the server default
    .session_type(SessionType::Regular)
    .currency(Currency::USD)
    .build();
let result = client.retrieve(request).await?;
println!(
    "{} bars, {} pages, stop={:?}, tick={:?}",
    result.len(),
    result.pages_fetched,
    result.pagination_stop,
    result.symbol_info.tick_size(),
);

// The same options for many symbols: at most `max_concurrency` connections,
// each fetching its share of the symbols in turn; a symbol whose connection
// failed is retried once on a new connection.
let symbols = vec![("BTCUSDT".to_string(), "BINANCE".to_string())];
let template = HistoricalRequest::builder()
    .interval(Interval::OneMinute)
    .max_bars(10_000)
    .build();
let batch = client
    .retrieve_batch_with(&symbols, &template, BatchConfig::default())
    .await;
# Ok(())
# }
```

`SymbolInfo` now keeps `session`, `pricescale`, `minmov`, `minmove2`,
`variable_tick_size`, `pointvalue`, `delay` and `isin` from `symbol_resolved`
(`timezone` was already kept). `SymbolInfo::tick_size()` returns
`minmov / pricescale`, and `SymbolInfo::tick_size_at(price)` applies
`variable_tick_size` (for HOSE:VCB, `"10 10000 50 50000 100"`: tick 10 below
10,000, 50 below 50,000, 100 above).

`ChartOptions` accepts `.`, `!`, `_`, `-` and `&` in exchange and symbol names,
so `OKX:BTCUSDT.P`, `HNX:VN30F1!`, `FX_IDC:USDVND` and `NSE:M&M` validate.

---

## WebSocket & Quote Streaming

```rust
use serde_json::Value;
use std::sync::Arc;
use tradingview::live::{handler::Handler, models::TradingViewDataEvent, websocket::WebSocketClient};
use tradingview::{DataServer, Error};

struct MyHandler;

impl Handler for MyHandler {
    fn handle_events(&self, event: TradingViewDataEvent, message: &[Value]) {
        match event {
            TradingViewDataEvent::OnQuoteData => {
                // message[0]: session ID
                // message[1]: quote object {"n": "...", "v": {...}}
            }
            TradingViewDataEvent::OnChartData | TradingViewDataEvent::OnChartDataUpdate => {
                // Candlestick update
            }
            _ => {}
        }
    }

    fn handle_quote_data(&self, message: &[Value]) {}
    fn handle_series_data(&self, _event: TradingViewDataEvent, _messages: &[Value]) {}
    fn notify_error(&self, error: Error, message: &[Value]) {
        eprintln!("Error: {:?}, payload: {:?}", error, message);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ws = WebSocketClient::builder()
        .auth_token("unauthorized_user_token")
        .server(DataServer::Data)
        .handler(MyHandler)
        .build()
        .await?;

    Arc::clone(&ws).spawn_reader_task();

    let qs = tradingview::utils::gen_session_id("qs");
    ws.create_quote_session(&qs).await?;
    ws.set_fields(&qs).await?;
    ws.add_symbols(&qs, &["BINANCE:BTCUSDT"]).await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    ws.delete_quote_session(&qs).await?;
    ws.close().await?;
    Ok(())
}
```

---

## Corporate Fundamentals & Economic Calendar

```rust
use tradingview::client::fin_calendar::{EconomicCalendarRequest, EconomicImportance, get_economic_calendar};
use tradingview::fundamental::{fetch_fundamental_registry, get_fundamental_data};
use tradingview::models::{FinancialPeriod, Interval};
use tradingview::live::models::DataServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Fundamentals via Registry
    let registry = fetch_fundamental_registry().await?;
    let study = get_fundamental_data(
        &registry,
        "total_revenue",
        Some(&FinancialPeriod::FiscalYear),
        "AAPL",
        "NASDAQ",
        Interval::OneDay,
        5,
        Some("unauthorized_user_token"),
        DataServer::Data,
    )
    .await?;
    println!("Fundamental points received: {}", study.len());

    // 2. Macroeconomic Calendar
    let request = EconomicCalendarRequest::builder()
        .from(chrono::Utc::now())
        .to(chrono::Utc::now() + chrono::Duration::days(7))
        .countries(vec!["US".to_string()])
        .min_importance(EconomicImportance::High)
        .build();

    let events = get_economic_calendar(&request).await?;
    println!("High-importance US events: {}", events.len());

    Ok(())
}
```

## User Authentication & Session Tokens

The `user` cargo feature enables authentication workflows against TradingView, including credential login, optional 2FA TOTP handling, automated reCAPTCHA solving via 2Captcha, and session token retrieval for authenticated WebSocket connections.

Module implementation resides in `crates/tradingview/src/user/` (`mod.rs` and private solver `captcha.rs`), using `wreq` with browser profile emulation for all authenticated user flows while public REST queries use `reqwest`.

### Usage Example

```rust
#[cfg(feature = "user")]
use tradingview::{UserCookies, client::misc::get_tradingview_token};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Credentials should be provided via environment variables (never committed to code)
    let username = std::env::var("TV_USERNAME")?;
    let password = std::env::var("TV_PASSWORD")?;
    // TOTP secret is optional: supports RFC 6238 Base32 secrets or otpauth:// URIs
    let totp_secret = std::env::var("TV_TOTP_SECRET").ok();
    // Optional 2Captcha API key: enables opt-in solver if reCAPTCHA is challenged
    let two_captcha_key = std::env::var("TWO_CAPTCHA_API_KEY").ok();

    let mut user = UserCookies::default();
    let user_cookies = match two_captcha_key.as_deref() {
        Some(key) if !key.trim().is_empty() => {
            // 1a. Explicit opt-in solver: solves reCAPTCHA v2 if challenged
            user.login_with_captcha(&username, &password, totp_secret.as_deref(), key).await?
        }
        _ => {
            // 1b. Standard credential login without third-party solver
            user.login(&username, &password, totp_secret.as_deref()).await?
        }
    };

    // 2. Retrieve a TradingView WebSocket auth token using authenticated cookies
    let auth_token = get_tradingview_token(&user_cookies).await?;
    println!("Retrieved auth token: {}", auth_token);

    Ok(())
}
```

### Authentication Semantics & Limitations

- **Returned `UserCookies`**: Successful login yields session cookies (`sessionid`, `sessionid_sign`, and `device_t`). These cookies can be reused or serialized to maintain session state across requests.
- **Token Retrieval (`get_tradingview_token`)**: Fetches a WebSocket authorization token from `/quote_token/` using an existing valid cookie session. Calling this without valid session cookies fails immediately with `LoginError::SessionNotFound` or `LoginError::InvalidSession`.
- **2FA TOTP Support**: The optional `totp_secret` parameter accepts raw RFC 6238 Base32 strings (including grouped strings with space separators, e.g. `JBSW Y3DP EHPK 3PXP` or `JBSWY3DPEHPK3PXP`) or `otpauth://totp/...` URIs.
- **CAPTCHA Challenges & Solver Opt-in**:
  - Standard `UserCookies::login` does not invoke external solvers. If TradingView presents a challenge, it returns `Error::Login` with `LoginError::CaptchaRequired` (or `AuthenticationError`).
  - `UserCookies::login_with_captcha` provides explicit opt-in automated solving for TradingView signin reCAPTCHA v2 (using sitekey and `g-recaptcha-response-v2` form field) via [2Captcha](https://2captcha.com).
  - **Charge & Attempt Budget**: Makes an initial login attempt. Only if challenged with `recaptcha_required` does it dispatch at most **one** paid `createTask` (`RecaptchaV2TaskProxyless`) to avoid unexpected spend. It never calls the solver on bad credentials, TOTP failures, HTTP 429 / HTTP 200 `rate_limit`, or server errors.
  - **Deadlines & Timeouts**: Total solver polling is bounded by a 120-second deadline (polling every 5 seconds), with 30-second per-request timeouts.
  - **Per-Login Cookie Context**: All cookies received during the initial challenge response are retained in the request jar and forwarded on the single retry attempt per RFC scope.
  - **Refund Semantics & Provider Review**: If the second signin attempt still rejects the token or requires captcha, the library reports the token once via [2Captcha reportIncorrect](https://2captcha.com/api-docs/report-incorrect). Feedback acceptance is subject to provider review and **not guaranteed** as a refund. Solvers that time out, cancel, or return [`ERROR_CAPTCHA_UNSOLVABLE` (error code 12)](https://2captcha.com/api-docs/error-codes) follow provider policy; timeout or cancellation does not guarantee a refund.
  - **No Guaranteed Bypass**: Opting into the solver does not guarantee TradingView session acceptance, as bot detection, Cloudflare managed challenges, or subsequent risk checks may still decline automated access.

### Safe Cookie Storage Example

The repository provides a runnable example in `crates/tradingview/examples/user.rs`:

```bash
cargo run --example user --features "user"
```

The example reads `TV_USERNAME`, `TV_PASSWORD`, optional `TV_TOTP_SECRET`, and optional `TWO_CAPTCHA_API_KEY` from environment variables, attempts login, and safely serializes the resulting cookies to `tv_user_cookies.json` using `create_new(true)` with Unix file permissions `0o600` (refusing to overwrite existing files to prevent accidental clobbering).

---


## Wire Protocol & Frame Encoding

- **Framing**: Packets follow the `~m~<length>~m~<payload>` framing structure.
- **Length Encoding**: The payload length MUST be counted in **UTF-16 code units** (matching JavaScript string length semantics). Multi-byte UTF-8 sequences (such as Vietnamese diacritics, CJK characters, or emojis) contain fewer UTF-16 code units than raw UTF-8 bytes. `tradingview-rs` automatically calculates this to prevent socket framing desynchronization.
- **Heartbeat Frames**: Ping packets `~h~<num>` are echoed directly as `~m~<len>~m~~h~<num>`.
