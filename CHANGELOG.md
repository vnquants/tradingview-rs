# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The core crate moves to 0.5.0 because of the source-compatibility changes listed under Changed; the Python binding is 0.4.7.

### Added
- Historical pagination: `HistoricalRequest` gains `from_timestamp`, `max_bars`, `max_pages`, `page_size` (default 5,000) and `page_delay` (default 500 ms). When any of the first three is set, `HistoricalClient::retrieve` follows the initial series with `request_more_data` pages until the oldest bar reaches `from_timestamp`, `max_bars` bars are collected, the server reports the start of history (`data_completed: "end"`) or the data server's bar limit (`"limit"`), a page adds no older bar, or `max_pages` pages were sent (default 100). Bars are de-duplicated by timestamp and sorted. `HistoricalResult` reports `pages_fetched`, `pagination_stop` (`PaginationStop`, `#[non_exhaustive]`) and `is_complete()`. Defaults keep the single-request behaviour.
- Truncated history is an error by default: a page timeout returns `Error::Timeout` and a closed connection returns `Error::WebSocket`. `HistoricalRequest::allow_partial` returns the bars received so far instead, with `PaginationStop::Timeout` or `PaginationStop::ConnectionClosed`.
- `HistoricalRequest::validate()` (run by `retrieve` before connecting) rejects a missing symbol or exchange and a `from_timestamp` in the future or above 10^11 (milliseconds).
- `HistoricalRequest` gains `adjustment` (`MarketAdjustment`), `session_type` (`SessionType`) and `currency`, sent in `resolve_symbol`; the server defaults are split adjustment and the regular session. `MarketAdjustment::Unadjusted` (wire value `none`) requests raw prices.
- `HistoricalResult` gains `dropped_rows` (unparseable bar rows, now also logged), `last_bar_close_time` (`lbs.bar_close_time`) and `last_bar_complete`, judged by the server's clock (message `t`/`t_ms`, local clock as fallback) against the close time plus the symbol's feed `delay` plus 2 s. `HistoricalRequest::drop_incomplete_last_bar` removes a still-forming newest bar; with `max_bars` it collects one extra bar so exactly `max_bars` closed bars remain.
- `Handler::handle_server_time` (provided method, default no-op) receives the server timestamp of each message.
- `HistoricalClient::retrieve_batch_with(symbols, &template, config)` applies a full `HistoricalRequest` template to every symbol; `HistoricalRequest::for_symbol` copies a template for one symbol.
- `HistoricalClient::with_endpoint(url)` and `WebSocketClient::builder().endpoint(url)` connect to another URL, such as a proxy or a local test server. It must be `wss://`; `ws://` is accepted only for a loopback host. `websocket::validate_endpoint` applies the rule.
- `RECOMMENDED_LOG_DIRECTIVES` (`tungstenite=debug,tokio_tungstenite=debug`) and a Logging section in the crate docs.
- `SymbolInfo` keeps `session`, `pricescale`, `minmov`, `minmove2`, `variable_tick_size`, `pointvalue`, `delay` and `isin` from `symbol_resolved`. `SymbolInfo::tick_size()` returns `minmov / pricescale` and `SymbolInfo::tick_size_at(price)` applies `variable_tick_size` (`None` for a malformed table or a non-finite price).
- Python binding 0.4.7: keyword-only `from_timestamp`, `max_bars`, `max_pages`, `page_size`, `page_delay`, `adjustment`, `session_type`, `currency`, `allow_partial` and `drop_incomplete_last_bar` on `get_historical`, `get_historical_df` and `get_historical_batch`; `MarketAdjustment`, `SessionType`, `SymbolInfo`, `HistoricalBatch` and `IncompleteHistoryError`, and the `PaginationStop` type alias; `HistoricalSeries.symbol_info`, `pages_fetched`, `pagination_stop`, `is_complete`, `dropped_rows` and `last_bar_complete`. Invalid keywords (non-positive sizes, negative `max_pages`, `max_concurrency` or `page_delay`, unknown currency, future or millisecond `from_timestamp`) raise `ValueError` when the method is called. The DataFrame path raises `IncompleteHistoryError` for an incomplete history unless `allow_partial=True`. `get_historical_batch` returns a `HistoricalBatch` (a `dict` subclass) whose `.failed` maps each symbol left out to its error. `n_bars` defaults to `None` (100 without pagination, as before).
- Python tests that talk to TradingView carry the `network` marker; `pytest -m "not network"` runs offline.
- `docs/history-depth.md` and the `history_depth` example: measured depth per data server for 1-minute, 15-minute and 1-hour bars. The data server caps the depth (about 5,000-8,000 bars on `Data`, `WidgetData` and `MobileData`, about 10,000-16,500 on `ProData`, the same anonymously and with a token), so pagination cannot reach further back than one large request.
- `tests/historical_mock.rs`: end-to-end tests against a local mock server covering pagination, connection reuse, dropped connections, close frames, unanswered pages, late pages and handshakes that never answer.
- Python binding 0.4.6: `TradingViewClient.export_cookies()` returns the session cookies as the JSON that Rust `UserCookies` serializes to, and `TradingViewClient.from_cookies(cookies_json, *, server=...)` rebuilds a cookie-session client from it without a network request, so `get_tradingview_token()` works across processes without a new password login. Only `session` and `session_signature` are required on import; malformed JSON raises `ValueError` and empty session cookies raise `AuthenticationError`, and neither message includes cookie values.
- Published PyPI wheels for Linux (x86_64, aarch64) with glibc 2.28 or newer, macOS (x86_64, arm64), and Windows (x86_64), plus a source distribution. Previously only a Linux x86_64 wheel requiring glibc 2.38 was published.

### Changed
- Batch retrieval opens at most `max_concurrency` WebSocket connections instead of one per symbol; each connection fetches its share of the symbols in turn, one chart session per symbol. A connection is replaced after a connection failure or a timeout, and a symbol whose connection closed or could not be opened is retried once on a new connection. `max_concurrency = 0` is treated as 1 instead of waiting forever. `BatchConfig::per_symbol_timeout` now bounds each wait of a symbol (connecting, the initial load, each page).
- Historical connections notice a closed connection (stream end, close frame, reset) at once and fail the waiting fetch instead of waiting for the timeout, and never reconnect on their own. A load or page that completed before the connection closed still succeeds.
- `WebSocketClient` sends its first keep-alive ping and health-check ping one interval after connecting instead of immediately. The immediate ping raced the session commands; when a server closed right after answering, the unread ping made it reset the connection and the client lost the last frames.
- Connecting a `WebSocketClient`, including the WebSocket handshake, is bounded by the connection timeout (30 s by default; the request timeout for historical fetches).
- The WebSocket reader stops on terminal read errors (closed, reset, I/O); it used to compare error strings that never matched.
- Historical fetches report a TradingView protocol error as `Error::TradingView { source }` with the first error received (for an unknown symbol `SymbolError`, which arrives before `series_error`) instead of `Error::Internal` with the last one; in Python an unknown symbol now raises `SymbolNotFoundError`.
- `HistoricalClient` no longer opens an unused quote session for each retrieval, and deletes its chart session when a fetch ends.
- When a historical fetch receives the same bar more than once (for example a real-time update of the newest bar), the latest value is kept instead of the first.
- Every `SymbolInfo` field is parsed leniently: a missing, `null` or wrongly typed value takes the default instead of failing the whole fetch. Integer metadata (`pricescale`, `minmov`, ...) accepts only integral, in-range values; others become 0, so `tick_size()` returns `None`.
- `ChartOptions` accepts `.`, `!`, `_`, `-` and `&` in exchange and symbol names, so `OKX:BTCUSDT.P`, `HNX:VN30F1!` and `FX_IDC:USDVND` validate.
- `HistoricalRequest::with_replay` is documented as having no effect.
- Source compatibility: the new public fields of `HistoricalRequest`, `HistoricalResult`, `HistoricalState` and `SymbolInfo` and the new `MarketAdjustment::Unadjusted` variant break code that builds these structs with literals that do not end in `..Default::default()` (or `..` from another value) or that matches `MarketAdjustment` exhaustively. Builders and constructors are unaffected.

### Security
- `UserCookies` now implements `Debug` by hand and redacts the auth token, session cookies, session hash, device token and private channel, so logging it no longer prints session secrets.
- Packet logging redacts the `set_auth_token` parameters. Every outgoing packet was logged at DEBUG, and `.cargo/config.toml` sets `RUST_LOG=debug`, so the session token reached the logs.
- `tungstenite` logs raw frames, including the auth token, at TRACE through `log`. Consumers that forward `log` records must cap `tungstenite` and `tokio_tungstenite` at DEBUG (`RECOMMENDED_LOG_DIRECTIVES`); `.cargo/config.toml` and `.env.example` now do. `tests/log_redaction.rs` bridges `log` records and checks that a real-looking token never appears at the recommended filter, and that it does appear in tungstenite's TRACE records without the cap.
- Endpoint overrides must use `wss://` unless the host is loopback, so the token is never sent in clear text over a network.

## [0.4.5] - 2026-11-07

### Fixed
- Fixed Pine study and fundamental retrieval after TradingView moved the study template out of the `translate` response's top-level `IL` and `ilTemplate` fields; `get_indicator_metadata` no longer fails to decode, and `PineMetadata::script_text()` reads `metaInfo.defaults.inputs.text`.
- Fixed range requests rejected with `invalid parameters`: `HistoricalClient`, `WebSocketClient::create_series`, and `WebSocketClient::modify_series` now send a positive bar count (#256, thanks @onichandame).
- Fixed the WebSocket connection and background task leak in `HistoricalClient::retrieve` and `StudyClient::retrieve`, including when the retrieval future is dropped (#259, thanks @onichandame).

### Changed
- Updated `pyo3` to 0.29.3.

### Credits
- Thanks to [@onichandame](https://github.com/onichandame) for reporting and fixing the WebSocket leak (#259) and the rejected range requests (#256).

## [0.4.4] - 2026-09-25

### Added
- Added optional `captcha_key` keyword argument to Python `TradingViewClient.login()` and `TradingViewClient.authenticate()` to support 2Captcha reCAPTCHA v2 solving directly from Python.

## [0.4.3] - 2026-09-25

### Added
- Added explicit opt-in reCAPTCHA v2 solving support via `UserCookies::login_with_captcha` using 2Captcha (`RecaptchaV2TaskProxyless` on sitekey `6Lcqv24UAAAAAIvkElDvwPxD0R8scDnMpizaBcHQ` with `g-recaptcha-response-v2` field).
- Added single-retry and spending controls for CAPTCHA solving: dispatches at most one paid `createTask` upon receiving a `recaptcha_required` challenge, bounded by a 120-second deadline (5-second polling) and 30-second per-request timeouts.
- Added automated single feedback reporting via 2Captcha `reportIncorrect` when a solved token is explicitly rejected by TradingView on retry (subject to provider review; not a guaranteed refund).
- Added optional `TWO_CAPTCHA_API_KEY` configuration in `crates/tradingview/examples/user.rs` and `.env.example`.

### Changed
- Reorganized `crates/tradingview/src/user.rs` into `crates/tradingview/src/user/mod.rs` and private solver module `crates/tradingview/src/user/captcha.rs`.
- Preserved standard `UserCookies::login` contract unchanged without third-party solver calls or automated bypass loops.
- Retained all challenge cookies across solver retry within RFC-scoped per-login cookie jar.
- Stopped solver dispatch on invalid credentials, 2FA failures, HTTP 429 or HTTP 200 `rate_limit` responses, and server errors to prevent unnecessary API spend.

### Fixed
- Fixed authentication error handling to properly detect and map empty credentials (`LoginError::EmptyCredentials`), missing session cookies (`LoginError::SessionNotFound`), and invalid sessions (`LoginError::InvalidSession`).
- Fixed TOTP 2FA parsing to support both standard RFC 6238 Base32 secrets and `otpauth://totp/...` URIs.
- Supported whitespace-grouped raw Base32 TOTP secrets (e.g., authenticator vault exports with space separators) by conditionally stripping internal ASCII whitespace while rejecting non-whitespace punctuation.
- Fixed Cloudflare and reCAPTCHA challenge response handling during login without attempting automated bypass or looping.
- Fixed WebSocket auth token retrieval (`get_tradingview_token`) to use canonical `/quote_token/` endpoint with strict JSON string deserialization and accurate cookie headers (`sessionid`, `sessionid_sign`, `device_t`).
- Hardened `examples/user.rs` by making `TV_TOTP_SECRET` optional, eliminating credentials/secrets logging, and saving user cookies to `tv_user_cookies.json` with `create_new(true)` and Unix permissions `0o600` refusing file overwrites.
## [0.4.2] - 2026-09-24

### Fixed
- Resolved Linux CI linker collision between OpenSSL (`native-tls`) and BoringSSL (`wreq`/`btls-sys`) by standardizing on `rustls-tls` (`aws-lc-rs`).
- Resolved response decompression errors on large Pine studies catalog endpoints (`pine-facade`) by adding `gzip` and `brotli` decompression features to `reqwest`.
- Handled non-JSON authentication responses (e.g. Cloudflare / CAPTCHA challenge pages) safely in `UserCookies::login`, mapping them cleanly to `AuthenticationError`.
- Cleaned up obsolete static `UA` string from `src/lib.rs` and `fundamental/fetch.rs`, retaining `User-Agent` explicitly inside `WEBSOCKET_HEADERS` for WebSocket connections.
- Marked external live network news tests as `#[ignore]` matching existing `fin_calendar` conventions.

### Changed
- Isolated `wreq` and `wreq-util` as optional dependencies dedicated strictly to `feature = "user"` for authenticated flows (`login`, 2FA, `get_tradingview_token`).
- Restored `reqwest` for all unauthenticated/public HTTP REST queries.
## [0.4.1] - 2026-09-24

### Added
- Python `DataServer` enum (`Data`, `ProData`, `WidgetData`, `MobileData`) for configurable TradingView WebSocket data server endpoint selection.
- Keyword-only `server` parameter (`server=DataServer.Data`) in `TradingViewClient` constructor and `login()`, with a read-only `server` property.
- Optional keyword-only `server` parameter override in high-level client methods: `get_historical`, `get_historical_df`, `get_historical_batch`, `subscribe_quotes`, `subscribe_bars`, and `get_fundamental`.
- Async `get_tradingview_token()` method on Python `TradingViewClient` (and Rust `get_tradingview_token`) to retrieve a TradingView session token using authenticated session cookies.
- Offline rejection with `AuthenticationError` when `get_tradingview_token()` is called on anonymous or token-only clients before making network requests.
- Support for both RFC 6238 Base32 secrets and `otpauth://totp/...` URIs in `totp_secret` parameter for `login()` and `authenticate()`.
- Automatic initial `set_auth_token` dispatch in `WebSocketClient` prior to session command execution.

### Changed
- Preserved default `DataServer.Data` endpoint and backward-compatible positional API across all client methods.
- Migrated entire HTTP client transport from `reqwest` to `wreq` (v6.0) + `wreq-util` (v3.0) with browser emulation.
- Migrated 2FA TOTP implementation from `google-authenticator` to `totp-rs` (v6.0) with `std`, `otpauth`, and `zeroize` features.
- Renamed `get_quote_token` to `get_tradingview_token` across Rust and Python without aliases.
- Cargo TLS flags (`rustls-tls`, `native-tls`) now scope exclusively to WebSocket connections via `tokio-tungstenite`.
- Clarified `DataServer.ProData` access semantics: anonymous connection is supported for public data, while paid market data feeds require account and feed entitlements.
## [0.4.0] - 2026-09-22

### Added
- Native Python bindings (`tradingview-py`) built with PyO3 0.29, `abi3` stable ABI (Python >= 3.10), and Tokio async runtime integration.
- Direct Polars DataFrame conversion across historical OHLCV data, batch retrieval, corporate fundamentals, and economic calendar queries (`as_dataframe=True` and `get_historical_df`).
- Dual-mode real-time streaming: async iterators (`async for`) and synchronous callbacks with exception isolation (`sys.unraisablehook`) and thread-safe event loop trampolining.
- Python exception hierarchy (`TradingViewError`, `AuthenticationError`, `SymbolNotFoundError`, `ConnectionError`, `TimeoutError`, `RateLimitError`, `ProtocolError`) with orphan-rule-safe Rust error mapping.
- Core error enrichment: added typed `Error::RateLimited` variant and enriched HTTP 429 status code handling.
- CI/CD workflow for automated Python wheel building and PyPI publishing via Twine.
- Comprehensive documentation with Mermaid architecture diagrams in `README.md` and dedicated guides in `docs/`.

### Changed
- Converted project into a virtual Cargo workspace with `crates/tradingview` (core library) and `crates/tradingview-py` (Python extension).
- Relocated core library source, tests, examples, and benchmarks into `crates/tradingview/`.
## [0.3.0] - 2026-09-10

### Added
- One-argument full fundamental fetch API (`fetch_full_fundamentals("AAPL") -> FullFundamentalResult`) with ranked stock resolution, typed per-study outcomes, and optional CSV export
- High-level one-shot study retrieval client (`tradingview::study::{StudyClient, StudyRequest, StudyResult}`) modeled after `HistoricalClient`
- Global economic calendar API client (`tradingview::client::fin_calendar::{get_economic_calendar, EconomicCalendarRequest, EconomicCalendarEvent, EconomicImportance}`)
- Runnable example for full fundamental catalog retrieval and lossless long-form CSV export (`examples/full_fundamental_fetch.rs`)
- Technical Analysis scanner API (`get_technical_analysis`, `TechnicalAnalysis`, `Period`, `TechnicalAnalysisRecommendations`)
- Chart `Range` options support and export in historical series retrieval
- Candle data source metadata and symbol data-feed prefix field support

### Changed
- `Symbol::id()` now prefers non-empty `prefix` (e.g. `AMEX:SPY`), falling back to `exchange`
- Updated default User-Agent header to modern browser target

### Performance
- Zero-copy structural length-prefixed WebSocket packet parser replacing regex cleaner and splitter
- Zero-copy packet parser tests and UTF-16 framing support for Unicode scripts (Vietnamese HOSE, Taiwanese TWSE)
- `gen_id()` returns `String` without permanently interning into global `Ustr` hash map
- Replaced 100ms sleep polling in `HistoricalClient::retrieve` with `tokio::sync::Notify`

### Fixed
- Protocol wire payloads for `modify_study` (4 args) and `replay_*` commands (request ID, milliseconds interval, replay session)
- Heartbeat echo framing and TradingView protocol frame handling

## [0.2.0] - 2026-06-10

### Added
- Event-driven `DataLoader` with source → fan-out → sinks architecture
- `DataSource` trait with TradingView WebSocket adapter (`source::tradingview`)
- `EventSink` trait with three built-in sinks: `ChannelSink`, `CallbackSink`, `KafkaSink`
- `MarketEvent` enum (Candle, Quote, Economic, News variants)
- `Subscription` and `LoaderConfig` for data pipeline configuration
- `BoundedChannel` with configurable capacity in all sink paths
- Channel-based write path in `WebSocketClient` (eliminates `Arc<Mutex<SplitSink>>` lock contention)
- Circuit breaker pattern in `WebSocketClient` for error recovery
- `ConnectionHealth` and `HealthMetrics` monitoring in WebSocket client
- `ErrorSeverity` classification for WebSocket errors
- Full API documentation (`///` doc comments) on all public types
- Module-level docs for `client`, `live`, `chart`, `quote`, `models`
- ROADMAP.md with version milestones
- CONTRIBUTING.md with developer guidelines
- GitHub project setup script (`scripts/gh_setup.sh`)

### Changed
- README: added architecture diagram, module table, feature table, roadmap link
- README: updated Installation section with crates.io + feature flag table
- README: updated Examples section with all current examples
- `models/mod.rs`: documented all ~30 public types (`Interval`, `Symbol`, `MarketType`, etc.)
- `error.rs`: documented `Error`, `TradingViewError`, `LoginError` variants
- `chart/options.rs`: documented `ChartOptions`, `Range`, `StudyOptions`
- `live/websocket.rs`: documented `WebSocketClient`, `ErrorSeverity`, `ConnectionHealth`, `SeriesInfo`
- `live/models.rs`: documented `TradingViewDataEvent`, `SocketMessageSer`, `SocketMessageDe`, `SocketServerInfo`, `DataServer`
- `live/handler/command.rs`: documented `CommandPriority`, `Command`, `ConnectionStatus`, `CommandRunner`

### Performance
- Fan-out optimization in `DataLoader` moving events into final sink and cloning only for preceding active sinks

### Fixed
- 7 broken intra-doc links in module-level docs (now zero `cargo doc` warnings)
- Duplicate `EconomicCategory` definition in models/mod.rs

### Known Issues
- 7 `parse_packet` / `SocketMessage` round-trip test failures (pre-existing, tracked in v0.2.0)

## [0.1.2] — 2025

Initial alpha release with core data retrieval capabilities.

### Features
- Historical OHLCV data (single + batch)
- Real-time WebSocket streaming (quotes, charts, studies)
- Symbol search with market type filtering
- User authentication with TOTP 2FA support
- Pine Script built-in indicator catalog
- Chart drawings retrieval
- Replay mode for historical market data
- News feed integration
