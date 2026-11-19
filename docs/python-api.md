# Python API Reference (`tradingview`)

The `tradingview` Python library provides native, high-performance bindings to `tradingview-rs` compiled with PyO3 0.29 and Maturin.

---

## Architecture & Threading Model

- **Tokio-to-AsyncIO Bridge**: Operations execute asynchronously on a background Tokio runtime via `pyo3-async-runtimes`. All client methods return standard Python `asyncio` awaitables/coroutines.
- **Global Interpreter Lock (GIL) Release**: All network I/O, batch tasks, and JSON/WebSocket deserialization release the Python GIL, allowing true multicore parallelism.
- **HTTP & WebSocket Transports**: Authenticated user flows (`login`, 2FA TOTP verification, and session token retrieval via `get_tradingview_token`) use `wreq` and `wreq-util` with browser emulation and BoringSSL under the `user` feature. All public and unauthenticated HTTP REST requests (e.g. quote search, chart tokens, economic calendar, Pine catalog) use `reqwest`. WebSocket streaming connects via Tokio tungstenite; the Cargo `rustls-tls` feature applies to `reqwest` and WebSocket transport.
- **Native Toolchain Requirement**: Building the native library from source requires CMake, Clang (or GCC), and Perl to compile BoringSSL. Pre-built wheels are not guaranteed across all environments.
- **Callback Dispatching & Trampoline**: Callbacks registered on subscriptions are invoked on the Python event loop thread via `loop.call_soon_threadsafe` with exception isolation (`sys.unraisablehook`). Unhandled callback exceptions do not crash the stream.

---

## Client: `TradingViewClient`

```python
from tradingview import (
    TradingViewClient,
    DataServer,
    Interval,
    FinancialPeriod,
    EconomicImportance,
    MarketAdjustment,
    SessionType,
)
```

### Initializing Client

```python
# Anonymous client (default server is DataServer.Data, uses default 'unauthorized_user_token')
client = TradingViewClient()

# Client initialized with a pre-authenticated session token and default Data server
client = TradingViewClient(auth_token="your_session_auth_token")

# Client configured for TradingView ProData endpoint
client = TradingViewClient(
    auth_token="your_session_auth_token",
    server=DataServer.ProData,
)

# Check authentication state and configured server
print(client.is_authenticated)  # True or False
print(client.auth_token)        # Token string or None
print(client.server)            # DataServer.Data or DataServer.ProData
```

### DataServer Endpoints & Entitlements Notice

The `DataServer` enum provides endpoints for TradingView WebSocket communication:
- `DataServer.Data` (`"data"`): Default standard data server.
- `DataServer.ProData` (`"prodata"`): High-throughput data server for authenticated Pro accounts.
- `DataServer.WidgetData` (`"widgetdata"`): Widget data server.
- `DataServer.MobileData` (`"mobile-data"`): Mobile application data server.

> **Important Account Entitlements Notice**:
> Anonymous connection to `DataServer.ProData` is supported for public market data. However, accessing paid market data feeds requires corresponding account and feed entitlements; changing the endpoint to `ProData` does **not** grant paid data access or bypass feed entitlement checks without appropriate account subscriptions.
> **Application Environment Variable Responsibility**:
> Loading authentication tokens or login credentials from `.env` files or system environment variables is an explicit application concern (e.g. using `python-dotenv` or `os.getenv`). The library never automatically loads `.env` files.

### Credential Login & Authenticate

```python
# Asynchronous classmethod login: returns authenticated client
# totp_secret supports either standard RFC 6238 Base32 or a full otpauth:// URI (e.g. from Bitwarden)
# captcha_key is an optional 2Captcha API key to automatically solve reCAPTCHA v2 challenges
client = await TradingViewClient.login(
    username="your_username",
    password="your_password",
    totp_secret="OPTIONAL_2FA_TOTP_SECRET_OR_OTPAUTH_URI",
    captcha_key="OPTIONAL_2CAPTCHA_API_KEY",
    server=DataServer.ProData,  # Optional keyword argument (default: DataServer.Data)
)

# Or authenticate an existing client instance
await client.authenticate(
    username="your_username",
    password="your_password",
    totp_secret="OPTIONAL_2FA_TOTP_SECRET_OR_OTPAUTH_URI",
    captcha_key="OPTIONAL_2CAPTCHA_API_KEY",
```

### Automated CAPTCHA Solving (2Captcha Integration)

When TradingView requires a CAPTCHA verification challenge during automated sign-in (`recaptcha_required`), the Python client can automatically solve it using [2Captcha](https://2captcha.com) by supplying `captcha_key`:

```python
client = await TradingViewClient.login(
    username=os.environ["TV_USERNAME"],
    password=os.environ["TV_PASSWORD"],
    totp_secret=os.getenv("TV_TOTP_SECRET"),
    captcha_key=os.getenv("TWO_CAPTCHA_API_KEY"),
)
```

#### CAPTCHA Solving Details & Safeguards
- **Target reCAPTCHA Type**: TradingView uses standard **reCAPTCHA v2** (visible checkbox, `RecaptchaV2TaskProxyless` on sitekey `6Lcqv24UAAAAAIvkElDvwPxD0R8scDnMpizaBcHQ` and domain `recaptcha.net`), sending the solved token in form field `g-recaptcha-response-v2`. (Note: reCAPTCHA v3 is not used for email/password sign-in).
- **Strict Spending Control**: The solver is **never** invoked unconditionally. The client first sends regular credentials; only when TradingView responds with `recaptcha_required` does it dispatch at most **one** paid `createTask` task.
- **Never Called on Non-CAPTCHA Failures**: The solver is skipped on invalid credentials, 2FA errors, HTTP 429 or HTTP 200 `rate_limit` responses, and server errors, preventing wasted balance.
- **Timeouts & Deadlines**: Overall solve polling is bounded to a 120-second deadline (5-second polling interval) with 30-second HTTP request timeouts.
- **Automated Refund Reporting (`reportIncorrect`)**: If TradingView rejects the solved token on retry, the client automatically submits a single feedback complaint via [2Captcha reportIncorrect](https://2captcha.com/api-docs/report-incorrect). Complaints are reviewed by 2Captcha and refunds are subject to provider review (not guaranteed). Unsolvable tasks (`ERROR_CAPTCHA_UNSOLVABLE`, error 12) are automatically refunded by 2Captcha.

### Session & TradingView Token Retrieval (`get_tradingview_token`)

```python
async def get_tradingview_token(self) -> str: ...
```

Retrieves a TradingView session token using authenticated session cookies (`sessionid`, `sessionid_sign`, `device_t`).

- **Prerequisites**: The client must hold an authenticated cookie session from `TradingViewClient.login()`, `client.authenticate()` or `TradingViewClient.from_cookies()`.
- **Rejection**: Anonymous clients or token-only clients initialized via `TradingViewClient(auth_token=...)` will immediately raise `AuthenticationError` before executing any network request. A supplied auth/websocket token is not a cookie session.
- **Token Disparity & Transport**: Token types are not equivalent; layout-sharing JWTs or standalone auth tokens do not grant cookie-authenticated HTTP session access. Authenticated cookie flows use `wreq` backed by BoringSSL, while public HTTP requests use `reqwest`; the Cargo `rustls-tls` feature applies to `reqwest` and WebSocket transport. No claim of CAPTCHA bypass is made.
- **Token Usage**: Returns the retrieved token string without mutating the current client's `auth_token`. The token can then be passed to instantiate a client targeting `DataServer.ProData`.

#### Example: Login, Retrieve Token, and Connect to ProData

```python
import os
from dotenv import load_dotenv
from tradingview import TradingViewClient, DataServer

load_dotenv()

# 1. Login with credentials to establish authenticated session cookies
login_client = await TradingViewClient.login(
    username=os.environ["TV_USERNAME"],
    password=os.environ["TV_PASSWORD"],
)

# 2. Retrieve the TradingView session token using session cookies
token = await login_client.get_tradingview_token()
await login_client.close()

# 3. Instantiate a data client using the retrieved token and ProData endpoint
data_client = TradingViewClient(
    auth_token=token,
    server=DataServer.ProData,
)
```

### Persisting a Cookie Session (`export_cookies` / `from_cookies`)

```python
def export_cookies(self) -> str: ...

@classmethod
def from_cookies(cls, cookies_json: str, *, server: DataServer = DataServer.Data) -> TradingViewClient: ...
```

A password login is rate limited and may trigger a CAPTCHA, so log in once, save the session cookies, and rebuild a cookie-session client from them in later processes.

- **Format**: `export_cookies()` returns the JSON that the Rust `UserCookies` serializes to (`id`, `username`, `private_channel`, `auth_token`, `session`, `session_signature`, `session_hash`, `device_token`). It raises `AuthenticationError` on anonymous or token-only clients.
- **Import**: `from_cookies()` makes no network request. Only `session` (`sessionid`) and `session_signature` (`sessionid_sign`) are required, so cookies copied from a browser also work. Malformed JSON raises `ValueError`; empty session cookies raise `AuthenticationError`. Error messages never include cookie values.
- **Restored state**: `username` and `auth_token` are restored from the JSON when present. The stored `auth_token` is the one issued at login and may have expired; call `get_tradingview_token()` for a fresh token.
- **Storage**: the JSON holds live session secrets. Write it with owner-only permissions (mode `0600`) and keep it out of version control. If TradingView expires or revokes the session, `get_tradingview_token()` raises `AuthenticationError`; log in again and save the new cookies.

```python
import os
from pathlib import Path
from tradingview import AuthenticationError, TradingViewClient

cookies_path = Path("cookies.json")

# First run: password login, then save the cookies with mode 0600.
client = await TradingViewClient.login(
    username=os.environ["TRADINGVIEW_USERNAME"],
    password=os.environ["TRADINGVIEW_PASSWORD"],
    totp_secret=os.environ.get("TRADINGVIEW_TOTP_SECRET"),
)
fd = os.open(cookies_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    f.write(client.export_cookies())

# Later runs: no password login.
client = TradingViewClient.from_cookies(cookies_path.read_text())
try:
    token = await client.get_tradingview_token()
except AuthenticationError:
    ...  # session expired or revoked: log in again and overwrite cookies.json
```

---

## Historical Data Retrieval

### `get_historical`

```python
async def get_historical(
    symbol: str,
    exchange: str,
    interval: Interval = Interval.OneDay,
    n_bars: int | None = None,
    with_replay: bool = False,
    as_dataframe: bool = False,
    *,
    server: DataServer | None = None,
    from_timestamp: int | None = None,
    max_bars: int | None = None,
    max_pages: int | None = None,
    page_size: int = 5000,
    page_delay: float = 0.5,
    adjustment: MarketAdjustment | None = None,
    session_type: SessionType | None = None,
    currency: str | None = None,
    allow_partial: bool = False,
    drop_incomplete_last_bar: bool = False,
) -> HistoricalSeries | polars.DataFrame: ...
```

- **`as_dataframe=True`**: Directly returns a `polars.DataFrame` with columns `["timestamp", "open", "high", "low", "close", "volume"]`.
- **`as_dataframe=False`**: Returns a `HistoricalSeries` container.
- **`n_bars`**: Bars in the first request. Without pagination it defaults to 100, as before. With pagination it defaults to `page_size` (capped at `max_bars`).
- **`with_replay`**: Has no effect; kept for compatibility. Use the pagination keywords to load older bars.
- All keywords after `*` are keyword-only, and their defaults keep the previous behaviour: one request, no adjustment, session or currency key sent.
- **Validation at call time**: invalid keywords raise `ValueError` when the method is called, before the coroutine is returned or awaited: a non-positive `n_bars`, `max_bars` or `page_size`, a negative `max_pages` or `page_delay`, an unknown `currency`, or a `from_timestamp` in the future or above 10^11 (which looks like milliseconds; pass Unix seconds).

```python
# Direct Polars DataFrame
df = await client.get_historical("AAPL", "NASDAQ", Interval.OneDay, n_bars=100, as_dataframe=True)

# Convenience method
df = await client.get_historical_df("AAPL", "NASDAQ", Interval.OneDay, n_bars=100)

# Structured HistoricalSeries
series = await client.get_historical("AAPL", "NASDAQ", Interval.OneDay, n_bars=100)
print(len(series))          # Number of bars
first = series[0]           # First Bar
last = series[-1]           # Negative indexing supported
df_pl = series.to_polars()  # Convert to Polars DataFrame
df_pd = series.to_pandas()  # Convert to Pandas DataFrame
print(series.symbol_info)   # SymbolInfo(id='NASDAQ:AAPL', ..., tick_size=0.01)
```

### Pagination (`from_timestamp`, `max_bars`, `max_pages`)

Setting `from_timestamp`, `max_bars` or `max_pages` makes the client page back with TradingView's `request_more_data` after the first request: `page_size` older bars per page, waiting `page_delay` seconds before each page. It stops at the first of:

| `pagination_stop` | Condition |
| --- | --- |
| `"reached_from"` | The oldest bar is at or before `from_timestamp` (Unix seconds). |
| `"reached_max_bars"` | At least `max_bars` distinct bars were received. |
| `"end_of_history"` | The server reports the start of the symbol's history. |
| `"bar_limit"` | The server reports the bar limit of the data server. |
| `"no_more_data"` | A page added no older bar. |
| `"max_pages"` | `max_pages` pages were sent (default 100 when another keyword enabled pagination). |
| `"timeout"` | Only with `allow_partial=True`: a page did not complete within 30 seconds. |
| `"connection_closed"` | Only with `allow_partial=True`: the connection closed during pagination. |

Bars are de-duplicated by timestamp (the latest update wins) and sorted ascending. Bars older than `from_timestamp` are dropped, and with `max_bars` only the newest `max_bars` are returned. `HistoricalSeries.pages_fetched` and `HistoricalSeries.pagination_stop` report what happened. The `PaginationStop` type alias lists the values.

**Truncated history is never a silent success.**

- A page that times out raises `TimeoutError`, and a connection that closes raises `ConnectionError`, as soon as the closure is noticed (no waiting for the timeout). Pass `allow_partial=True` to get the bars received so far instead, with `pagination_stop` `"timeout"` or `"connection_closed"`.
- `HistoricalSeries.is_complete` is `True` without pagination, or when pagination stopped at `"reached_from"`, `"reached_max_bars"` or `"end_of_history"`. After `"bar_limit"`, `"max_pages"`, `"no_more_data"`, `"timeout"` or `"connection_closed"`, older bars may exist and it is `False`.
- A Polars DataFrame cannot carry the stop reason, so the DataFrame path (`as_dataframe=True` or `get_historical_df`) raises `IncompleteHistoryError` (a `TradingViewError`) when `is_complete` would be `False`, unless `allow_partial=True`. For example, `from_timestamp` far beyond the data server's bar limit raises with `bar_limit` in the message.

### The last bar and dropped rows

- **`HistoricalSeries.last_bar_complete`**: whether the newest bar had closed when the fetch finished, from the server's `lbs.bar_close_time` and the local clock; `None` when unknown. During a session the newest bar is usually still forming.
- **`drop_incomplete_last_bar=True`** drops that bar when it had not closed, so training data never contains a partial bar.
- **`HistoricalSeries.dropped_rows`**: bar rows the server sent that could not be parsed (malformed or without OHLC values). They are left out and logged as a warning.

```python
import datetime as dt

start = int(dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).timestamp())
series = await client.get_historical(
    "VCB", "HOSE", Interval.OneHour,
    from_timestamp=start,
    page_delay=1.0,
    server=DataServer.ProData,
)
print(len(series), series.pages_fetched, series.pagination_stop)
```

> **History depth is capped by the data server.** Pagination cannot go past the server's bar limit: the server answers with `data_completed: "limit"` (`pagination_stop == "bar_limit"`). On 2026-11-19, `DataServer.Data`, `WidgetData` and `MobileData` returned about 5,000-8,000 bars and `DataServer.ProData` about 10,000-16,500 (a cap of about 5,000 or 10,000 bars, extended to TradingView's internal block boundary), with the same result anonymously and with a token session. A single request with a large `n_bars` reaches the same depth. See [`docs/history-depth.md`](history-depth.md) for the measured table (for example, 1-minute BINANCE:BTCUSDT reaches back about 4 days on `Data` and 11 days on `ProData`).

### Adjustment, session and currency

- **`adjustment`**: `MarketAdjustment.Splits` (split and stock-dividend adjusted; the server default), `MarketAdjustment.Dividends` (also back-adjusted for cash dividends) or `MarketAdjustment.Unadjusted` (raw traded prices, wire value `"none"`). Symbols that do not support adjustment, such as crypto pairs, return the same bars for every value.
- **`session_type`**: `SessionType.Regular` (server default), `Extended` (regular plus pre- and post-market), `PreMarket` or `PostMarket`. Only symbols with extended hours (for example US stocks) differ.
- **`currency`**: ISO 4217 code such as `"USD"` to convert prices; `symbol_info.currency_code` reflects the conversion. An unknown code raises `ValueError`.

```python
raw = await client.get_historical(
    "VCB", "HOSE", Interval.OneDay, n_bars=1000,
    adjustment=MarketAdjustment.Unadjusted,
)
aapl_ext = await client.get_historical(
    "AAPL", "NASDAQ", Interval.OneHour, n_bars=500,
    session_type=SessionType.Extended,
)
vcb_usd = await client.get_historical("VCB", "HOSE", Interval.OneDay, currency="USD")
```

### `SymbolInfo`

`HistoricalSeries.symbol_info` holds the metadata TradingView sends with the bars:

| Attribute | Example (HOSE:VCB) | Meaning |
| --- | --- | --- |
| `id`, `name`, `exchange`, `listed_exchange`, `description` | `"HOSE:VCB"`, `"VCB"`, `"HOSE"` | Identity |
| `market_type` | `"stock"` | TradingView instrument type |
| `currency_code` | `"VND"` | Price currency |
| `timezone`, `session` | `"Asia/Ho_Chi_Minh"`, `"0900-1500"` | Exchange time zone and session |
| `pricescale`, `minmov`, `minmove2` | `1`, `100`, `0` | Prices move in steps of `minmov / pricescale` |
| `variable_tick_size` | `"10 10000 50 50000 100"` | Tick 10 below 10,000, 50 below 50,000, 100 above; empty when constant |
| `pointvalue` | `1.0` | Value of a one-point move per contract |
| `delay` | `900` | Data delay in seconds |
| `isin` | `"VN000000VCB4"` | ISIN when known |
| `tick_size` (property) | `100.0` | `minmov / pricescale`, or `None` when unknown |
| `tick_size_at(price)` | `tick_size_at(9_990) == 10.0` | Tick at a price level, using `variable_tick_size` |

### `get_historical_batch`

```python
async def get_historical_batch(
    symbols: Sequence[tuple[str, str]],
    interval: Interval = Interval.OneDay,
    n_bars: int | None = None,
    max_concurrency: int = 4,
    as_dataframe: bool = False,
    *,
    server: DataServer | None = None,
    from_timestamp: int | None = None,
    max_bars: int | None = None,
    max_pages: int | None = None,
    page_size: int = 5000,
    page_delay: float = 0.5,
    adjustment: MarketAdjustment | None = None,
    session_type: SessionType | None = None,
    currency: str | None = None,
    allow_partial: bool = False,
    drop_incomplete_last_bar: bool = False,
) -> HistoricalBatch: ...
```

- Takes the keywords of `get_historical` and applies them to every symbol.
- Opens at most `max_concurrency` WebSocket connections (never more than there are symbols). Each connection fetches its share of the symbols one after another, each in its own chart session. A connection is replaced after a connection failure or a timeout, and a symbol whose connection closed or could not be opened is retried once on a new connection.
- Returns a `HistoricalBatch`: a `dict` keyed by `"EXCHANGE:SYMBOL"` (e.g. `"NASDAQ:AAPL"`) whose `failed` attribute maps every symbol left out to its error message, such as `"SymbolNotFoundError: Symbol resolution failed"` or `"TimeoutError: ..."`. With `as_dataframe=True` and without `allow_partial`, incomplete histories are listed in `failed` too.
- When `as_dataframe=True`, values are `polars.DataFrame` instances.

```python
batch = await client.get_historical_batch([("AAPL", "NASDAQ"), ("NOPE", "NASDAQ")])
for key, series in batch.items():
    print(key, len(series), series.is_complete)
for key, error in batch.failed.items():
    print("failed:", key, error)
```

---

## Real-Time Streaming Subscriptions

### `subscribe_quotes`

```python
async def subscribe_quotes(
    symbols: Sequence[str],
    callback: Callable[[QuoteTick], None] | None = None,
    *,
    server: DataServer | None = None,
) -> QuoteSubscription: ...
```

- **Lossy Queue Policy**: Drops older unconsumed snapshots if the internal 1024-item buffer fills, ensuring callers always receive the freshest price.
- **Dual Mode**: Yields `QuoteTick` instances via `async for` and/or synchronous callbacks.

```python
def on_tick(tick: QuoteTick):
    print(f"Callback: {tick.symbol} @ {tick.price} (bid={tick.bid}, ask={tick.ask})")

sub = await client.subscribe_quotes(["BINANCE:BTCUSDT", "NASDAQ:AAPL"], callback=on_tick)

# Asynchronous iterator
async for tick in sub:
    print(f"Iterator tick: {tick.symbol} price={tick.price}")
    break

# Cleanup
await sub.stop()
```

### `subscribe_bars`

```python
async def subscribe_bars(
    symbols: Sequence[str],
    interval: Interval = Interval.OneMinute,
    callback: Callable[[CandleUpdate], None] | None = None,
    *,
    server: DataServer | None = None,
) -> BarSubscription: ...
```

- **Lossless Backpressured Queue Policy**: Awaits capacity on the internal buffer, guaranteeing no dropped candlestick closing events.

```python
sub = await client.subscribe_bars(["BINANCE:ETHUSDT"], interval=Interval.OneMinute)

async for candle in sub:
    print(f"Candle: {candle.symbol} Close={candle.close} High={candle.high}")
    break

await sub.stop()
```

---

## Fundamentals & Economic Calendar

### `get_fundamental`

```python
async def get_fundamental(
    symbol: str,
    exchange: str,
    fund_id: str,
    period: FinancialPeriod = FinancialPeriod.FiscalYear,
    n_bars: int = 20,
    as_dataframe: bool = False,
    *,
    server: DataServer | None = None,
) -> FundamentalSeries | polars.DataFrame: ...
```

- `fund_id`: Metric identifier (e.g. `"total_revenue"`, `"net_income"`, `"ebitda"`, `"total_assets"`).
- `period`: `FinancialPeriod.FiscalYear` (`"FY"`), `FiscalQuarter` (`"FQ"`), `FiscalHalfYear` (`"FH"`), `TrailingTwelveMonths` (`"TTM"`), or `NoAggregation` (`"NOAGG"`).

### `get_economic_calendar`

```python
async def get_economic_calendar(
    countries: Sequence[str] | None = None,
    from_timestamp: int | None = None,
    to_timestamp: int | None = None,
    min_importance: EconomicImportance = EconomicImportance.Medium,
    as_dataframe: bool = False,
) -> list[EconomicEvent] | polars.DataFrame: ...
```

- `countries`: Optional list of 2-letter uppercase ISO country codes (`["US", "DE", "JP"]`).
- `min_importance`: `EconomicImportance.Low` (`-1`), `EconomicImportance.Medium` (`0`), `EconomicImportance.High` (`1`).

---

## Domain Exceptions (`tradingview.exceptions`)

```python
from tradingview.exceptions import (
    TradingViewError,
    AuthenticationError,
    SymbolNotFoundError,
    ConnectionError,
    TimeoutError,
    RateLimitError,
    ProtocolError,
)
```

| Exception | Base | When Raised |
| :--- | :--- | :--- |
| `TradingViewError` | `Exception` | Base class for all TradingView provider exceptions |
| `AuthenticationError` | `TradingViewError` | Invalid credentials, expired session, or rejected token |
| `SymbolNotFoundError` | `TradingViewError` | Symbol or exchange could not be resolved |
| `ConnectionError` | `TradingViewError` | WebSocket drop, HTTP connection failure, or I/O error |
| `TimeoutError` | `TradingViewError` | Request deadline exceeded |
| `RateLimitError` | `TradingViewError` | HTTP 429 or WebSocket throttling status received |
| `ProtocolError` | `TradingViewError` | Unexpected or unparseable frames received from server |
