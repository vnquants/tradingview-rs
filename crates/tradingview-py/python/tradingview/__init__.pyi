"""TradingView Data Provider Python API bindings backed by tradingview-rs."""

from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from enum import Enum
from typing import Any, ClassVar, Literal, TypeAlias

from tradingview._batch import HistoricalBatch as HistoricalBatch
from tradingview.exceptions import (
    AuthenticationError,
    ConnectionError,
    IncompleteHistoryError,
    ProtocolError,
    RateLimitError,
    SymbolNotFoundError,
    TimeoutError,
    TradingViewError,
)

class Interval(Enum):
    OneMinute = "1"
    ThreeMinutes = "3"
    FiveMinutes = "5"
    FifteenMinutes = "15"
    ThirtyMinutes = "30"
    FortyFiveMinutes = "45"
    OneHour = "60"
    TwoHours = "120"
    ThreeHours = "180"
    FourHours = "240"
    OneDay = "1D"
    OneWeek = "1W"
    OneMonth = "1M"

class FinancialPeriod(Enum):
    FiscalYear = "FY"
    FiscalQuarter = "FQ"
    FiscalHalfYear = "FH"
    TrailingTwelveMonths = "TTM"
    NoAggregation = "NOAGG"

class EconomicImportance(Enum):
    Low = -1
    Medium = 0
    High = 1

class DataServer(Enum):
    """TradingView WebSocket data server endpoint.

    Note:
        Connecting anonymously to `DataServer.ProData` is supported for public market data.
        Accessing paid market data feeds requires account and feed entitlements; changing
        the server endpoint to ProData does not grant paid access or bypass paywalled feeds.
    """

    Data = "data"
    ProData = "prodata"
    WidgetData = "widgetdata"
    MobileData = "mobile-data"

class MarketAdjustment:
    """Price adjustment applied to historical bars.

    A native enum (not an `enum.Enum`): members are class attributes with `name`
    and `value` and compare with `==`; they are not hashable. Without an
    adjustment TradingView applies `Splits`. Symbols that do not support
    adjustments (for example crypto pairs) return the same bars for every value.
    """

    Splits: ClassVar[MarketAdjustment]
    """Adjust for splits and stock dividends (the server default); value `"splits"`."""
    Dividends: ClassVar[MarketAdjustment]
    """Adjust for splits and cash dividends (back-adjusted prices); value `"dividends"`."""
    Unadjusted: ClassVar[MarketAdjustment]
    """Raw traded prices without any adjustment; value `"none"`."""

    @property
    def name(self) -> str: ...
    @property
    def value(self) -> str: ...

class SessionType:
    """Trading session of historical bars. Without a session TradingView returns `Regular`.

    A native enum (not an `enum.Enum`), like `MarketAdjustment`.
    """

    Regular: ClassVar[SessionType]
    """Regular trading hours; value `"regular"`."""
    Extended: ClassVar[SessionType]
    """Regular hours plus pre- and post-market; value `"extended"`."""
    PreMarket: ClassVar[SessionType]
    """Pre-market only; value `"premarket"`."""
    PostMarket: ClassVar[SessionType]
    """Post-market only; value `"postmarket"`."""

    @property
    def name(self) -> str: ...
    @property
    def value(self) -> str: ...

PaginationStop: TypeAlias = Literal[
    "reached_from",
    "reached_max_bars",
    "max_pages",
    "end_of_history",
    "bar_limit",
    "no_more_data",
    "timeout",
    "connection_closed",
]
"""Why pagination stopped walking back (`HistoricalSeries.pagination_stop`)."""

class SymbolInfo:
    """Symbol metadata returned by TradingView with historical bars."""

    id: str
    """Full TradingView id, e.g. `HOSE:VCB`."""
    name: str
    exchange: str
    listed_exchange: str
    description: str
    market_type: str
    """TradingView instrument type, e.g. `stock`, `spot`, `swap`, `futures`."""
    currency_code: str
    timezone: str
    session: str
    """Trading session in exchange time, e.g. `0900-1500` or `24x7`."""
    pricescale: int
    minmov: int
    minmove2: int
    variable_tick_size: str
    """Price-dependent tick sizes such as `10 10000 50 50000 100`; empty when constant."""
    pointvalue: float
    delay: int
    """Data delay in seconds (0 for real-time)."""
    isin: str

    def __init__(
        self,
        id: str = "",
        name: str = "",
        exchange: str = "",
        *,
        listed_exchange: str = "",
        description: str = "",
        market_type: str = "",
        currency_code: str = "",
        timezone: str = "",
        session: str = "",
        pricescale: int = 0,
        minmov: int = 0,
        minmove2: int = 0,
        variable_tick_size: str = "",
        pointvalue: float = 0.0,
        delay: int = 0,
        isin: str = "",
    ) -> None: ...
    @property
    def tick_size(self) -> float | None:
        """Minimum price increment `minmov / pricescale`, or None when unknown."""
    def tick_size_at(self, price: float) -> float | None:
        """Tick size at `price`, using `variable_tick_size` when it is set."""
    def to_dict(self) -> dict[str, Any]: ...

class Bar:
    """Individual historical OHLCV price bar."""

    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __init__(
        self,
        timestamp: int,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_tuple(self) -> tuple[int, float, float, float, float, float]: ...

class CandleUpdate:
    """Real-time in-flight candle progress or closed bar update."""

    symbol: str
    interval: Interval
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __init__(
        self,
        symbol: str,
        interval: Interval,
        timestamp: int,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_bar(self) -> Bar: ...

class HistoricalSeries:
    """Ordered collection of price bars for a symbol."""

    symbol: str
    exchange: str
    interval: Interval
    bars: list[Bar]
    symbol_info: SymbolInfo | None
    """Symbol metadata from TradingView, when the series came from the server."""
    pages_fetched: int
    """Number of `request_more_data` pages sent (0 without pagination)."""
    pagination_stop: PaginationStop | None
    """Why pagination stopped, or None without pagination."""
    dropped_rows: int
    """Bar rows the server sent that could not be parsed and were dropped."""
    last_bar_complete: bool | None
    """Whether the last bar had closed when the fetch finished, by the server's clock
    and including the feed delay (`symbol_info.delay`) plus 2 s; None when unknown."""

    def __init__(
        self,
        symbol: str,
        exchange: str,
        interval: Interval,
        bars: list[Bar],
        *,
        symbol_info: SymbolInfo | None = None,
        pages_fetched: int = 0,
        pagination_stop: str | None = None,
        dropped_rows: int = 0,
        last_bar_complete: bool | None = None,
    ) -> None: ...
    @property
    def is_complete(self) -> bool:
        """True without pagination, or when pagination stopped at `from_timestamp`,
        `max_bars` or the start of history (`reached_from`, `reached_max_bars`,
        `end_of_history`). False after `bar_limit`, `max_pages`, `no_more_data`,
        `timeout` or `connection_closed`, when older bars may exist."""
    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> Bar: ...
    def __iter__(self) -> Iterator[Bar]: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_polars(self) -> Any: ...
    def to_pandas(self) -> Any: ...

class QuoteTick:
    """Real-time market quote update."""

    symbol: str
    timestamp: int
    price: float
    volume: float
    bid: float | None
    ask: float | None
    change: float | None
    change_percent: float | None

    def __init__(
        self,
        symbol: str,
        timestamp: int,
        price: float,
        volume: float,
        bid: float | None = None,
        ask: float | None = None,
        change: float | None = None,
        change_percent: float | None = None,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

class FundamentalPoint:
    """Individual fundamental indicator point."""

    timestamp: int
    value: float
    index: int

    def __init__(
        self,
        timestamp: int,
        value: float,
        index: int,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

class FundamentalSeries:
    """Time series of fundamental data points."""

    symbol: str
    exchange: str
    fund_id: str
    period: FinancialPeriod
    points: list[FundamentalPoint]

    def __init__(
        self,
        symbol: str,
        exchange: str,
        fund_id: str,
        period: FinancialPeriod,
        points: list[FundamentalPoint],
    ) -> None: ...
    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> FundamentalPoint: ...
    def __iter__(self) -> Iterator[FundamentalPoint]: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_polars(self) -> Any: ...
    def to_pandas(self) -> Any: ...

class EconomicEvent:
    """Scheduled global macroeconomic event."""

    id: str
    title: str
    country: str
    indicator: str
    ticker: str
    date: int
    importance: EconomicImportance
    actual: float | None
    forecast: float | None
    previous: float | None

    def __init__(
        self,
        id: str,
        title: str,
        country: str,
        indicator: str,
        ticker: str,
        date: int,
        importance: EconomicImportance,
        actual: float | None = None,
        forecast: float | None = None,
        previous: float | None = None,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

QuoteCallback: TypeAlias = Callable[[QuoteTick], None]
CandleCallback: TypeAlias = Callable[[CandleUpdate], None]

class QuoteSubscription:
    """Active real-time market quote stream subscription."""
    def __aiter__(self) -> AsyncIterator[QuoteTick]: ...
    async def __anext__(self) -> QuoteTick: ...
    def add_callback(self, callback: QuoteCallback) -> None: ...
    async def stop(self) -> None: ...

class BarSubscription:
    """Active real-time candlestick bar progress subscription."""
    def __aiter__(self) -> AsyncIterator[CandleUpdate]: ...
    async def __anext__(self) -> CandleUpdate: ...
    def add_callback(self, callback: CandleCallback) -> None: ...
    async def stop(self) -> None: ...

class TradingViewClient:
    """Main client interface for accessing TradingView market data.

    Note:
        Connecting anonymously to `DataServer.ProData` is supported for public market data.
        However, accessing paid market data feeds requires account and feed entitlements;
        changing the endpoint to ProData does not grant paid access without entitlements.
        Loading credentials or tokens from `.env` or system environment variables
        is an explicit application concern.
    """

    auth_token: str | None
    username: str | None
    is_authenticated: bool
    @property
    def server(self) -> DataServer: ...
    def __init__(
        self,
        auth_token: str | None = None,
        *,
        server: DataServer = DataServer.Data,
    ) -> None: ...
    @classmethod
    async def login(
        cls,
        username: str,
        password: str,
        totp_secret: str | None = None,
        *,
        captcha_key: str | None = None,
        server: DataServer = DataServer.Data,
    ) -> TradingViewClient:
        """Authenticate with TradingView credentials and return an authenticated client.

        Args:
            username: TradingView username or email.
            password: Account password.
            totp_secret: Optional 2FA TOTP secret, accepting either a standard RFC 6238
                Base32 secret or a full `otpauth://totp/...` URI (e.g. from Bitwarden).
            captcha_key: Optional 2Captcha API key to automatically solve reCAPTCHA challenges.
        """
    async def authenticate(
        self,
        username: str,
        password: str,
        totp_secret: str | None = None,
        *,
        captcha_key: str | None = None,
    ) -> None:
        """Authenticate this client instance using TradingView credentials.

        Args:
            username: TradingView username or email.
            password: Account password.
            totp_secret: Optional 2FA TOTP secret, accepting either a standard RFC 6238
                Base32 secret or a full `otpauth://totp/...` URI (e.g. from Bitwarden).
            captcha_key: Optional 2Captcha API key to automatically solve reCAPTCHA challenges.
        """
    @classmethod
    def from_cookies(
        cls,
        cookies_json: str,
        *,
        server: DataServer = DataServer.Data,
    ) -> TradingViewClient:
        """Build a cookie-session client from JSON produced by `export_cookies()`.

        No network request is made. The returned client can call
        `get_tradingview_token()` without a password login, so a session saved
        after one `login()` can be reused across processes until TradingView
        expires it.

        Only `session` and `session_signature` are required; the other fields
        (`id`, `username`, `private_channel`, `auth_token`, `session_hash`,
        `device_token`) may be omitted. The stored `auth_token` is restored
        as-is and may already have expired; call `get_tradingview_token()` for a
        fresh token.

        Args:
            cookies_json: JSON object as returned by `export_cookies()`.
            server: Data server for the returned client.

        Raises:
            ValueError: If `cookies_json` is not a JSON object or a field has
                the wrong type. The message never includes cookie values.
            AuthenticationError: If `session` or `session_signature` is empty.
        """
    def export_cookies(self) -> str:
        """Export the session cookies as JSON for `TradingViewClient.from_cookies()`.

        The JSON is the serialized Rust `UserCookies`: `id`, `username`,
        `private_channel`, `auth_token`, `session`, `session_signature`,
        `session_hash` and `device_token`. It holds live session secrets, so
        store it with owner-only permissions (for example mode 0600) and keep it
        out of version control.

        Raises:
            AuthenticationError: If the client has no cookie session (anonymous
                or token-only clients).
        """
    async def get_tradingview_token(self) -> str:
        """Retrieve a TradingView session token using authenticated session cookies.

        The client must hold session cookies from `TradingViewClient.login()`,
        `client.authenticate()` or `TradingViewClient.from_cookies()`.

        Raises:
            AuthenticationError: If the client is anonymous or was initialized with only
                an auth_token, or if TradingView rejects the session cookies.
        """
    async def get_historical(
        self,
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
    ) -> Any:
        """Fetch OHLCV bars for one symbol.

        Returns a `HistoricalSeries`, or a Polars DataFrame when `as_dataframe=True`.

        Args:
            n_bars: Bars in the first request (positive). Defaults to 100 without
                pagination and to `page_size` (capped at `max_bars`) with pagination.
            with_replay: Has no effect; kept for compatibility.
            server: Data server override. `DataServer.ProData` serves about twice
                as many bars as the default `DataServer.Data`.
            from_timestamp: Oldest bar wanted, in Unix seconds, not in the future.
                Enables pagination; older bars are dropped from the result.
            max_bars: Bars wanted in total (positive). Enables pagination; only the
                newest `max_bars` bars are returned.
            max_pages: Maximum `request_more_data` pages (0 or more; default 100
                when another keyword enabled pagination). Enables pagination on its own.
            page_size: Bars requested per page (positive).
            page_delay: Seconds to wait before each page (non-negative).
            adjustment: Price adjustment; the server default is `Splits`.
            session_type: Trading session; the server default is `Regular`.
            currency: ISO 4217 code to convert prices to, e.g. `"USD"`.
            allow_partial: Return the bars received so far, instead of raising,
                when a page times out or the connection closes during pagination;
                with `as_dataframe=True`, also return incomplete histories.
            drop_incomplete_last_bar: Drop the newest bar if it had not closed yet (see
                `HistoricalSeries.last_bar_complete`); with `max_bars`, exactly
                `max_bars` closed bars are returned.

        Pagination stops at `from_timestamp` or `max_bars`, when the server reports
        the start of history or its bar limit, when a page adds no older bar, or
        after `max_pages` pages. See `HistoricalSeries.pagination_stop` and
        `HistoricalSeries.is_complete`.

        Raises:
            ValueError: Invalid keywords. Raised when the method is called, before
                the coroutine is awaited.
            TimeoutError: The initial load or, without `allow_partial`, a page timed out.
            ConnectionError: The connection failed or, without `allow_partial`,
                closed during pagination.
            IncompleteHistoryError: `as_dataframe=True` and the history is
                incomplete (`is_complete` would be False), without `allow_partial`.
        """
    async def get_historical_df(
        self,
        symbol: str,
        exchange: str,
        interval: Interval = Interval.OneDay,
        n_bars: int | None = None,
        with_replay: bool = False,
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
    ) -> Any:
        """`get_historical(..., as_dataframe=True)`: return a Polars DataFrame.

        Raises `IncompleteHistoryError` for an incomplete history unless
        `allow_partial=True`.
        """
    async def get_historical_batch(
        self,
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
    ) -> HistoricalBatch:
        """Fetch several `(symbol, exchange)` pairs.

        Returns a `HistoricalBatch`, a dict keyed by `"EXCHANGE:SYMBOL"` whose
        `failed` attribute maps every symbol left out to its error message
        (including, with `as_dataframe=True` and without `allow_partial`,
        incomplete histories). Takes the keywords of `get_historical`; invalid
        keywords raise `ValueError` when the method is called. At most
        `max_concurrency` WebSocket connections are opened; each fetches its share
        of the symbols one after another, and a symbol whose connection failed is
        retried once on a new connection.
        """
    async def subscribe_quotes(
        self,
        symbols: Sequence[str],
        callback: QuoteCallback | None = None,
        *,
        server: DataServer | None = None,
    ) -> QuoteSubscription: ...
    async def subscribe_bars(
        self,
        symbols: Sequence[str],
        interval: Interval = Interval.OneMinute,
        callback: CandleCallback | None = None,
        *,
        server: DataServer | None = None,
    ) -> BarSubscription: ...
    async def get_fundamental(
        self,
        symbol: str,
        exchange: str,
        fund_id: str,
        period: FinancialPeriod = FinancialPeriod.FiscalYear,
        n_bars: int = 20,
        as_dataframe: bool = False,
        *,
        server: DataServer | None = None,
    ) -> Any: ...
    async def get_economic_calendar(
        self,
        countries: Sequence[str] | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        min_importance: EconomicImportance = EconomicImportance.Medium,
        as_dataframe: bool = False,
    ) -> Any: ...
    async def close(self) -> None: ...

__all__ = [
    "AuthenticationError",
    "Bar",
    "BarSubscription",
    "CandleUpdate",
    "ConnectionError",
    "DataServer",
    "EconomicEvent",
    "EconomicImportance",
    "FinancialPeriod",
    "FundamentalPoint",
    "FundamentalSeries",
    "HistoricalBatch",
    "HistoricalSeries",
    "IncompleteHistoryError",
    "Interval",
    "MarketAdjustment",
    "PaginationStop",
    "ProtocolError",
    "QuoteSubscription",
    "QuoteTick",
    "RateLimitError",
    "SessionType",
    "SymbolInfo",
    "SymbolNotFoundError",
    "TimeoutError",
    "TradingViewClient",
    "TradingViewError",
]
