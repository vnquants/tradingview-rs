"""Domain-specific exception hierarchy for TradingView Python API."""

class TradingViewError(Exception):
    """Base exception for all TradingView client errors."""

class AuthenticationError(TradingViewError):
    """Raised when authentication credentials or token are invalid, expired, or rejected."""

class SymbolNotFoundError(TradingViewError):
    """Raised when a requested ticker symbol or exchange cannot be resolved or found."""

class ConnectionError(TradingViewError):
    """Raised when network transport, I/O, or WebSocket connection fails unexpectedly."""

class TimeoutError(TradingViewError):
    """Raised when a request or connection attempt exceeds the deadline."""

class RateLimitError(TradingViewError):
    """Raised when upstream TradingView rate limits or throttling are encountered."""

class ProtocolError(TradingViewError):
    """Raised when unexpected or unparseable frames are received from upstream."""

class IncompleteHistoryError(TradingViewError):
    """Raised when a DataFrame result would hide an incomplete history.

    A Polars DataFrame cannot carry `pagination_stop`, so `get_historical(...,
    as_dataframe=True)` and `get_historical_df` raise this when pagination stopped
    before `from_timestamp`, `max_bars` or the start of history was reached (for
    example at the data server's bar limit), unless `allow_partial=True`.
    """

__all__ = [
    "AuthenticationError",
    "ConnectionError",
    "IncompleteHistoryError",
    "ProtocolError",
    "RateLimitError",
    "SymbolNotFoundError",
    "TimeoutError",
    "TradingViewError",
]
