"""Domain-specific exception hierarchy for TradingView Python API."""

from tradingview import (
    AuthenticationError,
    ConnectionError,
    IncompleteHistoryError,
    ProtocolError,
    RateLimitError,
    SymbolNotFoundError,
    TimeoutError,
    TradingViewError,
)

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
