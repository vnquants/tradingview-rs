"""TradingView Data Provider Python API backed by tradingview-rs."""

from typing import Literal

from tradingview._batch import HistoricalBatch

PaginationStop = Literal[
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

# Import native extension
try:
    from tradingview import _tradingview as _tv
except ImportError:
    try:
        import _tradingview as _tv  # type: ignore[no-redef]
    except ImportError:
        _tv = None  # type: ignore[assignment]

if _tv is not None:
    _symbols = [
        "TradingViewError",
        "AuthenticationError",
        "SymbolNotFoundError",
        "ConnectionError",
        "TimeoutError",
        "RateLimitError",
        "ProtocolError",
        "IncompleteHistoryError",
        "Interval",
        "FinancialPeriod",
        "EconomicImportance",
        "DataServer",
        "MarketAdjustment",
        "SessionType",
        "Bar",
        "CandleUpdate",
        "HistoricalSeries",
        "SymbolInfo",
        "QuoteTick",
        "FundamentalPoint",
        "FundamentalSeries",
        "EconomicEvent",
        "QuoteSubscription",
        "BarSubscription",
        "TradingViewClient",
    ]
    for _sym in _symbols:
        if hasattr(_tv, _sym):
            globals()[_sym] = getattr(_tv, _sym)

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
