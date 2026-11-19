"""Tests for paginated historical retrieval, adjustment, session, currency and symbol metadata."""

import inspect
import math
import time
from collections.abc import Callable
from typing import Any, get_args

import pytest
import tradingview
from tradingview import (
    Bar,
    HistoricalBatch,
    HistoricalSeries,
    IncompleteHistoryError,
    Interval,
    MarketAdjustment,
    PaginationStop,
    SessionType,
    SymbolInfo,
    TradingViewClient,
    TradingViewError,
)

HISTORY_KEYWORDS = {
    "server": None,
    "from_timestamp": None,
    "max_bars": None,
    "max_pages": None,
    "page_size": 5000,
    "page_delay": 0.5,
    "adjustment": None,
    "session_type": None,
    "currency": None,
    "allow_partial": False,
    "drop_incomplete_last_bar": False,
}


# ---------------------------------------------------------------------------
# Offline
# ---------------------------------------------------------------------------


def test_market_adjustment_and_session_type_values() -> None:
    assert MarketAdjustment.Splits.value == "splits"
    assert MarketAdjustment.Dividends.value == "dividends"
    assert MarketAdjustment.Unadjusted.value == "none"
    assert MarketAdjustment.Unadjusted.name == "Unadjusted"
    assert SessionType.Regular.value == "regular"
    assert SessionType.Extended.value == "extended"
    assert SessionType.PreMarket.value == "premarket"
    assert SessionType.PostMarket.value == "postmarket"


def test_native_enums_behave_as_stubbed() -> None:
    # Stubbed as plain classes with ClassVar members, `name` and `value`.
    assert MarketAdjustment.Splits == MarketAdjustment.Splits
    assert MarketAdjustment.Splits != MarketAdjustment.Dividends
    assert isinstance(MarketAdjustment.Unadjusted, MarketAdjustment)
    assert str(MarketAdjustment.Unadjusted) == "none"
    assert SessionType.Extended.name == "Extended"
    assert isinstance(SessionType.PreMarket, SessionType)


def test_pagination_stop_is_exported_at_runtime() -> None:
    assert "PaginationStop" in tradingview.__all__
    assert set(get_args(PaginationStop)) == {
        "reached_from",
        "reached_max_bars",
        "max_pages",
        "end_of_history",
        "bar_limit",
        "no_more_data",
        "timeout",
        "connection_closed",
    }


def test_incomplete_history_error_hierarchy() -> None:
    from tradingview.exceptions import IncompleteHistoryError as Exported

    assert Exported is IncompleteHistoryError
    assert issubclass(IncompleteHistoryError, TradingViewError)


def test_historical_batch_is_a_dict_with_failures() -> None:
    series = HistoricalSeries("BTCUSDT", "BINANCE", Interval.OneDay, [])
    batch = HistoricalBatch(
        {"BINANCE:BTCUSDT": series}, failed={"HOSE:BAD": "SymbolNotFoundError: x"}
    )
    assert isinstance(batch, dict)
    assert list(batch) == ["BINANCE:BTCUSDT"]
    assert batch["BINANCE:BTCUSDT"] is series
    assert batch.failed == {"HOSE:BAD": "SymbolNotFoundError: x"}
    assert "failed=" in repr(batch)
    assert HistoricalBatch().failed == {}


@pytest.mark.parametrize(
    ("stop", "complete"),
    [
        (None, True),
        ("reached_from", True),
        ("reached_max_bars", True),
        ("end_of_history", True),
        ("bar_limit", False),
        ("max_pages", False),
        ("no_more_data", False),
        ("timeout", False),
        ("connection_closed", False),
    ],
)
def test_historical_series_is_complete(stop: str | None, complete: bool) -> None:
    series = HistoricalSeries(
        "BTCUSDT", "BINANCE", Interval.OneDay, [], pagination_stop=stop
    )
    assert series.is_complete is complete


def test_symbol_info_tick_size_variable() -> None:
    info = SymbolInfo(
        "HOSE:VCB",
        "VCB",
        "HOSE",
        pricescale=1,
        minmov=100,
        variable_tick_size="10 10000 50 50000 100",
        delay=900,
        isin="VN000000VCB4",
        session="0900-1500",
        timezone="Asia/Ho_Chi_Minh",
    )
    assert info.tick_size == 100.0
    assert info.tick_size_at(9_990.0) == 10.0
    assert info.tick_size_at(10_000.0) == 50.0
    assert info.tick_size_at(65_700.0) == 100.0
    d = info.to_dict()
    assert d["id"] == "HOSE:VCB"
    assert d["tick_size"] == 100.0
    assert d["delay"] == 900
    assert d["isin"] == "VN000000VCB4"
    assert repr(info).startswith("SymbolInfo(id='HOSE:VCB'")
    assert "tick_size=100.0" in repr(info)


def test_symbol_info_tick_size_constant_and_unknown() -> None:
    crypto = SymbolInfo(pricescale=100, minmov=1)
    assert crypto.tick_size == pytest.approx(0.01)
    assert crypto.tick_size_at(91_000.0) == pytest.approx(0.01)
    assert SymbolInfo().tick_size is None
    assert SymbolInfo().tick_size_at(1.0) is None


def test_historical_series_new_fields_default_and_kwargs() -> None:
    series = HistoricalSeries("AAPL", "NASDAQ", Interval.OneDay, [])
    assert series.symbol_info is None
    assert series.pages_fetched == 0
    assert series.pagination_stop is None
    assert series.dropped_rows == 0
    assert series.last_bar_complete is None

    info = SymbolInfo("NASDAQ:AAPL", "AAPL", "NASDAQ", pricescale=100, minmov=1)
    bar = Bar(1700000000, 1.0, 2.0, 0.5, 1.5, 10.0)
    series = HistoricalSeries(
        "AAPL",
        "NASDAQ",
        Interval.OneDay,
        [bar],
        symbol_info=info,
        pages_fetched=3,
        pagination_stop="bar_limit",
        dropped_rows=2,
        last_bar_complete=False,
    )
    assert series.symbol_info == info
    assert series.pages_fetched == 3
    assert series.pagination_stop == "bar_limit"
    assert series.dropped_rows == 2
    assert series.last_bar_complete is False
    assert series.is_complete is False
    assert len(series) == 1


@pytest.mark.parametrize(
    "method", ["get_historical", "get_historical_df", "get_historical_batch"]
)
def test_history_keywords_are_keyword_only_with_defaults(method: str) -> None:
    params = inspect.signature(getattr(TradingViewClient, method)).parameters
    for name, default in HISTORY_KEYWORDS.items():
        assert name in params, f"{method} lacks {name}"
        assert params[name].kind is inspect.Parameter.KEYWORD_ONLY
        if default is not None:
            assert params[name].default == default


def test_n_bars_stays_positional() -> None:
    params = list(
        inspect.signature(TradingViewClient.get_historical).parameters.values()
    )
    names = [
        p.name for p in params if p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]
    assert names == [
        "symbol",
        "exchange",
        "interval",
        "n_bars",
        "with_replay",
        "as_dataframe",
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"page_size": 0},
        {"page_size": -5},
        {"max_bars": 0},
        {"max_bars": -1},
        {"max_pages": -1},
        {"n_bars": 0},
        {"n_bars": -10},
        {"page_delay": -1.0},
        {"page_delay": math.nan},
        {"currency": "NOT_A_CURRENCY"},
        {"from_timestamp": int(time.time()) + 86_400},
        {"from_timestamp": 1_735_689_600_000},
    ],
)
@pytest.mark.asyncio
async def test_invalid_history_keywords_raise_value_error(
    kwargs: dict[str, object],
) -> None:
    # Validation happens before any connection is opened.
    client = TradingViewClient()
    with pytest.raises(ValueError):
        await client.get_historical("BTCUSDT", "BINANCE", Interval.OneDay, **kwargs)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await client.get_historical_batch([("BTCUSDT", "BINANCE")], **kwargs)  # type: ignore[arg-type]


def test_validation_happens_at_call_time_and_names_the_problem() -> None:
    # The methods raise before returning a coroutine, so no await is needed.
    client = TradingViewClient()
    get_historical: Callable[..., object] = client.get_historical
    get_historical_df: Callable[..., object] = client.get_historical_df
    get_historical_batch: Callable[..., object] = client.get_historical_batch
    with pytest.raises(ValueError, match="milliseconds"):
        get_historical("BTCUSDT", "BINANCE", from_timestamp=1_735_689_600_000)
    with pytest.raises(ValueError, match="future"):
        get_historical("BTCUSDT", "BINANCE", from_timestamp=int(time.time()) + 3_600)
    with pytest.raises(ValueError, match="page_size"):
        get_historical_df("BTCUSDT", "BINANCE", page_size=-1)
    with pytest.raises(ValueError, match="max_concurrency"):
        get_historical_batch([("BTCUSDT", "BINANCE")], max_concurrency=-1)


# ---------------------------------------------------------------------------
# Live (TradingView network, anonymous)
# ---------------------------------------------------------------------------


@pytest.mark.network
@pytest.mark.asyncio
async def test_get_historical_paginates_to_max_bars() -> None:
    client = TradingViewClient()
    series = await client.get_historical(
        "BTCUSDT",
        "BINANCE",
        Interval.OneDay,
        n_bars=10,
        max_bars=25,
        page_size=10,
        page_delay=0.2,
    )
    assert len(series) == 25
    assert series.pages_fetched == 2
    assert series.pagination_stop == "reached_max_bars"
    timestamps = [bar.timestamp for bar in series]
    assert timestamps == sorted(set(timestamps))

    info = series.symbol_info
    assert info is not None
    assert info.id == "BINANCE:BTCUSDT"
    assert info.pricescale == 100
    assert info.tick_size == pytest.approx(0.01)
    assert info.session == "24x7"
    assert info.timezone == "Etc/UTC"
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_get_historical_from_timestamp_trims_older_bars() -> None:
    client = TradingViewClient()
    from_timestamp = int(time.time()) - 400 * 86_400
    series = await client.get_historical(
        "BTCUSDT",
        "BINANCE",
        Interval.OneDay,
        from_timestamp=from_timestamp,
        page_delay=0.2,
    )
    assert series.pagination_stop == "reached_from"
    assert series[0].timestamp >= from_timestamp
    assert series[0].timestamp - from_timestamp < 2 * 86_400
    assert 395 <= len(series) <= 402
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_get_historical_unadjusted_differs_from_default() -> None:
    client = TradingViewClient()
    default = await client.get_historical("VCB", "HOSE", Interval.OneDay, n_bars=3000)
    raw = await client.get_historical(
        "VCB",
        "HOSE",
        Interval.OneDay,
        n_bars=3000,
        adjustment=MarketAdjustment.Unadjusted,
    )
    assert default[0].timestamp == raw[0].timestamp
    # VCB paid stock dividends since 2014, so split-adjusted old prices are lower.
    assert raw[0].close > default[0].close
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_get_historical_currency_conversion() -> None:
    client = TradingViewClient()
    series = await client.get_historical(
        "VCB", "HOSE", Interval.OneDay, n_bars=5, currency="usd"
    )
    assert series.symbol_info is not None
    assert series.symbol_info.currency_code == "USD"
    assert 0 < series[-1].close < 100  # VND ~50,000 per share, USD ~2
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_get_historical_batch_with_pagination_keywords() -> None:
    client = TradingViewClient()
    batch = await client.get_historical_batch(
        [("BTCUSDT", "BINANCE"), ("BTCUSDT.P", "OKX")],
        interval=Interval.OneDay,
        n_bars=10,
        max_concurrency=1,
        max_bars=15,
        page_size=10,
        page_delay=0.2,
        session_type=SessionType.Regular,
    )
    assert isinstance(batch, HistoricalBatch)
    assert batch.failed == {}
    assert set(batch) == {"BINANCE:BTCUSDT", "OKX:BTCUSDT.P"}
    for series in batch.values():
        assert len(series) == 15
        assert series.pages_fetched == 1
        assert series.pagination_stop == "reached_max_bars"
        assert series.symbol_info is not None
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_dataframe_path_refuses_incomplete_history_unless_allowed() -> None:
    import polars as pl

    client = TradingViewClient()
    kwargs: dict[str, Any] = {
        "n_bars": 10,
        "max_pages": 1,
        "page_size": 10,
        "page_delay": 0.2,
    }
    # One page of 10 older bars, then max_pages: older bars still exist.
    with pytest.raises(IncompleteHistoryError, match="max_pages"):
        await client.get_historical_df("BTCUSDT", "BINANCE", Interval.OneDay, **kwargs)
    df = await client.get_historical_df(
        "BTCUSDT", "BINANCE", Interval.OneDay, allow_partial=True, **kwargs
    )
    assert isinstance(df, pl.DataFrame)
    assert df.height == 20
    series = await client.get_historical(
        "BTCUSDT", "BINANCE", Interval.OneDay, **kwargs
    )
    assert series.pagination_stop == "max_pages"
    assert series.is_complete is False
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_batch_exposes_failed_symbols() -> None:
    client = TradingViewClient()
    batch = await client.get_historical_batch(
        [("BTCUSDT", "BINANCE"), ("NOSUCHSYMBOL123", "BINANCE")],
        interval=Interval.OneDay,
        n_bars=5,
        max_concurrency=1,
    )
    assert isinstance(batch, HistoricalBatch)
    assert set(batch) == {"BINANCE:BTCUSDT"}
    assert set(batch.failed) == {"BINANCE:NOSUCHSYMBOL123"}
    assert batch.failed["BINANCE:NOSUCHSYMBOL123"].startswith("SymbolNotFoundError")
    await client.close()


@pytest.mark.network
@pytest.mark.asyncio
async def test_drop_incomplete_last_bar() -> None:
    client = TradingViewClient()
    series = await client.get_historical(
        "BTCUSDT",
        "BINANCE",
        Interval.OneMinute,
        n_bars=5,
        drop_incomplete_last_bar=True,
    )
    assert len(series) in (4, 5)
    assert series.last_bar_complete in (True, None)
    # The newest returned bar closed (opened at least a minute ago).
    assert series[-1].timestamp + 60 <= time.time() + 1
    await client.close()
