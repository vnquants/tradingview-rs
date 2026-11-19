"""Tests for paginated historical retrieval, adjustment, session, currency and symbol metadata."""

import inspect
import math
import time

import pytest
from tradingview import (
    Bar,
    HistoricalSeries,
    Interval,
    MarketAdjustment,
    SessionType,
    SymbolInfo,
    TradingViewClient,
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
    )
    assert series.symbol_info == info
    assert series.pages_fetched == 3
    assert series.pagination_stop == "bar_limit"
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
        {"max_bars": 0},
        {"page_delay": -1.0},
        {"page_delay": math.nan},
        {"currency": "NOT_A_CURRENCY"},
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


# ---------------------------------------------------------------------------
# Live (TradingView network, anonymous)
# ---------------------------------------------------------------------------


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
    assert set(batch) == {"BINANCE:BTCUSDT", "OKX:BTCUSDT.P"}
    for series in batch.values():
        assert len(series) == 15
        assert series.pages_fetched == 1
        assert series.pagination_stop == "reached_max_bars"
        assert series.symbol_info is not None
    await client.close()
