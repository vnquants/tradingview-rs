# How far back historical data goes

`HistoricalRequest` can page back with TradingView's `request_more_data`
(see `from_timestamp`, `max_bars` and `max_pages`). This page records what the
server returned on 2026-11-19 (UTC), so callers know what pagination can and
cannot reach.

## Summary

- The depth is capped by the **data server**, not by pagination and not by the
  session: anonymous and token sessions returned identical results on each
  server. `DataServer::Data`, `WidgetData` and `MobileData` stopped after
  about 5,000 bars; `DataServer::ProData` after about 10,000. The server then
  extends the window back to an internal block boundary (in these runs the
  start of a week for 1-minute bars, of a month for 15-minute bars and of a
  year for 1-hour bars), which is why counts exceed 5,000 or 10,000.
- At the cap, `series_completed` carries `{"data_completed": "limit"}` and
  further `request_more_data` calls return no bars. At the start of a symbol's
  history it carries `{"data_completed": "end"}` instead (BINANCE:BTCUSDT
  daily bars reached 2017-08-17 this way).
- One `create_series` with a large bar count (30,000) returns the same bars as
  pagination, in one response. Pagination is useful to stop at a start time or
  bar count and to keep each response small, not to get past the cap.
- A range request (`r,from:to`) older than the cap does not help either: the
  server returns the newest bars up to the cap.
- The token used belongs to one account; a plan with a higher bar allowance was
  not tested.

## Measured depth

`cargo run -p tradingview-rs --example history_depth` with `page_size` 5,000
(the default), the initial request also 5,000 bars, `page_delay` 1 s and all
symbols of an interval fetched over one connection. "Pages" counts
`request_more_data` calls; the last one returned the remaining bars together
with `data_completed: "limit"`. Every run stopped with `bar_limit`. The newest
bar was 2026-11-19 18:00 UTC for the crypto pairs and 07:45 UTC (14:45 local)
for HOSE:VCB.

### `DataServer::Data` (anonymous; a token session gave the same counts)

| Symbol | Interval | Bars | Oldest bar (UTC) | Span | Pages |
| --- | --- | ---: | --- | --- | ---: |
| BINANCE:BTCUSDT | 1m | 5,401 | 2026-11-16 00:00 | 3.7 days | 1 |
| OKX:BTCUSDT.P | 1m | 5,401 | 2026-11-16 00:00 | 3.7 days | 1 |
| HOSE:VCB | 1m | 5,414 | 2026-10-19 02:15 | 1 month | 1 |
| BINANCE:BTCUSDT | 15m | 7,657 | 2026-09-01 00:00 | 2.6 months | 1 |
| OKX:BTCUSDT.P | 15m | 7,657 | 2026-09-01 00:00 | 2.6 months | 1 |
| HOSE:VCB | 15m | 5,156 | 2025-08-01 02:15 | 15.6 months | 1 |
| BINANCE:BTCUSDT | 1h | 7,747 | 2026-01-01 00:00 | 10.6 months | 1 |
| OKX:BTCUSDT.P | 1h | 7,747 | 2026-01-01 00:00 | 10.6 months | 1 |
| HOSE:VCB | 1h | 6,060 | 2022-01-04 02:00 | 4.9 years | 1 |

`WidgetData` and `MobileData` returned the same 7,747 bars for 1-hour
BINANCE:BTCUSDT.

### `DataServer::ProData` (token session; anonymous gave the same counts)

| Symbol | Interval | Bars | Oldest bar (UTC) | Span | Pages |
| --- | --- | ---: | --- | --- | ---: |
| BINANCE:BTCUSDT | 1m | 15,482 | 2026-11-09 00:00 | 10.7 days | 3 |
| OKX:BTCUSDT.P | 1m | 15,482 | 2026-11-09 00:00 | 10.7 days | 3 |
| HOSE:VCB | 1m | 11,040 | 2026-09-14 02:15 | 2.2 months | 2 |
| BINANCE:BTCUSDT | 15m | 10,633 | 2026-08-01 00:00 | 3.6 months | 2 |
| OKX:BTCUSDT.P | 15m | 10,633 | 2026-08-01 00:00 | 3.6 months | 2 |
| HOSE:VCB | 15m | 10,220 | 2024-05-02 02:15 | 2.5 years | 2 |
| BINANCE:BTCUSDT | 1h | 16,507 | 2025-01-01 00:00 | 22.6 months | 3 |
| OKX:BTCUSDT.P | 1h | 16,507 | 2025-01-01 00:00 | 22.6 months | 3 |
| HOSE:VCB | 1h | 11,066 | 2018-01-02 02:00 | 8.9 years | 2 |

## Protocol observed

```text
→ request_more_data ["cs_x", "sds_1", 5000]
← series_loading    ["cs_x", "sds_1", "s1"]
← timescale_update  ["cs_x", {"sds_1": {"s": [older bars, indices -5000..-1], ...}}, {...}]
← series_completed  ["cs_x", "sds_1", "streaming", "s1", {"rt_update_period": 1}]
```

At the cap the page has no `timescale_update` and the last element of
`series_completed` is `{"data_completed": "limit", "rt_update_period": 1}`.
Real-time `du` updates for the newest bar can arrive between pages; the client
keeps the latest value per timestamp.

## Adjustment, session and currency

Checked on the same day with `resolve_symbol` keys:

- `adjustment`: HOSE:VCB daily bars were identical with no key and with
  `"splits"` (the server default). `"dividends"` also back-adjusted for cash
  dividends (old bars up to 11.4 % below the split-adjusted price). `"none"`
  returned raw prices (the 2014-10-29 close was 26,300 VND unadjusted and
  8,647 split-adjusted), so `MarketAdjustment::Unadjusted` is supported. Crypto pairs report
  `allowed_adjustment: "none"` and are not adjusted.
- `session`: NASDAQ:AAPL 1-hour bars with no key and with `"regular"` covered
  13:00-20:00 UTC; `"extended"` added pre- and post-market hours;
  `"premarket"` and `"postmarket"` returned only those parts.
- `currency-id`: `"USD"` converted HOSE:VCB to dollars (about 2.0) and
  `symbol_resolved` reported `currency_code: "USD"`, `pricescale: 100`.

## Reproduce

```sh
# anonymous, DataServer::Data (default)
cargo run -p tradingview-rs --example history_depth --locked
# other servers, a token, other symbols or intervals
TV_SERVER=prodata TV_AUTH_TOKEN=... TV_INTERVALS=1,15,60 \
  cargo run -p tradingview-rs --example history_depth --locked -- HOSE:VCB
```

The example waits `TV_PAGE_DELAY_MS` (default 1,000 ms) before each page and
2 s between intervals, and never logs the token.
