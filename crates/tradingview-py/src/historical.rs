//! Keyword validation and result conversion shared by the historical methods.

use std::collections::HashMap;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tradingview::Currency;
use tradingview::chart::OHLCV;
use tradingview::historical::{BatchResult, HistoricalRequest, HistoricalResult};

use crate::PyObject;
use crate::errors::{IncompleteHistoryError, to_py_err};
use crate::models::bar::{Bar, HistoricalSeries};
use crate::models::enums::{Interval, MarketAdjustment, SessionType};
use crate::models::symbol::SymbolInfo;

/// Validated keywords of `get_historical`, `get_historical_df` and
/// `get_historical_batch`. Validation runs when the method is called, before
/// a coroutine is returned, so errors surface without awaiting.
#[derive(Debug, Clone)]
pub(crate) struct HistoryOptions {
    pub n_bars: Option<u64>,
    pub from_timestamp: Option<i64>,
    pub max_bars: Option<u64>,
    pub max_pages: Option<u32>,
    pub page_size: u64,
    pub page_delay: Duration,
    pub adjustment: Option<MarketAdjustment>,
    pub session_type: Option<SessionType>,
    pub currency: Option<Currency>,
    pub allow_partial: bool,
    pub drop_incomplete_last_bar: bool,
}

fn positive(name: &str, value: i64) -> PyResult<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            PyValueError::new_err(format!("{name} must be a positive integer, got {value}"))
        })
}

impl HistoryOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn parse(
        n_bars: Option<i64>,
        from_timestamp: Option<i64>,
        max_bars: Option<i64>,
        max_pages: Option<i64>,
        page_size: i64,
        page_delay: f64,
        adjustment: Option<MarketAdjustment>,
        session_type: Option<SessionType>,
        currency: Option<String>,
        allow_partial: bool,
        drop_incomplete_last_bar: bool,
    ) -> PyResult<Self> {
        let n_bars = n_bars.map(|n| positive("n_bars", n)).transpose()?;
        let max_bars = max_bars.map(|n| positive("max_bars", n)).transpose()?;
        let page_size = positive("page_size", page_size)?;
        let max_pages = max_pages
            .map(|pages| {
                u32::try_from(pages).map_err(|_| {
                    PyValueError::new_err(format!(
                        "max_pages must be an integer from 0 to {}, got {pages}",
                        u32::MAX
                    ))
                })
            })
            .transpose()?;
        if let Some(from) = from_timestamp {
            HistoricalRequest::check_from_timestamp(from)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        let page_delay = Duration::try_from_secs_f64(page_delay).map_err(|_| {
            PyValueError::new_err("page_delay must be a finite, non-negative number of seconds")
        })?;
        let currency = currency
            .map(|code| {
                Currency::from_code(&code.trim().to_ascii_uppercase()).ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "currency must be an ISO 4217 code such as 'USD', got {code:?}"
                    ))
                })
            })
            .transpose()?;
        Ok(Self {
            n_bars,
            from_timestamp,
            max_bars,
            max_pages,
            page_size,
            page_delay,
            adjustment,
            session_type,
            currency,
            allow_partial,
            drop_incomplete_last_bar,
        })
    }

    /// Request template without a symbol (see `HistoricalRequest::for_symbol`).
    pub fn request(&self, interval: Interval, with_replay: bool) -> HistoricalRequest {
        HistoricalRequest::builder()
            .interval(interval.into())
            .maybe_num_bars(self.n_bars)
            .with_replay(with_replay)
            .maybe_from_timestamp(self.from_timestamp)
            .maybe_max_bars(self.max_bars)
            .maybe_max_pages(self.max_pages)
            .page_size(self.page_size)
            .page_delay(self.page_delay)
            .maybe_adjustment(self.adjustment.map(Into::into))
            .maybe_session_type(self.session_type.map(Into::into))
            .maybe_currency(self.currency)
            .allow_partial(self.allow_partial)
            .drop_incomplete_last_bar(self.drop_incomplete_last_bar)
            .build()
    }
}

/// Convert a core result into the Python series.
pub(crate) fn series_from_result(
    symbol: String,
    exchange: String,
    interval: Interval,
    result: &HistoricalResult,
) -> HistoricalSeries {
    let bars = result
        .data
        .iter()
        .map(|dp| Bar {
            timestamp: dp.timestamp(),
            open: dp.open(),
            high: dp.high(),
            low: dp.low(),
            close: dp.close(),
            volume: dp.volume(),
        })
        .collect();
    HistoricalSeries {
        symbol,
        exchange,
        interval,
        bars,
        symbol_info: Some(SymbolInfo::from(&result.symbol_info)),
        pages_fetched: result.pages_fetched,
        pagination_stop: result.pagination_stop.map(|stop| stop.as_str().to_string()),
        dropped_rows: result.dropped_rows,
        last_bar_complete: result.last_bar_complete,
    }
}

/// Why a DataFrame result would hide an incomplete history, or `None` when it
/// is complete. A DataFrame cannot carry `pagination_stop`, so without
/// `allow_partial` the DataFrame path refuses incomplete results.
pub(crate) fn incomplete_reason(id: &str, result: &HistoricalResult) -> Option<String> {
    if result.is_complete() {
        return None;
    }
    let stop = result
        .pagination_stop
        .map_or("unknown", |stop| stop.as_str());
    let oldest = result
        .first_datetime()
        .map_or_else(|| "none".to_string(), |t| t.to_rfc3339());
    Some(format!(
        "incomplete history for {id}: pagination stopped with {stop} after {} bars \
         (oldest {oldest}); pass allow_partial=True to accept it",
        result.len()
    ))
}

/// Raise `IncompleteHistoryError` for an incomplete DataFrame result.
pub(crate) fn require_complete(id: &str, result: &HistoricalResult) -> PyResult<()> {
    match incomplete_reason(id, result) {
        Some(reason) => Err(IncompleteHistoryError::new_err(reason)),
        None => Ok(()),
    }
}

/// Build the `HistoricalBatch` mapping returned by `get_historical_batch`:
/// results keyed by `"EXCHANGE:SYMBOL"` plus `.failed`, the error message of
/// every symbol that failed (or, for DataFrames without `allow_partial`, came
/// back incomplete).
pub(crate) fn batch_to_python(
    py: Python<'_>,
    batch: BatchResult,
    interval: Interval,
    as_dataframe: bool,
    allow_partial: bool,
) -> PyResult<PyObject> {
    let results = PyDict::new(py);
    let mut failed: HashMap<String, String> = HashMap::new();
    for done in batch.successful.into_iter().chain(batch.failed) {
        let id = format!("{}:{}", done.exchange, done.symbol);
        let result = match done.result {
            Ok(result) => result,
            Err(error) => {
                let error = to_py_err(error);
                let kind = error.get_type(py).name()?;
                failed.insert(id, format!("{kind}: {}", error.value(py)));
                continue;
            }
        };
        if as_dataframe
            && !allow_partial
            && let Some(reason) = incomplete_reason(&id, &result)
        {
            failed.insert(id, reason);
            continue;
        }
        let series = series_from_result(done.symbol, done.exchange, interval, &result);
        if as_dataframe {
            results.set_item(&id, series.to_polars(py)?)?;
        } else {
            results.set_item(&id, series.into_pyobject(py)?)?;
        }
    }
    let batch_class = py
        .import("tradingview._batch")?
        .getattr("HistoricalBatch")?;
    Ok(batch_class.call1((results, failed))?.unbind())
}
