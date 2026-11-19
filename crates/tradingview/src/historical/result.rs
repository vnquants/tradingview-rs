use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::chart::OHLCV;
use crate::websocket::SeriesInfo;
use crate::{DataPoint, SymbolInfo};

/// Why pagination stopped walking back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PaginationStop {
    /// The oldest bar is at or before the requested `from_timestamp`.
    ReachedFrom,
    /// At least `max_bars` distinct bars were received.
    ReachedMaxBars,
    /// `max_pages` pages were requested.
    MaxPages,
    /// The server reported the start of the symbol's history
    /// (`data_completed: "end"`).
    EndOfHistory,
    /// The server reported the data server's bar limit
    /// (`data_completed: "limit"`). The limit belongs to the data server
    /// (`DataServer::ProData` serves about twice as many bars as
    /// `DataServer::Data`), not to the session.
    BarLimit,
    /// A page completed without adding an older bar.
    NoMoreData,
    /// With `allow_partial`: a page did not complete within the request
    /// timeout; the bars received until then are returned.
    Timeout,
    /// With `allow_partial`: the connection closed during pagination; the bars
    /// received until then are returned.
    ConnectionClosed,
}

impl PaginationStop {
    /// Stable snake_case name, e.g. `"bar_limit"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReachedFrom => "reached_from",
            Self::ReachedMaxBars => "reached_max_bars",
            Self::MaxPages => "max_pages",
            Self::EndOfHistory => "end_of_history",
            Self::BarLimit => "bar_limit",
            Self::NoMoreData => "no_more_data",
            Self::Timeout => "timeout",
            Self::ConnectionClosed => "connection_closed",
        }
    }

    /// Whether this stop means the request's target was met or the start of
    /// the symbol's history was reached.
    pub fn is_complete(&self) -> bool {
        matches!(
            self,
            Self::ReachedFrom | Self::ReachedMaxBars | Self::EndOfHistory
        )
    }
}

impl std::fmt::Display for PaginationStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The final result of a historical data retrieval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalResult {
    pub symbol_info: SymbolInfo,
    pub data: Vec<DataPoint>,
    pub series_info: Option<SeriesInfo>,
    pub total_bars_received: usize,
    pub replay_used: bool,
    pub elapsed: std::time::Duration,
    /// Number of `request_more_data` pages sent (0 without pagination).
    #[serde(default)]
    pub pages_fetched: u32,
    /// Why pagination stopped; `None` when pagination was not requested.
    #[serde(default)]
    pub pagination_stop: Option<PaginationStop>,
    /// Bar rows the server sent that could not be parsed and were dropped
    /// (they are still counted in `total_bars_received`).
    #[serde(default)]
    pub dropped_rows: usize,
    /// Close time (Unix seconds) the server reported for its newest bar
    /// (`lbs.bar_close_time`), if any.
    #[serde(default)]
    pub last_bar_close_time: Option<i64>,
    /// Whether the last bar in `data` had closed when the fetch finished, by
    /// the local clock; `None` when unknown.
    #[serde(default)]
    pub last_bar_complete: Option<bool>,
}

impl HistoricalResult {
    /// Whether the request was fully served: no pagination was requested, or
    /// pagination stopped because `from_timestamp` or `max_bars` was reached
    /// or the server reported the start of history. `false` after
    /// [`PaginationStop::BarLimit`], `MaxPages`, `NoMoreData`, `Timeout` or
    /// `ConnectionClosed`, when older bars may exist.
    pub fn is_complete(&self) -> bool {
        self.pagination_stop.is_none_or(|stop| stop.is_complete())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn first_timestamp(&self) -> Option<i64> {
        self.data.first().map(|dp| dp.timestamp())
    }
    pub fn last_timestamp(&self) -> Option<i64> {
        self.data.last().map(|dp| dp.timestamp())
    }
    pub fn first_datetime(&self) -> Option<DateTime<Utc>> {
        self.data.first().map(|dp| dp.datetime())
    }
    pub fn last_datetime(&self) -> Option<DateTime<Utc>> {
        self.data.last().map(|dp| dp.datetime())
    }
}
