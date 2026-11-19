use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::chart::OHLCV;
use crate::websocket::SeriesInfo;
use crate::{DataPoint, SymbolInfo};

/// Why pagination stopped walking back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// The server reported the session's bar limit
    /// (`data_completed: "limit"`); older bars need a plan with a higher limit.
    BarLimit,
    /// A page completed without adding an older bar.
    NoMoreData,
    /// A page did not complete within the request timeout; the bars received
    /// until then are returned.
    Timeout,
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
        }
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
}

impl HistoricalResult {
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
