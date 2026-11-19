use std::time::Duration;

use crate::chart::options::Range;
use crate::{Interval, MarketAdjustment, MarketSymbol, SessionType, Ticker};
use bon::Builder;
use iso_currency::Currency;

/// Bars requested per `request_more_data` page when
/// [`HistoricalRequest::page_size`] is not set.
pub const DEFAULT_PAGE_SIZE: u64 = 5_000;

/// Upper bound on `request_more_data` pages when pagination is enabled by
/// [`HistoricalRequest::from_timestamp`] or [`HistoricalRequest::max_bars`]
/// without an explicit [`HistoricalRequest::max_pages`].
pub const DEFAULT_MAX_PAGES: u32 = 100;

/// Pause before each `request_more_data` page when
/// [`HistoricalRequest::page_delay`] is not set.
pub const DEFAULT_PAGE_DELAY: Duration = Duration::from_millis(500);

/// Immutable request object for a historical data fetch.
///
/// # Pagination
///
/// By default one `create_series` request is sent for `num_bars` bars
/// (100 when omitted), as in earlier releases. Setting any of
/// [`from_timestamp`](Self::from_timestamp), [`max_bars`](Self::max_bars) or
/// [`max_pages`](Self::max_pages) enables pagination: after the initial series
/// loads, the client sends `request_more_data` for [`page_size`](Self::page_size)
/// older bars at a time, waiting [`page_delay`](Self::page_delay) before each
/// page, until one of these holds:
///
/// - the oldest bar is at or before `from_timestamp`;
/// - at least `max_bars` distinct bars were received;
/// - the server reports the start of the symbol's history or the account's
///   bar limit (`data_completed` of `end` or `limit` in `series_completed`);
/// - a page adds no older bar;
/// - `max_pages` pages were requested ([`DEFAULT_MAX_PAGES`] when unset);
/// - a page does not complete within [`timeout`](Self::timeout).
///
/// Bars are de-duplicated by timestamp (the last update wins) and returned in
/// ascending order. Bars older than `from_timestamp` are dropped, and with
/// `max_bars` only the newest `max_bars` bars are kept.
/// [`HistoricalResult::pagination_stop`](crate::historical::HistoricalResult::pagination_stop)
/// tells which condition ended the walk.
///
/// Pagination cannot go past the server-side bar limit of the session: the
/// server answers further pages with `data_completed: "limit"`. See
/// `docs/history-depth.md` for measured limits.
///
/// # Example
///
/// ```no_run
/// # async fn run() -> tradingview::Result<()> {
/// use std::time::Duration;
/// use tradingview::{DataServer, Interval, MarketAdjustment};
/// use tradingview::historical::{HistoricalClient, HistoricalRequest};
///
/// let request = HistoricalRequest::builder()
///     .symbol("VCB".to_string())
///     .exchange("HOSE".to_string())
///     .interval(Interval::OneHour)
///     .from_timestamp(1_735_689_600) // 2025-01-01T00:00:00Z
///     .page_delay(Duration::from_secs(1))
///     .adjustment(MarketAdjustment::Dividends)
///     .build();
/// let result = HistoricalClient::new("unauthorized_user_token", DataServer::Data)
///     .retrieve(request)
///     .await?;
/// tracing::info!(bars = result.len(), stop = ?result.pagination_stop, "fetched");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Builder)]
pub struct HistoricalRequest {
    pub ticker: Option<Ticker>,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    #[builder(default = Interval::OneDay)]
    pub interval: Interval,
    pub range: Option<Range>,
    /// Bars requested by the initial `create_series` (100 when omitted in
    /// count mode, 512 in range mode).
    pub num_bars: Option<u64>,
    /// Currently has no effect; kept for API compatibility. Use
    /// [`from_timestamp`](Self::from_timestamp) or [`max_bars`](Self::max_bars)
    /// to load older bars.
    #[builder(default = false)]
    pub with_replay: bool,
    /// Time allowed for the initial series load and, separately, for each
    /// additional page. An initial timeout is an error; a page timeout ends
    /// pagination and returns the bars received so far.
    #[builder(default = std::time::Duration::from_secs(30))]
    pub timeout: std::time::Duration,
    /// Oldest bar wanted, in Unix seconds. Enables pagination.
    pub from_timestamp: Option<i64>,
    /// Number of bars wanted in total. Enables pagination.
    pub max_bars: Option<u64>,
    /// Maximum number of `request_more_data` pages. Enables pagination on its
    /// own: walk back until the server stops or this many pages were sent.
    pub max_pages: Option<u32>,
    /// Bars requested per page.
    #[builder(default = DEFAULT_PAGE_SIZE)]
    pub page_size: u64,
    /// Pause before each page request.
    #[builder(default = DEFAULT_PAGE_DELAY)]
    pub page_delay: Duration,
    /// Price adjustment. `None` sends no adjustment key, which the server
    /// treats as [`MarketAdjustment::Splits`].
    pub adjustment: Option<MarketAdjustment>,
    /// Trading session. `None` sends no session key, which the server treats
    /// as [`SessionType::Regular`].
    pub session_type: Option<SessionType>,
    /// Convert prices to this currency (`currency-id`). `None` keeps the
    /// symbol's own currency.
    pub currency: Option<Currency>,
}

/// Pagination settings derived from a [`HistoricalRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationPlan {
    /// Stop once the oldest bar is at or before this Unix time.
    pub from_timestamp: Option<i64>,
    /// Stop once this many distinct bars were received.
    pub max_bars: Option<u64>,
    /// Maximum number of `request_more_data` pages.
    pub max_pages: u32,
    /// Bars requested per page (at least 1).
    pub page_size: u64,
    /// Pause before each page request.
    pub page_delay: Duration,
    /// Time allowed for each page to complete.
    pub page_timeout: Duration,
}

impl HistoricalRequest {
    pub fn resolve_symbol_exchange(&self) -> crate::Result<(String, String)> {
        if let Some(ref ticker) = self.ticker {
            return Ok((ticker.symbol().to_string(), ticker.exchange().to_string()));
        }
        match (&self.symbol, &self.exchange) {
            (Some(s), Some(e)) => Ok((s.clone(), e.clone())),
            (None, _) => Err(crate::error::TradingViewError::MissingSymbol.into()),
            (_, None) => Err(crate::error::TradingViewError::MissingExchange.into()),
        }
    }

    /// Whether this request walks back with `request_more_data`.
    pub fn paginates(&self) -> bool {
        self.from_timestamp.is_some() || self.max_bars.is_some() || self.max_pages.is_some()
    }

    /// The pagination settings, or `None` when pagination is disabled.
    pub fn pagination_plan(&self) -> Option<PaginationPlan> {
        self.paginates().then(|| PaginationPlan {
            from_timestamp: self.from_timestamp,
            max_bars: self.max_bars,
            max_pages: self.max_pages.unwrap_or(DEFAULT_MAX_PAGES),
            page_size: self.page_size.max(1),
            page_delay: self.page_delay,
            page_timeout: self.timeout,
        })
    }

    /// Copy of this request for another symbol: `ticker` is cleared and
    /// `symbol`/`exchange` are replaced; every other option is kept. Batch
    /// retrieval uses it to apply one template to many symbols.
    pub fn for_symbol(&self, symbol: &str, exchange: &str) -> Self {
        Self {
            ticker: None,
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            ..self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_keep_single_request_behaviour() {
        let request = HistoricalRequest::builder()
            .symbol("BTCUSDT".to_string())
            .exchange("BINANCE".to_string())
            .build();
        assert!(!request.paginates());
        assert_eq!(request.pagination_plan(), None);
        assert_eq!(request.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(request.page_delay, DEFAULT_PAGE_DELAY);
        assert_eq!(request.adjustment, None);
        assert_eq!(request.session_type, None);
        assert_eq!(request.currency, None);
    }

    #[test]
    fn test_any_pagination_option_enables_pagination() {
        let base = || {
            HistoricalRequest::builder()
                .symbol("VCB".to_string())
                .exchange("HOSE".to_string())
        };
        let by_time = base().from_timestamp(1_700_000_000).build();
        let by_count = base().max_bars(20_000).build();
        let by_pages = base().max_pages(3).build();

        let plan = by_time.pagination_plan().unwrap();
        assert_eq!(plan.from_timestamp, Some(1_700_000_000));
        assert_eq!(plan.max_pages, DEFAULT_MAX_PAGES);
        assert_eq!(plan.page_timeout, by_time.timeout);
        assert_eq!(by_count.pagination_plan().unwrap().max_bars, Some(20_000));
        assert_eq!(by_pages.pagination_plan().unwrap().max_pages, 3);
    }

    #[test]
    fn test_zero_page_size_is_clamped() {
        let request = HistoricalRequest::builder()
            .symbol("VCB".to_string())
            .exchange("HOSE".to_string())
            .max_pages(1)
            .page_size(0)
            .build();
        assert_eq!(request.pagination_plan().unwrap().page_size, 1);
    }

    #[test]
    fn test_for_symbol_overrides_identity_only() {
        let template = HistoricalRequest::builder()
            .ticker(Ticker::new("AAPL", "NASDAQ"))
            .interval(Interval::OneHour)
            .max_bars(10)
            .adjustment(MarketAdjustment::Unadjusted)
            .build();
        let request = template.for_symbol("VCB", "HOSE");
        assert!(request.ticker.is_none());
        assert_eq!(
            request.resolve_symbol_exchange().unwrap(),
            ("VCB".to_string(), "HOSE".to_string())
        );
        assert_eq!(request.interval, Interval::OneHour);
        assert_eq!(request.max_bars, Some(10));
        assert_eq!(request.adjustment, Some(MarketAdjustment::Unadjusted));
    }
}
