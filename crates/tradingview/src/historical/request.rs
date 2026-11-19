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
/// - the server reports the start of the symbol's history or the data
///   server's bar limit (`data_completed` of `end` or `limit` in
///   `series_completed`);
/// - a page adds no older bar;
/// - `max_pages` pages were requested ([`DEFAULT_MAX_PAGES`] when unset).
///
/// A page that does not complete within [`timeout`](Self::timeout), or a
/// connection that closes during the walk, is an error unless
/// [`allow_partial`](Self::allow_partial) is set; then the bars received so far
/// are returned with
/// [`PaginationStop::Timeout`](crate::historical::PaginationStop::Timeout) or
/// [`PaginationStop::ConnectionClosed`](crate::historical::PaginationStop::ConnectionClosed).
///
/// Bars are de-duplicated by timestamp (the last update wins) and returned in
/// ascending order. Bars older than `from_timestamp` are dropped, and with
/// `max_bars` only the newest `max_bars` bars are kept.
/// [`HistoricalResult::pagination_stop`](crate::historical::HistoricalResult::pagination_stop)
/// tells which condition ended the walk and
/// [`HistoricalResult::is_complete`](crate::historical::HistoricalResult::is_complete)
/// whether the request's target was met.
///
/// Pagination cannot go past the bar limit of the data server (not of the
/// account: anonymous and token sessions measured the same): the server
/// answers further pages with `data_completed: "limit"`. On 2026-11-19 that was
/// about 5,000-8,000 bars on `DataServer::Data` and about 10,000-16,500 on
/// `DataServer::ProData`. See `docs/history-depth.md`.
///
/// # Validation
///
/// [`HistoricalClient::retrieve`](crate::historical::HistoricalClient::retrieve)
/// rejects a request without symbol and exchange, and a `from_timestamp` in the
/// future or above 10^11 (which looks like milliseconds), before connecting.
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
    /// Return the bars received so far, instead of an error, when a page
    /// times out or the connection closes during pagination.
    #[builder(default = false)]
    pub allow_partial: bool,
    /// Drop the newest bar when it had not closed yet when the fetch finished.
    /// The bar counts as closed once the server's clock (the `t` timestamps of
    /// its messages, or the local clock when they are missing) passes
    /// `lbs.bar_close_time` plus the symbol's feed `delay` plus 2 s. With
    /// `max_bars`, pagination collects one extra bar so that `max_bars`
    /// closed bars remain. That can cost one more page: when `max_bars` is a
    /// multiple of the page size (counting the initial request), the extra
    /// bar needs a page of its own. If another stop condition ends the walk
    /// with exactly `max_bars` closed bars, the result still reports
    /// [`PaginationStop::ReachedMaxBars`](crate::historical::PaginationStop::ReachedMaxBars).
    #[builder(default = false)]
    pub drop_incomplete_last_bar: bool,
}

/// Largest accepted `from_timestamp`: larger values look like milliseconds.
pub const MAX_FROM_TIMESTAMP: i64 = 100_000_000_000;

/// Pagination settings derived from a [`HistoricalRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationPlan {
    /// Stop once the oldest bar is at or before this Unix time.
    pub from_timestamp: Option<i64>,
    /// Stop once this many distinct bars were received: `max_bars`, plus one
    /// with `drop_incomplete_last_bar`, so that many closed bars remain.
    pub max_bars: Option<u64>,
    /// Maximum number of `request_more_data` pages.
    pub max_pages: u32,
    /// Bars requested per page (at least 1).
    pub page_size: u64,
    /// Pause before each page request.
    pub page_delay: Duration,
    /// Time allowed for each page to complete.
    pub page_timeout: Duration,
    /// Return partial data instead of an error on a page timeout or a closed
    /// connection.
    pub allow_partial: bool,
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

    /// Check the request before any connection is made: symbol and exchange
    /// must be set, and `from_timestamp` must be Unix seconds no later than now.
    pub fn validate(&self) -> crate::Result<()> {
        self.resolve_symbol_exchange()?;
        if let Some(from) = self.from_timestamp {
            Self::check_from_timestamp(from)?;
        }
        Ok(())
    }

    /// Reject a `from_timestamp` above [`MAX_FROM_TIMESTAMP`] (it looks like
    /// milliseconds) or later than now.
    pub fn check_from_timestamp(from: i64) -> crate::Result<()> {
        if from > MAX_FROM_TIMESTAMP {
            return Err(crate::Error::ChronoOutOfRange(
                format!("from_timestamp {from} looks like milliseconds; pass Unix seconds").into(),
            ));
        }
        let now = chrono::Utc::now().timestamp();
        if from > now {
            return Err(crate::Error::ChronoOutOfRange(
                format!("from_timestamp {from} is in the future (now {now})").into(),
            ));
        }
        Ok(())
    }

    /// Whether this request walks back with `request_more_data`.
    pub fn paginates(&self) -> bool {
        self.from_timestamp.is_some() || self.max_bars.is_some() || self.max_pages.is_some()
    }

    /// The pagination settings, or `None` when pagination is disabled.
    pub fn pagination_plan(&self) -> Option<PaginationPlan> {
        self.paginates().then(|| PaginationPlan {
            from_timestamp: self.from_timestamp,
            max_bars: self.max_bars.map(|max_bars| {
                if self.drop_incomplete_last_bar {
                    max_bars.saturating_add(1)
                } else {
                    max_bars
                }
            }),
            max_pages: self.max_pages.unwrap_or(DEFAULT_MAX_PAGES),
            page_size: self.page_size.max(1),
            page_delay: self.page_delay,
            page_timeout: self.timeout,
            allow_partial: self.allow_partial,
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
        assert!(!request.allow_partial);
        assert!(!request.drop_incomplete_last_bar);
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
    fn test_validate_rejects_future_and_millisecond_from_timestamp() {
        let base = || {
            HistoricalRequest::builder()
                .symbol("VCB".to_string())
                .exchange("HOSE".to_string())
        };
        assert!(base().build().validate().is_ok());
        assert!(base().from_timestamp(0).build().validate().is_ok());
        assert!(
            base()
                .from_timestamp(1_735_689_600)
                .build()
                .validate()
                .is_ok()
        );

        let future = chrono::Utc::now().timestamp() + 3_600;
        let error = base()
            .from_timestamp(future)
            .build()
            .validate()
            .unwrap_err();
        assert!(error.to_string().contains("in the future"), "{error}");

        let millis = base()
            .from_timestamp(1_735_689_600_000)
            .build()
            .validate()
            .unwrap_err();
        assert!(millis.to_string().contains("milliseconds"), "{millis}");

        let missing = HistoricalRequest::builder().build().validate();
        assert!(missing.is_err());
    }

    #[test]
    fn test_drop_incomplete_last_bar_collects_one_extra_bar() {
        let base = || {
            HistoricalRequest::builder()
                .symbol("BTCUSDT".to_string())
                .exchange("BINANCE".to_string())
                .max_bars(30)
        };
        assert_eq!(base().build().pagination_plan().unwrap().max_bars, Some(30));
        let dropping = base().drop_incomplete_last_bar(true).build();
        assert_eq!(dropping.pagination_plan().unwrap().max_bars, Some(31));
        assert_eq!(
            dropping.max_bars,
            Some(30),
            "the result is still trimmed to 30"
        );
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
