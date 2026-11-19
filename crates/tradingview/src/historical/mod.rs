//! Historical data retrieval — clean architecture for fetching TradingView
//! chart data via WebSocket.
//!
//! # Architecture
//!
//! ```text
//! HistoricalClient::retrieve(request)
//!   ├── HistoricalRequest  (what to fetch, pagination, adjustment, session, currency)
//!   ├── HistoricalState    (mutable state during fetch)
//!   ├── create_series, then request_more_data pages when pagination is enabled
//!   └── returns HistoricalResult (bars, SymbolInfo, pages_fetched, pagination_stop)
//!
//! HistoricalClient::retrieve_batch_with(symbols, template, config)
//!   ├── At most `max_concurrency` connections, each fetching symbols in turn
//!   ├── One chart session per symbol, routed by session id
//!   └── returns BatchResult (successful + failed)
//! ```
//!
//! See [`HistoricalRequest`] for the pagination stop conditions and
//! `docs/history-depth.md` for how far back each data server goes.
//!
//! # Migration from v1
//!
//! The old `retrieve()` function in `single.rs` used scattered
//! `Arc<Mutex<...>>` fields and unbounded channels.  The new
//! `HistoricalClient` consolidates state, uses bounded channels, and
//! provides a builder-based `HistoricalRequest`.
//!
//! The old batch implementation used a complex `CommandRunner` +
//! `BatchTracker` + `DataRx` architecture.  The new `retrieve_batch()`
//! uses a `JoinSet` of workers that share a symbol queue.

pub mod batch;
pub mod client;
pub mod request;
pub mod result;
pub mod state;

// Re-export the public API.
pub use batch::{BatchConfig, BatchResult, BatchSymbolResult};
pub use client::HistoricalClient;
pub use request::{
    DEFAULT_MAX_PAGES, DEFAULT_PAGE_DELAY, DEFAULT_PAGE_SIZE, HistoricalRequest, PaginationPlan,
};
pub use result::{HistoricalResult, PaginationStop};
