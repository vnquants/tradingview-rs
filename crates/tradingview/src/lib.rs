//! TradingView data feed client: historical bars, live quotes and studies over
//! TradingView's WebSocket protocol, plus its REST endpoints.
//!
//! # Logging
//!
//! The crate logs through [`tracing`]. Its own packet logs redact the
//! parameters of `set_auth_token`, so the session token never appears in them,
//! even at `TRACE`.
//!
//! The WebSocket transport, `tungstenite`, logs every frame it sends and
//! receives at `TRACE` through the `log` crate, and the first frame carries
//! the auth token. When `log` records are forwarded to your subscriber (as
//! `tracing_subscriber::fmt().init()` and `tracing_log::LogTracer` do), cap
//! `tungstenite` and `tokio_tungstenite` at `DEBUG` by appending
//! [`RECOMMENDED_LOG_DIRECTIVES`] to your filter:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use tracing_subscriber::{filter::Targets, prelude::*};
//!
//! let filter: Targets = format!("trace,{}", tradingview::RECOMMENDED_LOG_DIRECTIVES).parse()?;
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(filter)
//!     .init();
//! # Ok(())
//! # }
//! ```
//!
//! With `EnvFilter`, the same directives work in `RUST_LOG`, for example
//! `RUST_LOG=trace,tungstenite=debug,tokio_tungstenite=debug`.

/// Filter directives (`EnvFilter`/`Targets` syntax) that cap the WebSocket
/// transport crates at `DEBUG`: at `TRACE` they log raw frames, including the
/// `set_auth_token` frame with the session token. Append them to any filter
/// that enables `TRACE`.
pub const RECOMMENDED_LOG_DIRECTIVES: &str = "tungstenite=debug,tokio_tungstenite=debug";

pub mod chart;
pub mod client;
pub mod error;
pub mod models;
pub mod prelude;
pub mod quote;

#[cfg(feature = "user")]
pub mod user;

pub mod utils;

pub use crate::client::misc::*;

pub mod websocket {
    pub use crate::live::websocket::*;
}

pub use crate::live::models::*;
pub use crate::models::*;

pub type Result<T> = std::result::Result<T, Error>;

pub use error::Error;

// Re-exporting some commonly used types
pub use iso_currency::{Country, Currency, CurrencySymbol};

pub mod fundamental;
pub mod historical;
pub mod live;
pub mod study;

// ---------------------------------------------------------------------------
// New event-driven data loader modules
// ---------------------------------------------------------------------------

/// Generic market event types (Candle, Quote, Economic, News, etc.).
pub mod events;

/// Event sink abstraction + built-in sinks (channel, callback, kafka).
pub mod sink;

/// Data source abstraction + TradingView adapter.
pub mod source;

/// Event-driven data loader orchestrator.
pub mod loader;
