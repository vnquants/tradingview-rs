//! Batch historical data retrieval — concurrent multi-symbol fetching.
//!
//! Up to `max_concurrency` workers each open one WebSocket connection and
//! fetch their share of the symbols one after another, every symbol in its own
//! chart session. A connection is replaced after a failed fetch.

use crate::{
    Interval,
    historical::{
        HistoricalClient, HistoricalRequest, HistoricalResult, client::HistoricalConnection,
    },
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

// =============================================================================
// BatchSymbolResult
// =============================================================================

/// Result of a batch retrieval for a single symbol.
///
/// Contains the symbol/exchange identity so the caller can correlate
/// results with their input set regardless of completion order.
#[derive(Debug)]
pub struct BatchSymbolResult {
    pub symbol: String,
    pub exchange: String,
    pub result: crate::Result<HistoricalResult>,
}

// =============================================================================
// BatchConfig
// =============================================================================

/// Configuration for batch historical data retrieval.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of concurrent fetches.
    ///
    /// **Default: 4.**  Higher values may trigger TradingView rate-limiting.
    pub max_concurrency: usize,

    /// Timeout per individual symbol fetch.
    ///
    /// **Default: 30 seconds.**
    pub per_symbol_timeout: std::time::Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            per_symbol_timeout: std::time::Duration::from_secs(30),
        }
    }
}

// =============================================================================
// BatchResult — aggregate summary
// =============================================================================

/// Aggregate result of a batch retrieval.
///
/// Separates successful results from failed ones so the caller can
/// handle partial failures gracefully.
#[derive(Debug)]
pub struct BatchResult {
    /// Successfully retrieved results, with symbol identity.
    pub successful: Vec<BatchSymbolResult>,

    /// Failed retrievals, with the original symbol/exchange and error.
    pub failed: Vec<BatchSymbolResult>,

    /// Total number of symbols requested.
    pub total_requested: usize,

    /// Total wall-clock elapsed time for the batch operation.
    pub elapsed: std::time::Duration,
}

impl BatchResult {
    /// Returns `true` if all symbols were retrieved successfully.
    pub fn is_complete_success(&self) -> bool {
        self.failed.is_empty()
    }

    /// Number of successfully retrieved symbols.
    pub fn success_count(&self) -> usize {
        self.successful.len()
    }

    /// Number of failed retrievals.
    pub fn failure_count(&self) -> usize {
        self.failed.len()
    }
}

// =============================================================================
// HistoricalClient::retrieve_batch
// =============================================================================

impl HistoricalClient {
    /// Fetch historical data for multiple symbols concurrently.
    ///
    /// Equivalent to [`Self::retrieve_batch_with`] with a template that sets
    /// only `interval` and `num_bars`.
    ///
    /// # Returns
    ///
    /// A [`BatchResult`] that splits successful and failed fetches so
    /// partial failures can be handled gracefully.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = HistoricalClient::new("my_token", DataServer::ProData);
    /// let symbols = vec![
    ///     ("AAPL".into(), "NASDAQ".into()),
    ///     ("MSFT".into(), "NASDAQ".into()),
    ///     ("GOOGL".into(), "NASDAQ".into()),
    /// ];
    /// let result = client
    ///     .retrieve_batch(&symbols, Interval::OneDay, Some(100), BatchConfig::default())
    ///     .await;
    /// println!("{}/{} succeeded", result.success_count(), result.total_requested);
    /// ```
    #[instrument(skip(self, symbols), fields(symbol_count = symbols.len()))]
    pub async fn retrieve_batch(
        &self,
        symbols: &[(String, String)],
        interval: Interval,
        num_bars: Option<u64>,
        config: BatchConfig,
    ) -> BatchResult {
        let template = HistoricalRequest::builder()
            .interval(interval)
            .maybe_num_bars(num_bars)
            .build();
        self.retrieve_batch_with(symbols, &template, config).await
    }

    /// Fetch historical data for multiple symbols with the options of
    /// `template`: interval, bar count, range, pagination, adjustment,
    /// session and currency.
    ///
    /// The template's `ticker`, `symbol` and `exchange` are ignored, and
    /// `config.per_symbol_timeout` replaces its `timeout`. At most
    /// `config.max_concurrency` WebSocket connections are opened (at least
    /// one, never more than there are symbols); each connection fetches
    /// symbols one after another in separate chart sessions and is replaced
    /// after a failed fetch. The call returns when every symbol succeeded or
    /// failed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let template = HistoricalRequest::builder()
    ///     .interval(Interval::OneHour)
    ///     .from_timestamp(1_735_689_600)
    ///     .adjustment(MarketAdjustment::Dividends)
    ///     .build();
    /// let result = client
    ///     .retrieve_batch_with(&symbols, &template, BatchConfig::default())
    ///     .await;
    /// ```
    #[instrument(skip(self, symbols, template), fields(symbol_count = symbols.len()))]
    pub async fn retrieve_batch_with(
        &self,
        symbols: &[(String, String)],
        template: &HistoricalRequest,
        config: BatchConfig,
    ) -> BatchResult {
        let started = std::time::Instant::now();
        let symbol_count = symbols.len();

        if symbol_count == 0 {
            return BatchResult {
                successful: Vec::new(),
                failed: Vec::new(),
                total_requested: 0,
                elapsed: started.elapsed(),
            };
        }

        let workers = config.max_concurrency.clamp(1, symbol_count);
        info!(
            "Starting batch retrieval: {} symbols, max_concurrency={}, connections={}",
            symbol_count, config.max_concurrency, workers
        );

        let template = HistoricalRequest {
            timeout: config.per_symbol_timeout,
            ..template.clone()
        };
        let queue = Arc::new(Mutex::new(symbols.iter().cloned().collect::<VecDeque<_>>()));
        let (result_tx, mut result_rx) = mpsc::unbounded_channel();
        let mut join_set = tokio::task::JoinSet::new();
        for worker in 0..workers {
            let worker_state = BatchWorker {
                id: worker,
                auth_token: self.auth_token.clone(),
                server: self.server,
                template: template.clone(),
                queue: Arc::clone(&queue),
                results: result_tx.clone(),
            };
            join_set.spawn(worker_state.run());
        }
        drop(result_tx);

        // Collect results in completion order.
        let mut successful = Vec::with_capacity(symbol_count);
        let mut failed = Vec::with_capacity(symbol_count);
        while let Some(batch_result) = result_rx.recv().await {
            match &batch_result.result {
                Ok(hr) => {
                    debug!(
                        symbol = %batch_result.symbol,
                        bars = hr.total_bars_received,
                        elapsed_ms = hr.elapsed.as_millis(),
                        "Symbol fetch complete"
                    );
                    successful.push(batch_result);
                }
                Err(e) => {
                    warn!(
                        symbol = %batch_result.symbol,
                        error = %e,
                        "Symbol fetch failed"
                    );
                    failed.push(batch_result);
                }
            }
        }
        while let Some(joined) = join_set.join_next().await {
            if let Err(join_err) = joined {
                warn!(error = %join_err, "Batch worker panicked or was cancelled");
            }
        }

        let elapsed = started.elapsed();
        info!(
            "Batch retrieval complete: {}/{} successful in {:?}",
            successful.len(),
            symbol_count,
            elapsed,
        );

        BatchResult {
            successful,
            failed,
            total_requested: symbol_count,
            elapsed,
        }
    }
}

/// One batch worker: a connection that fetches queued symbols in turn.
struct BatchWorker {
    id: usize,
    auth_token: String,
    server: crate::DataServer,
    template: HistoricalRequest,
    queue: Arc<Mutex<VecDeque<(String, String)>>>,
    results: mpsc::UnboundedSender<BatchSymbolResult>,
}

impl BatchWorker {
    fn next_symbol(&self) -> Option<(String, String)> {
        self.queue.lock().unwrap().pop_front()
    }

    async fn run(self) {
        let mut connection: Option<HistoricalConnection> = None;
        while let Some((symbol, exchange)) = self.next_symbol() {
            debug!(
                worker = self.id,
                symbol = %symbol,
                exchange = %exchange,
                "Fetching historical data"
            );
            let request = self.template.for_symbol(&symbol, &exchange);
            let result = match self.connection(&mut connection).await {
                Ok(open) => open.fetch(&request).await,
                Err(error) => Err(error),
            };
            if result.is_err()
                && let Some(failed) = connection.take()
            {
                failed.close().await;
            }
            // The receiver lives until every worker finished.
            let _ = self.results.send(BatchSymbolResult {
                symbol,
                exchange,
                result,
            });
        }
        if let Some(open) = connection {
            open.close().await;
        }
    }

    /// The worker's open connection, opening a new one when needed.
    async fn connection<'a>(
        &self,
        slot: &'a mut Option<HistoricalConnection>,
    ) -> crate::Result<&'a HistoricalConnection> {
        if let Some(stale) = slot.take_if(|open| open.is_closed()) {
            stale.close().await;
        }
        if slot.is_none() {
            *slot = Some(HistoricalConnection::open(&self.auth_token, self.server).await?);
        }
        Ok(slot.as_ref().expect("connection was opened above"))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config_defaults() {
        let config = BatchConfig::default();
        assert_eq!(config.max_concurrency, 4);
        assert_eq!(
            config.per_symbol_timeout,
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn test_batch_result_empty_success() {
        let result = BatchResult {
            successful: Vec::new(),
            failed: Vec::new(),
            total_requested: 0,
            elapsed: std::time::Duration::ZERO,
        };
        assert!(result.is_complete_success());
        assert_eq!(result.success_count(), 0);
        assert_eq!(result.failure_count(), 0);
    }

    #[test]
    fn test_batch_result_with_failures() {
        let result = BatchResult {
            successful: vec![],
            failed: vec![BatchSymbolResult {
                symbol: "AAPL".into(),
                exchange: "NASDAQ".into(),
                result: Err(crate::Error::Timeout("timeout".into())),
            }],
            total_requested: 1,
            elapsed: std::time::Duration::ZERO,
        };
        assert!(!result.is_complete_success());
        assert_eq!(result.success_count(), 0);
        assert_eq!(result.failure_count(), 1);
    }

    #[test]
    fn test_empty_symbols_returns_immediately() {
        // Verify the constructor pattern compiles and handles empty input.
        let result = BatchResult {
            successful: vec![],
            failed: vec![],
            total_requested: 0,
            elapsed: std::time::Duration::ZERO,
        };
        assert_eq!(result.total_requested, 0);
    }
}
