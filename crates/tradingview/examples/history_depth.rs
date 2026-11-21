//! Measure how far back paginated historical data reaches.
//!
//! For every interval, fetches each symbol with `request_more_data`
//! pagination until the server reports the start of history or the
//! session's bar limit, and logs the oldest bar, bar count and page count.
//! All symbols of one interval share one WebSocket connection
//! (`retrieve_batch_with` with one worker), one symbol after another.
//!
//! ```sh
//! cargo run -p tradingview-rs --example history_depth --locked
//! TV_SERVER=prodata TV_AUTH_TOKEN=... cargo run -p tradingview-rs --example history_depth --locked
//! cargo run -p tradingview-rs --example history_depth --locked -- HOSE:VCB NASDAQ:AAPL
//! ```
//!
//! `TV_SERVER` is `data` (default), `prodata`, `widgetdata` or `mobile-data`.
//! `TV_INTERVALS` (default `1,15,60`), `TV_PAGE_DELAY_MS` (default 1000) and
//! `TV_MAX_PAGES` (default 40) tune the run. The token is never logged.

use std::{env, time::Duration};

use tracing::{info, warn};
use tradingview::{
    DataServer, Interval,
    historical::{BatchConfig, HistoricalClient, HistoricalRequest},
};

fn parse_interval(raw: &str) -> Option<Interval> {
    Some(match raw.trim() {
        "1" => Interval::OneMinute,
        "5" => Interval::FiveMinutes,
        "15" => Interval::FifteenMinutes,
        "30" => Interval::ThirtyMinutes,
        "60" => Interval::OneHour,
        "240" => Interval::FourHours,
        "1D" => Interval::OneDay,
        _ => return None,
    })
}

fn date(timestamp: Option<chrono::DateTime<chrono::Utc>>) -> String {
    timestamp.map_or_else(
        || "-".to_string(),
        |t| t.format("%Y-%m-%d %H:%M").to_string(),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let token = env::var("TV_AUTH_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty());
    let auth = if token.is_some() {
        "token"
    } else {
        "anonymous"
    };
    let server = match env::var("TV_SERVER").as_deref() {
        Ok("prodata") => DataServer::ProData,
        Ok("widgetdata") => DataServer::WidgetData,
        Ok("mobile-data") => DataServer::MobileData,
        _ => DataServer::Data,
    };
    let page_delay = Duration::from_millis(
        env::var("TV_PAGE_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000),
    );
    let max_pages: u32 = env::var("TV_MAX_PAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    let intervals: Vec<Interval> = env::var("TV_INTERVALS")
        .unwrap_or_else(|_| "1,15,60".to_string())
        .split(',')
        .filter_map(parse_interval)
        .collect();
    let mut symbols: Vec<(String, String)> = env::args()
        .skip(1)
        .filter_map(|id| {
            let (exchange, symbol) = id.split_once(':')?;
            Some((symbol.to_string(), exchange.to_string()))
        })
        .collect();
    if symbols.is_empty() {
        symbols = ["BINANCE:BTCUSDT", "OKX:BTCUSDT.P", "HOSE:VCB"]
            .iter()
            .filter_map(|id| id.split_once(':'))
            .map(|(exchange, symbol)| (symbol.to_string(), exchange.to_string()))
            .collect();
    }

    let client = HistoricalClient::new(
        token.as_deref().unwrap_or("unauthorized_user_token"),
        server,
    );
    info!(
        auth,
        ?server,
        ?page_delay,
        max_pages,
        "| auth | server | symbol | interval | bars | oldest bar (UTC) | newest bar (UTC) | pages | stop |"
    );
    for interval in intervals {
        let template = HistoricalRequest::builder()
            .interval(interval)
            .max_pages(max_pages)
            .page_delay(page_delay)
            .build();
        let config = BatchConfig {
            max_concurrency: 1,
            per_symbol_timeout: Duration::from_secs(30),
        };
        let batch = client
            .retrieve_batch_with(&symbols, &template, config)
            .await;
        for done in batch.successful.iter().chain(&batch.failed) {
            match &done.result {
                Ok(result) => info!(
                    "| {auth} | {server:?} | {}:{} | {interval} | {} | {} | {} | {} | {} |",
                    done.exchange,
                    done.symbol,
                    result.len(),
                    date(result.first_datetime()),
                    date(result.last_datetime()),
                    result.pages_fetched,
                    result.pagination_stop.map_or("-", |stop| stop.as_str()),
                ),
                Err(error) => warn!(
                    "| {auth} | {server:?} | {}:{} | {interval} | error: {error} |",
                    done.exchange, done.symbol
                ),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}
