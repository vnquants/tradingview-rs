use parking_lot::RwLock;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;
use tradingview::Currency;
use tradingview::chart::OHLCV;
use tradingview::historical::{BatchConfig, HistoricalClient, HistoricalRequest, HistoricalResult};
use tradingview::live::models::DataServer as CoreDataServer;
use tradingview::live::websocket::WebSocketClient;
use tradingview::models::UserCookies;

use crate::PyObject;
use crate::callbacks::CallbackDispatcher;
use crate::errors::{AuthenticationError, to_py_err};
use crate::models::bar::{Bar, HistoricalSeries};
use crate::models::calendar::EconomicEvent;
use crate::models::candle::{BarSubscription, CandleUpdate};
use crate::models::enums::{
    DataServer, EconomicImportance, FinancialPeriod, Interval, MarketAdjustment, SessionType,
};
use crate::models::fundamental::{FundamentalPoint, FundamentalSeries};
use crate::models::quote::{QuoteSubscription, QuoteTick};
use crate::models::symbol::SymbolInfo;
use crate::session::{CookieImportError, export_user_cookies, parse_user_cookies};
use crate::streaming::{CandleStreamHandler, QuoteStreamHandler};

// The historical methods spell their `page_size = 5000` and `page_delay = 0.5`
// defaults as literals so Python's `inspect.signature` shows them; keep them
// equal to the core defaults.
const _: () = assert!(tradingview::historical::DEFAULT_PAGE_SIZE == 5000);
const _: () = assert!(tradingview::historical::DEFAULT_PAGE_DELAY.as_millis() == 500);

/// Validated pagination, adjustment, session and currency keywords shared by
/// the historical methods.
#[derive(Debug, Clone)]
struct HistoryOptions {
    from_timestamp: Option<i64>,
    max_bars: Option<u64>,
    max_pages: Option<u32>,
    page_size: u64,
    page_delay: Duration,
    adjustment: Option<MarketAdjustment>,
    session_type: Option<SessionType>,
    currency: Option<Currency>,
}

impl HistoryOptions {
    #[allow(clippy::too_many_arguments)]
    fn parse(
        from_timestamp: Option<i64>,
        max_bars: Option<u64>,
        max_pages: Option<u32>,
        page_size: u64,
        page_delay: f64,
        adjustment: Option<MarketAdjustment>,
        session_type: Option<SessionType>,
        currency: Option<String>,
    ) -> PyResult<Self> {
        if page_size == 0 {
            return Err(PyValueError::new_err(
                "page_size must be a positive number of bars",
            ));
        }
        if max_bars == Some(0) {
            return Err(PyValueError::new_err(
                "max_bars must be a positive number of bars",
            ));
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
            from_timestamp,
            max_bars,
            max_pages,
            page_size,
            page_delay,
            adjustment,
            session_type,
            currency,
        })
    }

    /// Request template without a symbol (see `HistoricalRequest::for_symbol`).
    fn request(
        &self,
        interval: Interval,
        n_bars: Option<u64>,
        with_replay: bool,
    ) -> HistoricalRequest {
        HistoricalRequest::builder()
            .interval(interval.into())
            .maybe_num_bars(n_bars)
            .with_replay(with_replay)
            .maybe_from_timestamp(self.from_timestamp)
            .maybe_max_bars(self.max_bars)
            .maybe_max_pages(self.max_pages)
            .page_size(self.page_size)
            .page_delay(self.page_delay)
            .maybe_adjustment(self.adjustment.map(Into::into))
            .maybe_session_type(self.session_type.map(Into::into))
            .maybe_currency(self.currency)
            .build()
    }
}

/// Convert a core result into the Python series.
fn series_from_result(
    symbol: String,
    exchange: String,
    interval: Interval,
    result: HistoricalResult,
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
    }
}

/// Main client interface for accessing TradingView market data.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TradingViewClient {
    pub(crate) auth_token: Arc<RwLock<Option<String>>>,
    pub(crate) username: Arc<RwLock<Option<String>>>,
    pub(crate) user_cookies: Arc<RwLock<Option<UserCookies>>>,
    pub(crate) server: DataServer,
}

#[pymethods]
impl TradingViewClient {
    #[new]
    #[pyo3(signature = (auth_token = None, *, server = DataServer::Data))]
    pub fn new(auth_token: Option<String>, server: DataServer) -> Self {
        Self {
            auth_token: Arc::new(RwLock::new(auth_token)),
            username: Arc::new(RwLock::new(None)),
            user_cookies: Arc::new(RwLock::new(None)),
            server,
        }
    }

    #[getter]
    pub fn server(&self) -> DataServer {
        self.server
    }

    #[getter]
    pub fn auth_token(&self) -> Option<String> {
        self.auth_token.read().clone()
    }

    #[getter]
    pub fn username(&self) -> Option<String> {
        self.username.read().clone()
    }

    #[getter]
    pub fn is_authenticated(&self) -> bool {
        let tok = self.auth_token.read();
        if let Some(t) = tok.as_deref() {
            t != "unauthorized_user_token" && !t.is_empty()
        } else {
            false
        }
    }

    /// Authenticate with TradingView credentials (username, password, optional TOTP) and return an authenticated client.
    ///
    /// The `totp_secret` parameter supports either a standard RFC 6238 Base32 secret or a full `otpauth://totp/...` URI (e.g. from Bitwarden).
    #[classmethod]
    #[pyo3(signature = (username, password, totp_secret = None, *, captcha_key = None, server = DataServer::Data))]
    pub fn login<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        username: String,
        password: String,
        totp_secret: Option<String>,
        captcha_key: Option<String>,
        server: DataServer,
    ) -> PyResult<Bound<'py, PyAny>> {
        let effective_captcha_key = captcha_key;
        future_into_py(py, async move {
            let mut cookies = UserCookies::new();
            let logged_in = if let Some(key) = &effective_captcha_key {
                cookies
                    .login_with_captcha(&username, &password, totp_secret.as_deref(), key)
                    .await
                    .map_err(to_py_err)?
            } else {
                cookies
                    .login(&username, &password, totp_secret.as_deref())
                    .await
                    .map_err(to_py_err)?
            };

            let client = TradingViewClient {
                auth_token: Arc::new(RwLock::new(Some(logged_in.auth_token.clone()))),
                username: Arc::new(RwLock::new(Some(logged_in.username.clone()))),
                user_cookies: Arc::new(RwLock::new(Some(logged_in))),
                server,
            };

            Ok(client)
        })
    }

    /// Authenticate the existing client instance using credentials.
    ///
    /// The `totp_secret` parameter supports either a standard RFC 6238 Base32 secret or a full `otpauth://totp/...` URI (e.g. from Bitwarden).
    #[pyo3(signature = (username, password, totp_secret = None, *, captcha_key = None))]
    pub fn authenticate<'py>(
        &self,
        py: Python<'py>,
        username: String,
        password: String,
        totp_secret: Option<String>,
        captcha_key: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let effective_captcha_key = captcha_key;
        let auth_token_lock = Arc::clone(&self.auth_token);
        let username_lock = Arc::clone(&self.username);
        let user_cookies_lock = Arc::clone(&self.user_cookies);

        future_into_py(py, async move {
            let mut cookies = UserCookies::new();
            let logged_in = if let Some(key) = &effective_captcha_key {
                cookies
                    .login_with_captcha(&username, &password, totp_secret.as_deref(), key)
                    .await
                    .map_err(to_py_err)?
            } else {
                cookies
                    .login(&username, &password, totp_secret.as_deref())
                    .await
                    .map_err(to_py_err)?
            };

            let mut tok = auth_token_lock.write();
            *tok = Some(logged_in.auth_token.clone());

            let mut user = username_lock.write();
            *user = Some(logged_in.username.clone());

            let mut uc = user_cookies_lock.write();
            *uc = Some(logged_in);

            Ok(())
        })
    }
    /// Build a cookie-session client from JSON produced by `export_cookies()`.
    ///
    /// No network request is made. The returned client can call `get_tradingview_token()`
    /// without a password login. Only `session` and `session_signature` are required;
    /// the other `UserCookies` fields may be omitted. The stored `auth_token` is restored
    /// as-is and may already have expired.
    ///
    /// Raises `ValueError` for malformed JSON and `AuthenticationError` when the session
    /// cookies are empty. Error messages never include cookie or token values.
    #[classmethod]
    #[pyo3(signature = (cookies_json, *, server = DataServer::Data))]
    pub fn from_cookies(
        _cls: &Bound<'_, PyType>,
        cookies_json: &str,
        server: DataServer,
    ) -> PyResult<Self> {
        let cookies = parse_user_cookies(cookies_json).map_err(|err| match err {
            CookieImportError::MissingSessionCookies => {
                AuthenticationError::new_err(err.to_string())
            }
            CookieImportError::Malformed { .. } | CookieImportError::NotAnObject => {
                PyValueError::new_err(err.to_string())
            }
        })?;
        debug!(
            has_auth_token = !cookies.auth_token.is_empty(),
            has_device_token = !cookies.device_token.is_empty(),
            "restored TradingView cookie session"
        );

        let non_empty = |value: &str| (!value.is_empty()).then(|| value.to_string());
        Ok(Self {
            auth_token: Arc::new(RwLock::new(non_empty(&cookies.auth_token))),
            username: Arc::new(RwLock::new(non_empty(&cookies.username))),
            user_cookies: Arc::new(RwLock::new(Some(cookies))),
            server,
        })
    }

    /// Export the session cookies as JSON so a later process can call `from_cookies()`.
    ///
    /// The JSON contains live session secrets: store it with owner-only permissions.
    /// Raises `AuthenticationError` if the client has no cookie session (anonymous or
    /// token-only clients).
    pub fn export_cookies(&self) -> PyResult<String> {
        let guard = self.user_cookies.read();
        let cookies = guard.as_ref().ok_or_else(|| {
            AuthenticationError::new_err(
                "Client has no cookie session to export; authenticate via login(), authenticate() or from_cookies() first",
            )
        })?;
        export_user_cookies(cookies)
            .map_err(|_| PyValueError::new_err("failed to serialize the cookie session"))
    }

    /// Retrieve a TradingView session token using authenticated session cookies.
    ///
    /// Requires an active cookie session obtained via `login()`, `authenticate()` or `from_cookies()`.
    /// Anonymous or token-only clients will raise `AuthenticationError` before making network calls.
    pub fn get_tradingview_token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cookies_opt = self.user_cookies.read().clone();
        future_into_py(py, async move {
            let cookies = cookies_opt.ok_or_else(|| {
                AuthenticationError::new_err(
                    "Client must be authenticated via login(), authenticate() or from_cookies() with session cookies to retrieve a TradingView token; a supplied auth_token is not a cookie session",
                )
            })?;

            let token = tradingview::client::misc::get_tradingview_token(&cookies)
                .await
                .map_err(to_py_err)?;

            Ok(token)
        })
    }

    /// Retrieve historical OHLCV candlestick bars for a single symbol asynchronously.
    ///
    /// If `as_dataframe=True`, returns a Polars DataFrame directly. Setting
    /// `from_timestamp`, `max_bars` or `max_pages` pages back with
    /// `request_more_data`; see `HistoricalRequest` in the Rust crate.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        symbol,
        exchange,
        interval = Interval::OneDay,
        n_bars = None,
        with_replay = false,
        as_dataframe = false,
        *,
        server = None,
        from_timestamp = None,
        max_bars = None,
        max_pages = None,
        page_size = 5000,
        page_delay = 0.5,
        adjustment = None,
        session_type = None,
        currency = None,
    ))]
    pub fn get_historical<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        interval: Interval,
        n_bars: Option<u64>,
        with_replay: bool,
        as_dataframe: bool,
        server: Option<DataServer>,
        from_timestamp: Option<i64>,
        max_bars: Option<u64>,
        max_pages: Option<u32>,
        page_size: u64,
        page_delay: f64,
        adjustment: Option<MarketAdjustment>,
        session_type: Option<SessionType>,
        currency: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = HistoryOptions::parse(
            from_timestamp,
            max_bars,
            max_pages,
            page_size,
            page_delay,
            adjustment,
            session_type,
            currency,
        )?;
        let auth_token = self
            .auth_token
            .read()
            .clone()
            .unwrap_or_else(|| "unauthorized_user_token".to_string());
        let target_server: CoreDataServer = server.unwrap_or(self.server).into();

        future_into_py(py, async move {
            let client = HistoricalClient::new(&auth_token, target_server);
            let req = options
                .request(interval, n_bars, with_replay)
                .for_symbol(&symbol, &exchange);

            let hist_result = client.retrieve(req).await.map_err(to_py_err)?;
            let series = series_from_result(symbol, exchange, interval, hist_result);

            if as_dataframe {
                Python::attach(|py| {
                    let df = series.to_polars(py)?;
                    Ok(df)
                })
            } else {
                Python::attach(|py| {
                    let py_series = series.into_pyobject(py)?;
                    Ok(py_series.into_any().unbind())
                })
            }
        })
    }

    /// Retrieve historical data directly as a Polars DataFrame.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        symbol,
        exchange,
        interval = Interval::OneDay,
        n_bars = None,
        with_replay = false,
        *,
        server = None,
        from_timestamp = None,
        max_bars = None,
        max_pages = None,
        page_size = 5000,
        page_delay = 0.5,
        adjustment = None,
        session_type = None,
        currency = None,
    ))]
    pub fn get_historical_df<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        interval: Interval,
        n_bars: Option<u64>,
        with_replay: bool,
        server: Option<DataServer>,
        from_timestamp: Option<i64>,
        max_bars: Option<u64>,
        max_pages: Option<u32>,
        page_size: u64,
        page_delay: f64,
        adjustment: Option<MarketAdjustment>,
        session_type: Option<SessionType>,
        currency: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.get_historical(
            py,
            symbol,
            exchange,
            interval,
            n_bars,
            with_replay,
            true,
            server,
            from_timestamp,
            max_bars,
            max_pages,
            page_size,
            page_delay,
            adjustment,
            session_type,
            currency,
        )
    }

    /// Retrieve historical OHLCV data for multiple symbols concurrently.
    ///
    /// If `as_dataframe=True`, returns a dictionary mapping "EXCHANGE:SYMBOL" to Polars DataFrames.
    /// At most `max_concurrency` WebSocket connections are opened; each fetches its
    /// share of the symbols one after another. Symbols that fail are left out.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        symbols,
        interval = Interval::OneDay,
        n_bars = None,
        max_concurrency = 4,
        as_dataframe = false,
        *,
        server = None,
        from_timestamp = None,
        max_bars = None,
        max_pages = None,
        page_size = 5000,
        page_delay = 0.5,
        adjustment = None,
        session_type = None,
        currency = None,
    ))]
    pub fn get_historical_batch<'py>(
        &self,
        py: Python<'py>,
        symbols: Vec<(String, String)>,
        interval: Interval,
        n_bars: Option<u64>,
        max_concurrency: usize,
        as_dataframe: bool,
        server: Option<DataServer>,
        from_timestamp: Option<i64>,
        max_bars: Option<u64>,
        max_pages: Option<u32>,
        page_size: u64,
        page_delay: f64,
        adjustment: Option<MarketAdjustment>,
        session_type: Option<SessionType>,
        currency: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = HistoryOptions::parse(
            from_timestamp,
            max_bars,
            max_pages,
            page_size,
            page_delay,
            adjustment,
            session_type,
            currency,
        )?;
        let auth_token = self
            .auth_token
            .read()
            .clone()
            .unwrap_or_else(|| "unauthorized_user_token".to_string());
        let target_server: CoreDataServer = server.unwrap_or(self.server).into();

        future_into_py(py, async move {
            let client = HistoricalClient::new(&auth_token, target_server);
            let config = BatchConfig {
                max_concurrency,
                per_symbol_timeout: Duration::from_secs(30),
            };
            let template = options.request(interval, n_bars, false);

            let batch_result = client
                .retrieve_batch_with(&symbols, &template, config)
                .await;

            let mut series_map = HashMap::new();
            for sym_res in batch_result.successful {
                if let Ok(hist_result) = sym_res.result {
                    let key = format!("{}:{}", sym_res.exchange, sym_res.symbol);
                    series_map.insert(
                        key,
                        series_from_result(sym_res.symbol, sym_res.exchange, interval, hist_result),
                    );
                }
            }

            if as_dataframe {
                Python::attach(|py| {
                    let dict = PyDict::new(py);
                    for (k, series) in series_map {
                        let df = series.to_polars(py)?;
                        dict.set_item(k, df)?;
                    }
                    Ok(dict.into_any().unbind())
                })
            } else {
                Python::attach(|py| {
                    let py_map = series_map.into_pyobject(py)?;
                    Ok(py_map.into_any().unbind())
                })
            }
        })
    }

    /// Subscribe to live market quote streams asynchronously.
    #[pyo3(signature = (symbols, callback = None, *, server = None))]
    pub fn subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        symbols: Vec<String>,
        callback: Option<PyObject>,
        server: Option<DataServer>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let auth_token = self
            .auth_token
            .read()
            .clone()
            .unwrap_or_else(|| "unauthorized_user_token".to_string());
        let target_server: CoreDataServer = server.unwrap_or(self.server).into();
        let dispatcher = CallbackDispatcher::new();
        if let Some(cb) = callback {
            dispatcher.add_callback(py, cb)?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<QuoteTick>(1024);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        future_into_py(py, async move {
            let handler = QuoteStreamHandler {
                tx,
                dispatcher: dispatcher.clone(),
            };

            let ws = WebSocketClient::builder()
                .auth_token(&auth_token)
                .server(target_server)
                .handler(handler)
                .build()
                .await
                .map_err(to_py_err)?;

            Arc::clone(&ws).spawn_reader_task();

            let quote_session = tradingview::utils::gen_session_id("qs");
            ws.create_quote_session(&quote_session)
                .await
                .map_err(to_py_err)?;
            ws.set_fields(&quote_session).await.map_err(to_py_err)?;
            let symbol_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
            ws.add_symbols(&quote_session, &symbol_refs)
                .await
                .map_err(to_py_err)?;

            let ws_clone = Arc::clone(&ws);
            let qs_clone = quote_session.clone();
            tokio::spawn(async move {
                let _ = stop_rx.await;
                let _ = ws_clone.delete_quote_session(&qs_clone).await;
                let _ = ws_clone.close().await;
            });

            Ok(QuoteSubscription::new(rx, dispatcher, stop_tx))
        })
    }

    /// Subscribe to live candle/bar progress updates.
    #[pyo3(signature = (symbols, interval = Interval::OneMinute, callback = None, *, server = None))]
    pub fn subscribe_bars<'py>(
        &self,
        py: Python<'py>,
        symbols: Vec<String>,
        interval: Interval,
        callback: Option<PyObject>,
        server: Option<DataServer>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let auth_token = self
            .auth_token
            .read()
            .clone()
            .unwrap_or_else(|| "unauthorized_user_token".to_string());
        let target_server: CoreDataServer = server.unwrap_or(self.server).into();
        let dispatcher = CallbackDispatcher::new();
        if let Some(cb) = callback {
            dispatcher.add_callback(py, cb)?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<CandleUpdate>(1024);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        future_into_py(py, async move {
            let default_symbol = symbols.first().cloned().unwrap_or_default();
            let handler = CandleStreamHandler {
                tx,
                dispatcher: dispatcher.clone(),
                interval,
                default_symbol,
            };

            let ws = WebSocketClient::builder()
                .auth_token(&auth_token)
                .server(target_server)
                .handler(handler)
                .build()
                .await
                .map_err(to_py_err)?;

            Arc::clone(&ws).spawn_reader_task();

            let chart_session = format!("cs_{}", tradingview::utils::gen_id());
            ws.create_chart_session(&chart_session)
                .await
                .map_err(to_py_err)?;

            for (idx, sym) in symbols.iter().enumerate() {
                let symbol_series_id = format!("sds_sym_{}_{}", idx, tradingview::utils::gen_id());
                let series_identifier = format!("sds_{}", idx + 1);
                let series_id = format!("s{}", idx + 1);

                let symbol_init_str = tradingview::utils::symbol_init()
                    .instrument(sym)
                    .call()
                    .map_err(to_py_err)?;

                ws.send(
                    "resolve_symbol",
                    &[
                        serde_json::Value::from(chart_session.as_str()),
                        serde_json::Value::from(symbol_series_id.as_str()),
                        serde_json::Value::from(symbol_init_str),
                    ],
                )
                .await
                .map_err(to_py_err)?;

                let args = vec![
                    serde_json::Value::from(chart_session.as_str()),
                    serde_json::Value::from(series_identifier.as_str()),
                    serde_json::Value::from(series_id.as_str()),
                    serde_json::Value::from(symbol_series_id.as_str()),
                    serde_json::Value::from(interval.as_str()),
                    serde_json::Value::from(100u64),
                ];
                ws.send("create_series", &args).await.map_err(to_py_err)?;
            }

            let ws_clone = Arc::clone(&ws);
            let cs_clone = chart_session.clone();
            tokio::spawn(async move {
                let _ = stop_rx.await;
                let _ = ws_clone.delete_chart_session(&cs_clone).await;
                let _ = ws_clone.close().await;
            });

            Ok(BarSubscription::new(rx, dispatcher, stop_tx))
        })
    }

    /// Retrieve corporate fundamental metric history.
    ///
    /// If `as_dataframe=True`, returns a Polars DataFrame directly.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (symbol, exchange, fund_id, period = FinancialPeriod::FiscalYear, n_bars = 20, as_dataframe = false, *, server = None))]
    pub fn get_fundamental<'py>(
        &self,
        py: Python<'py>,
        symbol: String,
        exchange: String,
        fund_id: String,
        period: FinancialPeriod,
        n_bars: u64,
        as_dataframe: bool,
        server: Option<DataServer>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let auth_token = self
            .auth_token
            .read()
            .clone()
            .unwrap_or_else(|| "unauthorized_user_token".to_string());
        let target_server: CoreDataServer = server.unwrap_or(self.server).into();
        future_into_py(py, async move {
            let registry = tradingview::fundamental::fetch_fundamental_registry()
                .await
                .map_err(to_py_err)?;

            let tv_period: tradingview::models::FinancialPeriod = period.into();
            let study_res = tradingview::fundamental::get_fundamental_data(
                &registry,
                &fund_id,
                Some(&tv_period),
                &symbol,
                &exchange,
                tradingview::models::Interval::OneDay,
                n_bars,
                Some(&auth_token),
                target_server,
            )
            .await
            .map_err(to_py_err)?;

            let points: Vec<FundamentalPoint> = study_res
                .data
                .into_iter()
                .map(|dp| {
                    let ts = dp.value.first().copied().map(|v| v as i64).unwrap_or(0);
                    let val = if dp.value.len() >= 2 {
                        dp.value[1]
                    } else {
                        dp.value.first().copied().unwrap_or(0.0)
                    };
                    FundamentalPoint {
                        timestamp: ts,
                        value: val,
                        index: dp.index as i32,
                    }
                })
                .collect();

            let series = FundamentalSeries {
                symbol,
                exchange,
                fund_id,
                period,
                points,
            };

            if as_dataframe {
                Python::attach(|py| {
                    let df = series.to_polars(py)?;
                    Ok(df)
                })
            } else {
                Python::attach(|py| {
                    let py_series = series.into_pyobject(py)?;
                    Ok(py_series.into_any().unbind())
                })
            }
        })
    }

    /// Query global macroeconomic calendar events.
    ///
    /// If `as_dataframe=True`, returns a Polars DataFrame directly.
    #[pyo3(signature = (countries = None, from_timestamp = None, to_timestamp = None, min_importance = EconomicImportance::Medium, as_dataframe = false))]
    pub fn get_economic_calendar<'py>(
        &self,
        py: Python<'py>,
        countries: Option<Vec<String>>,
        from_timestamp: Option<i64>,
        to_timestamp: Option<i64>,
        min_importance: EconomicImportance,
        as_dataframe: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let now = chrono::Utc::now();
            let from_dt = from_timestamp
                .and_then(|ts| chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0))
                .unwrap_or(now);
            let to_dt = to_timestamp
                .and_then(|ts| chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0))
                .unwrap_or_else(|| now + chrono::Duration::days(7));

            let req = tradingview::client::fin_calendar::EconomicCalendarRequest::builder()
                .from(from_dt)
                .to(to_dt)
                .countries(countries.unwrap_or_default())
                .min_importance(min_importance.into())
                .build();

            let raw_events = tradingview::client::fin_calendar::get_economic_calendar(&req)
                .await
                .map_err(to_py_err)?;

            let events: Vec<EconomicEvent> = raw_events
                .into_iter()
                .map(|e| {
                    let importance = e
                        .importance_level()
                        .map(Into::into)
                        .unwrap_or(EconomicImportance::Medium);
                    EconomicEvent {
                        id: e.id,
                        title: e.title,
                        country: e.country,
                        indicator: e.indicator,
                        ticker: e.ticker.unwrap_or_default(),
                        date: e.date.timestamp(),
                        importance,
                        actual: e.actual,
                        forecast: e.forecast,
                        previous: e.previous,
                    }
                })
                .collect();

            if as_dataframe {
                Python::attach(|py| {
                    let polars = py.import("polars")?;
                    let dict = PyDict::new(py);
                    let ids: Vec<String> = events.iter().map(|e| e.id.clone()).collect();
                    let titles: Vec<String> = events.iter().map(|e| e.title.clone()).collect();
                    let countries: Vec<String> = events.iter().map(|e| e.country.clone()).collect();
                    let indicators: Vec<String> =
                        events.iter().map(|e| e.indicator.clone()).collect();
                    let tickers: Vec<String> = events.iter().map(|e| e.ticker.clone()).collect();
                    let dates: Vec<i64> = events.iter().map(|e| e.date).collect();
                    let importances: Vec<i32> =
                        events.iter().map(|e| e.importance.value()).collect();
                    let actuals: Vec<Option<f64>> = events.iter().map(|e| e.actual).collect();
                    let forecasts: Vec<Option<f64>> = events.iter().map(|e| e.forecast).collect();
                    let previouses: Vec<Option<f64>> = events.iter().map(|e| e.previous).collect();

                    dict.set_item("id", ids)?;
                    dict.set_item("title", titles)?;
                    dict.set_item("country", countries)?;
                    dict.set_item("indicator", indicators)?;
                    dict.set_item("ticker", tickers)?;
                    dict.set_item("date", dates)?;
                    dict.set_item("importance", importances)?;
                    dict.set_item("actual", actuals)?;
                    dict.set_item("forecast", forecasts)?;
                    dict.set_item("previous", previouses)?;

                    let df = polars.getattr("DataFrame")?.call1((dict,))?;
                    Ok(df.unbind())
                })
            } else {
                Python::attach(|py| {
                    let py_events = events.into_pyobject(py)?;
                    Ok(py_events.into_any().unbind())
                })
            }
        })
    }

    /// Close client and release background connections.
    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(()) })
    }
}
