use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

/// Symbol metadata returned by TradingView with historical bars.
#[pyclass(name = "SymbolInfo", eq, from_py_object)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SymbolInfo {
    /// Full TradingView id, e.g. `HOSE:VCB`.
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub exchange: String,
    #[pyo3(get)]
    pub listed_exchange: String,
    #[pyo3(get)]
    pub description: String,
    /// TradingView instrument type, e.g. `stock`, `spot`, `swap`, `futures`.
    #[pyo3(get)]
    pub market_type: String,
    #[pyo3(get)]
    pub currency_code: String,
    #[pyo3(get)]
    pub timezone: String,
    /// Trading session in exchange time, e.g. `0900-1500` or `24x7`.
    #[pyo3(get)]
    pub session: String,
    #[pyo3(get)]
    pub pricescale: u64,
    #[pyo3(get)]
    pub minmov: u64,
    #[pyo3(get)]
    pub minmove2: u64,
    /// Price-dependent tick sizes such as `10 10000 50 50000 100`; empty when constant.
    #[pyo3(get)]
    pub variable_tick_size: String,
    #[pyo3(get)]
    pub pointvalue: f64,
    /// Data delay in seconds (0 for real-time).
    #[pyo3(get)]
    pub delay: u64,
    #[pyo3(get)]
    pub isin: String,
}

impl SymbolInfo {
    fn as_core(&self) -> tradingview::SymbolInfo {
        tradingview::SymbolInfo {
            pricescale: self.pricescale,
            minmov: self.minmov,
            variable_tick_size: Ustr::from(&self.variable_tick_size),
            ..Default::default()
        }
    }
}

#[pymethods]
impl SymbolInfo {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        id = String::new(),
        name = String::new(),
        exchange = String::new(),
        *,
        listed_exchange = String::new(),
        description = String::new(),
        market_type = String::new(),
        currency_code = String::new(),
        timezone = String::new(),
        session = String::new(),
        pricescale = 0,
        minmov = 0,
        minmove2 = 0,
        variable_tick_size = String::new(),
        pointvalue = 0.0,
        delay = 0,
        isin = String::new(),
    ))]
    pub fn new(
        id: String,
        name: String,
        exchange: String,
        listed_exchange: String,
        description: String,
        market_type: String,
        currency_code: String,
        timezone: String,
        session: String,
        pricescale: u64,
        minmov: u64,
        minmove2: u64,
        variable_tick_size: String,
        pointvalue: f64,
        delay: u64,
        isin: String,
    ) -> Self {
        Self {
            id,
            name,
            exchange,
            listed_exchange,
            description,
            market_type,
            currency_code,
            timezone,
            session,
            pricescale,
            minmov,
            minmove2,
            variable_tick_size,
            pointvalue,
            delay,
            isin,
        }
    }

    /// Minimum price increment `minmov / pricescale`, or None when unknown.
    #[getter]
    pub fn tick_size(&self) -> Option<f64> {
        self.as_core().tick_size()
    }

    /// Tick size at `price`, honouring `variable_tick_size` when it is set.
    pub fn tick_size_at(&self, price: f64) -> Option<f64> {
        self.as_core().tick_size_at(price)
    }

    /// Return the metadata as a Python dictionary (including `tick_size`).
    pub fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", &self.id)?;
        dict.set_item("name", &self.name)?;
        dict.set_item("exchange", &self.exchange)?;
        dict.set_item("listed_exchange", &self.listed_exchange)?;
        dict.set_item("description", &self.description)?;
        dict.set_item("market_type", &self.market_type)?;
        dict.set_item("currency_code", &self.currency_code)?;
        dict.set_item("timezone", &self.timezone)?;
        dict.set_item("session", &self.session)?;
        dict.set_item("pricescale", self.pricescale)?;
        dict.set_item("minmov", self.minmov)?;
        dict.set_item("minmove2", self.minmove2)?;
        dict.set_item("variable_tick_size", &self.variable_tick_size)?;
        dict.set_item("pointvalue", self.pointvalue)?;
        dict.set_item("delay", self.delay)?;
        dict.set_item("isin", &self.isin)?;
        dict.set_item("tick_size", self.tick_size())?;
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        let tick_size = self
            .tick_size()
            .map_or_else(|| "None".to_string(), |tick| format!("{tick:?}"));
        format!(
            "SymbolInfo(id='{}', market_type='{}', currency_code='{}', timezone='{}', pricescale={}, minmov={}, tick_size={tick_size})",
            self.id,
            self.market_type,
            self.currency_code,
            self.timezone,
            self.pricescale,
            self.minmov,
        )
    }
}

impl From<&tradingview::SymbolInfo> for SymbolInfo {
    fn from(info: &tradingview::SymbolInfo) -> Self {
        Self {
            id: info.id.to_string(),
            name: info.name.to_string(),
            exchange: info.exchange.to_string(),
            listed_exchange: info.listed_exchange.to_string(),
            description: info.description.to_string(),
            market_type: info.market_type.to_string(),
            currency_code: info.currency_code.to_string(),
            timezone: info.timezone.to_string(),
            session: info.session.to_string(),
            pricescale: info.pricescale,
            minmov: info.minmov,
            minmove2: info.minmove2,
            variable_tick_size: info.variable_tick_size.to_string(),
            pointvalue: info.pointvalue,
            delay: info.delay,
            isin: info.isin.to_string(),
        }
    }
}
