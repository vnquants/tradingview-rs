use crate::{MarketSymbol, MarketType, websocket::SeriesInfo};
use bon::Builder;
use chrono::{DateTime, Utc};
use iso_currency::Currency;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ustr::Ustr;

#[derive(Debug, Clone, Deserialize, Serialize, Copy, PartialEq, Eq, Hash)]
pub enum ChartType {
    HeikinAshi,
    Renko,
    LineBreak,
    Kagi,
    PointAndFigure,
    Range,
}

impl std::fmt::Display for ChartType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let chart_type = match self {
            ChartType::HeikinAshi => "BarSetHeikenAshi@tv-basicstudies-60!",
            ChartType::Renko => "BarSetRenko@tv-prostudies-40!",
            ChartType::LineBreak => "BarSetPriceBreak@tv-prostudies-34!",
            ChartType::Kagi => "BarSetKagi@tv-prostudies-34!",
            ChartType::PointAndFigure => "BarSetPnF@tv-prostudies-34!",
            ChartType::Range => "BarSetRange@tv-basicstudies-72!",
        };
        write!(f, "{chart_type}")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SeriesDataResponse {
    String(Ustr),
    ChartResponseData(ChartResponseData),
    SymbolInfo(Box<SymbolInfo>),
    StudyResponseData(StudyResponseData),
    JsonValue(Value),
}

#[derive(Debug, Clone, Deserialize, Serialize, Builder)]
pub struct ChartHistoricalData {
    pub symbol_info: SymbolInfo,
    pub series_info: SeriesInfo,
    pub data: Vec<DataPoint>,
}

impl ChartHistoricalData {
    pub fn new() -> Self {
        Self {
            symbol_info: SymbolInfo::default(),
            series_info: SeriesInfo::default(),
            data: Vec::new(),
        }
    }
}

impl Default for ChartHistoricalData {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceIterable for ChartHistoricalData {
    type Item = DataPoint;

    fn to_vec(&self) -> impl Iterator<Item = &Self::Item> + '_ {
        self.data.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ChartResponseData {
    #[serde(default)]
    pub node: Option<Ustr>,
    #[serde(rename(deserialize = "s"))]
    pub series: Vec<DataPoint>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct StudyResponseData {
    #[serde(default)]
    pub node: Option<Ustr>,
    #[serde(rename(deserialize = "st"))]
    pub studies: Vec<DataPoint>,
    #[serde(rename(deserialize = "ns"))]
    pub raw_graphics: GraphicDataResponse,
}

// TODO: Implement graphic parser for indexes response
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct GraphicDataResponse {
    pub d: Ustr,
    pub indexes: Value,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug, Default)]
pub struct DataPoint {
    #[serde(rename(deserialize = "i"))]
    pub index: i64,
    #[serde(rename(deserialize = "v"))]
    pub value: Vec<f64>,
}

pub trait PriceIterable {
    type Item: OHLCV;

    fn to_vec(&self) -> impl Iterator<Item = &Self::Item> + '_;

    fn closes(&self) -> impl Iterator<Item = f64> + '_ {
        self.to_vec().map(|dp| dp.close())
    }

    fn opens(&self) -> impl Iterator<Item = f64> + '_ {
        self.to_vec().map(|dp| dp.open())
    }

    fn highs(&self) -> impl Iterator<Item = f64> + '_ {
        self.to_vec().map(|dp| dp.high())
    }

    fn lows(&self) -> impl Iterator<Item = f64> + '_ {
        self.to_vec().map(|dp| dp.low())
    }

    fn volumes(&self) -> impl Iterator<Item = f64> + '_ {
        self.to_vec().map(|dp| dp.volume())
    }

    fn datetimes(&self) -> impl Iterator<Item = DateTime<Utc>> + '_ {
        self.to_vec().map(|dp| dp.datetime())
    }

    fn timestamps(&self) -> impl Iterator<Item = i64> + '_ {
        self.to_vec().map(|dp| dp.timestamp())
    }
}

impl PriceIterable for Vec<DataPoint> {
    type Item = DataPoint;

    fn to_vec(&self) -> impl Iterator<Item = &Self::Item> + '_ {
        self.iter()
    }
}

pub trait OHLCV {
    fn datetime(&self) -> DateTime<Utc>;
    fn timestamp(&self) -> i64;
    fn open(&self) -> f64;
    fn high(&self) -> f64;
    fn low(&self) -> f64;
    fn close(&self) -> f64;
    fn volume(&self) -> f64;
    fn validate(&self) -> bool {
        !(self.close() > self.high() || self.close() < self.low() || self.high() < self.low())
            && self.close() > 0.
            && self.open() > 0.
            && self.high() > 0.
            && self.low() > 0.
            && self.close().is_finite()
            && self.open().is_finite()
            && self.high().is_finite()
            && self.low().is_finite()
            && (self.volume().is_nan() || self.volume() >= 0.0)
    }

    fn tr(&self, prev_candle: &dyn OHLCV) -> f64 {
        self.tr_close(prev_candle.close())
    }

    fn tr_close(&self, prev_close: f64) -> f64 {
        self.high().max(prev_close) - self.low().min(prev_close)
    }

    fn clv(&self) -> f64 {
        if self.high() == self.low() {
            0.
        } else {
            let twice: f64 = 2.;
            (twice.mul_add(self.close(), -self.low()) - self.high()) / (self.high() - self.low())
        }
    }

    fn ohlc4(&self) -> f64 {
        (self.high() + self.low() + self.close() + self.open()) * 0.25
    }

    fn hl2(&self) -> f64 {
        (self.high() + self.low()) * 0.5
    }

    fn tp(&self) -> f64 {
        (self.high() + self.low() + self.close()) / 3.
    }

    fn volumed_price(&self) -> f64 {
        self.tp() * self.volume()
    }

    fn is_rising(&self) -> bool {
        self.close() > self.open()
    }

    fn is_falling(&self) -> bool {
        self.close() < self.open()
    }
}

impl OHLCV for DataPoint {
    fn datetime(&self) -> DateTime<Utc> {
        let ts = self.timestamp();
        DateTime::<Utc>::from_timestamp(ts, 0).unwrap_or_default()
    }

    fn timestamp(&self) -> i64 {
        self.value.first().copied().map(|v| v as i64).unwrap_or(0)
    }

    fn open(&self) -> f64 {
        if self.value.len() < 5 {
            return f64::NAN;
        }
        self.value[1]
    }

    fn high(&self) -> f64 {
        if self.value.len() < 5 {
            return f64::NAN;
        }
        self.value[2]
    }

    fn low(&self) -> f64 {
        if self.value.len() < 5 {
            return f64::NAN;
        }
        self.value[3]
    }

    fn close(&self) -> f64 {
        if self.value.len() < 5 {
            return f64::NAN;
        }
        self.value[4]
    }

    fn volume(&self) -> f64 {
        if self.value.len() < 6 {
            return f64::NAN;
        }
        self.value[5]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartDataChanges {
    pub changes: Vec<f64>,
    pub index: i64,
    pub index_diff: Vec<Value>,
    pub marks: Vec<Value>,
    pub zoffset: i64,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Hash, Debug, Default, Copy)]
pub struct SeriesCompletedMessage {
    #[serde(default)]
    pub id: Ustr,
    #[serde(default)]
    pub update_mode: Ustr,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default, Builder, Copy)]
pub struct Ticker {
    pub symbol: Ustr,
    pub exchange: Ustr,
    pub currency: Option<Currency>,
    pub country: Option<Currency>,
    pub market_type: Option<MarketType>,
}

impl Ticker {
    pub fn new(symbol: &str, exchange: &str) -> Self {
        Self {
            symbol: Ustr::from(symbol),
            exchange: Ustr::from(exchange),
            currency: None,
            country: None,
            market_type: None,
        }
    }
}

impl MarketSymbol for Ticker {
    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn exchange(&self) -> &str {
        &self.exchange
    }

    fn id(&self) -> String {
        format!("{}:{}", self.exchange, self.symbol)
    }

    fn new<S: Into<String>>(symbol: S, exchange: S) -> Self {
        Self {
            symbol: Ustr::from(&symbol.into()),
            exchange: Ustr::from(&exchange.into()),
            currency: None,
            country: None,
            market_type: None,
        }
    }
}

impl From<&SymbolInfo> for Ticker {
    fn from(symbol_info: &SymbolInfo) -> Self {
        Self {
            symbol: symbol_info.name,
            exchange: symbol_info.exchange,
            currency: Currency::from_code(symbol_info.currency_id.as_str()),
            country: None,
            market_type: Some(MarketType::from(symbol_info.market_type.as_str())),
        }
    }
}

impl From<SymbolInfo> for Ticker {
    fn from(val: SymbolInfo) -> Self {
        Ticker::from(&val)
    }
}

/// Symbol metadata from TradingView's `symbol_resolved` message.
///
/// Every field is parsed leniently: a missing, `null` or wrongly typed value
/// takes the field's default instead of failing the whole payload.
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct SymbolInfo {
    #[serde(rename(deserialize = "pro_name"))]
    #[serde(deserialize_with = "lenient::ustr")]
    pub id: Ustr,

    #[serde(rename(deserialize = "original_name"))]
    #[serde(deserialize_with = "lenient::ustr")]
    pub original_name: Ustr,

    #[serde(deserialize_with = "lenient::ustr")]
    pub name: Ustr,
    #[serde(deserialize_with = "lenient::ustr")]
    pub exchange: Ustr,
    #[serde(deserialize_with = "lenient::ustr")]
    pub description: Ustr,

    #[serde(rename = "business_description")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub business_description: Ustr,

    #[serde(rename = "listed_exchange")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub listed_exchange: Ustr,

    #[serde(rename = "provider_id")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub provider_id: Ustr,

    #[serde(rename = "base_currency")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub base_currency: Ustr,

    #[serde(rename = "base_currency_id")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub base_currency_id: Ustr,

    #[serde(rename = "total_revenue")]
    #[serde(deserialize_with = "lenient::f64")]
    pub total_revenue: f64,

    #[serde(rename = "price_earnings_ttm")]
    #[serde(deserialize_with = "lenient::f64")]
    pub price_earnings_ttm: f64,

    #[serde(rename = "currency_id")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub currency_id: Ustr,

    #[serde(rename = "currency_code")]
    #[serde(deserialize_with = "lenient::ustr")]
    pub currency_code: Ustr,

    #[serde(deserialize_with = "lenient::ustr")]
    pub session_holidays: Ustr,

    #[serde(deserialize_with = "lenient::vec")]
    pub subsessions: Vec<Subsession>,

    #[serde(deserialize_with = "lenient::ustr")]
    pub timezone: Ustr,

    #[serde(rename(deserialize = "type"))]
    #[serde(deserialize_with = "lenient::ustr")]
    pub market_type: Ustr,

    #[serde(deserialize_with = "lenient::vec")]
    pub typespecs: Vec<Ustr>,

    #[serde(deserialize_with = "lenient::vec")]
    pub aliases: Vec<Ustr>,

    #[serde(deserialize_with = "lenient::f64")]
    pub total_shares_outstanding_calculated: f64,

    #[serde(deserialize_with = "lenient::f64")]
    pub market_cap_basic: f64,

    #[serde(deserialize_with = "lenient::i64")]
    pub earnings_release_date: i64,

    #[serde(deserialize_with = "lenient::vec")]
    pub base_name: Vec<Ustr>,

    #[serde(deserialize_with = "lenient::ustr")]
    pub sector: Ustr,

    #[serde(deserialize_with = "lenient::ustr")]
    pub current_session: Ustr,

    #[serde(deserialize_with = "lenient::u16")]
    pub founded: u16,

    #[serde(deserialize_with = "lenient::f64")]
    pub last_annual_eps: f64,

    #[serde(deserialize_with = "lenient::bool")]
    pub fractional: bool,

    #[serde(deserialize_with = "lenient::ustr")]
    pub industry: Ustr,

    /// Trading session in exchange time, e.g. `0900-1500` or `24x7`.
    #[serde(deserialize_with = "lenient::ustr")]
    pub session: Ustr,

    /// Price scale. Prices move in steps of `minmov / pricescale`; see
    /// [`SymbolInfo::tick_size`]. `0` when the server did not send it.
    #[serde(deserialize_with = "lenient::u64")]
    pub pricescale: u64,

    /// Minimum price movement in units of `1 / pricescale`.
    #[serde(deserialize_with = "lenient::u64")]
    pub minmov: u64,

    /// Fraction-of-a-fraction denominator for fractional price formats
    /// (`0` for decimal prices). It affects formatting, not the tick size.
    #[serde(deserialize_with = "lenient::u64")]
    pub minmove2: u64,

    /// Price-dependent tick sizes, e.g. `10 10000 50 50000 100`: tick 10
    /// below 10000, tick 50 below 50000, tick 100 above. Empty when the tick
    /// size is constant; see [`SymbolInfo::tick_size_at`].
    #[serde(deserialize_with = "lenient::ustr")]
    pub variable_tick_size: Ustr,

    /// Currency value of a one-point price move for one contract (futures);
    /// `1.0` for most stocks and crypto pairs, `0.0` when not sent.
    #[serde(deserialize_with = "lenient::f64")]
    pub pointvalue: f64,

    /// Data delay in seconds (`900` for 15-minute delayed feeds, `0` for
    /// real-time or when not sent).
    #[serde(deserialize_with = "lenient::u64")]
    pub delay: u64,

    /// ISIN, when TradingView knows it.
    #[serde(deserialize_with = "lenient::ustr")]
    pub isin: Ustr,
}

impl SymbolInfo {
    /// Minimum price increment, `minmov / pricescale`.
    ///
    /// Returns `None` when the server did not send a positive `pricescale`
    /// and `minmov`. For symbols with a [`variable_tick_size`] this is the
    /// tick at the price the server last saw; use [`Self::tick_size_at`] for
    /// another price level.
    ///
    /// [`variable_tick_size`]: SymbolInfo::variable_tick_size
    pub fn tick_size(&self) -> Option<f64> {
        (self.pricescale > 0 && self.minmov > 0)
            .then(|| self.minmov as f64 / self.pricescale as f64)
    }

    /// Tick size that applies at `price`.
    ///
    /// Uses [`variable_tick_size`] when it is set (tick, upper bound, tick,
    /// upper bound, ..., tick, where a bound is exclusive) and
    /// [`Self::tick_size`] when it is empty. Returns `None` for a non-finite
    /// price or a `variable_tick_size` that cannot be parsed.
    ///
    /// [`variable_tick_size`]: SymbolInfo::variable_tick_size
    pub fn tick_size_at(&self, price: f64) -> Option<f64> {
        if !price.is_finite() {
            return None;
        }
        if self.variable_tick_size.trim().is_empty() {
            return self.tick_size();
        }
        let levels = parse_variable_tick_size(&self.variable_tick_size)?;
        levels.chunks(2).find_map(|level| match *level {
            [tick, upper_bound] => (price < upper_bound).then_some(tick),
            [tick] => Some(tick),
            _ => None,
        })
    }
}

/// Parse `"t0 b0 t1 b1 ... tn"` into numbers. Returns `None` for an empty or
/// malformed value: an even number of items, a non-numeric or non-positive
/// item, or bounds that do not increase.
fn parse_variable_tick_size(raw: &str) -> Option<Vec<f64>> {
    let values = raw
        .split_whitespace()
        .map(|item| {
            item.parse::<f64>()
                .ok()
                .filter(|v| v.is_finite() && *v > 0.0)
        })
        .collect::<Option<Vec<f64>>>()?;
    if values.len() % 2 == 0 {
        return None;
    }
    let bounds_increase = values
        .iter()
        .skip(1)
        .step_by(2)
        .zip(values.iter().skip(3).step_by(2))
        .all(|(lower, upper)| lower < upper);
    bounds_increase.then_some(values)
}

/// Deserializers that never fail on a value of an unexpected JSON type, so
/// one odd metadata field cannot make the whole `symbol_resolved` payload
/// unreadable. Unusable values become the field's default.
mod lenient {
    use serde::{Deserialize, Deserializer};
    use serde_json::Value;
    use ustr::Ustr;

    /// An integral value in `[min, max]`, from a JSON number or numeric
    /// string; `None` for fractions, out-of-range or non-numeric values.
    fn integral(value: &Value, min: f64, max: f64) -> Option<f64> {
        let number = match value {
            Value::Number(n) => n.as_f64()?,
            Value::String(s) => s.trim().parse::<f64>().ok()?,
            _ => return None,
        };
        (number.is_finite() && number.fract() == 0.0 && number >= min && number <= max)
            .then_some(number)
    }

    /// Unsigned integer; fractional, negative or out-of-range values become 0.
    pub(super) fn u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if let Some(exact) = value.as_u64() {
            return Ok(exact);
        }
        // 2^53: larger f64 values are not exact integers.
        Ok(integral(&value, 0.0, 9_007_199_254_740_992.0).map_or(0, |v| v as u64))
    }

    pub(super) fn i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if let Some(exact) = value.as_i64() {
            return Ok(exact);
        }
        Ok(
            integral(&value, -9_007_199_254_740_992.0, 9_007_199_254_740_992.0)
                .map_or(0, |v| v as i64),
        )
    }

    pub(super) fn u16<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Ok(integral(&value, 0.0, f64::from(u16::MAX)).map_or(0, |v| v as u16))
    }

    pub(super) fn f64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        Ok(match Value::deserialize(deserializer)? {
            Value::Number(n) => n.as_f64().unwrap_or_default(),
            Value::String(s) => s.trim().parse().unwrap_or_default(),
            _ => 0.0,
        })
    }

    pub(super) fn bool<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
        Ok(match Value::deserialize(deserializer)? {
            Value::Bool(b) => b,
            Value::String(s) => s.trim().eq_ignore_ascii_case("true"),
            _ => false,
        })
    }

    /// A list whose unreadable elements are skipped; anything but an array
    /// becomes an empty list.
    pub(super) fn vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: serde::de::DeserializeOwned,
    {
        Ok(match Value::deserialize(deserializer)? {
            Value::Array(items) => items
                .into_iter()
                .filter_map(|item| serde_json::from_value(item).ok())
                .collect(),
            _ => Vec::new(),
        })
    }

    pub(super) fn ustr<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Ustr, D::Error> {
        Ok(match Value::deserialize(deserializer)? {
            Value::String(s) => Ustr::from(&s),
            Value::Number(n) => Ustr::from(&n.to_string()),
            _ => Ustr::default(),
        })
    }
}

impl MarketSymbol for SymbolInfo {
    fn symbol(&self) -> &str {
        self.name.as_str()
    }

    fn exchange(&self) -> &str {
        self.exchange.as_str()
    }

    fn id(&self) -> String {
        self.id.to_string()
    }

    fn new<S: Into<String>>(symbol: S, exchange: S) -> Self {
        Self {
            name: Ustr::from(&symbol.into()),
            exchange: Ustr::from(&exchange.into()),
            ..Default::default()
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Hash, Debug, Default, Copy)]
#[serde(rename_all = "camelCase", default)]
pub struct Subsession {
    pub id: Ustr,
    pub description: Ustr,
    pub private: bool,
    pub session: Ustr,
    #[serde(rename(deserialize = "session-display"))]
    pub session_display: Ustr,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a valid OHLCV `DataPoint` from raw values.
    /// Layout: [timestamp, open, high, low, close, volume]
    fn dp(ts: i64, o: f64, h: f64, l: f64, c: f64, v: f64) -> DataPoint {
        DataPoint {
            index: 0,
            value: vec![ts as f64, o, h, l, c, v],
        }
    }

    /// Build a vector of `N` consecutive daily candles starting at `base_ts`.
    fn make_candles(n: usize) -> Vec<DataPoint> {
        let base_ts = 1_700_000_000i64; // Nov 2023
        (0..n)
            .map(|i| {
                let ts = base_ts + i as i64 * 86_400;
                let o = 100.0 + i as f64;
                let c = 101.0 + i as f64;
                dp(ts, o, o + 1.0, o - 1.0, c, 1000.0 + i as f64)
            })
            .collect()
    }

    /// Helper: build a `ChartHistoricalData` with the given candles.
    /// Constructs a valid `ChartOptions` to satisfy the `SeriesInfo` invariant.
    fn make_chart(data: Vec<DataPoint>) -> ChartHistoricalData {
        let options = crate::chart::ChartOptions::builder()
            .instrument("NASDAQ:AAPL")
            .build()
            .expect("valid ChartOptions for testing");
        ChartHistoricalData {
            symbol_info: SymbolInfo::default(),
            series_info: SeriesInfo {
                chart_session: Ustr::default(),
                options,
            },
            data,
        }
    }

    // ------------------------------------------------------------------
    // Vec<DataPoint> — PriceIterable
    // ------------------------------------------------------------------

    #[test]
    fn test_vec_to_vec_returns_references() {
        let candles = make_candles(5);
        let collected: Vec<&DataPoint> = candles.to_vec().collect();
        assert_eq!(collected.len(), 5);
        // The original Vec is still usable — we only borrowed.
        assert_eq!(candles.len(), 5);
    }

    #[test]
    fn test_vec_closes() {
        let candles = make_candles(3);
        let closes: Vec<f64> = candles.closes().collect();
        assert_eq!(closes, vec![101.0, 102.0, 103.0]);
    }

    #[test]
    fn test_vec_opens() {
        let candles = make_candles(3);
        let opens: Vec<f64> = candles.opens().collect();
        assert_eq!(opens, vec![100.0, 101.0, 102.0]);
    }

    #[test]
    fn test_vec_highs() {
        let candles = make_candles(3);
        let highs: Vec<f64> = candles.highs().collect();
        assert_eq!(highs, vec![101.0, 102.0, 103.0]);
    }

    #[test]
    fn test_vec_lows() {
        let candles = make_candles(3);
        let lows: Vec<f64> = candles.lows().collect();
        assert_eq!(lows, vec![99.0, 100.0, 101.0]);
    }

    #[test]
    fn test_vec_volumes() {
        let candles = make_candles(3);
        let volumes: Vec<f64> = candles.volumes().collect();
        assert_eq!(volumes, vec![1000.0, 1001.0, 1002.0]);
    }

    #[test]
    fn test_vec_timestamps() {
        let candles = make_candles(3);
        let base = 1_700_000_000i64;
        let timestamps: Vec<i64> = candles.timestamps().collect();
        assert_eq!(timestamps, vec![base, base + 86_400, base + 2 * 86_400]);
    }

    #[test]
    fn test_vec_datetimes() {
        let candles = make_candles(2);
        let base = 1_700_000_000i64;
        let datetimes: Vec<DateTime<Utc>> = candles.datetimes().collect();
        assert_eq!(datetimes.len(), 2);
        // First candle timestamp
        let expected = DateTime::<Utc>::from_timestamp(base, 0).unwrap();
        assert_eq!(datetimes[0], expected);
    }

    // ------------------------------------------------------------------
    // ChartHistoricalData — PriceIterable
    // ------------------------------------------------------------------

    #[test]
    fn test_chart_historical_to_vec_returns_references() {
        let candles = make_candles(5);
        let chart = make_chart(candles.clone());
        let collected: Vec<&DataPoint> = chart.to_vec().collect();
        assert_eq!(collected.len(), 5);
        // Original chart still usable — to_vec() borrows.
        assert_eq!(chart.data.len(), 5);
    }

    #[test]
    fn test_chart_historical_closes() {
        let candles = make_candles(3);
        let chart = make_chart(candles);
        let closes: Vec<f64> = chart.closes().collect();
        assert_eq!(closes, vec![101.0, 102.0, 103.0]);
    }

    #[test]
    fn test_chart_historical_opens() {
        let candles = make_candles(3);
        let chart = make_chart(candles);
        let opens: Vec<f64> = chart.opens().collect();
        assert_eq!(opens, vec![100.0, 101.0, 102.0]);
    }

    #[test]
    fn test_chart_historical_highs() {
        let candles = make_candles(3);
        let chart = make_chart(candles);
        let highs: Vec<f64> = chart.highs().collect();
        assert_eq!(highs, vec![101.0, 102.0, 103.0]);
    }

    #[test]
    fn test_chart_historical_lows() {
        let candles = make_candles(3);
        let chart = make_chart(candles);
        let lows: Vec<f64> = chart.lows().collect();
        assert_eq!(lows, vec![99.0, 100.0, 101.0]);
    }

    #[test]
    fn test_chart_historical_volumes() {
        let candles = make_candles(3);
        let chart = make_chart(candles);
        let volumes: Vec<f64> = chart.volumes().collect();
        assert_eq!(volumes, vec![1000.0, 1001.0, 1002.0]);
    }

    #[test]
    fn test_chart_historical_timestamps() {
        let candles = make_candles(3);
        let chart = make_chart(candles);
        let base = 1_700_000_000i64;
        let timestamps: Vec<i64> = chart.timestamps().collect();
        assert_eq!(timestamps, vec![base, base + 86_400, base + 2 * 86_400]);
    }

    #[test]
    fn test_empty_vec_no_panic() {
        let empty: Vec<DataPoint> = vec![];
        // All iterator methods should work on empty data.
        assert_eq!(empty.to_vec().count(), 0);
        assert_eq!(empty.closes().count(), 0);
        assert_eq!(empty.opens().count(), 0);
        assert_eq!(empty.highs().count(), 0);
        assert_eq!(empty.lows().count(), 0);
        assert_eq!(empty.volumes().count(), 0);
        assert_eq!(empty.timestamps().count(), 0);
        assert_eq!(empty.datetimes().count(), 0);
    }

    #[test]
    fn test_empty_chart_no_panic() {
        let chart = make_chart(vec![]);
        assert_eq!(chart.to_vec().count(), 0);
        assert_eq!(chart.closes().count(), 0);
        assert_eq!(chart.opens().count(), 0);
        assert_eq!(chart.highs().count(), 0);
        assert_eq!(chart.lows().count(), 0);
        assert_eq!(chart.volumes().count(), 0);
        assert_eq!(chart.timestamps().count(), 0);
        assert_eq!(chart.datetimes().count(), 0);
    }

    // ------------------------------------------------------------------
    // Consistency: both impls must produce the same output for the same data
    // ------------------------------------------------------------------

    #[test]
    fn test_vec_and_chart_produce_same_closes() {
        let candles = make_candles(50);
        let chart = make_chart(candles.clone());
        let from_vec: Vec<f64> = candles.closes().collect();
        let from_chart: Vec<f64> = chart.closes().collect();
        assert_eq!(from_vec, from_chart);
    }

    #[test]
    fn test_vec_and_chart_produce_same_opens() {
        let candles = make_candles(50);
        let chart = make_chart(candles.clone());
        let from_vec: Vec<f64> = candles.opens().collect();
        let from_chart: Vec<f64> = chart.opens().collect();
        assert_eq!(from_vec, from_chart);
    }

    #[test]
    fn test_vec_and_chart_produce_same_timestamps() {
        let candles = make_candles(50);
        let chart = make_chart(candles.clone());
        let from_vec: Vec<i64> = candles.timestamps().collect();
        let from_chart: Vec<i64> = chart.timestamps().collect();
        assert_eq!(from_vec, from_chart);
    }

    // ------------------------------------------------------------------
    // OHLCV trait methods
    // ------------------------------------------------------------------

    #[test]
    fn test_ohlcv_validate_rejects_invalid_high_low() {
        // high < low
        let bad = dp(1_700_000_000, 100.0, 99.0, 100.0, 100.5, 1000.0);
        assert!(!bad.validate());
    }

    #[test]
    fn test_ohlcv_validate_rejects_close_above_high() {
        let bad = dp(1_700_000_000, 100.0, 105.0, 99.0, 106.0, 1000.0);
        assert!(!bad.validate());
    }

    #[test]
    fn test_ohlcv_validate_rejects_close_below_low() {
        let bad = dp(1_700_000_000, 100.0, 105.0, 99.0, 98.0, 1000.0);
        assert!(!bad.validate());
    }

    #[test]
    fn test_ohlcv_validate_accepts_valid_candle() {
        let good = dp(1_700_000_000, 100.0, 105.0, 99.0, 102.0, 1000.0);
        assert!(good.validate());
    }

    #[test]
    fn test_ohlcv_tr() {
        let prev = dp(1_700_000_000, 100.0, 105.0, 99.0, 102.0, 1000.0);
        let curr = dp(1_700_086_400, 101.0, 108.0, 100.0, 104.0, 1100.0);
        // TR = max(high, prev_close) - min(low, prev_close)
        // = max(108, 102) - min(100, 102) = 108 - 100 = 8
        let tr = curr.tr(&prev);
        assert!((tr - 8.0).abs() < 1e-10);
    }

    #[test]
    fn test_ohlcv_is_rising() {
        let rising = dp(1_700_000_000, 100.0, 105.0, 99.0, 102.0, 1000.0);
        assert!(rising.is_rising());
    }

    // ------------------------------------------------------------------
    // SymbolInfo metadata
    // ------------------------------------------------------------------

    /// The `symbol_resolved` payload for HOSE:VCB in the recorded fixture.
    fn fixture_symbol_info_value() -> Value {
        let events: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/data/events.json")).unwrap();
        events
            .into_iter()
            .find(|event| event["m"] == "symbol_resolved")
            .map(|event| event["p"][2].clone())
            .expect("fixture contains symbol_resolved")
    }

    #[test]
    fn test_symbol_info_parses_metadata_from_fixture() {
        let info = SymbolInfo::deserialize(&fixture_symbol_info_value()).unwrap();
        assert_eq!(info.id, "HOSE:VCB");
        assert_eq!(info.name, "VCB");
        assert_eq!(info.exchange, "HOSE");
        assert_eq!(info.pricescale, 1);
        assert_eq!(info.minmov, 100);
        assert_eq!(info.minmove2, 0);
        assert_eq!(info.variable_tick_size, "10 10000 50 50000 100");
        assert_eq!(info.pointvalue, 1.0);
        assert_eq!(info.delay, 900);
        assert_eq!(info.isin, "VN000000VCB4");
        assert_eq!(info.session, "0900-1500");
        assert_eq!(info.timezone, "Asia/Ho_Chi_Minh");
        assert_eq!(info.currency_code, "VND");
    }

    #[test]
    fn test_symbol_info_tick_size_from_fixture() {
        let info = SymbolInfo::deserialize(&fixture_symbol_info_value()).unwrap();
        assert_eq!(info.tick_size(), Some(100.0));
        assert_eq!(info.tick_size_at(9_990.0), Some(10.0));
        assert_eq!(info.tick_size_at(10_000.0), Some(50.0));
        assert_eq!(info.tick_size_at(49_950.0), Some(50.0));
        assert_eq!(info.tick_size_at(65_700.0), Some(100.0));
    }

    #[test]
    fn test_symbol_info_tick_size_constant_and_missing() {
        let crypto = SymbolInfo {
            pricescale: 100,
            minmov: 1,
            ..Default::default()
        };
        assert_eq!(crypto.tick_size(), Some(0.01));
        assert_eq!(crypto.tick_size_at(91_000.0), Some(0.01));

        let single_level = SymbolInfo {
            pricescale: 100,
            minmov: 1,
            variable_tick_size: Ustr::from("0.01"),
            ..Default::default()
        };
        assert_eq!(single_level.tick_size_at(2.0), Some(0.01));

        let malformed = SymbolInfo {
            pricescale: 4,
            minmov: 1,
            variable_tick_size: Ustr::from("10 abc 50"),
            ..Default::default()
        };
        assert_eq!(malformed.tick_size(), Some(0.25));
        assert_eq!(malformed.tick_size_at(5.0), None, "no silent fallback");
        assert_eq!(crypto.tick_size_at(f64::NAN), None);
        assert_eq!(crypto.tick_size_at(f64::INFINITY), None);

        let missing = SymbolInfo::default();
        assert_eq!(missing.tick_size(), None);
        assert_eq!(missing.tick_size_at(1.0), None);
    }

    #[test]
    fn test_symbol_info_tolerates_unexpected_metadata_types() {
        let mut value = fixture_symbol_info_value();
        value["pricescale"] = Value::from("100");
        value["minmov"] = Value::from(1.0);
        value["minmove2"] = Value::Null;
        value["delay"] = Value::from(-5);
        value["pointvalue"] = Value::from("2.5");
        value["isin"] = Value::Null;
        value["session"] = Value::from(24);
        let info = SymbolInfo::deserialize(&value).expect("lenient metadata parsing");
        assert_eq!(info.pricescale, 100);
        assert_eq!(info.minmov, 1);
        assert_eq!(info.minmove2, 0);
        assert_eq!(info.delay, 0);
        assert_eq!(info.pointvalue, 2.5);
        assert_eq!(info.isin, "");
        assert_eq!(info.session, "24");
        assert_eq!(info.tick_size(), Some(0.01));
    }

    #[test]
    fn test_symbol_info_rejects_fractional_and_out_of_range_scales() {
        for (pricescale, minmov) in [
            (Value::from(2.5), Value::from(1)),
            (Value::from(1e30), Value::from(1)),
            (Value::from(100), Value::from("0.5")),
            (Value::from(-100), Value::from(1)),
        ] {
            let mut value = fixture_symbol_info_value();
            value["pricescale"] = pricescale.clone();
            value["minmov"] = minmov.clone();
            let info = SymbolInfo::deserialize(&value).unwrap();
            assert_eq!(
                info.tick_size(),
                None,
                "pricescale={pricescale} minmov={minmov} must not yield a tick size"
            );
        }
        let mut value = fixture_symbol_info_value();
        value["pricescale"] = Value::from("1e2");
        assert_eq!(SymbolInfo::deserialize(&value).unwrap().pricescale, 100);
    }

    #[test]
    fn test_symbol_info_tolerates_null_and_odd_legacy_fields() {
        let mut value = fixture_symbol_info_value();
        value["founded"] = Value::Null;
        value["typespecs"] = Value::Null;
        value["fractional"] = Value::Null;
        value["earnings_release_date"] = Value::from("soon");
        value["subsessions"] = serde_json::json!([{"id": "regular", "private": null}, 7]);
        value["base_name"] = serde_json::json!(["HOSE:VCB", null, 3]);
        value["description"] = Value::Null;
        let info = SymbolInfo::deserialize(&value).expect("one odd field must not fail the fetch");
        assert_eq!(info.founded, 0);
        assert!(info.typespecs.is_empty());
        assert!(!info.fractional);
        assert_eq!(info.earnings_release_date, 0);
        assert!(info.subsessions.is_empty());
        assert_eq!(info.base_name, vec![Ustr::from("HOSE:VCB")]);
        assert_eq!(info.description, "");
        assert_eq!(info.pricescale, 1);
        assert_eq!(info.id, "HOSE:VCB");
    }

    #[test]
    fn test_symbol_info_without_new_fields_keeps_defaults() {
        let info: SymbolInfo =
            serde_json::from_value(serde_json::json!({"pro_name": "X:Y", "name": "Y"})).unwrap();
        assert_eq!(info.pricescale, 0);
        assert_eq!(info.variable_tick_size, "");
        assert_eq!(info.tick_size(), None);
    }

    #[test]
    fn test_ohlcv_is_falling() {
        let falling = dp(1_700_000_000, 102.0, 105.0, 99.0, 100.0, 1000.0);
        assert!(falling.is_falling());
    }
}
