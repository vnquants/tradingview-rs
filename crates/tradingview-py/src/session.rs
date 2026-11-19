//! Export and import of TradingView cookie sessions.
//!
//! A password login produces a [`UserCookies`] value. Serializing it lets a
//! caller persist the session and rebuild a cookie-authenticated client later
//! without logging in again. The JSON format is exactly the one `UserCookies`
//! derives with serde.
//!
//! Error messages never echo cookie or token values: decode errors report only
//! the error category and position.

use std::fmt;

use serde_json::{Map, Value};
use tradingview::models::UserCookies;

/// Reasons a cookie JSON document cannot be turned into a cookie session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CookieImportError {
    /// The document is not valid JSON, or a field has the wrong type.
    Malformed {
        category: &'static str,
        line: usize,
        column: usize,
    },
    /// The document is valid JSON but not an object.
    NotAnObject,
    /// `session` (`sessionid`) or `session_signature` (`sessionid_sign`) is empty.
    MissingSessionCookies,
}

impl fmt::Display for CookieImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed {
                category,
                line,
                column,
            } => write!(
                f,
                "invalid cookie JSON: {category} error at line {line}, column {column}"
            ),
            Self::NotAnObject => f.write_str("invalid cookie JSON: expected a JSON object"),
            Self::MissingSessionCookies => f.write_str(
                "cookie JSON has no session: 'session' and 'session_signature' must be non-empty",
            ),
        }
    }
}

impl std::error::Error for CookieImportError {}

fn malformed(err: &serde_json::Error) -> CookieImportError {
    let category = match err.classify() {
        serde_json::error::Category::Io => "io",
        serde_json::error::Category::Syntax => "syntax",
        serde_json::error::Category::Data => "data",
        serde_json::error::Category::Eof => "eof",
    };
    CookieImportError::Malformed {
        category,
        line: err.line(),
        column: err.column(),
    }
}

/// Parse a cookie session from JSON.
///
/// Accepts the full `UserCookies` JSON written by [`export_user_cookies`]. Every
/// field other than `session` and `session_signature` may be omitted, so a
/// session copied by hand from a browser (`sessionid`, `sessionid_sign`,
/// optionally `device_t`) also works. Unknown keys are ignored.
///
/// # Errors
/// Returns [`CookieImportError`] when the input is not a JSON object, a field
/// has the wrong type, or the session cookies are empty.
pub fn parse_user_cookies(json: &str) -> Result<UserCookies, CookieImportError> {
    let provided: Value = serde_json::from_str(json).map_err(|e| malformed(&e))?;
    let Value::Object(provided) = provided else {
        return Err(CookieImportError::NotAnObject);
    };

    // Start from the defaults so optional fields may be omitted.
    let mut merged = match serde_json::to_value(UserCookies::default()) {
        Ok(Value::Object(defaults)) => defaults,
        _ => Map::new(),
    };
    merged.extend(provided);

    let cookies: UserCookies =
        serde_json::from_value(Value::Object(merged)).map_err(|e| malformed(&e))?;

    if cookies.session.trim().is_empty() || cookies.session_signature.trim().is_empty() {
        return Err(CookieImportError::MissingSessionCookies);
    }
    Ok(cookies)
}

/// Serialize a cookie session to compact JSON.
///
/// # Errors
/// Returns the serde error if serialization fails, which does not happen for
/// the plain string and integer fields of `UserCookies`.
pub fn export_user_cookies(cookies: &UserCookies) -> Result<String, serde_json::Error> {
    serde_json::to_string(cookies)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> UserCookies {
        UserCookies {
            id: 42,
            username: "alice".into(),
            private_channel: "channel".into(),
            auth_token: "header.payload.signature".into(),
            session: "session-value".into(),
            session_signature: "signature-value".into(),
            session_hash: "hash".into(),
            device_token: "device".into(),
        }
    }

    #[test]
    fn round_trip_preserves_every_field() {
        let original = sample();
        let json = export_user_cookies(&original).expect("serialize");
        let restored = parse_user_cookies(&json).expect("parse");

        assert_eq!(restored.id, original.id);
        assert_eq!(restored.username, original.username);
        assert_eq!(restored.private_channel, original.private_channel);
        assert_eq!(restored.auth_token, original.auth_token);
        assert_eq!(restored.session, original.session);
        assert_eq!(restored.session_signature, original.session_signature);
        assert_eq!(restored.session_hash, original.session_hash);
        assert_eq!(restored.device_token, original.device_token);
        assert_eq!(export_user_cookies(&restored).expect("serialize"), json);
    }

    #[test]
    fn minimal_browser_session_is_accepted() {
        let restored =
            parse_user_cookies(r#"{"session": "s", "session_signature": "sig", "extra": 1}"#)
                .expect("parse");
        assert_eq!(restored.session, "s");
        assert_eq!(restored.session_signature, "sig");
        assert_eq!(restored.id, 0);
        assert!(restored.auth_token.is_empty());
        assert!(restored.device_token.is_empty());
    }

    #[test]
    fn empty_session_cookies_are_rejected() {
        let mut cookies = sample();
        cookies.session_signature = "  ".into();
        let json = export_user_cookies(&cookies).expect("serialize");
        assert_eq!(
            parse_user_cookies(&json).unwrap_err(),
            CookieImportError::MissingSessionCookies
        );
        assert_eq!(
            parse_user_cookies("{}").unwrap_err(),
            CookieImportError::MissingSessionCookies
        );
    }

    #[test]
    fn malformed_input_is_rejected_without_echoing_values() {
        let err =
            parse_user_cookies(r#"{"session": "secret-value", "id": "not-a-number"}"#).unwrap_err();
        assert!(matches!(
            err,
            CookieImportError::Malformed {
                category: "data",
                ..
            }
        ));
        let message = err.to_string();
        assert!(!message.contains("secret-value"));
        assert!(!message.contains("not-a-number"));

        assert!(matches!(
            parse_user_cookies("not json").unwrap_err(),
            CookieImportError::Malformed {
                category: "syntax",
                ..
            }
        ));
        assert_eq!(
            parse_user_cookies(r#"["session"]"#).unwrap_err(),
            CookieImportError::NotAnObject
        );
    }
}
