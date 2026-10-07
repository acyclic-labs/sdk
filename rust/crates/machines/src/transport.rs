use crate::ProviderError;
use url::Url;

/// Maximum bearer credential size accepted by native and WASM adapters.
pub const MAX_BEARER_BYTES: usize = 8 * 1024;

/// Validates a hosted Machines endpoint without changing its spelling.
pub fn validate_https_endpoint(endpoint: &str) -> Result<(), ProviderError> {
    let parsed = Url::parse(endpoint)
        .map_err(|_| ProviderError::Invalid("invalid Machines endpoint".into()))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none_or(str::is_empty)
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ProviderError::Invalid(
            "endpoint must be an absolute HTTPS URL without credentials, query, or fragment".into(),
        ));
    }
    Ok(())
}

/// Parses and normalizes a hosted Machines endpoint.
///
/// This is intentionally independent of the native gRPC transport so the same
/// acceptance policy can be called from the browser/WASM adapter. Route joining
/// and fetching remain adapter responsibilities.
pub fn normalize_https_endpoint(endpoint: &str) -> Result<String, ProviderError> {
    validate_https_endpoint(endpoint)?;
    let mut parsed = Url::parse(endpoint)
        .map_err(|_| ProviderError::Invalid("invalid Machines endpoint".into()))?;
    if !parsed.path().ends_with('/') {
        parsed.set_path(&format!("{}/", parsed.path()));
    }
    Ok(parsed.to_string())
}

/// Validates an opaque bearer credential before an adapter formats its header.
pub fn validate_bearer_token(token: &str) -> Result<(), ProviderError> {
    if token.chars().all(is_js_whitespace)
        || token.len() > MAX_BEARER_BYTES
        || token
            .chars()
            .any(|character| matches!(character, '\r' | '\n' | '\0'))
    {
        return Err(ProviderError::Invalid(
            "token must be a non-empty bearer token of at most 8 KiB without CR, LF, or NUL".into(),
        ));
    }
    Ok(())
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_policy_matches_the_host_adapter_contract() {
        assert_eq!(
            normalize_https_endpoint("https://example.test").unwrap(),
            "https://example.test/"
        );
        assert_eq!(
            normalize_https_endpoint("https://example.test/api").unwrap(),
            "https://example.test/api/"
        );
        for endpoint in [
            "http://example.test",
            "https://user@example.test",
            "https://example.test?query",
            "https://example.test#fragment",
            "https:///missing-host",
            "not a URL",
        ] {
            assert!(normalize_https_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn bearer_policy_rejects_browser_header_hazards() {
        for token in [
            "",
            " \t\n",
            "\u{feff}",
            "bad\rtoken",
            "bad\ntoken",
            "bad\0token",
        ] {
            assert!(validate_bearer_token(token).is_err(), "{token:?}");
        }
        assert!(validate_bearer_token("opaque.account+/=").is_ok());
        assert!(validate_bearer_token(&"x".repeat(MAX_BEARER_BYTES)).is_ok());
        assert!(validate_bearer_token(&"x".repeat(MAX_BEARER_BYTES + 1)).is_err());
    }
}
