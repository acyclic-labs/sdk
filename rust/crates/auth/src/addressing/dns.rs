//! Strict DNS presentation. Canonical hosts are used unchanged for URL authority,
//! HTTP Host, TLS SNI and signing; accepting aliases would split those identities.

/// Invalid DNS label or host presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DnsError {
    /// Empty labels, including a trailing root dot, are not canonical.
    #[error("DNS label is empty")]
    EmptyLabel,
    /// DNS wire labels contain at most 63 bytes.
    #[error("DNS label exceeds 63 bytes")]
    LabelTooLong,
    /// Only lowercase ASCII letters, digits and hyphens are accepted.
    #[error("DNS label must contain lowercase ASCII letters, digits or hyphens")]
    InvalidCharacter,
    /// A hyphen cannot occur at either edge.
    #[error("DNS label starts or ends with a hyphen")]
    EdgeHyphen,
    /// The textual host (without a root dot) exceeds 253 bytes.
    #[error("DNS host exceeds 253 bytes")]
    HostTooLong,
    /// URL parsers can reinterpret a numeric final label as an IP address.
    #[error("DNS host has an ambiguous numeric final label")]
    NumericHost,
}

/// Validate bytes without lowercasing, IDNA conversion or normalization.
pub fn validate_dns_label(label: &[u8]) -> Result<(), DnsError> {
    if label.is_empty() {
        return Err(DnsError::EmptyLabel);
    }
    if label.len() > 63 {
        return Err(DnsError::LabelTooLong);
    }
    if label.first() == Some(&b'-') || label.last() == Some(&b'-') {
        return Err(DnsError::EdgeHyphen);
    }
    if !label
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
    {
        return Err(DnsError::InvalidCharacter);
    }
    Ok(())
}

/// A single canonical spelling, suitable for URL, HTTP, TLS and signing APIs.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CanonicalHost(String);

impl CanonicalHost {
    /// Validate all labels. Ports, userinfo, wildcards, uppercase, trailing dots,
    /// percent escapes, whitespace and non-ASCII input are rejected.
    pub fn parse(host: &str) -> Result<Self, DnsError> {
        if host.len() > 253 {
            return Err(DnsError::HostTooLong);
        }
        for label in host.split('.') {
            validate_dns_label(label.as_bytes())?;
        }
        let tail = host.rsplit('.').next().ok_or(DnsError::EmptyLabel)?;
        if tail.bytes().all(|b| b.is_ascii_digit())
            || tail
                .strip_prefix("0x")
                .is_some_and(|digits| digits.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(DnsError::NumericHost);
        }
        Ok(Self(host.to_owned()))
    }

    /// Return the one spelling used for authority, SNI and signed Host.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
