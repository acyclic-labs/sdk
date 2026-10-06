//! Rust-owned credential admission policy projected into generated clients.

/// Credential policy shared by native callers and every generated language facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialPolicy {
    /// Stable policy identifier emitted into target metadata.
    pub name: &'static str,
    /// Whether empty or whitespace-only credentials are rejected.
    pub require_nonempty: bool,
    /// Whether HTTP header line breaks are rejected.
    pub reject_crlf: bool,
}

/// Bearer credentials suitable for HTTP authorization and gRPC ASCII metadata.
pub const BEARER_NO_CRLF: CredentialPolicy = CredentialPolicy {
    name: "bearer-no-crlf",
    require_nonempty: true,
    reject_crlf: true,
};

/// Validate one caller-supplied bearer credential against the shared policy.
pub fn validate(policy: CredentialPolicy, token: &str) -> bool {
    (!policy.require_nonempty || !token.trim().is_empty())
        && (!policy.reject_crlf || !token.contains(['\r', '\n']))
}

#[cfg(test)]
mod tests {
    use super::{BEARER_NO_CRLF, validate};

    #[test]
    fn bearer_policy_rejects_empty_and_header_injection() {
        assert!(validate(BEARER_NO_CRLF, "exact-token"));
        assert!(!validate(BEARER_NO_CRLF, "   "));
        assert!(!validate(BEARER_NO_CRLF, "token\r\nInjected: yes"));
    }
}
