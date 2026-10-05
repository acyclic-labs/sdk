//! Rust-owned remote transport size limits.
//!
//! These values are part of the public service contract. Language adapters
//! consume the generated projection rather than choosing independent defaults.

/// Message and HTTP body limits for one public SDK family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyRemoteLimits {
    /// Maximum encoded protobuf message accepted by the native transport.
    pub maximum_message_bytes: u64,
    /// Maximum encoded request body accepted by an HTTP transport.
    pub maximum_http_request_bytes: u64,
    /// Maximum cumulative response body accepted by an HTTP transport.
    pub maximum_http_response_bytes: u64,
}

const MIB: u64 = 1024 * 1024;

/// Returns the canonical size policy for a generated remote family.
pub fn family_remote_limits(family: &str) -> Option<FamilyRemoteLimits> {
    match family {
        "actors" | "workers" | "objects" => Some(FamilyRemoteLimits {
            maximum_message_bytes: 16 * MIB,
            maximum_http_request_bytes: 16 * MIB,
            maximum_http_response_bytes: 16 * MIB,
        }),
        "stream" => Some(FamilyRemoteLimits {
            maximum_message_bytes: 8 * MIB,
            maximum_http_request_bytes: 8 * MIB,
            maximum_http_response_bytes: 8 * MIB,
        }),
        "inference" => Some(FamilyRemoteLimits {
            maximum_message_bytes: 8 * MIB,
            maximum_http_request_bytes: 16 * MIB,
            maximum_http_response_bytes: 16 * MIB,
        }),
        "machines" => Some(FamilyRemoteLimits {
            maximum_message_bytes: 64 * MIB,
            maximum_http_request_bytes: 64 * MIB,
            maximum_http_response_bytes: 64 * MIB,
        }),
        "filesystem" | "harness" => Some(FamilyRemoteLimits {
            maximum_message_bytes: 16 * MIB,
            maximum_http_request_bytes: 16 * MIB,
            maximum_http_response_bytes: 16 * MIB,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::family_remote_limits;

    #[test]
    fn limits_cover_all_generated_remote_families() {
        for family in [
            "actors", "workers", "objects", "stream", "inference", "machines",
            "filesystem", "harness",
        ] {
            let limits = family_remote_limits(family).expect("generated family policy");
            assert!(limits.maximum_message_bytes > 0);
            assert!(limits.maximum_http_request_bytes > 0);
            assert!(limits.maximum_http_response_bytes > 0);
        }
    }
}
