//! Shared error-code mapping table used by every wire adapter.

use crate::{Error, wire};

/// Maps a semantic error to its canonical wire code.
#[must_use]
pub fn error_code(error: &Error) -> wire::ErrorCode {
    match error {
        Error::NotFound(_) => wire::ErrorCode::NotFound,
        Error::Conflict(_) => wire::ErrorCode::Conflict,
        Error::Unsupported(_) => wire::ErrorCode::Unsupported,
        Error::Invalid(_) => wire::ErrorCode::Invalid,
        Error::Unauthorized(_) => wire::ErrorCode::Unauthorized,
        Error::Storage(_) => wire::ErrorCode::Storage,
        Error::Indeterminate(_) => wire::ErrorCode::Indeterminate,
    }
}

/// HTTP status for a wire error code. Unspecified maps to 500.
#[must_use]
pub fn http_status(code: wire::ErrorCode) -> u16 {
    match code {
        wire::ErrorCode::NotFound => 404,
        wire::ErrorCode::Conflict => 409,
        wire::ErrorCode::Unsupported => 422,
        wire::ErrorCode::Invalid => 400,
        wire::ErrorCode::Unauthorized => 403,
        wire::ErrorCode::Storage | wire::ErrorCode::Indeterminate => 503,
        wire::ErrorCode::Unspecified => 500,
    }
}

/// gRPC status code for a wire error code. Unspecified maps to INTERNAL (13).
#[must_use]
pub fn grpc_code(code: wire::ErrorCode) -> i32 {
    match code {
        wire::ErrorCode::NotFound => 5,
        wire::ErrorCode::Conflict => 10,
        wire::ErrorCode::Unsupported => 12,
        wire::ErrorCode::Invalid => 3,
        wire::ErrorCode::Unauthorized => 7,
        wire::ErrorCode::Storage | wire::ErrorCode::Indeterminate => 14,
        wire::ErrorCode::Unspecified => 13,
    }
}

/// Inverse of [`http_status`]. Ambiguous statuses read as INDETERMINATE, the
/// retry-safe interpretation; unknown statuses return `None`.
#[must_use]
pub fn from_http_status(status: u16) -> Option<wire::ErrorCode> {
    match status {
        404 => Some(wire::ErrorCode::NotFound),
        409 => Some(wire::ErrorCode::Conflict),
        422 => Some(wire::ErrorCode::Unsupported),
        400 => Some(wire::ErrorCode::Invalid),
        403 => Some(wire::ErrorCode::Unauthorized),
        503 => Some(wire::ErrorCode::Indeterminate),
        _ => None,
    }
}

/// Inverse of [`grpc_code`]. Ambiguous codes read as INDETERMINATE; unknown
/// codes return `None`.
#[must_use]
pub fn from_grpc_code(code: i32) -> Option<wire::ErrorCode> {
    match code {
        5 => Some(wire::ErrorCode::NotFound),
        10 => Some(wire::ErrorCode::Conflict),
        12 => Some(wire::ErrorCode::Unsupported),
        3 => Some(wire::ErrorCode::Invalid),
        7 => Some(wire::ErrorCode::Unauthorized),
        14 => Some(wire::ErrorCode::Indeterminate),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_table_matches_the_conformance_vector() -> Result<(), Error> {
        let vector: serde_json::Value =
            serde_json::from_str(include_str!("../conformance/error-mapping-v1.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let mappings = vector
            .get("mappings")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| Error::Invalid("error mapping vector is missing".into()))?;
        assert_eq!(
            mappings.len(),
            wire::ErrorCode::Indeterminate as usize,
            "every named wire code must appear in the vector"
        );
        for mapping in mappings {
            let name = mapping
                .get("code")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| Error::Invalid("mapping code is missing".into()))?;
            let code = wire::ErrorCode::from_str_name(&format!("ERROR_CODE_{name}"))
                .ok_or_else(|| Error::Invalid(format!("unknown error code {name}")))?;
            let http = mapping
                .get("http_status")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| Error::Invalid("mapping http_status is missing".into()))?;
            let grpc = mapping
                .get("grpc_code")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| Error::Invalid("mapping grpc_code is missing".into()))?;
            assert_eq!(u64::from(http_status(code)), http, "{name}");
            assert_eq!(i64::from(grpc_code(code)), grpc, "{name}");
            let http_status_value =
                u16::try_from(http).map_err(|error| Error::Invalid(error.to_string()))?;
            let grpc_code_value =
                i32::try_from(grpc).map_err(|error| Error::Invalid(error.to_string()))?;
            let inverse_http = from_http_status(http_status_value)
                .ok_or_else(|| Error::Invalid(format!("http status {http} is unmapped")))?;
            assert_eq!(http_status(inverse_http), http_status_value, "{name}");
            let inverse_grpc = from_grpc_code(grpc_code_value)
                .ok_or_else(|| Error::Invalid(format!("grpc code {grpc} is unmapped")))?;
            assert_eq!(grpc_code(inverse_grpc), grpc_code_value, "{name}");
        }
        Ok(())
    }

    #[test]
    fn unspecified_maps_to_internal() {
        assert_eq!(http_status(wire::ErrorCode::Unspecified), 500);
        assert_eq!(grpc_code(wire::ErrorCode::Unspecified), 13);
        assert_eq!(from_http_status(418), None);
        assert_eq!(from_grpc_code(2), None);
    }
}
