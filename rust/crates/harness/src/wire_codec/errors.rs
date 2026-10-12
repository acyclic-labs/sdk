use super::*;

/// Converts a semantic error without losing storage or indeterminate state.
#[must_use]
pub fn encode_error(error: &Error) -> wire::Error {
    wire::Error {
        code: error.code() as i32,
        message: error.to_string(),
        operation_id: match error {
            Error::Indeterminate(operation_id) => operation_id.to_string(),
            _ => String::new(),
        },
    }
}
