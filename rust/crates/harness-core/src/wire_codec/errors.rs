use super::*;
/// Converts an error without losing its completion identity.
#[must_use]
pub fn encode_error(error: &Error) -> wire::Error {
    wire::Error {
        code: error.code() as i32,
        message: error.to_string(),
        operation_id: error
            .operation_id()
            .map_or_else(String::new, |id| id.to_string()),
    }
}
