use super::*;
/// Converts an error without losing its completion identity.
#[must_use]
pub fn encode_error(error: &Error) -> wire::Error {
    wire::Error {
        code: error.code() as i32,
        message: error.to_string(),
        operation_id: match error {
            Error::Indeterminate(id)
            | Error::CommandIndeterminate {
                operation_id: id, ..
            } => id.to_string(),
            _ => String::new(),
        },
    }
}
