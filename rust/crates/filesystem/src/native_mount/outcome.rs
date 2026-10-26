//! Compatibility names for the sole shared callback outcome classifier.
#[cfg(any(test, target_os = "linux", target_os = "macos"))]
pub(super) use super::shared::outcome::callback_is_terminal;
pub(super) use super::shared::outcome::{
    CallbackFailure as NativeFailure, callback_failure, source_failure, workspace_failure,
};
#[cfg(test)]
mod tests;
