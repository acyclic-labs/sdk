//! Fixed evidence from the original process capture completion owner.
use tracing::field::{Field, Visit};

/// Original containment/direct-child cleanup, not final allocation or kernel
/// I/O retirement, command success, or authenticated operation identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CaptureRetirement {
    /// The original capture owner's cleanup result. Absence is unknown;
    /// false remains false even if later destruction retries cleanup.
    pub cleanup_completed: Option<bool>,
    /// The original capture or cleanup error's native code, when present.
    pub os_error: Option<i32>,
}

impl Visit for CaptureRetirement {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "capture.cleanup_completed" {
            self.cleanup_completed = Some(value);
        }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "error.os" && self.os_error.is_none() {
            self.os_error = i32::try_from(value).ok();
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "error.os" && self.os_error.is_none() {
            self.os_error = i32::try_from(value).ok();
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
