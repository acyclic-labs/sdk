//! Fixed observations of the original native task's transfer and publication.
use tracing::field::{Field, Visit};

/// Original completion state captured before its result can be consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeTaskTerminal {
    /// The original closure stored a successful result.
    Ok,
    /// The original closure stored an I/O error.
    IoError,
    /// The original worker retained a panic independently of the result.
    Panic,
    /// Publication had neither a result nor a panic. This is incomplete.
    Missing,
}

/// Evidence from one original task, not an authenticated operation identity or
/// a physical resource retirement receipt. Absent fields remain unknown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeTask {
    /// Successful transfer into the original queue. False means inline entry;
    /// absence supplies no admission evidence. Queue delay remains unknown.
    pub queued: Option<bool>,
    /// The original worker entered the closure path.
    pub started: Option<bool>,
    /// Original result/panic disposition at publication, independent of whether
    /// the caller still observes it. This does not certify final deallocation.
    pub terminal: Option<NativeTaskTerminal>,
    /// Original retained panic, independent of a stored I/O result. Both may
    /// coexist; an I/O cause is never replaced by the panic disposition.
    pub panicked: Option<bool>,
    /// Original I/O error code, captured before result publication.
    pub os_error: Option<i32>,
}

impl Visit for NativeTask {
    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "native.queued" => self.queued = Some(value),
            "native.started" => self.started = Some(value),
            "native.panicked" => self.panicked = Some(value),
            _ => {}
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "native.terminal" {
            self.terminal = match value {
                "ok" => Some(NativeTaskTerminal::Ok),
                "io_error" => Some(NativeTaskTerminal::IoError),
                "panic" => Some(NativeTaskTerminal::Panic),
                "missing" => Some(NativeTaskTerminal::Missing),
                _ => None,
            };
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
