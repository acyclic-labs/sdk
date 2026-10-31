//! Fixed physical-attempt evidence from the original native producer.
use tracing::field::{Field, Visit};

/// Durability requested by the original caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityRequest {
    /// The caller requested full durability.
    Full,
    /// The caller requested write ordering.
    Barrier,
}

/// The actual native call attempted, independently of the requested guarantee.
/// Success means only what the named OS API promises for that descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilitySyscall {
    /// POSIX `fsync`.
    Fsync,
    /// POSIX `fdatasync`.
    Fdatasync,
    /// Apple `fcntl(F_FULLFSYNC)`.
    FullFsync,
    /// Apple `fcntl(F_BARRIERFSYNC)`.
    BarrierFsync,
    /// Windows `FlushFileBuffers`.
    FlushFileBuffers,
    /// Linux `syncfs`.
    Syncfs,
}

/// Bounded fields of one `acyclic.runtime.durability` attempt. Each absent or
/// unrecognized field stays unknown. These fields certify neither publication
/// range coverage nor OS-wide counts or a device-cache drain.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurabilityAttempt {
    /// Original requested guarantee.
    pub requested: Option<DurabilityRequest>,
    /// Actual attempted native API.
    pub syscall: Option<DurabilitySyscall>,
    /// Opaque held-descriptor identity, meaningful only for this attempt.
    /// It is not a resource name, globally unique identity or metric label.
    pub descriptor: Option<u64>,
    /// Original native error code, when supplied by the producer. Absence
    /// supplies no error-code evidence; the sample retains its own outcome.
    pub os_error: Option<i32>,
}

impl Visit for DurabilityAttempt {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "requested" if self.requested.is_none() => {
                self.requested = match value {
                    "full" => Some(DurabilityRequest::Full),
                    "barrier" => Some(DurabilityRequest::Barrier),
                    _ => None,
                };
            }
            "syscall" if self.syscall.is_none() => {
                self.syscall = match value {
                    "fsync" => Some(DurabilitySyscall::Fsync),
                    "fdatasync" => Some(DurabilitySyscall::Fdatasync),
                    "F_FULLFSYNC" => Some(DurabilitySyscall::FullFsync),
                    "F_BARRIERFSYNC" => Some(DurabilitySyscall::BarrierFsync),
                    "FlushFileBuffers" => Some(DurabilitySyscall::FlushFileBuffers),
                    "syncfs" => Some(DurabilitySyscall::Syncfs),
                    _ => None,
                };
            }
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "descriptor" && self.descriptor.is_none() {
            self.descriptor = Some(value);
        } else if field.name() == "error.os" && self.os_error.is_none() {
            self.os_error = i32::try_from(value).ok();
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "error.os" && self.os_error.is_none() {
            self.os_error = i32::try_from(value).ok();
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
