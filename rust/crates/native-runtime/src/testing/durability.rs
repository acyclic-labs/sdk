//! Faults at original file operations; no replacement store or forced flush.
use super::*;
use std::{fmt, io};

/// The operation whose acknowledgement is interrupted by a simulated crash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableFault {
    /// Stop before the operation touches the file.
    Before,
    /// Perform the original operation, then lose its acknowledgement.
    After,
    /// Write only this prefix, then reject all subsequent operations.
    Torn(usize),
    /// Fail the operation with a retained I/O cause.
    Failed,
    /// Report the platform-independent disk-full category.
    DiskFull,
}

/// Original I/O cause retained inside the returned `io::Error`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InjectedStorageFault {
    /// Zero-based operation index in the complete scope.
    pub ordinal: usize,
    /// Production operation name, without host-specific paths.
    pub operation: &'static str,
    /// Selected fault, including a torn write's requested prefix.
    pub fault: DurableFault,
}

impl fmt::Display for InjectedStorageFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DST {:?} at {} operation {}",
            self.fault, self.operation, self.ordinal
        )
    }
}
impl std::error::Error for InjectedStorageFault {}

impl InjectedStorageFault {
    fn error(&self) -> io::Error {
        io::Error::new(
            match self.fault {
                DurableFault::DiskFull => io::ErrorKind::StorageFull,
                DurableFault::Failed => io::ErrorKind::Other,
                _ => io::ErrorKind::ConnectionAborted,
            },
            self.clone(),
        )
    }
}

/// One original file call, including its actual transferred prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableWrite {
    /// Zero-based global index across stores and owned workers.
    pub ordinal: usize,
    /// Original production boundary.
    pub operation: &'static str,
    /// Bytes offered to this call; zero for namespace and durability calls.
    pub requested: usize,
    /// Bytes reported by the original file call, when it ran successfully.
    pub transferred: Option<usize>,
    /// Error returned to the original caller.
    pub failure: Option<io::ErrorKind>,
}

#[derive(Default)]
pub(super) struct Writes {
    cut: Option<(usize, DurableFault)>,
    failed: Option<InjectedStorageFault>,
    trace: Vec<DurableWrite>,
    #[cfg(test)]
    skip_write: bool,
    #[cfg(test)]
    forget_crash: bool,
}

struct Call {
    schedule: Schedule,
    ordinal: usize,
    fault: Option<InjectedStorageFault>,
}

impl Schedule {
    /// Arms a fresh scope. No operation can clear a crash while descendants live.
    pub fn cut_at(&self, ordinal: usize, fault: DurableFault) {
        let mut state = self.0.state();
        assert_eq!(state.active, 0, "cannot arm live owned work");
        assert!(state.writes.trace.is_empty(), "cut must precede all writes");
        state.writes.cut = Some((ordinal, fault));
    }

    /// Complete ordered record of the original file calls in this scope.
    pub fn durable_writes(&self) -> Vec<DurableWrite> {
        self.0.state().writes.trace.clone()
    }

    /// Latched cause of the first cut, retained until this scope is discarded.
    pub fn storage_fault(&self) -> Option<InjectedStorageFault> {
        self.0.state().writes.failed.clone()
    }
}

thread_local! {
    // The boundary whose original call is running on this thread, if any.
    static OPEN: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

/// Exactly one boundary per physical call. A hooked call reached from inside
/// another boundary would see the outer cut latched first and skip its own
/// physical operation, so nesting is rejected instead of silently recorded.
struct Open;

impl Open {
    fn enter(operation: &'static str) -> Self {
        if let Some(outer) = OPEN.get() {
            panic!("nested durable boundary: {operation} inside {outer}");
        }
        OPEN.set(Some(operation));
        Self
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        OPEN.set(None);
    }
}

fn begin(schedule: Schedule, operation: &'static str, requested: usize) -> io::Result<Call> {
    let (ordinal, fault) = {
        let mut state = schedule.0.state();
        let writes = &mut state.writes;
        if let Some(failed) = &writes.failed {
            return Err(failed.error());
        }
        let ordinal = writes.trace.len();
        let fault = writes
            .cut
            .filter(|(cut, _)| *cut == ordinal)
            .map(|(_, fault)| InjectedStorageFault {
                ordinal,
                operation,
                fault,
            });
        writes.failed = fault.clone();
        #[cfg(test)]
        if writes.forget_crash {
            writes.failed = None;
        }
        writes.trace.push(DurableWrite {
            ordinal,
            operation,
            requested,
            transferred: None,
            failure: None,
        });
        (ordinal, fault)
    };
    Ok(Call {
        schedule,
        ordinal,
        fault,
    })
}

impl Call {
    fn finish(&self, result: &io::Result<usize>) {
        let mut state = self.schedule.0.state();
        let event = &mut state.writes.trace[self.ordinal];
        event.transferred = result.as_ref().ok().copied();
        event.failure = result.as_ref().err().map(io::Error::kind);
    }

    fn before(&self) -> io::Result<()> {
        match &self.fault {
            Some(fault) if !matches!(fault.fault, DurableFault::After | DurableFault::Torn(_)) => {
                Err(fault.error())
            }
            _ => Ok(()),
        }
    }
}

/// Calls the original write once, retaining short writes and their accounting.
/// The controller lock is never held across I/O, callbacks or other locks.
pub fn durable_write(
    operation: &'static str,
    bytes: &[u8],
    write: impl FnOnce(&[u8]) -> io::Result<usize>,
) -> io::Result<usize> {
    let Some(context) = Context::simulation() else {
        return write(bytes);
    };
    let _open = Open::enter(operation);
    let call = begin(context.schedule, operation, bytes.len())?;
    let result = call.before().and_then(|()| {
        #[cfg(test)]
        if call.schedule.0.state().writes.skip_write {
            return Ok(bytes.len());
        }
        let offered = match call.fault.as_ref().map(|fault| fault.fault) {
            Some(DurableFault::Torn(prefix)) => &bytes[..prefix.min(bytes.len().saturating_sub(1))],
            _ => bytes,
        };
        if offered.is_empty() && !bytes.is_empty() {
            return Err(call.fault.as_ref().expect("torn prefix").error());
        }
        write(offered)
    });
    call.finish(&result);
    if let Some(fault) = &call.fault
        && fault.fault == DurableFault::After
        && result.is_ok()
    {
        call.schedule.0.state().writes.trace[call.ordinal].failure = Some(fault.error().kind());
        return Err(fault.error());
    }
    result
}

/// Runs the original flush, truncation or namespace operation without changing
/// its durability policy. A torn non-data operation fails before dispatch.
pub fn durable_operation<T>(
    operation: &'static str,
    work: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let Some(context) = Context::simulation() else {
        return work();
    };
    let _open = Open::enter(operation);
    let call = begin(context.schedule, operation, 0)?;
    let result = call.before().and_then(|()| {
        if let Some(fault) = &call.fault
            && matches!(fault.fault, DurableFault::Torn(_))
        {
            return Err(fault.error());
        }
        work()
    });
    call.finish(
        &result
            .as_ref()
            .map(|_| 0)
            .map_err(|error| io::Error::from(error.kind())),
    );
    if let Some(fault) = &call.fault
        && fault.fault == DurableFault::After
        && result.is_ok()
    {
        call.schedule.0.state().writes.trace[call.ordinal].failure = Some(fault.error().kind());
        return Err(fault.error());
    }
    result
}

#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod models;
#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod native;
#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod tests;
