//! Metered process recovery delegates identity and OS queries to native-runtime.
use super::super::custody::{ProcessObservation, require_dead_incarnation};
use super::*;
use acyclic_native_runtime::process_identity::{ProcessProbe, current_process_started};

struct ProcessHandle {
    process: ProcessProbe,
    close: crate::NativeWorkReservation,
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        self.close.started_drop_io(WriteOperations.delta(1));
    }
}

pub(in crate::native_mount) fn current_process_owner(
    work: &crate::NativeMaterializationWork,
) -> Result<(u32, u64), NativeMountError> {
    let time = (&mut &*work).attempt_io(ReadOperations.delta(1), current_process_started)?;
    Ok((std::process::id(), time))
}

pub(in crate::native_mount) fn require_dead_process_owner(
    pid: u32,
    started: u64,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    if acyclic_native_runtime::process_identity::ProcessIncarnation::new(pid, started).is_none() {
        return require_dead_incarnation(pid, started, ProcessObservation::Unknown);
    }
    let close = work.reserve_work(WriteOperations.delta(1))?;
    let opened = (&mut &*work).attempt_io(ReadOperations.delta(1), || {
        ProcessProbe::open(pid)
            .map(|process| process.map(|process| ProcessHandle { process, close }))
    });
    let handle = match opened {
        Ok(Some(handle)) => handle,
        Ok(None) => return require_dead_incarnation(pid, started, ProcessObservation::Absent),
        Err(crate::NativeTreeMaterializationError::Io(_)) => {
            return require_dead_incarnation(pid, started, ProcessObservation::Unknown);
        }
        Err(error) => return Err(error.into()),
    };
    let observed =
        (&mut &*work).attempt_io(ReadOperations.delta(1), || handle.process.started())?;
    let exited = (&mut &*work).attempt_io(ReadOperations.delta(1), || handle.process.exited());
    let observation = match exited {
        Ok(exited) => ProcessObservation::Present {
            started: observed,
            exited,
        },
        Err(crate::NativeTreeMaterializationError::Io(_)) => ProcessObservation::Unknown,
        Err(error) => return Err(error.into()),
    };
    require_dead_incarnation(pid, started, observation)
}
