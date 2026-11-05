use super::*;

/// Terminal data remains owned independently of physical cleanup progress.
#[derive(Default)]
pub(super) struct TerminalFailures {
    pub(super) producer: Option<NativeMountError>,
    pub(super) source: Option<NativeMountError>,
    pub(super) cleanup: Option<NativeMountError>,
}

impl TerminalFailures {
    /// Taking one cause never consumes a distinct cause retained by the owner.
    pub(super) fn take(&mut self) -> Option<NativeMountError> {
        self.producer
            .take()
            .or_else(|| self.source.take())
            .or_else(|| self.cleanup.take())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.producer.is_none() && self.source.is_none() && self.cleanup.is_none()
    }
}

/// Caller waits can refuse; the existing physical worker joins accepted I/O.
#[derive(Clone, Copy)]
pub(super) enum SourceRetirementWait {
    Caller,
    Physical,
}

/// Exactly one source is retained by operative or failed construction.
pub(super) enum RetainedSource {
    Filesystem(Arc<dyn MountFilesystem>),
    Watch(lazy::NativeMountSourceWatchOwner),
}

/// Retires the original producer before releasing its owning reference.
pub(super) fn retire_sources(
    source: &mut Option<RetainedSource>,
    terminal_failure: &mut Option<NativeMountError>,
    work: &NativeSessionStopWork<'_>,
    join: SourceRetirementWait,
) -> Result<bool, NativeMountError> {
    let failure = match source.as_ref() {
        None => return Ok(false),
        Some(RetainedSource::Filesystem(owner)) => {
            let retired = match join {
                SourceRetirementWait::Physical => owner.shutdown_physical_cleanup(),
                SourceRetirementWait::Caller => match work {
                    NativeSessionStopWork::Meter(meter) => owner.shutdown_with_meter(meter),
                    NativeSessionStopWork::AcceptedWait(wait) => {
                        owner.shutdown_with_cleanup_wait(wait)
                    }
                    NativeSessionStopWork::AcceptedDrop => owner.shutdown_accepted_cleanup(),
                },
            }?;
            retired.failure.map(NativeMountError::from)
        }
        Some(RetainedSource::Watch(owner)) => {
            let result = match work {
                NativeSessionStopWork::Meter(meter) => {
                    owner.stop(&mut adapter::NativeDemandCarrier::Native(meter))
                }
                NativeSessionStopWork::AcceptedWait(wait) => owner.stop_with_cleanup_wait(wait),
                NativeSessionStopWork::AcceptedDrop => owner.stop_accepted_cleanup(),
            };
            if !owner.is_stopped() {
                result?;
                return Err(NativeMountError::ControlPending);
            }
            // A refused caller wait may not have observed the saved callback
            // outcome. Physical retirement permits its final birth-paid
            // observation before custody is released, without restarting it.
            let result = if result.is_err() {
                owner.stop_accepted_cleanup().and(result)
            } else {
                result
            };
            result.err().map(NativeMountError::from)
        }
    };
    // Acknowledgement permits custody release even when recording failed.
    // The same owner reports that failure only after destination retirement.
    *terminal_failure = failure;
    drop(source.take());
    Ok(true)
}

/// Reports a physically retired producer only after its dependent cleanup.
pub(super) fn finish_retirement(
    terminal_failure: &mut Option<NativeMountError>,
    changed: bool,
) -> Result<bool, NativeMountError> {
    match terminal_failure.take() {
        Some(error) => Err(error),
        None => Ok(changed),
    }
}

/// Called after physical namespace retirement and producer quiescence.
pub(super) fn retire_destination(
    destination: &mut Option<MountDestinationGuard>,
    work: &NativeSessionStopWork<'_>,
) -> Result<bool, NativeMountError> {
    let Some(guard) = destination.as_mut() else {
        return Ok(false);
    };
    guard.remove_destination()?;
    if guard.birth_cleanup.is_some() {
        // The physical namespace has retired. Its accepted fence release owns
        // birth-paid credit even when the caller has stopped waiting.
        guard.release_cleanup(None)?;
    } else {
        match work {
            NativeSessionStopWork::Meter(meter) => guard.release(&mut &**meter),
            NativeSessionStopWork::AcceptedWait(wait) => guard.release_cleanup(Some(wait)),
            NativeSessionStopWork::AcceptedDrop => guard.release_cleanup(None),
        }?;
    }
    drop(destination.take());
    Ok(true)
}
