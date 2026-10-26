use super::*;

/// Terminal data remains owned independently of physical cleanup progress.
#[derive(Default)]
pub(super) struct TerminalFailures {
    pub(super) producer: Option<NativeMountError>,
    pub(super) cleanup: Option<NativeMountError>,
}

impl TerminalFailures {
    /// Taking one cause never consumes a distinct cause retained by the owner.
    pub(super) fn take(&mut self) -> Option<NativeMountError> {
        self.producer.take().or_else(|| self.cleanup.take())
    }

    pub(super) fn take_pair(&mut self) -> Option<(NativeMountError, Option<NativeMountError>)> {
        self.take().map(|first| (first, self.take()))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.producer.is_none() && self.cleanup.is_none()
    }
}

/// Caller waits can refuse; the existing physical worker joins accepted I/O.
#[derive(Clone, Copy)]
pub(super) enum SourceRetirementWait {
    Caller,
    Physical,
}

/// Retires the original producers before releasing their owning references.
pub(super) fn retire_sources(
    source: &mut Option<Arc<dyn MountFilesystem>>,
    watch: &mut Option<lazy::NativeMountSourceWatchOwner>,
    terminal_failure: &mut Option<NativeMountError>,
    work: &NativeSessionStopWork<'_>,
    join: SourceRetirementWait,
) -> Result<bool, NativeMountError> {
    let mut changed = false;
    if let Some(owner) = source.as_ref() {
        match join {
            SourceRetirementWait::Physical => owner.shutdown_physical_cleanup(),
            SourceRetirementWait::Caller => match work {
                NativeSessionStopWork::Meter(meter) => owner.shutdown_with_meter(meter),
                NativeSessionStopWork::AcceptedWait(wait) => owner.shutdown_with_cleanup_wait(wait),
                NativeSessionStopWork::AcceptedDrop => owner.shutdown_accepted_cleanup(),
            },
        }?;
        // Failed or cancelled joins leave the original source owned.
        drop(source.take());
        changed = true;
    }
    if let Some(owner) = watch.as_mut() {
        let result = if owner.is_stopped() {
            Ok(false)
        } else {
            match work {
                NativeSessionStopWork::Meter(meter) => {
                    owner.stop(&mut adapter::NativeDemandCarrier::Native(meter))
                }
                NativeSessionStopWork::AcceptedWait(wait) => owner.stop_with_cleanup_wait(wait),
                NativeSessionStopWork::AcceptedDrop => owner.stop_accepted_cleanup(),
            }
        };
        if owner.is_stopped() {
            // Physical quiescence, including an error after its acknowledgement,
            // permits terminal data transfer without fresh native admission.
            drop(watch.take());
            changed = true;
            // A stopped producer cannot make further progress. Retain its
            // actual outcome while the same owner releases the destination;
            // reporting it here would strand that accepted cleanup plan.
            *terminal_failure = result.err().map(NativeMountError::from);
        } else {
            result?;
        }
    }
    Ok(changed)
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
