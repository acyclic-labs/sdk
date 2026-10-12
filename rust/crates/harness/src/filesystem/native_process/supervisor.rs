use super::*;

pub(super) struct NativeActiveAttempt {
    pub(super) attempt: crate::EffectAttemptId,
    pub(super) stop_requested: bool,
    pub(super) quiescent: bool,
    pub(super) completed: bool,
    pub(super) process: Option<(
        acyclic_native_runtime::ProcessTree,
        Vec<acyclic_fs::native_mount::NativeMountWorkScope>,
        NativeAllocation,
    )>,
}
pub(super) struct NativeSupervisorControl {
    pub(super) active: Mutex<Option<NativeActiveAttempt>>,
    pub(super) completion: tokio::sync::Notify,
}
impl NativeSupervisorControl {
    pub(super) fn begin(&self, attempt: crate::EffectAttemptId) -> Result<()> {
        let mut active = self.active.lock().map_err(|_| {
            boundary(
                crate::wire::ErrorCode::Storage,
                "native supervisor control is poisoned",
            )
        })?;
        if active.is_some() {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "native supervisor already started",
            ));
        }
        *active = Some(NativeActiveAttempt {
            attempt,
            stop_requested: false,
            quiescent: true,
            completed: false,
            process: None,
        });
        Ok(())
    }
    pub(super) fn snapshot(&self, attempt: crate::EffectAttemptId) -> Result<(bool, bool)> {
        let active = self.active.lock().map_err(|_| {
            boundary(
                crate::wire::ErrorCode::Storage,
                "native supervisor control is poisoned",
            )
        })?;
        let active = active.as_ref().ok_or(boundary(
            crate::wire::ErrorCode::Storage,
            "original owned supervisor is unavailable",
        ))?;
        if active.attempt != attempt {
            return Err(boundary(
                crate::wire::ErrorCode::Storage,
                "stop attempt differs from original supervisor",
            ));
        }
        Ok((active.completed, active.quiescent))
    }
    pub(super) fn settle_process(
        &self,
        original: &NativeMaterializationWork,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let mut state = self.active.lock().map_err(|_| {
            boundary(
                crate::wire::ErrorCode::Storage,
                "native supervisor control is poisoned",
            )
        })?;
        let Some(active) = state.as_mut() else {
            return Ok(());
        };
        // Only an owned tree can establish physical collection.
        let Some((process, _, custody)) = active.process.as_mut() else {
            return Ok(());
        };
        if !custody.work().shares_ledger(original) {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "original process belongs to another work family",
            ));
        }
        process
            .terminate(Some(work.runtime_meter().as_ref()))
            .map_err(|error| Error::NativeWork {
                cause: work.runtime_error(error),
                custody: None,
            })?;
        active.process.take();
        active.quiescent = true;
        Ok(())
    }
    pub(super) fn record_quiescence(&self, quiescent: bool) -> std::io::Result<()> {
        let mut state = self
            .active
            .lock()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        state
            .as_mut()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::Other))?
            .quiescent = quiescent;
        Ok(())
    }
    pub(super) fn poll_stop(&self) -> std::io::Result<()> {
        let state = self
            .active
            .lock()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        let state = state
            .as_ref()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::Other))?;
        if state.stop_requested {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
}
pub(super) struct NativeSupervisorCompletion(pub(super) Arc<NativeSupervisorControl>);
impl Drop for NativeSupervisorCompletion {
    fn drop(&mut self) {
        if let Ok(mut active) = self.0.active.lock()
            && let Some(active) = active.as_mut()
        {
            active.completed = true;
        }
        self.0.completion.notify_waiters();
    }
}
