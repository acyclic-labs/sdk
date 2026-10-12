use super::*;

impl DarwinMountContext {
    pub(super) fn record_failure(&self, cause: NativeMountError, work: crate::WorkCounters) -> i32 {
        let status = callback_errno(&cause);
        if fatal(&cause) {
            let mut state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.first_fatal.is_none() {
                state.first_fatal = Some((Some(cause), work));
            }
        }
        status
    }

    pub(super) fn run<T>(
        &self,
        authored_write: bool,
        operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, NativeMountError>,
    ) -> Result<T, i32> {
        let active = self.callback_meter().map_err(|cause| {
            let work = self
                .source_original_work
                .as_ref()
                .map_or(crate::WorkCounters::UNCHARGED, |work| {
                    work.state.lock().work
                });
            self.record_failure(cause, work)
        })?;
        {
            let state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some((cause, _)) = &state.first_fatal {
                return Err(cause.as_ref().map_or(libc::EIO, callback_errno));
            }
        }
        let operation = |work: &crate::NativeMaterializationWork| {
            work.admit(crate::WorkCounters::UNCHARGED)?;
            if authored_write {
                self.admit_write(work)?;
            }
            let value = operation(work)?;
            if authored_write {
                self.admit_write(work)?;
            }
            Ok(value)
        };
        let future_request = active.is_none();
        let work = match active {
            Some(work) => work,
            None => crate::NativeMaterializationWork::new(
                self.handler_budget,
                &self.handler_cancellation,
            )
            .map_err(|(cause, work)| self.record_failure(cause.into(), work))?,
        };
        let result = crate::native_mount::run_native_mount_handler_on_meter(&work, operation);
        let completed = work.receipt();
        if future_request {
            if let Err(overflow) = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .completed_future_request_work
                .try_add_assign(&completed)
            {
                match &result {
                    Ok(_) => return Err(self.record_failure(overflow.into(), completed)),
                    Err(cause) if !fatal(cause) => {
                        self.record_failure(overflow.into(), completed);
                    }
                    Err(_) => {}
                }
            }
        }
        result.map_err(|cause| self.record_failure(cause, completed))
    }
}
