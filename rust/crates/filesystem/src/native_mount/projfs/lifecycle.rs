use super::*;

impl Projector {
    pub(super) fn run_lifecycle(&self, runtime: Arc<Runtime>) {
        let mut lifecycle = lock_recover(&self.lifecycle);
        while lifecycle.context.is_none() && !lifecycle.requested {
            lifecycle = self
                .lifecycle_changed
                .wait(lifecycle)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        let context = lifecycle.context.take();
        drop(lifecycle);
        if let Some(context) = context {
            self.run(&runtime, context.0);
        }
        drop(runtime);
        loop {
            let mut lifecycle = lock_recover(&self.lifecycle);
            while !lifecycle.requested || lifecycle.owner.is_none() {
                lifecycle = self
                    .lifecycle_changed
                    .wait(lifecycle)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            lifecycle.requested = false;
            let mut owner = lifecycle.owner.take().expect("original retirement owner");
            lifecycle.custody = owner.retirement_ownership();
            drop(lifecycle);
            while owner
                .runtime
                .as_ref()
                .is_some_and(|runtime| lock_recover(&runtime.active_work).upgrade().is_some())
            {
                std::thread::sleep(HELD_RETRY);
            }
            let result = owner.stop_physical();
            let mut lifecycle = lock_recover(&self.lifecycle);
            lifecycle.custody = owner.retirement_ownership();
            lifecycle.complete = result.is_ok();
            lifecycle.failure = result.err();
            lifecycle.owner = Some(owner);
            self.lifecycle_changed.notify_all();
            if lifecycle.complete {
                let owner = lifecycle
                    .owner
                    .as_mut()
                    .expect("completed retirement owner");
                let credit = owner
                    .worker_close_credit
                    .as_mut()
                    .expect("birth close credit");
                credit.started_drop_io(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..crate::WorkCounters::UNCHARGED
                });
                drop(lock_recover(&self.worker).take());
                return;
            }
            if lifecycle.abandoned {
                lifecycle = self
                    .lifecycle_changed
                    .wait_timeout(lifecycle, HELD_RETRY)
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
                lifecycle.requested = true;
            }
        }
    }
}

impl ProjFsSession {
    pub(super) fn request_retirement(&mut self) -> Result<Arc<Projector>, NativeMountError> {
        let projector = self
            .controller
            .as_ref()
            .cloned()
            .ok_or(NativeMountError::ControlPending)?;
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.callbacks.close_admission();
            lock_recover(&projector.state).stopping = true;
            projector.changed.notify_all();
            let owner = Self {
                controller: None,
                context: self.context.take(),
                runtime: self.runtime.take(),
                retirement: self.retirement.take(),
                original_stop_work: self.original_stop_work.clone(),
                sdk_stop_credit: self.sdk_stop_credit.take(),
                worker_close_credit: self.worker_close_credit.take(),
                namespace_outcome: self.namespace_outcome.take(),
                qualification: self.qualification,
                capture_failure: self.capture_failure.take(),
                capture_attempted: self.capture_attempted,
            };
            self.original_stop_work = self.original_stop_work.allocation_work();
            let mut lifecycle = lock_recover(&projector.lifecycle);
            assert!(lifecycle.owner.is_none(), "one physical retirement owner");
            lifecycle.custody = owner.retirement_ownership();
            lifecycle.owner = Some(owner);
        }
        let mut lifecycle = lock_recover(&projector.lifecycle);
        lifecycle.requested = true;
        projector.lifecycle_changed.notify_all();
        drop(lifecycle);
        Ok(projector)
    }

    pub(in crate::native_mount) fn stop(
        &mut self,
        wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        wait.check()
            .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        if self.controller.is_none() {
            return self.stop_physical();
        }
        if self
            .runtime
            .as_ref()
            .is_some_and(|runtime| lock_recover(&runtime.active_work).upgrade().is_some())
        {
            return Err(NativeMountError::ControlPending);
        }
        let projector = self.request_retirement()?;
        let mut lifecycle = lock_recover(&projector.lifecycle);
        loop {
            if lifecycle.complete {
                return Ok(());
            }
            if let Some(error) = lifecycle.failure.take() {
                return Err(error);
            }
            wait.check()
                .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
            lifecycle = projector
                .lifecycle_changed
                .wait_timeout(lifecycle, HELD_RETRY)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

impl Drop for ProjFsSession {
    fn drop(&mut self) {
        if self.controller.is_some()
            && let Ok(projector) = self.request_retirement()
        {
            lock_recover(&projector.lifecycle).abandoned = true;
            projector.lifecycle_changed.notify_all();
        }
    }
}
