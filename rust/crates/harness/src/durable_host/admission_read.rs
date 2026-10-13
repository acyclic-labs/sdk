//! One canonical admission reader on the supplied IO context.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn admission(
        &self,
        operation: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<TaskAdmissionRecord> {
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(
            &mut work,
            std::mem::size_of::<TaskAdmissionRecord>() as u64,
        )?;
        let spec = self
            .retained_declaration(operation, quota.work())
            .await?
            .ok_or_else(|| Error::NotFound("task admission".into()))?;
        quota.adopt(spec.logical_owned_bytes()?)?;
        let value = self.read_json(&spec.state, io).await?;
        quota.adopt(crate::executor::json::json_value_logical_owned_bytes(
            &value,
        )?)?;
        let (admission, held) = TaskAdmissionRecord::from_canonical_value(value, quota.work())?;
        quota.adopt(held)?;
        if admission.operation_id != operation
            || admission.task.name != spec.entrypoint.name
            || admission.task.version != spec.entrypoint.version
            || admission.task.digest != spec.entrypoint.digest
            || admission.output_schema != spec.entrypoint.result_schema
            || admission
                .parent
                .map(|task| OperationId::from_bytes(task.into_bytes()))
                != spec.parent.as_ref().map(|parent| parent.operation_id)
            || spec
                .parent
                .as_ref()
                .is_some_and(|parent| parent.slot != operation.to_string())
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || admission.policy != self.policy.as_ref().map(|policy| policy.identity())
            || admission.extensions.as_ref() != self.root_scope.extensions()
        {
            return Err(Error::Conflict(
                "retained task admission disagrees with its declaration".into(),
            ));
        }
        if let Some(limits) = self.session_limits {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(quota.work()).await?;
            if coordinator
                .scheduler()
                .session_limits(operation, quota.work())?
                != limits
            {
                return Err(Error::Conflict(
                    "retained session ceilings differ from host".into(),
                ));
            }
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate(quota.work())?;
        }
        self.tasks.validate_durable_admission(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input),
            quota.work(),
        )?;
        if self
            .machines
            .resolve(&admission.machine, quota.work())?
            .is_none()
        {
            return Err(Error::Unsupported(
                "pinned task machine is unavailable".into(),
            ));
        }
        if admission.resource_proofs.is_empty() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = io.and_then(|io| io.original_native_work()) {
                self.root_scope.validate_narrowing_with_native_work(
                    &admission.grants,
                    &admission.limits,
                    native,
                )?;
            } else {
                self.root_scope
                    .narrow(admission.grants.clone(), admission.limits)?
                    .with_run_limits(admission.run_limits, quota.work())?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            self.root_scope
                .narrow(admission.grants.clone(), admission.limits)?
                .with_run_limits(admission.run_limits, quota.work())?;
            let ancestor = self.root_scope.run_limits();
            for (bound, actual) in [
                (
                    ancestor.concurrency.map(|v| v as u64),
                    admission.run_limits.concurrency.map(|v| v as u64),
                ),
                (
                    ancestor.max_steps.map(|v| v as u64),
                    admission.run_limits.max_steps.map(|v| v as u64),
                ),
                (
                    ancestor.deadline_epoch_ms,
                    admission.run_limits.deadline_epoch_ms,
                ),
            ] {
                if bound.is_some_and(|bound| actual.is_none_or(|actual| actual > bound)) {
                    return Err(Error::Unauthorized(
                        "task run limits widen root authority".into(),
                    ));
                }
            }
        }
        let retained =
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + admission.logical_owned_bytes()?;
        drop(spec);
        quota.replace(retained)?;
        quota.transfer();
        Ok(admission)
    }
}
