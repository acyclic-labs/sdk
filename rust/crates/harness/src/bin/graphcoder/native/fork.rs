use super::*;
impl NativeSession {
    pub async fn select_fork(
        &self,
        caller: TaskId,
        lease: &WorkLease,
        operation: OperationId,
        history: acyclic_harness::fork::ForkHistoryPolicy,
    ) -> Result<String> {
        self.caller_context(caller, lease).await?;
        let source = self
            .config
            .fork_sources
            .iter()
            .find(|source| source.caller == caller && source.request.operation_id == operation)
            .ok_or_else(|| Error::NotFound("original fork source".into()))?;
        let reducer = acyclic_harness::core::Reducer::restore(
            source.snapshot.clone(),
            source.owner.verifier()?,
            SchemaRegistry::default(),
            &mut None,
        )?;
        let request = source
            .request
            .clone()
            .with_history_policy(&reducer, history, &mut None)?;
        json_line("fork-selection", &request)
    }
    pub async fn publish_preparation(
        &self,
        caller: TaskId,
        lease: &WorkLease,
        preparation: TaskId,
    ) -> Result<String> {
        self.caller_context(caller, lease).await?;
        let admission = self
            .runtime
            .task_host()
            .observe_admission(preparation)
            .await?;
        if admission.parent != Some(caller) || admission.task.name != "acyclic.fork_preparation" {
            return Err(Error::Unauthorized(
                "preparation does not belong to this original caller".into(),
            ));
        }
        let input: ResourcePreparationRequest = serde_json::from_value(admission.input)
            .map_err(|error| Error::Invalid(format!("retained preparation input: {error}")))?;
        input.validate(&mut None)?;
        let Some(acyclic_harness::Outcome::Succeeded(value)) =
            self.runtime.task_host().outcome(preparation).await?
        else {
            return Err(Error::Conflict(
                "preparation has no authoritative successful report".into(),
            ));
        };
        let report: ForkReport = serde_json::from_value(value)
            .map_err(|error| Error::Invalid(format!("retained fork report: {error}")))?;
        report.validate(&mut None)?;
        if input.child.parent != Some(caller)
            || serde_json::to_value(&report.request).map_err(invalid)? != input.payload
        {
            return Err(Error::Unauthorized(
                "report changed the exact retained preparation/caller".into(),
            ));
        }
        let parent_binding = self
            .config
            .fork_sources
            .iter()
            .find(|source| {
                source.caller == caller
                    && source.owner.authority == report.request.parent
                    && source.snapshot.revision == report.request.parent_revision
            })
            .ok_or_else(|| {
                Error::Unsupported("original source publication binding unavailable".into())
            })?;
        let child_binding = self
            .config
            .observed_authorities
            .iter()
            .find(|binding| binding.authority == report.request.child)
            .ok_or_else(|| {
                Error::Unsupported(
                    "explicit child conversation provider binding unavailable".into(),
                )
            })?;
        let mut parent = open_aggregate(
            &self.stream,
            &self.files,
            &self.config.stream_provider,
            &parent_binding.owner,
        )
        .await?;
        let mut child = open_aggregate(
            &self.stream,
            &self.files,
            &self.config.stream_provider,
            child_binding,
        )
        .await?;
        let seed = child
            .spawn_from_report(
                &mut parent,
                report,
                parent_binding.owner.scope.clone(),
                child_binding.scope.clone(),
            )
            .await?;
        let operation = input.child.operation_id;
        let admitted = self
            .runtime
            .task_host()
            .admit_owned(input.child, LeaseFence::from(&lease.reservation))
            .await?;
        json_line(
            "fork",
            &serde_json::json!({
                "preparation": preparation, "new_child_agent": seed.child_agent,
                "child_admission_operation": operation, "admission": admitted, "seed": seed,
            }),
        )
    }
}
