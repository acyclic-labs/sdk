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
        let binding = self.caller_binding(caller).await?;
        let reducer = StreamAggregate::open(
            &self.stream,
            binding.authority.clone(),
            binding.verifier()?,
            SchemaRegistry::default(),
            None,
            HISTORY_LIMITS,
            acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
            Some(source.request.parent_revision),
            &mut None,
        )
        .await?
        .into_reducer();
        json_line(
            "fork-selection",
            &source
                .request
                .clone()
                .with_history_policy(&reducer, history, &mut None)?,
        )
    }
    pub async fn publish_preparation(
        &self,
        caller: TaskId,
        lease: &WorkLease,
        preparation: TaskId,
    ) -> Result<String> {
        self.caller_context(caller, lease).await?;
        let host = self.runtime.task_host();
        let admission = host.observe_admission(preparation).await?;
        if admission.parent != Some(caller) || admission.task.name != "acyclic.fork_preparation" {
            return Err(Error::Unauthorized(
                "preparation belongs to another caller".into(),
            ));
        }
        let input: ResourcePreparationRequest =
            serde_json::from_value(admission.input).map_err(invalid)?;
        input.validate(&mut None)?;
        let Some(acyclic_harness::Outcome::Succeeded(value)) = host.outcome(preparation).await?
        else {
            return Err(Error::Conflict(
                "preparation has no successful report".into(),
            ));
        };
        let report: ForkReport = serde_json::from_value(value).map_err(invalid)?;
        report.validate(&mut None)?;
        if input.child.parent != Some(caller)
            || serde_json::to_value(&report.request).map_err(invalid)? != input.payload
        {
            return Err(Error::Unauthorized(
                "report changed the retained preparation".into(),
            ));
        }
        if !self.config.fork_sources.iter().any(|source| {
            source.caller == caller
                && source.request.parent == report.request.parent
                && source.request.parent_revision == report.request.parent_revision
        }) {
            return Err(Error::Unsupported(
                "source publication binding unavailable".into(),
            ));
        }
        let owner = self.caller_binding(caller).await?;
        let binding = self
            .config
            .bindings()
            .find(|binding| binding.authority == report.request.child)
            .ok_or_else(|| Error::Unsupported("child conversation binding unavailable".into()))?;
        let mut parent = open_aggregate(
            &self.stream,
            &self.files,
            &self.config.stream_provider,
            owner,
        )
        .await?;
        let mut child = open_aggregate(
            &self.stream,
            &self.files,
            &self.config.stream_provider,
            binding,
        )
        .await?;
        let seed = child
            .spawn_from_report(
                &mut parent,
                report,
                owner.scope.clone(),
                binding.scope.clone(),
            )
            .await?;
        let operation = input.child.operation_id;
        let admitted = host
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
