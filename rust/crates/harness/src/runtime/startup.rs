use super::*;

impl AgentHarness {
    /// Derives an immutable application view with no more authority or resources than its parent.
    pub fn scoped(self: &Arc<Self>, grants: Capabilities, limits: Limits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.narrow(grants, limits)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

    /// Narrows the effective task execution bounds for newly admitted work.
    pub fn scoped_run_limits(self: &Arc<Self>, limits: TaskRunLimits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.clone().with_run_limits(limits, &mut None)?;
        child.live = TaskGroup::new(child.scope.concurrency_bound(self.concurrency), None)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

    /// Selects a different qualified execution route for newly admitted work under attenuated authority.
    pub fn scoped_execution(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Arc<Self>> {
        let identity = execution.identity();
        validate_policy_identity(&identity, None)?;
        let mut routed = self.as_ref().clone();
        routed.scope = self.scope.narrow(grants, limits)?;
        routed.fork_preparer = None;
        routed.workspaces = None;
        routed.host = None;
        routed.state = Some(execution.state());
        routed.spawner = Some(execution.spawner());
        routed.execution = Some(execution);
        routed.assert_durable_policy_bindings(None)?;
        Ok(Arc::new(routed))
    }

    /// Submits one signed, versioned answer through its conversation owner.
    pub async fn resolve_answer(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> Result<ResolutionReceipt> {
        let receipt = self
            .interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .resolve_answer(operation_id, scope, id, expected_version, response)
            .await?;
        receipt.validate()?;
        if receipt.operation_id != operation_id
            || receipt.version != expected_version
            || receipt.id
                != uuid::Uuid::parse_str(&id.to_string())
                    .map_err(|error| Error::Invalid(error.to_string()))?
            || !matches!(receipt.outcome, InteractionOutcome::Answered { .. })
        {
            return Err(Error::Conflict(
                "interaction resolver returned another answer".into(),
            ));
        }
        Ok(receipt)
    }

    /// Submits one signed decision for the ticket's exact approved intent.
    pub async fn resolve_approval(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ResolutionReceipt> {
        let receipt = self
            .interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .resolve_approval(operation_id, scope, id, expected_version, approved, reason)
            .await?;
        receipt.validate()?;
        if receipt.operation_id != operation_id
            || receipt.version != expected_version
            || receipt.id
                != uuid::Uuid::parse_str(&id.to_string())
                    .map_err(|error| Error::Invalid(error.to_string()))?
            || !matches!(
                (&receipt.outcome, approved),
                (InteractionOutcome::Approved, true) | (InteractionOutcome::Declined, false)
            )
        {
            return Err(Error::Conflict(
                "interaction resolver returned another decision".into(),
            ));
        }
        Ok(receipt)
    }

    /// Resolves one registered task with exact Rust input/output types.
    pub fn task<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        self.tasks.get(name)
    }

    /// Resolves one model-visible tool definition by exact registered name.
    pub fn tool(&self, name: &str) -> Result<ToolDefinition> {
        self.tools
            .get(name, None, None)?
            .map(|tool| tool.definition.clone())
            .ok_or_else(|| Error::NotFound(format!("tool {name}")))
    }

    /// Verifies an already staged input or output through a bound content or artifact owner and the runtime's effective read grant.
    pub async fn verify_readable_file(&self, file: &FileRef) -> Result<()> {
        file.validate(&mut None)?;
        self.scope.limits.validate_file(file, &mut None)?;
        if !read_granted(self.scope.grants(), file)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this file".into(),
            ));
        }
        let mut failure = Error::Unsupported("content or artifact reader is not bound".into());
        for binding in [self.content.as_ref(), self.artifacts.as_ref()]
            .into_iter()
            .flatten()
        {
            match verified_content_bytes(
                binding.reader.as_ref(),
                file,
                (file).descriptor().byte_length(),
                None,
            )
            .await
            .map(|(bytes, _)| bytes)
            {
                Ok(_) => return Ok(()),
                Err(error) => failure = error,
            }
        }
        Err(failure)
    }
}
