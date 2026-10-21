use super::*;

impl TaskContext {
    /// Calls a local tool under this task's scope and validates both schemas.
    pub async fn call<I, O>(&self, tool: &ToolDefinition, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable tool calls require recorded effect admission".into(),
            ));
        }
        let binding = self
            .harness
            .tools
            .get(&tool.name, Some(&tool.revision), None)?
            .ok_or_else(|| Error::NotFound(format!("tool {}", tool.name)))?;
        if binding.definition != *tool {
            return Err(Error::Conflict(
                "tool definition changed after lookup".into(),
            ));
        }
        let required = capability::tool_call(&tool.name);
        if !self.scope.grants.contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        let arguments =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        crate::executor::validate_json_schema_value(
            &tool.input_schema,
            &arguments,
            "tool input",
            &mut None,
        )?;
        let operation_id = OperationId::new();
        let invocation = ToolInvocation {
            operation_id,
            call_id: operation_id.to_string(),
            name: tool.name.clone(),
            arguments,
        };
        let context = ToolContext::new(
            std::sync::Arc::new(self.clone()),
            invocation.operation_id,
            &invocation.call_id,
            None,
        )?;
        binding
            .executor
            .authorize(None, Some(&context), &invocation, None)?;
        if let Some(operation) = self.authorize_tool(tool, &invocation).await? {
            return Err(Error::Indeterminate(operation));
        }
        let result = binding.executor.execute(invocation, Some(context)).await?;
        crate::executor::validate_json_schema_value(
            &tool.output_schema,
            &result.value,
            "tool output",
            &mut None,
        )?;
        serde_json::from_value(result.value).map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Returns any pending approval before dispatching the action.
    pub(crate) async fn authorize_tool(
        &self,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
    ) -> Result<Option<OperationId>> {
        self.authorize_tool_policies(definition, invocation, None, None, &self.scope)
            .await
    }

    /// Borrows the original configured policy and its protected binding identity.
    pub(crate) fn stock_tool_policy_binding(
        &self,
    ) -> Option<(&ComponentIdentity, &Arc<dyn ToolPolicy>)> {
        self.harness
            .policy_identity
            .as_ref()
            .zip(self.harness.policy.as_ref())
    }

    /// Borrows scoped vetoes in their original admission order.
    pub(crate) fn stock_tool_policy_overrides(
        &self,
    ) -> impl Iterator<Item = (&ComponentIdentity, &Arc<dyn ToolPolicy>)> {
        self.policy_overrides
            .iter()
            .map(|(identity, policy)| (identity, policy))
    }

    /// A borrowed scope can restrict the original grant, never replace its sealed selections, created-access owner, or runtime provenance.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_native_scope_restriction(
        &self,
        scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let limits = scope.limits;
        let run = scope.run_limits;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        limits.validate(&mut Some(&mut io))?;
        run.validate(&mut Some(&mut io))?;
        let same_runtime = match (&self.scope.extension_runtime, &scope.extension_runtime) {
            (None, None) => true,
            (Some(original), Some(candidate)) => Arc::ptr_eq(original, candidate),
            _ => false,
        };
        let same_created_access = match (&self.scope.created_access, &scope.created_access) {
            (None, None) => true,
            (Some(original), Some(candidate)) => Arc::ptr_eq(original, candidate),
            _ => false,
        };
        if !self.scope.extensions_sealed
            || !scope.extensions_sealed
            || self.scope.extensions != scope.extensions
            || self.scope.extension_schemas != scope.extension_schemas
            || self.scope.resource_proofs != scope.resource_proofs
            || !same_runtime
            || !same_created_access
            || !self.scope.allows_authority(&scope.grants, &limits)
            || !self.scope.run_limits.allows(&run)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native scope cannot widen or replace its original scope",
            });
        }
        Ok(())
    }

    pub(super) async fn authorize_tool_policies(
        &self,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        work: Option<&NativeWork>,
        command_router: Option<&dyn InteractionRouter>,
        scope: &RuntimeScope,
    ) -> Result<Option<OperationId>> {
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = command_router;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
            if let Some(router) = command_router {
                router.supports_native_interactions(work)?;
            }
            for (_, policy) in self
                .stock_tool_policy_binding()
                .into_iter()
                .chain(self.stock_tool_policy_overrides())
            {
                policy.identity_with_native_work(work)?;
            }
        }
        if self.harness.policy.is_some() && self.harness.policy_identity.is_none() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "runtime policy identity changed after binding",
                });
            }
            return Err(Error::Conflict(
                "runtime policy identity changed after binding".into(),
            ));
        }
        for (identity, policy) in self
            .stock_tool_policy_binding()
            .into_iter()
            .chain(self.stock_tool_policy_overrides())
        {
            if let Some(paid) = self
                .policy_approval(
                    policy.as_ref(),
                    Some(identity),
                    definition,
                    invocation,
                    work,
                    scope,
                )
                .await?
            {
                let ((approval_id, request), custody) = paid;
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let _ = custody;
                let outcome: (InteractionOutcome, Option<NativeCustody>) = match work {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    Some(work) => {
                        let (value, custody) = self
                            .route_native_interaction(
                                approval_id,
                                request,
                                custody.ok_or(Error::NativeBoundary {
                                    code: crate::wire::ErrorCode::Invalid,
                                    reason: "native approval has no retained prompt",
                                })?,
                                work,
                                command_router,
                                scope,
                            )
                            .await?;
                        (value, Some(custody))
                    }
                    _ => (self.interact(approval_id, request).await?, None),
                };
                if let InteractionOutcome::Indeterminate { operation_id } = &outcome.0 {
                    return Ok(Some(*operation_id));
                }
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.is_some() && matches!(&outcome.0, InteractionOutcome::Answered { .. }) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        reason: "approval resolved with a question answer",
                    });
                }
                check_tool_approval(outcome.0)?;
            }
        }
        Ok(None)
    }

    pub(crate) async fn policy_approval(
        &self,
        policy: &dyn ToolPolicy,
        expected: Option<&ComponentIdentity>,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        work: Option<&NativeWork>,
        scope: &RuntimeScope,
    ) -> Result<Option<((OperationId, Interaction), Option<NativeCustody>)>> {
        match self
            .policy_decision(policy, expected, definition, invocation, work, scope)
            .await?
        {
            PolicyVerdict::Allow => Ok(None),
            PolicyVerdict::Deny(denial) => Err(denial),
            PolicyVerdict::Approval(request) => Ok(Some(request)),
        }
    }

    /// [`Self::policy_approval`] with the policy's denial kept apart from evaluation failures, so
    /// a caller can return the denial to the model while every failure stays strict.
    pub(crate) async fn policy_decision(
        &self,
        policy: &dyn ToolPolicy,
        expected: Option<&ComponentIdentity>,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        work: Option<&NativeWork>,
        scope: &RuntimeScope,
    ) -> Result<PolicyVerdict> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        let identity: (ComponentIdentity, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let (value, custody) = policy.identity_with_native_work(work)?;
                check_policy_metadata(&value, &custody, work)?;
                (value, Some(custody))
            }
            _ => (policy.identity(), None),
        };
        if expected.is_some_and(|expected| expected != &identity.0) {
            return Err(crate::executor::stock_boundary(
                io.as_ref(),
                crate::wire::ErrorCode::Conflict,
                "policy identity changed after binding",
            ));
        }
        let future = policy.evaluate(invocation, scope, io.as_ref())?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work
            && !matches!(&future, crate::store::HistoryContentFuture::Native(future) if future.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "policy evaluator belongs to another ledger",
            });
        }
        let decision = future.await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            validate_native_policy_decision(&decision, work)?;
        }
        let after: (ComponentIdentity, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let (value, custody) = policy.identity_with_native_work(work)?;
                check_policy_metadata(&value, &custody, work)?;
                (value, Some(custody))
            }
            _ => (policy.identity(), None),
        };
        if after.0 != identity.0 {
            return Err(crate::executor::stock_boundary(
                io.as_ref(),
                crate::wire::ErrorCode::Conflict,
                "policy identity changed during evaluation",
            ));
        }
        drop(after);
        let (decision, custody) = decision;
        match decision {
            ToolPolicyDecision::Allow => Ok(PolicyVerdict::Allow),
            ToolPolicyDecision::Deny { reason } => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(custody) = custody {
                    return Ok(PolicyVerdict::Deny(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Unauthorized,
                        message: reason,
                        custody,
                    }));
                }
                Ok(PolicyVerdict::Deny(Error::Unauthorized(reason)))
            }
            ToolPolicyDecision::RequireApproval { prompt } => {
                let task = self
                    .durable_task
                    .unwrap_or_else(|| TaskId::from_bytes(self.task_id.into_bytes()));
                Ok(PolicyVerdict::Approval(tool_approval_request(
                    task,
                    &identity.0,
                    definition,
                    invocation,
                    (prompt, custody),
                    work,
                )?))
            }
        }
    }
}

/// A bound policy's evaluated decision for one call.
pub(crate) enum PolicyVerdict {
    /// The call may proceed.
    Allow,
    /// The policy denied the call; the error carries its reason.
    Deny(Error),
    /// The call waits on this approval request.
    Approval(((OperationId, Interaction), Option<NativeCustody>)),
}
