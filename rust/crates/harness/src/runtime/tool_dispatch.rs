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
            .get_version(&tool.name, &tool.revision, None)?
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
        validate_value(&tool.input_schema, &arguments, "tool input", None)?;
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
            &(invocation.call_id.clone()),
            None,
        )?;
        binding
            .executor
            .authorize_with_context(&context, &invocation)?;
        if let Some(operation) = self.authorize_tool(tool, &invocation).await? {
            return Err(Error::Indeterminate(operation));
        }
        let result = binding
            .executor
            .execute_with_context(context, invocation)
            .await?;
        validate_value(&tool.output_schema, &result.value, "tool output", None)?;
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
                policy.supports_native_policy(work)?;
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
            let bound_reason = if self
                .harness
                .policy
                .as_ref()
                .is_some_and(|global| Arc::ptr_eq(global, policy))
            {
                "runtime policy identity changed after binding"
            } else {
                "scoped policy identity changed after binding"
            };
            if let Some(paid) = self
                .policy_approval(
                    policy.as_ref(),
                    Some((identity, bound_reason)),
                    definition,
                    invocation,
                    work,
                    scope,
                )
                .await?
            {
                let ((approval_id, request), custody) = paid;
                let outcome: (InteractionOutcome, Option<NativeCustody>) = match work {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    Some(work) => {
                        let (value, custody) = self
                            .interact_with_native_work_and_router(
                                approval_id,
                                request,
                                custody.expect("native approval retains its prompt"),
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn stock_policy_approval(
        &self,
        policy: &dyn ToolPolicy,
        scope: &RuntimeScope,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<((OperationId, Interaction), acyclic_fs::NativeAllocation)>> {
        self.require_original_native_work(work)?;
        self.require_native_scope_restriction(scope, work)?;
        let expected = self
            .stock_tool_policy_binding()
            .into_iter()
            .chain(self.stock_tool_policy_overrides())
            .find_map(|(identity, original)| {
                std::ptr::eq(original.as_ref(), policy).then_some(identity)
            })
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "policy is not an original task binding",
            })?;
        Ok(self
            .policy_approval(
                policy,
                Some((expected, "policy identity changed after binding")),
                definition,
                invocation,
                Some(work),
                scope,
            )
            .await?
            .map(|paid| (paid.0, paid.1.expect("native approval retains its prompt"))))
    }

    pub(super) async fn policy_approval(
        &self,
        policy: &dyn ToolPolicy,
        expected: Option<(&ComponentIdentity, &'static str)>,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        work: Option<&NativeWork>,
        scope: &RuntimeScope,
    ) -> Result<Option<((OperationId, Interaction), Option<NativeCustody>)>> {
        let identity: (ComponentIdentity, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                policy.supports_native_policy(work)?;
                let (value, custody) = policy.identity_with_native_work(work)?;
                let paid = (value, Some(custody));
                validate_native_policy_identity(&paid, work)?;
                paid
            }
            _ => (policy.identity(), None),
        };
        if let Some((expected, reason)) = expected {
            if expected != &identity.0 {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.is_some() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        reason,
                    });
                }
                return Err(Error::Conflict(reason.into()));
            }
        }
        let decision: (ToolPolicyDecision, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let future = policy.evaluate_with_native_work(invocation, scope, work)?;
                if !future.work().shares_ledger(work) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        reason: "policy evaluator belongs to another ledger",
                    });
                }
                let (value, custody) = future.await?;
                let paid = (value, Some(custody));
                validate_native_policy_decision(&paid, work)?;
                paid
            }
            _ => (policy.evaluate(invocation, scope).await?, None),
        };
        let after: (ComponentIdentity, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let (value, custody) = policy.identity_with_native_work(work)?;
                let paid = (value, Some(custody));
                validate_native_policy_identity(&paid, work)?;
                paid
            }
            _ => (policy.identity(), None),
        };
        if after.0 != identity.0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "policy identity changed during evaluation",
                });
            }
            return Err(Error::Conflict(
                "policy identity changed during evaluation".into(),
            ));
        }
        drop(after);
        let (decision, custody) = decision;
        match decision {
            ToolPolicyDecision::Allow => Ok(None),
            ToolPolicyDecision::Deny { reason } => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(custody) = custody {
                    return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Unauthorized,
                        message: reason,
                        custody,
                    });
                }
                Err(Error::Unauthorized(reason))
            }
            ToolPolicyDecision::RequireApproval { prompt } => {
                let task = self
                    .durable_task
                    .unwrap_or_else(|| TaskId::from_bytes(self.task_id.into_bytes()));
                Ok(Some(tool_approval_request(
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
