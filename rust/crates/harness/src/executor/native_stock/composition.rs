use super::*;

/// An admitted stock composition.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct NativeStockExecutor {
    pub(in crate::executor) stock: StockExecutor,
    pub(in crate::executor) interactions: Option<Arc<dyn crate::runtime::InteractionRouter>>,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl StockExecutor {
    /// Copies only the known stock configuration after original-work admission.
    pub fn new_with_native_work(
        model: &Model,
        provider: Arc<dyn ModelProvider>,
        pipeline: &ContextPipeline,
        tools: &ToolRegistry,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeStockExecutor> {
        if !provider.supports_native_model(work)? {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native model provider is unavailable",
            });
        }
        pipeline.validate(Some(&PreparationIo::from_native_work(work)?))?;
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.claim(
            std::mem::size_of::<NativeStockExecutor>() as u64,
            0,
            acyclic_fs::WorkCounters::default(),
        )?)?;
        let model = crate::model::clone_model(model, Some(&io))?;
        let pipeline = pipeline.copy(Some(&io))?;
        let (tools, tools_custody) = tools.clone_with_native_work(work)?;
        io.absorb_native_custody(tools_custody)?;
        let stock = Self::new(model, provider, pipeline, tools);
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(NativeStockExecutor {
            stock,
            interactions: None,
            custody,
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeStockExecutor {
    fn require_original_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        if !std::ptr::eq(
            self.custody.work().runtime_meter().as_ref(),
            work.runtime_meter().as_ref(),
        ) {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock configuration differs from its original work",
            });
        }
        Ok(())
    }

    /// Carries the original command's native journal route without altering its issued context.
    pub(crate) fn with_native_interactions(
        mut self,
        interactions: Arc<dyn crate::runtime::InteractionRouter>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        interactions.supports_native_interactions(work)?;
        self.interactions = Some(interactions);
        Ok(self)
    }

    /// Binds the same issued context, durable owner and fence without scope cloning.
    pub fn with_task_context_with_native_work(
        mut self,
        context: Arc<crate::runtime::TaskContext>,
        operation: OperationId,
        host: Arc<dyn DurableTaskHost>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        if !context.native_metadata_matches(work)
            || !context.as_ref().original_task_binding_matches(task, fence)
        {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock owner differs from the issued native task context",
            });
        }
        if !host.supports_native_execution(work)? {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native stock task owner is unavailable",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let (fence, fence_custody) = fence.clone_with_native_work(work)?;
        io.absorb_native_custody(fence_custody)?;
        let retained = io.owned_bytes()?;
        let mut incoming = io.into_native_custody(retained)?;
        self.custody.absorb(&mut incoming)?;
        self.stock.task = Some((host, task, fence));
        self.stock.task_context = Some((context, operation));
        Ok(self)
    }

    /// Uses the issued task's policy and original native metadata admission.
    pub fn with_tool_authority_with_native_work(
        mut self,
        scope: &RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        let Some((context, _)) = self.stock.task_context.as_ref() else {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock policy has no original context binding",
            });
        };
        context.validate_stock_scope_with_native_work(scope, self.stock.limits, work)?;
        if let Some(policy) = &policy {
            let Some((_, original)) = context.as_ref().stock_tool_policy_binding() else {
                return Err(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "stock policy is not issued by this context",
                });
            };
            if !Arc::ptr_eq(policy, original) {
                return Err(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "stock policy is not issued by this context",
                });
            }
        }
        let identity = if let Some(policy) = &policy {
            policy.supports_native_policy(work)?;
            let (identity, mut custody) = policy.identity_with_native_work(work)?;
            self.custody.absorb(&mut custody)?;
            Some(identity)
        } else {
            None
        };
        let (scope, mut custody) = scope.clone_with_native_work(work)?;
        self.custody.absorb(&mut custody)?;
        self.stock.tool_scope = scope;
        self.stock.policy = policy;
        self.stock.policy_identity = identity;
        Ok(self)
    }

    /// Replaces the compaction value only after its actual copy is admitted.
    pub fn with_compaction_policy_with_native_work(
        mut self,
        policy: &crate::context::CompactionPolicy,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        let io = PreparationIo::from_native_work(work)?;
        let policy = stock_compaction_copy(policy, Some(&io))?;
        let retained = io.owned_bytes()?;
        let mut incoming = io.into_native_custody(retained)?;
        self.custody.absorb(&mut incoming)?;
        self.stock.compaction = policy;
        Ok(self)
    }

    /// Applies existing checked scalar limits without allocating a new scope.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.stock.limits = limits;
        self
    }

    /// Sets the same output token budget as the ordinary composition.
    pub fn with_max_output_tokens(mut self, maximum: u32) -> Result<Self> {
        if maximum == 0 {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "model output token budget is zero",
            });
        }
        self.stock.max_output_tokens = Some(maximum);
        Ok(self)
    }

    /// Prevents every fresh model, tool and interaction intent on this same engine.
    #[must_use]
    pub fn into_reconciliation(mut self) -> Self {
        self.stock.reconciliation_only = true;
        self
    }

    pub(in crate::executor) fn admit_execution(
        &self,
        context: &Arc<crate::runtime::TaskContext>,
        input: &TurnInput,
        journal: &dyn ExecutionJournal,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_work(work)?;
        if self
            .stock
            .task_context
            .as_ref()
            .is_none_or(|(original, operation)| {
                *operation != input.operation_id || !Arc::ptr_eq(original, context)
            })
            || !context.native_metadata_matches(work)
        {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native stock execution differs from its original binding",
            });
        }
        native_execution_supported(&self.stock, context, journal, work)
    }

    /// Runs the complete ordinary/native stock loop using borrowed issued inputs.
    pub(crate) fn execute_progress_with_native_context<'a>(
        &'a self,
        context: &'a Arc<crate::runtime::TaskContext>,
        input: &'a TurnInput,
        journal: &'a dyn ExecutionJournal,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (ExecutionProgress<TurnOutput>, acyclic_fs::NativeAllocation),
        >,
    > {
        self.admit_execution(context, input, journal, work)?;
        native_progress(
            &self.stock,
            context,
            input,
            journal,
            work,
            self.interactions.as_ref(),
        )
    }
}
