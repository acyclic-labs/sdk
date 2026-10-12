use super::*;

/// Stock configuration retained on its issued task's original ledger.
pub(crate) struct NativeStockExecutor {
    stock: StockExecutor,
    interactions: Option<Arc<dyn crate::runtime::InteractionRouter>>,
    custody: acyclic_fs::NativeAllocation,
}

impl NativeStockExecutor {
    /// Copies main's stock configuration after validating its issued task binding.
    pub(crate) fn new(
        model: &Model,
        provider: Arc<dyn ModelProvider>,
        pipeline: &ContextPipeline,
        tools: &ToolRegistry,
        context: Arc<crate::runtime::TaskContext>,
        operation: OperationId,
        host: Arc<dyn DurableTaskHost>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        policy: Option<Arc<dyn ToolPolicy>>,
        interactions: Option<Arc<dyn crate::runtime::InteractionRouter>>,
        compaction: &crate::context::CompactionPolicy,
        max_output_tokens: Option<u32>,
    ) -> Result<Self> {
        let work = context.native_work()?;
        let invalid = |code, reason| Error::NativeBoundary { code, reason };
        if max_output_tokens == Some(0) {
            return Err(invalid(
                crate::wire::ErrorCode::Invalid,
                "model output token budget is zero",
            ));
        }
        if !context.original_task_binding_matches(task, fence) {
            return Err(invalid(
                crate::wire::ErrorCode::Unauthorized,
                "stock owner differs from the issued native task context",
            ));
        }
        if !provider.supports_native_model(work)? || !host.supports_native_execution(work)? {
            return Err(invalid(
                crate::wire::ErrorCode::Unsupported,
                "native stock producer is unavailable",
            ));
        }
        let io = PreparationIo::from_native_work(work)?;
        pipeline.validate(Some(&io))?;
        context.validate_stock_scope_with_native_work(
            context.scope(),
            context.scope().limits(),
            work,
        )?;
        if let Some(policy) = &policy {
            let original = context
                .stock_tool_policy_binding()
                .map(|(_, policy)| policy);
            if original.is_none_or(|original| !Arc::ptr_eq(policy, original)) {
                return Err(invalid(
                    crate::wire::ErrorCode::Unauthorized,
                    "stock policy is not issued by this context",
                ));
            }
            policy.supports_native_policy(work)?;
        }
        if let Some(interactions) = &interactions {
            interactions.supports_native_interactions(work)?;
        }
        io.absorb_native_custody(work.claim(
            std::mem::size_of::<Self>() as u64,
            0,
            acyclic_fs::WorkCounters::default(),
        )?)?;
        let model = crate::model::clone_model(model, Some(&io))?;
        let pipeline = pipeline.copy(Some(&io))?;
        let (tools, custody) = tools.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (fence, custody) = fence.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (scope, custody) = context.scope().clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let policy_identity = policy
            .as_ref()
            .map(|policy| {
                let (identity, custody) = policy.identity_with_native_work(work)?;
                io.absorb_native_custody(custody)?;
                Ok::<_, Error>(identity)
            })
            .transpose()?;
        let compaction = stock_compaction_copy(compaction, Some(&io))?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        let limits = context.scope().limits();
        let mut stock = StockExecutor::new(model, provider, pipeline, tools);
        stock.limits = limits;
        stock.tool_scope = scope;
        stock.policy = policy;
        stock.policy_identity = policy_identity;
        stock.task = Some((host, task, fence));
        stock.task_context = Some((context, operation));
        stock.compaction = compaction;
        stock.max_output_tokens = max_output_tokens;
        Ok(Self {
            stock,
            interactions,
            custody,
        })
    }

    /// Refuses fresh effects on the same configuration and retained journal.
    pub(crate) fn into_reconciliation(mut self) -> Self {
        self.stock.reconciliation_only = true;
        self
    }

    /// Runs the shared stock engine with the exact issued context and operation.
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
        work.remaining()?;
        if !std::ptr::eq(
            self.custody.work().runtime_meter().as_ref(),
            work.runtime_meter().as_ref(),
        ) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock configuration differs from its original work",
            });
        }
        if self
            .stock
            .task_context
            .as_ref()
            .is_none_or(|(original, operation)| {
                *operation != input.operation_id || !Arc::ptr_eq(original, context)
            })
            || !context.native_metadata_matches(work)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native stock execution differs from its original binding",
            });
        }
        native_execution_supported(&self.stock, context, journal, work)?;
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
