use super::*;
use crate::{executor::stock_boundary, wire::ErrorCode};

/// Replaceable ordered context transformation.
pub trait ContextStage: acyclic_stream::ProviderPlatform {
    /// Stable stage name used for diagnostics and composition.
    fn name(&self) -> &str;
    /// Immutable contract under the optional original ledger.
    fn contract(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Value>;
    /// Validates a replacement before installation.
    fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io
            .and_then(crate::executor::PreparationIo::original_native_work)
            .is_some()
            && !self.supports_native()
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Unsupported,
                "context stage has no native capability",
            ));
        }
        if !crate::is_valid_component_label(self.name()) {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "context stage identity is invalid",
            ));
        }
        if !self.contract(io)?.is_object() {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "context stage contract must be an object",
            ));
        }
        Ok(())
    }
    /// Transforms context under admitted limits and the optional original ledger.
    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>>;
    /// Reports original-ledger support before constructing a future.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        false
    }
}

/// Explicit ordered pipeline; no hidden framework stages are inserted.
#[derive(Clone, Default)]
pub struct ContextPipeline(Vec<Arc<dyn ContextStage>>);

impl ContextPipeline {
    /// Validates stage identities on the optional preparation ledger.
    pub fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            work.remaining()?;
            if self.0.iter().any(|stage| !stage.supports_native()) {
                return Err(crate::Error::NativeBoundary {
                    code: ErrorCode::Unsupported,
                    reason: "context stage has no native capability",
                });
            }
        }
        for stage in &self.0 {
            stage.validate(io)?;
        }
        Ok(())
    }

    /// Appends to a caller-guarded native pipeline, returning only its actual backing-capacity delta.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn append_with_native_work(
        &mut self,
        stage: Arc<dyn ContextStage>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeAllocation> {
        work.remaining()?;
        if !stage.supports_native() {
            return Err(crate::Error::NativeBoundary {
                code: ErrorCode::Unsupported,
                reason: "context stage has no native capability",
            });
        }
        stage.validate(Some(&crate::executor::PreparationIo::from_native_work(
            work,
        )?))?;
        let capacity = self
            .0
            .len()
            .checked_add(1)
            .ok_or_else(|| crate::Error::NativeBoundary {
                code: ErrorCode::Invalid,
                reason: "context stage count overflows",
            })?;
        let guard = reserve_context_vec_native(&mut self.0, capacity, work)?;
        self.0.push(stage);
        Ok(guard)
    }

    /// Installs a structurally valid replacement for future operations only.
    pub fn reload(&self, replacement: Self) -> Result<Self> {
        replacement.validate(None)?;
        Ok(replacement)
    }

    /// Creates a pipeline in exact execution order.
    #[must_use]
    pub fn new(stages: impl IntoIterator<Item = Arc<dyn ContextStage>>) -> Self {
        Self(stages.into_iter().collect())
    }

    /// Appends one stage and returns the pipeline for code-first composition.
    #[must_use]
    pub fn with(mut self, stage: Arc<dyn ContextStage>) -> Self {
        self.0.push(stage);
        self
    }

    /// Builds the canonical selection/current input and in-turn messages before stages.
    pub fn base_context(
        input: &ContextInput,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        let metadata = BTreeMap::new();
        let messages = ContextMessages {
            first: input
                .selected_context
                .as_ref()
                .map_or(&[], |selected| selected.messages.as_slice()),
            user: input.selected_context.is_none().then_some(&input.input),
            second: &input.prior_messages,
        };
        let base_count = input
            .selected_context
            .as_ref()
            .map_or(1, |selected| selected.messages.len());
        let current_input_index = u32::try_from(base_count.checked_sub(1).ok_or_else(|| {
            stock_boundary(
                io,
                ErrorCode::Invalid,
                "current input is missing from context",
            )
        })?)
        .map_err(|_| {
            stock_boundary(
                io,
                ErrorCode::Invalid,
                "current input index exceeds portable count",
            )
        })?;
        let base = BorrowedContext {
            messages,
            metadata: &metadata,
            current_input_index: Some(current_input_index),
        };
        base.validate_bounds(limits, io)?;
        crate::executor::with_history_io(io, |work| {
            input.input.validate_limits(limits, work)?;
            input.input.validate_user_input(work)
        })?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input, io)?;
        }
        if input
            .selected_context
            .as_ref()
            .is_some_and(|selected| selected.selection.checkpoint.is_some())
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Unsupported,
                "canonical checkpoint requires owner-resolved base context",
            ));
        }
        let messages = crate::executor::with_history_io(io, |work| {
            compaction::clone_messages(
                base.messages.first,
                base.messages.user,
                base.messages.second,
                work,
            )
            .map(|(copy, _)| copy)
        })?;
        let context = Context {
            messages,
            metadata,
            current_input_index: Some(current_input_index),
        };
        validate_projected_context(&context, limits, io)?;
        Ok(context)
    }

    /// Adds an exact new delta to an owner-resolved base before running stages.
    pub fn continue_base(
        mut retained: Context,
        delta: Context,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        validate_projected_context(&retained, limits, io)?;
        if delta.current_input_index.is_none() {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "continued base has no current input",
            ));
        }
        // The retained parent's old active input is historical in this admission.
        retained.current_input_index = None;
        selection::place_context(retained, delta, ContextPlacement::Append, limits, io)
    }

    /// Bounds the initial view and every intermediate projection, without truncation.
    pub async fn run(
        &self,
        input: &ContextInput,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        self.transform(input, Self::base_context(input, limits, io)?, limits, io)
            .await
    }

    /// Applies declared stages to an explicit base, including a retained canonical checkpoint plus its new history delta.
    pub async fn transform(
        &self,
        input: &ContextInput,
        mut context: Context,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        self.validate(io)?;
        if self.0.len() > limits.context_messages {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "context stage count exceeds limit",
            ));
        }
        crate::executor::with_history_io(io, |work| input.input.validate_user_input(work))?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input, io)?;
        }
        validate_projected_context(&context, limits, io)?;
        if context.current_input_index.is_none() {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "base context has no current input marker",
            ));
        }
        for stage in &self.0 {
            context = stage.apply(input, context, limits, io)?.await?;
            validate_projected_context(&context, limits, io)?;
            if context.current_input_index.is_none() {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "context stage removed the current input marker",
                ));
            }
        }
        Ok(context)
    }

    /// Returns immutable stage contracts on the optional preparation ledger.
    pub fn contracts(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Vec<Value>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io
            .and_then(crate::executor::PreparationIo::original_native_work)
            .is_some()
            && self.0.iter().any(|stage| !stage.supports_native())
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Unsupported,
                "context stage has no native capability",
            ));
        }
        let mut contracts = Vec::new();
        crate::executor::with_history_io(io, |work| {
            compaction::reserve_vector(&mut contracts, self.0.len(), work)
        })?;
        for stage in &self.0 {
            contracts.push(stage.contract(io)?);
        }
        Ok(contracts)
    }

    /// Copies stage handles on the optional preparation ledger.
    pub fn copy(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        let mut stages = Vec::new();
        crate::executor::with_history_io(io, |work| {
            compaction::reserve_vector(&mut stages, self.0.len(), work)?;
            let copied = self
                .0
                .len()
                .checked_mul(std::mem::size_of::<Arc<dyn ContextStage>>())
                .ok_or_else(|| {
                    stock_boundary(io, ErrorCode::Invalid, "context stage copy overflows")
                })?;
            if let Some(work) = work.as_deref_mut() {
                work.admit_copy(copied as u64)?;
                work.copy_bytes(copied as u64)?;
            }
            stages.extend(self.0.iter().cloned());
            Ok::<(), crate::Error>(())
        })?;
        Ok(Self(stages))
    }

    pub(crate) fn summary_source(
        source: Context,
        revision: String,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            if work.is_none() {
                return Ok(Self::new([Arc::new(SourceStage::new(
                    "summary-source",
                    revision,
                    Arc::new(source),
                    ContextPlacement::Prepend,
                )) as Arc<dyn ContextStage>]));
            }
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut stages: Vec<Arc<dyn ContextStage>> = Vec::new();
            let made = (|| {
                let name = quota
                    .work()
                    .as_deref_mut()
                    .expect("observed summary")
                    .clone_text("summary-source")?;
                quota.adopt(name.capacity() as u64)?;
                let bytes = std::mem::size_of::<Context>() as u64;
                quota
                    .work()
                    .as_deref_mut()
                    .expect("observed summary")
                    .admit_allocation(bytes)?;
                quota
                    .work()
                    .as_deref_mut()
                    .expect("observed summary")
                    .retain_owned(bytes)?;
                quota.adopt(bytes)?;
                let source = Arc::new(source);
                let bytes = std::mem::size_of::<SourceStage>() as u64;
                quota
                    .work()
                    .as_deref_mut()
                    .expect("observed summary")
                    .admit_allocation(bytes)?;
                quota
                    .work()
                    .as_deref_mut()
                    .expect("observed summary")
                    .retain_owned(bytes)?;
                quota.adopt(bytes)?;
                let stage: Arc<dyn ContextStage> = Arc::new(SourceStage::new(
                    name,
                    revision,
                    source,
                    ContextPlacement::Prepend,
                ));
                compaction::reserve_vector(&mut stages, 1, quota.work())?;
                quota.adopt(
                    (stages.capacity() * std::mem::size_of::<Arc<dyn ContextStage>>()) as u64,
                )?;
                stages.push(stage);
                Ok(())
            })();
            if let Err(error) = made {
                drop(stages);
                return Err(error);
            }
            quota.transfer();
            Ok(Self(stages))
        })
    }
}
