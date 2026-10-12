//! Original-work admission and custody for the shared stock execution engine.
use super::*;
use crate::runtime::TaskStateProvider;
use crate::projection::ModelContextSelection;
use std::fmt::{Arguments, Write as _};

pub(super) struct StockExecution<'a> {
    pub(super) stock: &'a StockExecutor,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native: Option<NativeStockExecution<'a>>,
    pub(super) summary: Option<(ContextPipeline, Option<u32>)>,
    pub(super) reconciliation_only: bool,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Clone, Copy)]
pub(super) struct NativeStockExecution<'a> {
    pub(super) context: &'a Arc<crate::runtime::NativeTaskContext>,
    pub(super) io: &'a PreparationIo,
    pub(super) operation: OperationId,
    pub(super) interactions: Option<&'a Arc<dyn crate::runtime::InteractionRouter>>,
}

impl<'a> StockExecution<'a> {
    pub(super) fn ordinary(stock: &'a StockExecutor) -> Self {
        Self { stock,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native: None, summary: None, reconciliation_only: stock.reconciliation_only }
    }
    pub(super) fn io(&self) -> Option<&PreparationIo> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { return self.native.map(|native| native.io); }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        { None }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        self.io().and_then(PreparationIo::original_native_work)
    }
    pub(super) fn original_context(&self) -> Option<&crate::runtime::TaskContext> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = self.native { return Some(native.context.context()); }
        self.stock.task_context.as_ref().map(|(context, _)| context)
    }
    pub(super) fn context_pipeline(&self) -> &ContextPipeline {
        self.summary.as_ref().map_or(&self.stock.context, |(pipeline, _)| pipeline)
    }
    pub(super) fn compaction_policy(&self) -> &crate::context::CompactionPolicy {
        if self.summary.is_some() { &crate::context::CompactionPolicy::Disabled } else { &self.stock.compaction }
    }
    pub(super) fn output_tokens(&self) -> Option<u32> {
        self.summary.as_ref().map_or(self.stock.max_output_tokens, |(_, tokens)| *tokens)
    }
    pub(super) fn inherited_prefix(&self) -> Option<&(FileRef, Arc<dyn crate::conversation::ContentResidencyVerifier>)> {
        if self.summary.is_some() { None } else { self.stock.inherited_prefix.as_ref() }
    }
}

pub(crate) fn stock_boundary(io: Option<&PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(PreparationIo::original_native_work).is_some() { return crate::Error::NativeBoundary { code, reason }; }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(crate) fn stock_string_copy(value: &str, io: Option<&PreparationIo>) -> Result<String> {
    let mut copied = String::new();
    stock_string_push(&mut copied, value, io)?;
    Ok(copied)
}

pub(super) fn stock_selection_copy(value: &ModelContextSelection, io: Option<&PreparationIo>) -> Result<ModelContextSelection> {
    let mut ids = Vec::new();
    for id in &value.message_ids { stock_vec_push(&mut ids, *id, io)?; }
    let checkpoint = value.checkpoint.as_ref().map(|reference| crate::model::clone_file(reference, io)).transpose()?;
    Ok(ModelContextSelection { conversation_revision: value.conversation_revision, message_ids: ids, checkpoint })
}

pub(super) fn stock_compaction_copy(value: &crate::context::CompactionPolicy, io: Option<&PreparationIo>) -> Result<crate::context::CompactionPolicy> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        return Ok(match value {
            crate::context::CompactionPolicy::Disabled => crate::context::CompactionPolicy::Disabled,
            crate::context::CompactionPolicy::Threshold(policy) => {
                let (retention, custody) = crate::context::clone_retention_native(&policy.retention, work)?;
                io.absorb_native_custody(custody)?;
                crate::context::CompactionPolicy::Threshold(crate::context::ThresholdCompaction {
                    response_reserve_tokens: policy.response_reserve_tokens, recent_tokens: policy.recent_tokens, retention,
                })
            }
        });
    }
    Ok(value.clone())
}

pub(crate) fn stock_key(arguments: Arguments<'_>, io: Option<&PreparationIo>) -> Result<String> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        let (key, custody) = work.display(&arguments)?;
        io.absorb_native_custody(custody)?;
        return Ok(key);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        struct Size(usize);
        impl std::fmt::Write for Size {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut size = Size(0);
        size.write_fmt(arguments).map_err(|_| Error::Invalid("stock key size overflow".into()))?;
        io.retain_owned(size.0 as u64)?;
        io.allocation_attempt(size.0 as u64)?;
        let mut key = String::new();
        key.try_reserve_exact(size.0).map_err(Error::Allocation)?;
        io.retain_owned(key.capacity().saturating_sub(size.0) as u64)?;
        io.preflight(acyclic_fs::WorkCounters { bytes_copied: size.0 as u64, ..Default::default() })?;
        key.write_fmt(arguments).map_err(|_| Error::Invalid("stock key formatting failed".into()))?;
        io.spent(acyclic_fs::WorkCounters { bytes_copied: key.len() as u64, ..Default::default() })?;
        return Ok(key);
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    Ok(arguments.to_string())
}

pub(crate) fn stock_vec_push<T>(values: &mut Vec<T>, value: T, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(acyclic_fs::WorkCounters { items_returned: 1, ..Default::default() })?;
        if values.len() == values.capacity() && std::mem::size_of::<T>() != 0 {
            let before = values.capacity();
            let bytes = std::mem::size_of::<T>() as u64;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                let mut custody = work.claim(bytes)?;
                values.try_reserve_exact(1).map_err(Error::Allocation)?;
                let actual = (values.capacity() - before).checked_mul(std::mem::size_of::<T>())
                    .ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "stock vector capacity overflow" })?;
                let completed = custody.reconcile_capacity(actual as u64);
                io.absorb_native_custody(custody)?;
                completed?;
            }
            if values.len() == values.capacity() {
                io.retain_owned(bytes)?;
                io.allocation_attempt(bytes)?;
                values.try_reserve_exact(1).map_err(Error::Allocation)?;
                io.retain_owned(((values.capacity() - before - 1) * std::mem::size_of::<T>()) as u64)?;
            }
        }
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    values.push(value);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.spent(acyclic_fs::WorkCounters { items_returned: 1, ..Default::default() })?;
    }
    Ok(())
}

pub(crate) fn stock_string_push(text: &mut String, suffix: &str, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let target = text.len().checked_add(suffix.len()).ok_or_else(|| Error::Invalid("stock text size overflow".into()))?;
        let copied = acyclic_fs::WorkCounters { bytes_copied: suffix.len() as u64, ..Default::default() };
        io.preflight(copied)?;
        if target > text.capacity() {
            let before = text.capacity();
            let extra = target - before;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                let mut custody = work.claim(extra as u64)?;
                text.try_reserve_exact(target - text.len()).map_err(Error::Allocation)?;
                let completed = custody.reconcile_capacity((text.capacity() - before) as u64);
                io.absorb_native_custody(custody)?;
                completed?;
            }
            if target > text.capacity() {
                io.retain_owned(extra as u64)?;
                io.allocation_attempt(extra as u64)?;
                text.try_reserve_exact(target - text.len()).map_err(Error::Allocation)?;
                io.retain_owned((text.capacity() - target) as u64)?;
            }
        }
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    text.push_str(suffix);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.spent(acyclic_fs::WorkCounters { bytes_copied: suffix.len() as u64, ..Default::default() })?;
    }
    Ok(())
}

pub(super) async fn stock_append(journal: &dyn ExecutionJournal, operation: OperationId,
    key: &str, event: &ExecutionEvent, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        return journal.append_with_native_work(operation, key, event, work)?.await;
    }
    journal.append(operation, key.to_owned(), event.clone()).await
}

pub(super) async fn stock_append_if_tail(journal: &dyn ExecutionJournal, operation: OperationId,
    tail: u64, key: &str, event: &ExecutionEvent, io: Option<&PreparationIo>) -> Result<bool> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        return journal.append_if_tail_with_native_work(operation, tail, key, event, work)?.await;
    }
    journal.append_if_tail(operation, tail, key.to_owned(), event.clone()).await
}

pub(super) async fn stage_media_bytes(journal: &dyn ExecutionJournal, operation: OperationId,
    key: &str, bytes: &[u8], media: &'static str, io: Option<&PreparationIo>) -> Result<FileRef> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        let (reference, custody) = journal.stage_with_native_work(operation, key, bytes, media, work)?.await?;
        io.absorb_native_custody(custody)?;
        return Ok(reference);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.retain_owned(bytes.len() as u64)?;
        io.allocation_attempt(bytes.len() as u64)?;
        io.charge(acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() })?;
    }
    journal.stage(operation, key.to_owned(), bytes.to_vec(), media).await
}

pub(super) async fn stage_bytes(journal: &dyn ExecutionJournal, operation: OperationId,
    key: &str, bytes: &[u8], io: Option<&PreparationIo>) -> Result<FileRef> {
    let copied;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64 * 2, ..Default::default() })?;
    }
    use sha2::Digest as _;
    let digest: [u8; 32] = sha2::Sha256::digest(bytes).into();
    let identity = blake3::hash(bytes);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.spent(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64 * 2, ..Default::default() })?;
    }
    copied = stock_key(format_args!("{key}:{}", identity.to_hex()), io)?;
    let reference = stage_media_bytes(journal, operation, &copied, bytes, "application/json", io).await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() != bytes.len() as u64
        || reference.descriptor().sha256() != &digest {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "execution journal staged different private JSON bytes" });
        }
        return Err(Error::Storage("execution journal staged different private JSON bytes".into()));
    }
    Ok(reference)
}

pub(crate) async fn stage_json<T: Serialize>(journal: &dyn ExecutionJournal,
    operation: OperationId, key: &str, value: &T, io: Option<&PreparationIo>) -> Result<FileRef> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let hold;
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, true)?;
        hold = io.guard_bytes(&bytes)?;
        let result = stage_bytes(journal, operation, key, &bytes, Some(io)).await;
        drop(bytes);
        drop(hold);
        return result;
    }
    let bytes = crate::contract::canonical_json_bytes(value)?;
    stage_bytes(journal, operation, key, &bytes, None).await
}

pub(super) fn stock_tool_definitions(registry: &ToolRegistry, scope: &RuntimeScope,
    io: Option<&PreparationIo>) -> Result<Vec<crate::tool::ToolDefinition>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && io.original_native_work().is_some() {
        let definitions = registry.definitions(Some(io))?;
        let mut selected = Vec::new();
        for definition in definitions {
            let key = stock_key(format_args!("tool:call:{}", definition.name), Some(io))?;
            if scope.grants().contains(&key) { stock_vec_push(&mut selected, definition, Some(io))?; }
        }
        return Ok(selected);
    }
    Ok(registry.definitions(None)?.into_iter().filter(|definition| scope.grants()
        .contains(&capability::tool_call(&definition.name))).collect())
}

#[cfg(feature = "filesystem")]
pub(super) const fn stock_serde_diagnostic_label_bytes() -> u64 {
    crate::model::model_serde_diagnostic_label_bytes() + concat!(
        "ExecutionEvent", "kind", "started", "context_prepared", "context_compacted",
        "model_started", "model", "tool_started", "tool_completed", "tool_failed",
        "request_digest", "step", "projection", "accounting", "compaction",
        "purpose", "request", "event", "call_id", "invocation",
        "result", "reason", "ModelPurpose", "response", "summary",
        "ToolFailureKind", "executor_rejected", "invalid_output", "projection_rejected", "publication_rejected",
        "TurnInput", "operation_id", "input", "selected_context", "max_steps",
        "SelectedModelContext", "selection", "messages", "ModelContextSelection", "conversation_revision",
        "message_ids", "checkpoint", "LeaseFence", "reservation_id", "placement",
        "CompactionRetention", "roles", "native_media", "CompactionPolicy", "kind",
        "config", "disabled", "threshold", "ThresholdCompaction", "response_reserve_tokens",
        "recent_tokens", "retention", "ModelTokenCount", "request_digest", "fixed_tokens",
        "message_tokens", "ComponentIdentity", "name", "version", "digest",
        "ContextAccounting", "capacity", "count", "ModelContextCapacity", "context_tokens",
        "output_tokens",
    ).len() as u64 + 71 * 4
}

#[cfg(feature = "filesystem")]
macro_rules! stock_records {
    ($($record:ty),+ $(,)?) => {$(impl PreparationRecord for $record {
        fn diagnostic_label_bytes() -> u64 { stock_serde_diagnostic_label_bytes() }
        fn maximum_slot_bytes() -> u64 {
            (std::mem::size_of::<Self>() as u64).max(<crate::context::Context as PreparationRecord>::maximum_slot_bytes())
        }
    })+};
}
#[cfg(feature = "filesystem")]
stock_records!(String, ExecutionEvent, TurnInput, crate::scheduler::LeaseFence,
    crate::conversation::ModelContextSelection, crate::context::CompactionRetention,
    crate::context::CompactionPolicy, crate::context::ModelTokenCount,
    Vec<ModelEvent>, crate::registry::ComponentIdentity);

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn replay_execution_with_native_work<'a>(journal: &'a dyn ExecutionJournal,
    operation: OperationId, maximum: usize,
    select: impl Fn(&ExecutionEvent) -> bool + Send + 'a,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (u64, Vec<ExecutionRecord>, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native execution journal is unavailable" });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.retain_owned(std::mem::size_of::<Vec<ExecutionRecord>>() as u64)?)?;
        let (tail, records) = replay_execution(journal, operation, maximum, select, Some(&io)).await?;
        let mut retained = native_size_add(std::mem::size_of::<Vec<ExecutionRecord>>() as u64,
            (records.capacity() as u64).checked_mul(std::mem::size_of::<ExecutionRecord>() as u64)
                .ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "execution replay size overflow" })?)?;
        for record in &records {
            retained = native_size_add(retained, record.idempotency_key.capacity() as u64)?;
            let event_bytes = match &record.event {
                ExecutionEvent::Started { .. } => 0,
                ExecutionEvent::ContextPrepared { projection, accounting, .. } =>
                    native_size_add(projection.logical_owned_bytes()?, match accounting {
                        Some(reference) => reference.logical_owned_bytes()?, None => 0,
                    })?,
                ExecutionEvent::ContextCompacted { projection, compaction, accounting, .. } =>
                    native_size_add(native_size_add(projection.logical_owned_bytes()?,
                        compaction.logical_owned_bytes()?)?, accounting.logical_owned_bytes()?)?,
                ExecutionEvent::ModelStarted { request, .. } => request.logical_owned_bytes()?,
                ExecutionEvent::Model { event, .. } => event.logical_owned_bytes()?,
                ExecutionEvent::ToolStarted { call_id, invocation, .. } =>
                    native_size_add(call_id.capacity() as u64, invocation.logical_owned_bytes()?)?,
                ExecutionEvent::ToolCompleted { call_id, result, projection, .. } =>
                    native_size_add(call_id.capacity() as u64,
                        native_size_add(result.logical_owned_bytes()?, projection.logical_owned_bytes()?)?)?,
                ExecutionEvent::ToolFailed { call_id, .. } => call_id.capacity() as u64,
            };
            retained = native_size_add(retained, event_bytes)?;
        }
        work.remaining()?;
        let custody = io.into_native_custody(retained)?;
        Ok((tail, records, custody))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn stage_json_with_native_work<'a, T: Serialize + Sync>(journal: &'a dyn ExecutionJournal,
    operation: OperationId, key: &'a str, value: &'a T, work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native execution journal is unavailable" });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        let reference = stage_json(journal, operation, key, value, Some(&io)).await?;
        let retained = reference.logical_owned_bytes()?;
        work.remaining()?;
        let custody = io.into_native_custody(retained)?;
        Ok((reference, custody))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn load_json_with_native_work<'a, T: PreparationRecord + crate::filesystem::UploadRecord + Send>(journal: &'a dyn ExecutionJournal,
    reference: &'a FileRef, work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (T, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native execution journal is unavailable" });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        let value: T = retain_preparation(load_json::<T>(journal, reference, Some(&io)).await?);
        let retained = native_size_add(std::mem::size_of::<T>() as u64,
            crate::filesystem::UploadRecord::logical_owned_bytes(&value)?)?;
        work.remaining()?;
        let custody = io.into_native_custody(retained)?;
        Ok((value, custody))
    })
}

/// An admitted stock composition.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct NativeStockExecutor {
    pub(super) stock: StockExecutor,
    context: Option<(Arc<crate::runtime::NativeTaskContext>, OperationId)>,
    pub(super) interactions: Option<Arc<dyn crate::runtime::InteractionRouter>>,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl StockExecutor {
    /// Copies only the known stock configuration after original-work admission.
    pub fn new_with_native_work(
        model: &Model, provider: Arc<dyn ModelProvider>, pipeline: &ContextPipeline,
        tools: &ToolRegistry, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeStockExecutor> {
        if !provider.supports_native_model(work)? {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native model provider is unavailable" });
        }
        pipeline.validate(Some(&PreparationIo::from_native_work(work)?))?;
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.retain_owned(std::mem::size_of::<NativeStockExecutor>() as u64)?)?;
        let model = crate::model::clone_model(model, Some(&io))?;
        let pipeline = pipeline.copy(Some(&io))?;
        let (tools, tools_custody) = tools.clone_with_native_work(work)?;
        io.absorb_native_custody(tools_custody)?;
        let stock = Self::new(model, provider, pipeline, tools);
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(NativeStockExecutor { stock, context: None, interactions: None, custody })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeStockExecutor {
    fn require_original_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        if !std::ptr::eq(self.custody.work().runtime_meter(), work.runtime_meter()) {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock configuration differs from its original work" });
        }
        Ok(())
    }

    /// Audits the configured producer while preserving this capsule's work identity.
    pub(crate) fn supports_native_execution(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.require_original_work(work)?;
        self.stock.supports_native_execution(work)
    }

    /// Copies configuration through its existing admitted typed producer.
    pub(crate) fn clone_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        self.require_original_work(work)?;
        self.stock.clone_with_native_work(work)
    }

    /// Carries the original command's native journal route without altering its issued context.
    pub(crate) fn with_native_interactions(
        mut self, interactions: Arc<dyn crate::runtime::InteractionRouter>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        interactions.supports_native_interactions(work)?;
        self.interactions = Some(interactions);
        Ok(self)
    }

    /// Binds the same issued context, durable owner and fence without scope cloning.
    pub fn with_task_context_with_native_work(
        mut self, context: Arc<crate::runtime::NativeTaskContext>, operation: OperationId,
        host: Arc<dyn DurableTaskHost>, task: TaskId, fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        if !context.native_metadata_matches(work) || !context.context().original_task_binding_matches(task, fence) {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock owner differs from the issued native task context" });
        }
        if !host.supports_native_execution(work)? {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native stock task owner is unavailable" });
        }
        let io = PreparationIo::from_native_work(work)?;
        let (fence, fence_custody) = fence.clone_with_native_work(work)?;
        io.absorb_native_custody(fence_custody)?;
        let retained = io.owned_bytes()?;
        let mut incoming = io.into_native_custody(retained)?;
        self.custody.absorb(&mut incoming)?;
        self.stock.task = Some((host, task, fence));
        self.context = Some((context, operation));
        Ok(self)
    }

    /// Uses the issued task's policy and original native metadata admission.
    pub fn with_tool_authority_with_native_work(
        mut self, scope: &RuntimeScope, policy: Option<Arc<dyn ToolPolicy>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        self.require_original_work(work)?;
        let Some((context, _)) = self.context.as_ref() else {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock policy has no original context binding" });
        };
        context.context().validate_stock_scope_with_native_work(scope, self.stock.limits, work)?;
        if let Some(policy) = &policy {
            let Some((_, original)) = context.context().stock_tool_policy_binding() else {
                return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock policy is not issued by this context" });
            };
            if !Arc::ptr_eq(policy, original) {
                return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock policy is not issued by this context" });
            }
        }
        let identity = if let Some(policy) = &policy {
            policy.supports_native_policy(work)?;
            let (identity, mut custody) = policy.identity_with_native_work(work)?;
            self.custody.absorb(&mut custody)?;
            Some(identity)
        } else { None };
        let (scope, mut custody) = scope.clone_with_native_work(work)?;
        self.custody.absorb(&mut custody)?;
        self.stock.tool_scope = scope;
        self.stock.policy = policy;
        self.stock.policy_identity = identity;
        Ok(self)
    }

    /// Replaces the compaction value only after its actual copy is admitted.
    pub fn with_compaction_policy_with_native_work(
        mut self, policy: &crate::context::CompactionPolicy,
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
    pub fn with_limits(mut self, limits: Limits) -> Self { self.stock.limits = limits; self }

    /// Sets the same output token budget as the ordinary composition.
    pub fn with_max_output_tokens(mut self, maximum: u32) -> Result<Self> {
        if maximum == 0 {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "model output token budget is zero" });
        }
        self.stock.max_output_tokens = Some(maximum);
        Ok(self)
    }

    /// Prevents every fresh model, tool and interaction intent on this same engine.
    #[must_use]
    pub fn into_reconciliation(mut self) -> Self { self.stock.reconciliation_only = true; self }

    pub(super) fn admit_execution(
        &self, context: &Arc<crate::runtime::NativeTaskContext>, input: &TurnInput,
        journal: &dyn ExecutionJournal, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_work(work)?;
        if self.context.as_ref().is_none_or(|(original, operation)|
            *operation != input.operation_id || !Arc::ptr_eq(original, context))
            || !context.native_metadata_matches(work) {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "native stock execution differs from its original binding" });
        }
        native_execution_supported(&self.stock, context, journal, work)
    }

    /// Runs the complete ordinary/native stock loop using borrowed issued inputs.
    pub(crate) fn execute_progress_with_native_context<'a>(
        &'a self, context: &'a Arc<crate::runtime::NativeTaskContext>, input: &'a TurnInput,
        journal: &'a dyn ExecutionJournal, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (StockTurnProgress, acyclic_fs::NativeAllocation)>> {
        self.admit_execution(context, input, journal, work)?;
        native_progress(&self.stock, context, input, journal, work, self.stock.reconciliation_only, self.interactions.as_ref())
    }

    /// Reconciles the same journal without admitting a replacement effect.
    pub(crate) fn execute_reconcile_progress_with_native_context<'a>(
        &'a self, context: &'a Arc<crate::runtime::NativeTaskContext>, input: &'a TurnInput,
        journal: &'a dyn ExecutionJournal, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (StockTurnProgress, acyclic_fs::NativeAllocation)>> {
        self.admit_execution(context, input, journal, work)?;
        native_progress(&self.stock, context, input, journal, work, true, self.interactions.as_ref())
    }

}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_execution_supported(
    stock: &StockExecutor, context: &Arc<crate::runtime::NativeTaskContext>,
    journal: &dyn ExecutionJournal, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    work.remaining()?;
    if !context.native_metadata_matches(work) {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock context lacks its original native custody" });
    }
    if !journal.supports_native_execution(work)? || !stock.provider.supports_native_model(work)? {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native stock producer is unavailable" });
    }
    if context.context().durable_task_id() != stock.task.as_ref().map(|(_, task, _)| *task) {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock owner differs from the issued context" });
    }
    if let Some((host, task, fence)) = stock.task.as_ref() {
        if !context.context().original_task_binding_matches(*task, fence) {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock execution owner differs from the issued context" });
        }
        if !host.supports_native_execution(work)? {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native stock task owner is unavailable" });
        }
    }
    stock.context.validate(Some(&PreparationIo::from_native_work(work)?))?;
    for (_, policy) in context.context().stock_tool_policy_binding().into_iter()
        .chain(context.context().stock_tool_policy_overrides()) {
        policy.supports_native_policy(work)?;
    }
    if let Some(policy) = &stock.policy {
        let Some((_, original)) = context.context().stock_tool_policy_binding() else {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock policy is not the original context policy" });
        };
        if !Arc::ptr_eq(policy, original) {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock policy is not the original context policy" });
        }
    }
    context.context().validate_stock_scope_with_native_work(&stock.tool_scope, stock.limits, work)?;
    if context.context().scope().run_limits() != stock.tool_scope.run_limits()
        || context.context().scope().extensions() != stock.tool_scope.extensions() {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock task context differs from execution binding" });
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_session<'a>(
    stock: &'a StockExecutor, context: &'a Arc<crate::runtime::NativeTaskContext>,
    operation: OperationId, io: &'a PreparationIo, reconcile: bool,
    interactions: Option<&'a Arc<dyn crate::runtime::InteractionRouter>>,
) -> StockExecution<'a> {
    StockExecution { stock, native: Some(NativeStockExecution { context, io, operation, interactions }),
        summary: None,
        reconciliation_only: reconcile }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_progress<'a>(
    stock: &'a StockExecutor, context: &'a Arc<crate::runtime::NativeTaskContext>, input: &'a TurnInput,
    journal: &'a dyn ExecutionJournal, work: &'a acyclic_fs::NativeMaterializationWork, reconcile: bool,
    interactions: Option<&'a Arc<dyn crate::runtime::InteractionRouter>>,
) -> Result<crate::NativeFutureBundle<'a, (StockTurnProgress, acyclic_fs::NativeAllocation)>> {
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.retain_owned(std::mem::size_of::<StockTurnProgress>() as u64)?)?;
        let progress = {
            let session = native_session(stock, context, input.operation_id, &io, reconcile, interactions);
            session.execute_progress(input, journal).await?
        };
        let retained = native_progress_owned_bytes(&progress)?;
        work.remaining()?;
        Ok((progress, io.into_native_custody(retained)?))
    })
}

impl StockExecutor {
    /// Summarizes with the same stock admission, accounting and recovery engine.
    pub async fn summarize(&self, journal: &dyn ExecutionJournal, operation: OperationId,
        source: crate::context::Context, instruction: ModelContent) -> Result<crate::context::ContextSummary> {
        StockExecution::ordinary(self).summarize(journal, None, operation, 0, source, instruction).await
    }
    /// Summarizes inside the parent's existing execution journal.
    pub async fn summarize_in_turn(&self, journal: &dyn ExecutionJournal, input: &TurnInput, step: u32,
        source: crate::context::Context, instruction: ModelContent) -> Result<crate::context::ContextSummary> {
        StockExecution::ordinary(self).summarize(journal, Some(input), input.operation_id, step, source, instruction).await
    }
    /// Executes a retained/reconciled/admitted model step through the stock engine.
    pub async fn model_step(&self, journal: &dyn ExecutionJournal, input: &TurnInput,
        step: u32, prior: &[ModelMessage]) -> Result<Vec<ModelEvent>> {
        StockExecution::ordinary(self).model_step(journal, input, step, prior).await
    }
    /// Freezes a completed tool call prefix without dispatching effects.
    pub async fn completed_tool_prefix(&self, journal: &dyn ExecutionJournal, input: &TurnInput,
        step: u32, call: Option<&str>) -> Result<crate::model::PreparedModelRequest> {
        StockExecution::ordinary(self).completed_tool_prefix(journal, input, step, call).await
    }

}

pub(super) fn stock_is_native(io: Option<&PreparationIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { return io.and_then(PreparationIo::original_native_work).is_some(); }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = io; false }
}

pub(super) fn stock_native_failure(error: &Error) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { return error.is_native_failure(); }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = error; false }
}

pub(super) fn stock_model_call(parent: OperationId, step: u32, call: String, name: String,
    arguments: Value, io: Option<&PreparationIo>) -> Result<ToolInvocation> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        return ToolInvocation::for_model_call(parent, step, call, name, arguments, Some(work));
    }
    ToolInvocation::for_model_call(parent, step, call, name, arguments, None)
}

pub(super) fn stock_tool_lookup<'a>(registry: &'a ToolRegistry, name: &str,
    io: Option<&PreparationIo>) -> Result<&'a crate::tool::Tool> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return registry.get(name, io)?.ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::NotFound, reason: "tool is not bound to this stock composition" });
    }
    registry.get(name, io)?.ok_or_else(|| Error::NotFound(format!("tool {name}")))
}

pub(super) fn stock_tool_support(tool: &crate::tool::Tool, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        tool.executor.supports_native_execution(work)?;
        tool.projection.supports_native_projection(work)?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = (tool, io);
    Ok(())
}

pub(super) fn stock_authorize_tool(tool: &crate::tool::Tool, context: Option<&crate::runtime::ToolContext>,
    scope: &RuntimeScope, invocation: &ToolInvocation, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        let context = context.ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "native tool authorization has no issued context" })?;
        return tool.executor.authorize_with_native_context(context, invocation, work);
    }
    if let Some(context) = context {
        tool.executor.authorize_with_context(context, invocation)
    } else {
        tool.executor.authorize(Some(scope), invocation)
    }
}

pub(super) async fn stock_execute_tool(tool: &crate::tool::Tool,
    context: &mut Option<crate::runtime::ToolContext>, invocation: &ToolInvocation,
    io: Option<&PreparationIo>) -> Result<ToolResult> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        let context = context.as_ref().ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "native tool execution has no issued context" })?;
        let (result, custody) = tool.executor.execute_with_native_context(context, invocation, work)?.await?;
        io.absorb_native_custody(custody)?;
        return Ok(result);
    }
    if let Some(context) = context.take() {
        tool.executor.execute_with_context(context, invocation.clone()).await
    } else {
        tool.executor.execute(invocation.clone()).await
    }
}

pub(super) async fn stock_reconcile_tool(tool: &crate::tool::Tool,
    context: &mut Option<crate::runtime::ToolContext>, invocation: &ToolInvocation,
    io: Option<&PreparationIo>) -> Result<Option<ToolResult>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        let context = context.as_ref().ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "native tool reconciliation has no issued context" })?;
        let (result, custody) = tool.executor.reconcile_with_native_context(context, invocation, work)?.await?;
        io.absorb_native_custody(custody)?;
        return Ok(result);
    }
    if let Some(context) = context.take() {
        tool.executor.reconcile_with_context(context, invocation.clone()).await
    } else {
        tool.executor.reconcile(invocation.clone()).await
    }
}

pub(super) fn stock_project_tool(tool: &crate::tool::Tool, invocation: &ToolInvocation, result: &ToolResult,
    limits: Limits, io: Option<&PreparationIo>) -> Result<(Value, crate::model::ToolResultContent)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && let Some(work) = io.original_native_work() {
        let (value, custody) = tool.projection.project_with_native_work(invocation, result, work)?;
        io.absorb_native_custody(custody)?;
        let content = tool.definition.validate_projection(&value, limits, Some(io))?;
        io.check_json_byte_bound(&value, limits.render_bytes)?;
        return Ok((value, content));
    }
    let value = tool.projection.project(invocation, result)?;
    let content = tool.definition.validate_projection(&value, limits, io)?;
    if crate::contract::canonical_json_bytes(&value)?.len() as u64 > limits.render_bytes {
        return Err(Error::Invalid("tool projection exceeds render limit".into()));
    }
    Ok((value, content))
}

pub(super) fn stock_task_interaction_id(task: TaskId, operation: OperationId,
    io: Option<&PreparationIo>) -> Result<InteractionId> {
    #[cfg(feature = "filesystem")]
    let hashed = acyclic_fs::WorkCounters {
        bytes_hashed: (b"harness/v2/task-interaction\0".len() + 32) as u64, ..Default::default()
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.preflight(hashed)?; }
    let id = crate::durable_host::task_interaction_id(task, operation);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.spent(hashed)?; }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    Ok(id)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl StockExecution<'_> {
    pub(super) async fn settle_native_tool_policy(
        &self, journal: &dyn ExecutionJournal, operation: OperationId,
        invocation: &ToolInvocation, definition: &crate::tool::ToolDefinition, started: bool,
    ) -> Result<Option<ToolCallProgress>> {
        let Some(native) = self.native else { return Ok(None); };
        let context = native.context.context();
        let work = native.io.original_native_work().ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "stock policy has no original work" })?;
        for (_, policy) in context.stock_tool_policy_binding().into_iter().chain(context.stock_tool_policy_overrides()) {
            let Some(((approval_operation, request), custody)) = context.stock_policy_approval(
                policy.as_ref(), &self.stock.tool_scope, definition, invocation, work,
            ).await? else { continue; };
            native.io.absorb_native_custody(custody)?;
            if !self.stock.tool_scope.grants().contains(capability::INTERACTION_ROUTE) {
                return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "stock tool approval requires interaction:route" });
            }
            let task = self.stock.task.as_ref().map_or(TaskId::from_bytes(operation.into_bytes()), |(_, task, _)| *task);
            let approval = stock_task_interaction_id(task, approval_operation, Some(native.io))?;
            journal.open_interaction_with_native_work(approval, &request, work)?.await?;
            let (outcome, custody) = journal.interaction_outcome_with_native_work(approval, work)?.await?;
            native.io.absorb_native_custody(custody)?;
            let Some(outcome) = outcome else {
                return if started { Err(Error::Indeterminate(invocation.operation_id)) }
                    else { Ok(Some(ToolCallProgress::Pending(invocation.operation_id))) };
            };
            if matches!(outcome, InteractionOutcome::Answered { .. }) {
                return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason: "approval resolved with a question answer" });
            }
            match check_tool_approval(outcome) {
                Ok(()) => {}
                Err(Error::InteractionRejected(reason)) if !started => return Ok(Some(ToolCallProgress::Rejected(reason))),
                Err(Error::InteractionRejected(_)) => return Err(Error::Indeterminate(invocation.operation_id)),
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }
}

pub(super) fn stock_validate_invocation(invocation: &ToolInvocation, io: Option<&PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work|
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work))?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && io.original_native_work().is_some() {
        crate::contract::json_owned_value_shape_bounded(
            &invocation.arguments, crate::contract::JsonBounds {
                maximum_bytes: u64::MAX, maximum_items: io.remaining()?.items_examined, maximum_depth: 256,
            }, crate::contract::JsonValueSchema::None, &mut |next| io.codec_charge(next, false),
        )?;
        return Ok(());
    }
    Ok(())
}

pub(super) async fn stock_tool_rejection(
    session: &StockExecution<'_>, journal: &dyn ExecutionJournal, operation: OperationId,
    step: u32, call: &str, kind: ToolFailureKind, error: Error,
) -> Result<ToolCallProgress> {
    if stock_native_failure(&error) { return Err(error); }
    session.record_tool_failure(journal, operation, step, call, kind).await?;
    Err(stock_boundary(session.io(), crate::wire::ErrorCode::Invalid, kind.message()))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_size_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native stock logical payload size overflow" })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_events_owned_bytes(events: &Vec<ModelEvent>) -> Result<u64> {
    let slots = (events.capacity() as u64).checked_mul(std::mem::size_of::<ModelEvent>() as u64)
        .ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native event slots overflow" })?;
    events.iter().try_fold(native_size_add(std::mem::size_of::<Vec<ModelEvent>>() as u64, slots)?,
        |bytes, event| native_size_add(bytes, event.logical_owned_bytes()?.checked_sub(
            std::mem::size_of::<ModelEvent>() as u64,
        ).ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native event metadata extent differs" })?))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_output_payload_bytes(output: &TurnOutput) -> Result<u64> {
    let slots = (output.attachments.capacity() as u64).checked_mul(std::mem::size_of::<Attachment>() as u64)
        .ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native attachment slots overflow" })?;
    let mut bytes = native_size_add(output.text.capacity() as u64, slots)?;
    bytes = native_size_add(bytes, crate::contract::json_value_logical_owned_bytes(&output.metadata)?)?;
    for attachment in &output.attachments {
        bytes = native_size_add(bytes, attachment.file.logical_owned_bytes()?)?;
        if let Some(label) = &attachment.label { bytes = native_size_add(bytes, label.capacity() as u64)?; }
    }
    Ok(bytes)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_progress_owned_bytes(progress: &StockTurnProgress) -> Result<u64> {
    let root = std::mem::size_of::<StockTurnProgress>() as u64;
    match progress {
        StockTurnProgress::Ready(output) => native_size_add(root, native_output_payload_bytes(output)?),
        StockTurnProgress::Pending(_) | StockTurnProgress::Rejected(_) => Ok(root),
    }
}

impl StockExecutor {
    pub(crate) async fn execute_progress(&self, input: TurnInput, journal: &dyn ExecutionJournal) -> Result<StockTurnProgress> {
        let span = obs_span!("acyclic.harness.executor.execute", operation_id = format_args!("{}", input.operation_id));
        traced(span, StockExecution::ordinary(self).execute_progress(&input, journal)).await
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl StockExecutor {
    /// Audits the native callback boundaries without cloning configuration or boxing work.
    pub(crate) fn supports_native_execution(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        if !self.provider.supports_native_model(work)? { return Ok(false); }
        self.context.validate(Some(&PreparationIo::from_native_work(work)?))?;
        if let Some((host, _, _)) = &self.task
            && !host.supports_native_execution(work)? { return Ok(false); }
        if let Some(policy) = &self.policy { policy.supports_native_policy(work)?; }
        Ok(true)
    }

    /// Copies the actual configured stock composition, not its issued task context.
    pub(crate) fn clone_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<NativeStockExecutor> {
        if !self.supports_native_execution(work)? {
            return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native stock composition is unavailable" });
        }
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.retain_owned(std::mem::size_of::<NativeStockExecutor>() as u64)?)?;
        let model = crate::model::clone_model(&self.model, Some(&io))?;
        let context = self.context.copy(Some(&io))?;
        let (tools, custody) = self.tools.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (tool_scope, custody) = self.tool_scope.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let policy_identity = self.policy_identity.as_ref().map(|identity| -> Result<ComponentIdentity> {
            Ok(ComponentIdentity {
                name: stock_string_copy(&identity.name, Some(&io))?,
                version: stock_string_copy(&identity.version, Some(&io))?, digest: identity.digest,
            })
        }).transpose()?;
        let task = self.task.as_ref().map(|(host, task, fence)| -> Result<_> {
            let (fence, custody) = fence.clone_with_native_work(work)?;
            io.absorb_native_custody(custody)?;
            Ok((Arc::clone(host), *task, fence))
        }).transpose()?;
        let inherited_prefix = self.inherited_prefix.as_ref().map(|(reference, verifier)| -> Result<_> {
            Ok((crate::model::clone_file(reference, Some(&io))?, Arc::clone(verifier)))
        }).transpose()?;
        let stock = StockExecutor {
            model, max_output_tokens: self.max_output_tokens, provider: Arc::clone(&self.provider),
            context, tools, limits: self.limits, tool_scope, policy: self.policy.as_ref().map(Arc::clone),
            policy_identity, task, inherited_prefix, task_context: None,
            compaction: stock_compaction_copy(&self.compaction, Some(&io))?,
            reconciliation_only: self.reconciliation_only,
        };
        let retained = io.owned_bytes()?;
        work.remaining()?;
        Ok(NativeStockExecutor {
            stock, context: None, interactions: None, custody: io.into_native_custody(retained)?,
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn retained_model_step_with_native_work<'a>(
    journal: &'a dyn ExecutionJournal, records: &'a [ExecutionRecord], step: u32,
    purpose: ModelPurpose, limits: Limits, work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (RetainedModelStep, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native execution journal is unavailable" });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.retain_owned(std::mem::size_of::<RetainedModelStep>() as u64)?)?;
        let retained = retained_model_step(journal, records, step, purpose, limits, Some(&io)).await?;
        let mut bytes = std::mem::size_of::<RetainedModelStep>() as u64;
        if let Some(request) = &retained.request {
            bytes = native_size_add(bytes, request.logical_owned_bytes()? - std::mem::size_of::<crate::model::PreparedModelRequest>() as u64)?;
        }
        bytes = native_size_add(bytes, native_events_owned_bytes(&retained.events)? - std::mem::size_of::<Vec<ModelEvent>>() as u64)?;
        bytes = native_size_add(bytes, (retained.admission.calls.capacity() as u64)
            .checked_mul(std::mem::size_of::<String>() as u64)
            .ok_or_else(|| crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native admission slots overflow" })?)?;
        for call in &retained.admission.calls { bytes = native_size_add(bytes, call.capacity() as u64)?; }
        work.remaining()?;
        Ok((retained, io.into_native_custody(bytes)?))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn completed_tool_projection_with_native_work<'a>(
    journal: &'a dyn ExecutionJournal, records: &'a [ExecutionRecord], step: u32,
    expected: &'a ToolInvocation, tools: &'a [crate::tool::ToolDefinition], limits: Limits,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (crate::model::ToolResultContent, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "native execution journal is unavailable" });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        let content = completed_tool_projection(journal, records, step, expected, tools, limits, Some(&io)).await?;
        let retained = content.logical_owned_bytes()?;
        work.remaining()?;
        Ok((content, io.into_native_custody(retained)?))
    })
}
