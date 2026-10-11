//! Primitive threshold compaction using provider-owned token accounting.

use super::{CompactionRetention, Context, ModelContextCapacity, ModelTokenCount};
use crate::{Error, Result, model::ModelContentPart};
use serde::{Deserialize, Serialize};

/// Ordinary configurable threshold compaction; no model-family heuristic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ThresholdCompaction {
    /// Input headroom retained for each response.
    pub response_reserve_tokens: u32,
    /// Minimum recent suffix token budget retained alongside mandatory content.
    pub recent_tokens: u32,
    /// Consumer-selected roles and media that remain verbatim.
    pub retention: CompactionRetention,
}

impl Default for ThresholdCompaction {
    fn default() -> Self {
        Self {
            response_reserve_tokens: 16_384,
            recent_tokens: 20_000,
            retention: CompactionRetention::default(),
        }
    }
}

impl ThresholdCompaction {
    /// Validates the defaults or replacement against the actual selected capacity.
    /// Returns a finite effective output ceiling when the caller did not supply one.
    pub fn validate(
        &self,
        capacity: ModelContextCapacity,
        output_tokens: Option<u32>,
    ) -> Result<u32> {
        capacity.validate()?;
        self.retention.validate()?;
        let required = u64::from(self.response_reserve_tokens) + u64::from(self.recent_tokens);
        let output =
            output_tokens.unwrap_or(capacity.output_tokens.min(self.response_reserve_tokens));
        if self.response_reserve_tokens == 0
            || self.recent_tokens == 0
            || required >= u64::from(capacity.context_tokens)
            || output == 0
            || output > capacity.output_tokens
            || output > self.response_reserve_tokens
        {
            return Err(Error::Invalid(
                "compaction budgets exceed selected model capacity".into(),
            ));
        }
        Ok(output)
    }

    /// Tests input pressure against the validated response reserve.
    pub fn needs_compaction(
        &self,
        capacity: ModelContextCapacity,
        input_tokens: u64,
    ) -> Result<bool> {
        self.validate(capacity, None)?;
        Ok(input_tokens > u64::from(capacity.context_tokens - self.response_reserve_tokens))
    }

    /// Selects a complete-message recent suffix. An oversized message is retained
    /// whole; later mandatory/request validation may explicitly reject the budget.
    pub fn recent_start(&self, count: &ModelTokenCount) -> Result<usize> {
        if self.recent_tokens == 0 {
            return Err(Error::Invalid(
                "recent token budget must be positive".into(),
            ));
        }
        let mut recent = 0_u64;
        let mut start = count.message_tokens.len();
        while start > 0 && recent < u64::from(self.recent_tokens) {
            start -= 1;
            let tokens = count
                .message_tokens
                .get(start)
                .ok_or_else(|| Error::Invalid("recent token position is missing".into()))?;
            recent = recent
                .checked_add(u64::from(*tokens))
                .ok_or_else(|| Error::Invalid("recent token count overflows".into()))?;
        }
        Ok(start)
    }

    /// Plans against the shared mandatory-message selection used by replay.
    pub fn projection_budget(
        &self,
        context: &Context,
        count: &ModelTokenCount,
    ) -> Result<(usize, usize)> {
        self.projection_budget_with_message_limit(context, count, usize::MAX)
    }

    /// Applies a hard retained-message allowance alongside the recent token budget.
    /// The summary occupies one position. Mandatory messages and complete tool
    /// exchanges remain verbatim; an impossible allowance fails explicitly.
    pub fn projection_budget_with_message_limit(
        &self,
        context: &Context,
        count: &ModelTokenCount,
        maximum_messages: usize,
    ) -> Result<(usize, usize)> {
        let recent_messages = maximum_messages.checked_sub(1).ok_or_else(|| {
            Error::Invalid("compaction requires a summary message allowance".into())
        })?;
        let mut through = self
            .recent_start(count)?
            .max(context.messages.len().saturating_sub(recent_messages));
        if count.message_tokens.len() != context.messages.len() || through == 0 {
            return Err(Error::Invalid(
                "no older source fits the compaction policy".into(),
            ));
        }
        let mut pending = std::collections::BTreeMap::new();
        for (position, message) in context.messages.iter().take(through).enumerate() {
            for part in message.content.parts() {
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        pending.insert(call_id, position);
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        pending.remove(call_id);
                    }
                    _ => {}
                }
            }
        }
        if let Some(first) = pending.values().min() {
            through = *first;
        }
        if through == 0 && maximum_messages == usize::MAX {
            return Err(Error::Invalid(
                "tool exchange leaves no complete older compaction source".into(),
            ));
        }
        if through == 0
            || super::mandatory_positions(context, through, &self.retention).len()
                >= maximum_messages
        {
            through = bounded_source_cut(context, through, &self.retention, maximum_messages)?;
        }
        let mandatory = super::mandatory_positions(context, through, &self.retention);
        let maximum = mandatory
            .len()
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("compaction projection count overflows".into()))?;
        if maximum > context.messages.len() || maximum > maximum_messages {
            return Err(Error::Invalid(
                "mandatory content leaves no compaction source".into(),
            ));
        }
        Ok((through, maximum))
    }
}

// Mandatory closure shrinks monotonically as the covered prefix advances.
// Enumerate complete exchange boundaries once, then search them without a
// quadratic scan over all possible message cuts. The current final message
// remains outside the summary source.
fn bounded_source_cut(
    context: &Context,
    minimum: usize,
    retention: &CompactionRetention,
    maximum_messages: usize,
) -> Result<usize> {
    let mut pending = std::collections::BTreeSet::new();
    let mut cuts = Vec::new();
    for (position, message) in context
        .messages
        .iter()
        .take(context.messages.len().saturating_sub(1))
        .enumerate()
    {
        for part in message.content.parts() {
            match part {
                ModelContentPart::ToolCall { call_id, .. } => {
                    pending.insert(call_id);
                }
                ModelContentPart::ToolResult { call_id, .. } => {
                    pending.remove(call_id);
                }
                _ => {}
            }
        }
        if pending.is_empty() && position + 1 >= minimum {
            cuts.push(position + 1);
        }
    }
    let position = cuts.partition_point(|cut| {
        super::mandatory_positions(context, *cut, retention).len() >= maximum_messages
    });
    cuts.get(position)
        .copied()
        .ok_or_else(|| Error::Invalid("mandatory content leaves no compaction source".into()))
}

/// Replaceable primitive policy. Custom context transformations can disable
/// the stock threshold and own their explicit admission/projection decisions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "config",
    rename_all = "snake_case"
)]
pub enum CompactionPolicy {
    /// Preserve the installed context pipeline without automatic summarization.
    Disabled,
    /// Perform admitted threshold compaction under the selected model's capacities.
    Threshold(ThresholdCompaction),
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::Threshold(ThresholdCompaction::default())
    }
}

pub(crate) fn clone_context_with_work(
    context: &Context, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    let (mut copy, claim) = clone_context_prefix_with_work(context, context.messages.len(), work)?;
    copy.current_input_index = context.current_input_index;
    Ok((copy, claim))
}

pub(crate) fn clone_context_prefix_with_work(
    context: &Context, through: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    let source = context.messages.get(..through)
        .ok_or_else(|| Error::Invalid("summary extent exceeds source projection".into()))?;
    if work.is_none() {
        return Ok((Context { messages: source.to_vec(), metadata: context.metadata.clone(),
            current_input_index: context.current_input_index.filter(|index| (*index as usize) < through) }, 0));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Context::default();
    let copied = (|| {
        let (messages, claim) = clone_messages_with_work(source, quota.work())?;
        result.messages = messages;
        quota.adopt(claim)?;
        let (metadata, claim) = clone_metadata_with_work(&context.metadata, quota.work())?;
        result.metadata = metadata;
        quota.adopt(claim)?;
        result.current_input_index = context.current_input_index.filter(|index| (*index as usize) < through);
        Ok(())
    })();
    if let Err(error) = copied { drop(result); return Err(error); }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn clone_metadata_with_work(
    metadata: &std::collections::BTreeMap<String, crate::conversation::FileRef>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(std::collections::BTreeMap<String, crate::conversation::FileRef>, u64)> {
    if work.is_none() { return Ok((metadata.clone(), 0)); }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = std::collections::BTreeMap::new();
    let copied = (|| {
        for (key, file) in metadata {
            let key = quota.work().as_deref_mut().expect("observed metadata").clone_text(key)?;
            quota.adopt(key.capacity() as u64)?;
            let file = file.clone_in_quota(&mut quota)?;
            let bytes = std::mem::size_of::<(String, crate::conversation::FileRef)>() as u64;
            let io = quota.work().as_deref_mut().expect("observed metadata");
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            result.insert(key, file);
        }
        Ok(())
    })();
    if let Err(error) = copied { drop(result); return Err(error); }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn clone_messages_with_work(
    messages: &[crate::model::ModelMessage],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Vec<crate::model::ModelMessage>, u64)> {
    clone_base_messages_with_work(messages, None, &[], work)
}

pub(crate) fn clone_base_messages_with_work(
    first: &[crate::model::ModelMessage], user: Option<&crate::model::ModelContent>,
    second: &[crate::model::ModelMessage],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Vec<crate::model::ModelMessage>, u64)> {
    use crate::model::{ModelMessage, ModelRole};
    if work.is_none() {
        let mut result = first.to_vec();
        if let Some(content) = user { result.push(ModelMessage { role: ModelRole::User, content: content.clone() }); }
        result.extend_from_slice(second);
        return Ok((result, 0));
    }
    let count = first.len().checked_add(second.len()).and_then(|n| n.checked_add(usize::from(user.is_some())))
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))?;
    let bytes = count.checked_mul(std::mem::size_of::<ModelMessage>())
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))? as u64;
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Vec::new();
    let copied = (|| {
        let io = quota.work().as_deref_mut().expect("observed context");
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
        result.try_reserve_exact(count).map_err(|cause|
            quota.work().as_deref().expect("observed context").allocation_failure(cause))?;
        let actual = (result.capacity() * std::mem::size_of::<ModelMessage>()) as u64;
        if actual > bytes {
            let io = quota.work().as_deref_mut().expect("observed context");
            io.admit_owned(actual - bytes)?;
            io.retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
        for (role, content) in first.iter().map(|m| (m.role, &m.content))
            .chain(user.into_iter().map(|content| (ModelRole::User, content)))
            .chain(second.iter().map(|m| (m.role, &m.content))) {
            let slot = std::mem::size_of::<ModelMessage>() as u64;
            quota.work().as_deref().expect("observed context").admit_copy(slot)?;
            let (content, claim) = crate::model::clone_content_with_work(content, quota.work())?;
            quota.adopt(claim)?;
            quota.work().as_deref_mut().expect("observed context").copy_bytes(slot)?;
            result.push(ModelMessage { role, content });
        }
        Ok(())
    })();
    if let Err(error) = copied { drop(result); return Err(error); }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn reserve_vector_with_work<T>(
    values: &mut Vec<T>, capacity: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if capacity <= values.capacity() { return Ok(()); }
    let Some(io) = work.as_deref_mut() else {
        values.reserve(capacity.saturating_sub(values.len()));
        return Ok(());
    };
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))? as u64;
    let old = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let copied = (values.len() * std::mem::size_of::<T>()) as u64;
    io.admit_copy(copied)?;
    io.admit_allocation(bytes)?;
    let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
    quota.work().as_deref_mut().expect("observed vector").copy_bytes(copied)?;
    if let Err(cause) = values.try_reserve_exact(capacity - values.len()) {
        return Err(quota.work().as_deref().expect("observed vector").allocation_failure(cause));
    }
    let actual = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let reconciled = (|| {
        if actual > bytes {
            let io = quota.work().as_deref_mut().expect("observed vector");
            io.admit_owned(actual - bytes)?;
            io.retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
        quota.work().as_deref_mut().expect("observed vector").release_owned(old)?;
        Ok(())
    })();
    if let Err(error) = reconciled {
        drop(std::mem::take(values));
        return Err(error);
    }
    quota.transfer();
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use crate::conversation::FileRef;
    use crate::model::{ModelContent, ModelMessage};
    use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
    use std::{collections::BTreeMap, mem::size_of};


    fn overflow() -> Error {
        Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "context clone storage overflows" }
    }

    /// A recipe describes the actual typed clone, not its serialized representation.
    #[derive(Default)]
    struct CloneRecipe { owned: u64, copied: u64, allocations: u64 }

    impl CloneRecipe {
        fn add(&mut self, bytes: usize, copied: bool, allocated: bool) -> Result<()> {
            self.owned = self.owned.checked_add(bytes as u64).ok_or_else(overflow)?;
            if copied { self.copied = self.copied.checked_add(bytes as u64).ok_or_else(overflow)?; }
            if allocated { self.allocations = self.allocations.checked_add(1).ok_or_else(overflow)?; }
            Ok(())
        }
        fn text(&mut self, text: &str) -> Result<()> { self.add(text.len(), true, !text.is_empty()) }
        fn vector<T>(&mut self, count: usize) -> Result<()> {
            self.add(count.checked_mul(size_of::<T>()).ok_or_else(overflow)?, false, count != 0)
        }
        fn fund(&self, work: &NativeMaterializationWork) -> Result<NativeAllocation> {
            work.admit(WorkCounters { bytes_copied: self.copied, allocation_operations: self.allocations,
                ..WorkCounters::default() }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            work.retain_owned(self.owned).map_err(|cause| Error::NativeWork { cause, custody: None })
        }
        fn begin(&self, work: &NativeMaterializationWork) -> Result<()> {
            work.charge(WorkCounters { bytes_copied: self.copied, allocation_operations: self.allocations,
                ..WorkCounters::default() }).map_err(|cause| Error::NativeWork { cause, custody: None })
        }
        fn admit(self, work: &NativeMaterializationWork) -> Result<NativeAllocation> {
            let guard = self.fund(work)?;
            self.begin(work)?;
            Ok(guard)
        }
    }

    /// Actual tree storage emitted by the closed SDK metadata producer. UUID
    /// strings here are real to_value output, never a typed source-clone proxy.
    pub(crate) struct NativeContextValueRecipe<'a> {
        inner: CloneRecipe,
        work: &'a NativeMaterializationWork,
        field_label_bytes: u64,
    }

    impl<'a> NativeContextValueRecipe<'a> {
        pub(crate) fn new(work: &'a NativeMaterializationWork) -> Self {
            Self { inner: CloneRecipe::default(), work, field_label_bytes: 0 }
        }
        pub(crate) fn field_keys(&mut self, keys: &[&str]) -> Result<()> {
            self.inner.add(keys.len().checked_mul(size_of::<(String, serde_json::Value)>()).ok_or_else(overflow)?, false, false)?;
            for key in keys {
                self.field_label_bytes = self.field_label_bytes.checked_add(key.len() as u64).ok_or_else(overflow)?;
                self.text(key)?;
            }
            Ok(())
        }
        pub(crate) fn text(&mut self, text: &str) -> Result<()> { self.inner.text(text) }
        pub(crate) fn array(&mut self, count: usize) -> Result<()> { self.inner.vector::<serde_json::Value>(count) }
        pub(crate) fn uuid(&mut self) -> Result<()> { self.inner.add(36, true, true) }
        pub(crate) fn limits(&mut self) -> Result<()> {
            self.field_keys(&["file_bytes", "path_bytes", "attachments", "render_bytes",
                "model_steps", "model_events_per_step", "tool_calls_per_step", "context_messages"])
        }
        pub(crate) fn file(&mut self, file: &FileRef) -> Result<()> {
            use crate::conversation::{VolumeClass, VolumeOwner};
            self.field_keys(&["volume", "path", "version", "descriptor", "display_name"])?;
            self.text(file.path())?; self.text(file.version())?; self.text(file.display_name())?;
            self.field_keys(&["provider", "id", "class", "owner"])?;
            let volume = file.volume();
            self.text(volume.id())?;
            self.text(match volume.class() {
                VolumeClass::Project => "project",
                VolumeClass::AgentPrivate => "agent_private",
                VolumeClass::SessionShared => "session_shared",
            })?;
            self.field_keys(&["namespace", "family", "version"])?;
            self.text(volume.provider().namespace())?;
            self.text(volume.provider().family())?;
            self.text(volume.provider().version())?;
            self.field_keys(&["kind", "id"])?;
            match volume.owner() {
                VolumeOwner::Project(id) => { self.text("project")?; self.text(id)?; }
                VolumeOwner::Agent(_) => { self.text("agent")?; self.uuid()?; }
                VolumeOwner::Session(id) => { self.text("session")?; self.text(id)?; }
            }
            self.field_keys(&["sha256", "byte_length", "media_type"])?;
            self.array(file.descriptor().sha256().len())?;
            self.text(file.descriptor().media_type())
        }
        pub(crate) fn value(&mut self, value: &serde_json::Value) -> Result<()> {
            fn examine_value(value: &serde_json::Value, work: &NativeMaterializationWork) -> Result<()> {
                examine(work)?;
                match value {
                    serde_json::Value::Array(values) => for value in values { examine_value(value, work)?; },
                    serde_json::Value::Object(values) => for value in values.values() { examine_value(value, work)?; },
                    _ => {}
                }
                Ok(())
            }
            examine_value(value, self.work)?;
            let (allocations, copied) = crate::core::value_clone_work(value)?;
            let owned = crate::core::value_clone_owned_bytes(value)?;
            self.inner.allocations = self.inner.allocations.checked_add(allocations).ok_or_else(overflow)?;
            self.inner.copied = self.inner.copied.checked_add(copied).ok_or_else(overflow)?;
            self.inner.owned = self.inner.owned.checked_add(owned).ok_or_else(overflow)?;
            Ok(())
        }
        pub(crate) fn produce<T: Serialize>(
            self, value: &T, work: &NativeMaterializationWork,
        ) -> Result<(serde_json::Value, NativeAllocation)> {
            if !work.shares_ledger(self.work) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "context metadata recipe belongs to another original work" });
            }
            let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(0,
                self.field_label_bytes.checked_add(std::any::type_name::<T>().len() as u64).ok_or_else(overflow)?)?;
            let diagnostic = work.retain_owned(diagnostic_bytes)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let guard = self.inner.admit(work)?;
            // Only the audited SDK field serializers reach this original
            // producer; no encoded buffer, parser or ordinary trait callback.
            match serde_json::to_value(value) {
                Ok(value) => Ok((value, guard)),
                Err(source) => Err(Error::NativeEncode { code: crate::wire::ErrorCode::Invalid,
                    source, custody: Some(diagnostic) }),
            }
        }
    }

    pub(crate) fn native_vector<T>(capacity: usize, work: &NativeMaterializationWork) -> Result<(Vec<T>, NativeAllocation)> {
        let bytes = capacity.checked_mul(size_of::<T>()).ok_or_else(overflow)? as u64;
        let mut guard = work.retain_owned(bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if bytes != 0 {
            work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|cause| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
        })?;
        guard.resize_retained(values.capacity().checked_mul(size_of::<T>()).ok_or_else(overflow)? as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok((values, guard))
    }

    pub(crate) fn clone_text_native(text: &str, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation)> {
        work.admit(WorkCounters { bytes_copied: text.len() as u64,
            allocation_operations: u64::from(!text.is_empty()), ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut guard = work.retain_owned(text.len() as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        work.charge(WorkCounters { allocation_operations: u64::from(!text.is_empty()),
            ..WorkCounters::default() }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut copy = String::new();
        copy.try_reserve_exact(text.len()).map_err(|cause| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
        })?;
        guard.resize_retained(copy.capacity() as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        work.charge(WorkCounters { bytes_copied: text.len() as u64, ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        copy.push_str(text);
        Ok((copy, guard))
    }

    fn examine(work: &NativeMaterializationWork) -> Result<()> {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })
    }

    pub(crate) fn mandatory_positions_native(
        context: &Context, covered: usize, retention: &CompactionRetention, work: &NativeMaterializationWork,
    ) -> Result<(Vec<usize>, NativeAllocation)> {
        let parts = context.messages.iter().try_fold(0_usize, |count, message|
            count.checked_add(message.content.parts().len()).ok_or_else(overflow))?;
        // Declare local guards first: scratch buffers must drop first on errors.
        let (pending_guard, mut pending) = native_vector::<(&str, usize)>(parts, work).map(|(data, guard)| (guard, data))?;
        let (edges_guard, mut edges) = native_vector::<(usize, usize)>(parts, work).map(|(data, guard)| (guard, data))?;
        let (selected_guard, mut selected) = native_vector::<bool>(context.messages.len(), work).map(|(data, guard)| (guard, data))?;
        for (position, message) in context.messages.iter().enumerate() {
            examine(work)?;
            selected.push(context.current_input_index.map(|index| index as usize) == Some(position)
                || position + 1 == context.messages.len() || position >= covered
                || retention.roles.contains(&message.role)
                || (retention.native_media && message.content.contains_native_media()));
            for part in message.content.parts() {
                examine(work)?;
                match part {
                    ModelContentPart::ToolCall { call_id, .. } => {
                        if let Some(prior) = pending.iter_mut().find(|(id, _)| *id == call_id) {
                            prior.1 = position;
                        } else { pending.push((call_id, position)); }
                    }
                    ModelContentPart::ToolResult { call_id, .. } => {
                        if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) {
                            let (_, call) = pending.swap_remove(prior);
                            edges.push((call, position));
                        }
                    }
                    _ => {}
                }
            }
        }
        loop {
            let mut changed = false;
            for &(call, result) in &edges {
                examine(work)?;
                if selected[call] || selected[result] {
                    changed |= !selected[call] || !selected[result];
                    selected[call] = true; selected[result] = true;
                }
            }
            if !changed { break; }
        }
        let count = selected.iter().filter(|value| **value).count();
        let (guard, mut positions) = native_vector::<usize>(count, work).map(|(data, guard)| (guard, data))?;
        for (position, retained) in selected.iter().enumerate() {
            if *retained { positions.push(position); }
        }
        drop(selected); drop(selected_guard); drop(edges); drop(edges_guard); drop(pending); drop(pending_guard);
        Ok((positions, guard))
    }

    impl ThresholdCompaction {
        /// Uses the same complete-exchange cuts and mandatory closure on the original work.
        pub fn projection_budget_with_message_limit_native(
            &self, context: &Context, count: &ModelTokenCount, maximum_messages: usize,
            work: &NativeMaterializationWork,
        ) -> Result<((usize, usize), NativeAllocation)> {
            let invalid = |reason| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
            let recent_messages = maximum_messages.checked_sub(1)
                .ok_or_else(|| invalid("compaction requires a summary message allowance"))?;
            if self.recent_tokens == 0 { return Err(invalid("recent token budget must be positive")); }
            let mut recent = 0_u64;
            let mut start = count.message_tokens.len();
            while start > 0 && recent < u64::from(self.recent_tokens) {
                examine(work)?;
                start -= 1;
                recent = recent.checked_add(u64::from(count.message_tokens[start])).ok_or_else(overflow)?;
            }
            let mut through = start.max(context.messages.len().saturating_sub(recent_messages));
            if count.message_tokens.len() != context.messages.len() || through == 0 {
                return Err(invalid("no older source fits the compaction policy"));
            }
            let parts = context.messages.iter().try_fold(0_usize, |count, message|
                count.checked_add(message.content.parts().len()).ok_or_else(overflow))?;
            let (pending_guard, mut pending) = native_vector::<(&str, usize)>(parts, work).map(|(data, guard)| (guard, data))?;
            for (position, message) in context.messages.iter().take(through).enumerate() {
                examine(work)?;
                for part in message.content.parts() {
                    examine(work)?;
                    match part {
                        ModelContentPart::ToolCall { call_id, .. } => {
                            if let Some(prior) = pending.iter_mut().find(|(id, _)| *id == call_id) { prior.1 = position; }
                            else { pending.push((call_id, position)); }
                        }
                        ModelContentPart::ToolResult { call_id, .. } => {
                            if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) { pending.swap_remove(prior); }
                        }
                        _ => {}
                    }
                }
            }
            if let Some(first) = pending.iter().map(|(_, position)| *position).min() { through = first; }
            if through == 0 && maximum_messages == usize::MAX {
                return Err(invalid("tool exchange leaves no complete older compaction source"));
            }
            let (mandatory_guard, mandatory) = mandatory_positions_native(context, through, &self.retention, work).map(|(data, guard)| (guard, data))?;
            let needs_cut = through == 0 || mandatory.len() >= maximum_messages;
            drop(mandatory); drop(mandatory_guard);
            if needs_cut {
                pending.clear();
                let (cuts_guard, mut cuts) = native_vector::<usize>(context.messages.len().saturating_sub(1), work).map(|(data, guard)| (guard, data))?;
                for (position, message) in context.messages.iter().take(context.messages.len().saturating_sub(1)).enumerate() {
                    examine(work)?;
                    for part in message.content.parts() {
                        examine(work)?;
                        match part {
                            ModelContentPart::ToolCall { call_id, .. } => {
                                if !pending.iter().any(|(id, _)| *id == call_id) { pending.push((call_id, position)); }
                            }
                            ModelContentPart::ToolResult { call_id, .. } => {
                                if let Some(prior) = pending.iter().position(|(id, _)| *id == call_id) { pending.swap_remove(prior); }
                            }
                            _ => {}
                        }
                    }
                    if pending.is_empty() && position + 1 >= through { cuts.push(position + 1); }
                }
                let mut lower = 0;
                let mut upper = cuts.len();
                while lower < upper {
                    let middle = lower + (upper - lower) / 2;
                    let (guard, mandatory) = mandatory_positions_native(context, cuts[middle], &self.retention, work).map(|(data, guard)| (guard, data))?;
                    let too_many = mandatory.len() >= maximum_messages;
                    drop(mandatory); drop(guard);
                    if too_many { lower = middle + 1; } else { upper = middle; }
                }
                through = *cuts.get(lower).ok_or_else(|| invalid("mandatory content leaves no compaction source"))?;
                drop(cuts); drop(cuts_guard);
            }
            let (guard, mandatory) = mandatory_positions_native(context, through, &self.retention, work).map(|(data, guard)| (guard, data))?;
            let maximum = mandatory.len().checked_add(1).ok_or_else(overflow)?;
            drop(mandatory); drop(guard); drop(pending); drop(pending_guard);
            if maximum > context.messages.len() || maximum > maximum_messages {
                return Err(invalid("mandatory content leaves no compaction source"));
            }
            let guard = work.retain_owned(0).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            Ok(((through, maximum), guard))
        }
    }

    macro_rules! observer {
        ($work:expr) => {
            crate::store::history::native_work_io($work)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?
        };
    }

    pub(crate) fn clone_file_native(file: &FileRef, work: &NativeMaterializationWork) -> Result<(FileRef, NativeAllocation)> {
        file.clone_with_native_work(work)
    }
    pub(crate) fn clone_content_native(content: &ModelContent, work: &NativeMaterializationWork) -> Result<(ModelContent, NativeAllocation)> {
        let mut io = observer!(work);
        let (copy, _) = crate::model::clone_content_with_work(content, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_messages_native(messages: &[ModelMessage], work: &NativeMaterializationWork) -> Result<(Vec<ModelMessage>, NativeAllocation)> {
        let mut io = observer!(work);
        let (copy, _) = clone_messages_with_work(messages, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_metadata_native(metadata: &BTreeMap<String, FileRef>, work: &NativeMaterializationWork) -> Result<(BTreeMap<String, FileRef>, NativeAllocation)> {
        let mut io = observer!(work);
        let (copy, _) = clone_metadata_with_work(metadata, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_context_native(context: &Context, work: &NativeMaterializationWork) -> Result<(Context, NativeAllocation)> {
        let mut io = observer!(work);
        let (copy, _) = clone_context_with_work(context, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_context_prefix_native(
        context: &Context, through: usize, work: &NativeMaterializationWork,
    ) -> Result<(Context, NativeAllocation)> {
        let mut io = observer!(work);
        let (copy, _) = clone_context_prefix_with_work(context, through, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }
    pub(crate) fn clone_base_messages_native(
        first: &[ModelMessage], user: Option<&ModelContent>, second: &[ModelMessage],
        work: &NativeMaterializationWork,
    ) -> Result<(Vec<ModelMessage>, NativeAllocation)> {
        let mut io = observer!(work);
        let (copy, _) = clone_base_messages_with_work(first, user, second, &mut Some(&mut io))?;
        Ok((copy, io.into_custody()))
    }


    pub(crate) fn clone_retention_native(
        retention: &CompactionRetention, work: &NativeMaterializationWork,
    ) -> Result<(CompactionRetention, NativeAllocation)> {
        let copied = retention.roles.len().checked_mul(size_of::<crate::model::ModelRole>()).ok_or_else(overflow)? as u64;
        work.admit(WorkCounters { bytes_copied: copied,
            allocation_operations: u64::from(!retention.roles.is_empty()), ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut roles = native_vector::<crate::model::ModelRole>(retention.roles.len(), work)?;
        work.charge(WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::default()
        }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        roles.0.extend_from_slice(&retention.roles);
        Ok((CompactionRetention { roles: roles.0, native_media: retention.native_media }, roles.1))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::{clone_base_messages_native, clone_content_native, clone_context_native, clone_context_prefix_native, clone_file_native, clone_messages_native, clone_metadata_native, clone_retention_native, clone_text_native, mandatory_positions_native, native_vector, NativeContextValueRecipe};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelContent, ModelMessage, ModelRole, ToolResultContent};

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn partial_field_copy_releases_original_custody_and_cancelled_copy_is_empty() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 3,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4096,
            items_examined: 128,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let messages = vec![
            ModelMessage { role: ModelRole::User, content: ModelContent::Text("first".into()) },
            ModelMessage { role: ModelRole::User, content: ModelContent::Text("second".into()) },
        ];
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        assert!(clone_messages_with_work(&messages, &mut Some(&mut observer)).is_err());
        assert!(work.receipt().bytes_copied >= 5);
        let custody = observer.into_custody();
        assert_eq!(custody.bytes(), 0, "failed partial payload drops before its quota releases");
        drop(custody);
        cancellation.cancel();
        let before = work.receipt();
        assert!(native::clone_messages_native(&messages, &work).is_err());
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
    }

    #[test]
    fn summary_prefix_preserves_ordinary_input_selection() -> Result<()> {
        let mut context = Context {
            messages: vec![
                ModelMessage { role: ModelRole::User, content: ModelContent::Text("first".into()) },
                ModelMessage { role: ModelRole::Assistant, content: ModelContent::Parts(vec![
                    ModelContentPart::Text { text: "second".into() },
                ]) },
            ],
            current_input_index: Some(1),
            ..Context::default()
        };
        let (prefix, claim) = clone_context_prefix_with_work(&context, 1, &mut None)?;
        assert_eq!(prefix.messages, context.messages[..1]);
        assert_eq!(prefix.current_input_index, None);
        assert_eq!(claim, 0);
        context.current_input_index = Some(0);
        assert_eq!(clone_context_prefix_with_work(&context, 1, &mut None)?.0.current_input_index, Some(0));
        assert!(clone_context_prefix_with_work(&context, 3, &mut None).is_err());
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn physical_vector_denial_does_not_copy_source_fields() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4096,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let messages = vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text("original field".into()) }];
        let before = work.receipt();
        assert!(native::clone_messages_native(&messages, &work).is_err());
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(messages[0].content, ModelContent::Text("original field".into()));
    }

    #[test]
    fn count_pressure_preserves_instructions_and_complete_exchanges() -> Result<()> {
        let mut context = Context {
            messages: (0..8)
                .map(|index| ModelMessage {
                    role: if index == 0 {
                        ModelRole::System
                    } else {
                        ModelRole::User
                    },
                    content: ModelContent::Text(format!("message {index}")),
                })
                .collect(),
            current_input_index: Some(7),
            ..Context::default()
        };
        let count = ModelTokenCount {
            request_digest: [0; 32],
            fixed_tokens: 1,
            message_tokens: vec![1; 8],
        };
        let policy = ThresholdCompaction::default();
        assert_eq!(
            policy.projection_budget_with_message_limit(&context, &count, 6)?,
            (4, 6)
        );
        context.messages[3] = ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall {
                call_id: "call".into(),
                name: "tool".into(),
                arguments: serde_json::Value::Null,
            }),
        };
        context.messages[4] = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "call".into(),
                name: "tool".into(),
                content: ToolResultContent::Json {
                    value: serde_json::Value::Null,
                },
            }),
        };
        assert_eq!(
            policy.projection_budget_with_message_limit(&context, &count, 6)?,
            (5, 5)
        );
        assert!(
            policy
                .projection_budget_with_message_limit(&context, &count, 2)
                .is_err()
        );
        // Explicit token-only callers retain their original no-pressure behavior.
        assert!(policy.projection_budget(&context, &count).is_err());
        Ok(())
    }
}
