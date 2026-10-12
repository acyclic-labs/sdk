//! Funded copies of main reducer records.
use super::*;
/// Copies text into the supplied quota.
pub(crate) fn copy_text(
    source: &str, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<String> {
    if quota.work().is_none() { return Ok(source.to_owned()); }
    let text = quota.work().as_deref_mut().expect("component copy observer").clone_text(source)?;
    quota.adopt(text.capacity() as u64)?;
    Ok(text)
}
pub(crate) fn copy_authority(
    source: &Authority, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Authority> {
    if quota.work().is_none() { return Ok(source.clone()); }
    let authority = quota.work().as_deref_mut().expect("component copy observer").clone_authority(source)?;
    quota.adopt(authority.id.capacity() as u64)?;
    Ok(authority)
}
/// Copies the canonical three provider identity fields through the same quota.
pub(crate) fn copy_provider(
    source: &crate::resources::ProviderRef, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<crate::resources::ProviderRef> {
    if quota.work().is_none() { return Ok(source.clone()); }
    let namespace = copy_text(source.namespace(), quota)?;
    let family = copy_text(source.family(), quota)?;
    let version = copy_text(source.version(), quota)?;
    crate::resources::ProviderRef::new(namespace, family, version, quota.work())
}

pub(crate) fn copy_resource(
    source: &crate::resources::ResourceRef, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<crate::resources::ResourceRef> {
    crate::resources::ResourceRef::new(
        source.kind(), copy_provider(source.provider(), quota)?,
        copy_copy_vector(source.key(), quota)?,
        source.version().map(|value| copy_text(value, quota)).transpose()?,
        quota.work(),
    )
}

pub(crate) fn copy_file(
    source: &FileRef, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<FileRef> {
    if quota.work().is_none() { return Ok(source.clone()); }
    let file = source.copy(quota.work())?;
    quota.adopt(file.logical_owned_bytes()?)?;
    Ok(file)
}
pub(crate) fn copy_vector<T>(
    source: &[T], quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    copy: impl FnMut(&T, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
) -> Result<Vec<T>> {
    copy_iter(source.len(), source.iter(), quota, copy)
}
pub(crate) fn copy_iter<'s, T: 's, U>(
    count: usize, source: impl Iterator<Item = &'s T>,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    mut copy: impl FnMut(&T, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<U>,
) -> Result<Vec<U>> {
    let mut output = reserve_copy_vector::<U>(count, quota)?;
    for value in source { output.push(copy(value, quota)?); }
    Ok(output)
}
pub(crate) fn reserve_copy_vector<T>(
    count: usize, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Vec<T>> {
    if quota.work().is_none() { return Ok(Vec::with_capacity(count)); }
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("component vector ownership overflows".into()))? as u64;
    if bytes != 0 {
        admit_buffer_allocation(quota.work())?;
        let io = quota.work().as_deref_mut().expect("component vector observer");
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    let mut output = Vec::new();
    if bytes != 0 {
        begin_buffer_allocation(quota.work())?;
        output.try_reserve_exact(count).map_err(|cause| {
            quota.work().as_deref().expect("component vector observer").allocation_failure(cause)
        })?;
        let actual = output.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| Error::Invalid("component vector capacity overflows".into()))? as u64;
        if actual > bytes {
            quota.work().as_deref_mut().expect("component vector observer").retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
    }
    Ok(output)
}
pub(crate) fn copy_copy_vector<T: Copy>(
    source: &[T], quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Vec<T>> {
    if quota.work().is_none() { return Ok(source.to_vec()); }
    let bytes = source.len().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("component copy bytes overflow".into()))? as u64;
    quota.work().as_deref().expect("component copy observer").admit_copy(bytes)?;
    let mut output = reserve_copy_vector::<T>(source.len(), quota)?;
    quota.work().as_deref_mut().expect("component copy observer").copy_bytes(bytes)?;
    output.extend_from_slice(source);
    Ok(output)
}
pub(crate) fn copy_owned<T: Clone + Serialize>(
    source: &T, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    owned: impl FnOnce(&T, &mut Option<&mut dyn HistoryIo>) -> Result<u64>,
) -> Result<T> {
    if quota.work().is_none() { return Ok(source.clone()); }
    source_metadata_inspection_with_work(source, quota.work())?;
    let bytes = owned(source, quota.work())?;
    let io = quota.work().as_deref_mut().expect("observed copy");
    io.admit_copy(bytes)?;
    io.admit_owned(bytes)?;
    io.retain_owned(bytes)?;
    quota.adopt(bytes)?;
    quota.work().as_deref_mut().expect("observed copy").copy_bytes(bytes)?;
    Ok(source.clone())
}

macro_rules! copy_fields {
    ($name:ident, $ty:ty, $owned:expr) => {
        pub(crate) fn $name(source: &$ty, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<$ty> {
            copy_owned(source, quota, $owned)
        }
    };
}
copy_fields!(copy_reference, EventReference, |value: &EventReference, _| Ok(value.authority.id.capacity() as u64));
copy_fields!(copy_dependency, ExtensionDependency, |value: &ExtensionDependency, _| Ok(value.name.capacity() as u64));
copy_fields!(copy_configuration, ExtensionConfiguration, |value: &ExtensionConfiguration, work| {
    value.content.validate(work)?;
    owned_configuration(value)
});
copy_fields!(copy_extension_record, ExtensionRecord, |value: &ExtensionRecord, work| {
    value.content.validate(work)?;
    owned_record(value)
});
copy_fields!(copy_status, EffectStatus, |value: &EffectStatus, work| {
    if let EffectStatus::Succeeded { result } = value { result.validate(work)?; }
    owned_status(value)
});
copy_fields!(copy_effect, EffectState, |value: &EffectState, work| {
    value.request.validate(work)?;
    value.result_schema.validate(work)?;
    if let EffectStatus::Succeeded { result } = &value.status { result.validate(work)?; }
    owned_effect(value)
});
copy_fields!(copy_value, Value, |value: &Value, _| value_owned_bytes(value));
copy_fields!(copy_ticket, InteractionTicket, |value: &InteractionTicket, work| {
    value.request.validate(work)?;
    owned_ticket(value)
});
copy_fields!(copy_interaction_outcome, InteractionOutcome, |value: &InteractionOutcome, work| {
    match value {
        InteractionOutcome::Answered { answer } => {
            answer.validate(work)?;
            owned_sum([std::mem::size_of::<FileRef>() as u64, answer.logical_owned_bytes()?])
        }
        _ => Ok(0),
    }
});
copy_fields!(copy_resolution, InteractionResolution, |value: &InteractionResolution, work| {
    if let InteractionOutcome::Answered { answer } = &value.outcome { answer.validate(work)?; }
    if let Some(file) = &value.detail { file.validate(work)?; }
    owned_resolution(value)
});
copy_fields!(copy_capabilities, Capabilities, |value: &Capabilities, _| value.logical_owned_bytes());
copy_fields!(copy_observation, EffectAttestation, |value: &EffectAttestation, work| {
    if let EffectStatus::Succeeded { result } = &value.status { result.validate(work)?; }
    owned_sum([value.provider.capacity() as u64, owned_status(&value.status)?])
});
copy_fields!(copy_selection, ModelContextSelection, |value: &ModelContextSelection, work| {
    if let Some(file) = &value.checkpoint { file.validate(work)?; }
    owned_sum([owned_buffer::<uuid::Uuid>(value.message_ids.capacity())?,
        value.checkpoint.as_ref().map(FileRef::logical_owned_bytes).transpose()?.unwrap_or(0)])
});

impl<'a, 'w> crate::wire_codec::CodecQuota<'a, 'w> {
    pub(crate) fn produce<T>(work: &'a mut Option<&'w mut dyn HistoryIo>,
        copy: impl FnOnce(&mut Self) -> Result<T>) -> Result<(T, u64)> {
        let mut quota = Self::new(work, 0)?;
        let value = copy(&mut quota)?;
        let retained = quota.retained();
        quota.transfer();
        Ok((value, retained))
    }
}

pub(crate) fn copy_boxed<T>(
    source: &T, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    copy: impl FnOnce(&T, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
) -> Result<Box<T>> {
    retain_entry::<T>(quota)?;
    Ok(Box::new(copy(source, quota)?))
}
pub(in crate::core) fn copy_receipt(
    source: &ProjectMergeReceipt, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ProjectMergeReceipt> {
    let (receipt, bytes) = source.clone_with_work_and_custody(quota.work())?;
    quota.adopt(bytes)?;
    Ok(receipt)
}
pub(in crate::core) fn copy_message(
    source: &ConversationMessage, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ConversationMessage> {
    copy_owned(source, quota, |value, work| { value.validate(work)?; owned_message(value) })
}

pub(in crate::core) fn copy_payload(
    source: &EventPayload, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<EventPayload> {
    copy_owned(source, quota, |value, work| {
        match value {
            EventPayload::Custom { record } | EventPayload::ExtensionStateMigrated {
                migration: ExtensionStateMigration { record, .. }
            } => record.content.validate(work)?,
            EventPayload::ExtensionsSelected { configurations, .. } =>
                for value in configurations { value.content.validate(work)?; },
            EventPayload::ExtensionConfigured { previous, record } => {
                record.content.validate(work)?;
                if let Some(value) = previous { value.content.validate(work)?; }
            }
            EventPayload::EffectPlanned { request, result_schema, .. } => {
                request.validate(work)?; result_schema.validate(work)?;
            }
            EventPayload::EffectResolved { observation } =>
                if let EffectStatus::Succeeded { result } = &observation.status { result.validate(work)?; },
            EventPayload::ForkPublished { seed } => seed.validate(work)?,
            EventPayload::ProjectMergePublished { receipt } => {
                receipt.validate_shape(work)?;
            }
            EventPayload::ConversationMessageAppended { message } => message.validate(work)?,
            EventPayload::ModelContextSelected { selection } =>
                if let Some(file) = &selection.checkpoint { file.validate(work)?; },
            EventPayload::InteractionOpened { ticket } => ticket.request.validate(work)?,
            EventPayload::InteractionResolved { resolution } => {
                if let InteractionOutcome::Answered { answer } = &resolution.outcome { answer.validate(work)?; }
                if let Some(file) = &resolution.detail { file.validate(work)?; }
            }
            _ => {}
        }
        owned_payload(value)
    })
}
pub(crate) fn copy_event(
    source: &Event, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Event> {
    let payload = copy_payload(&source.payload, quota)?;
    let scope = copy_owned(&source.scope, quota, |value, _| value.logical_owned_bytes())?;
    let causal_parent = source.causal_parent.as_ref().map(|parent| copy_reference(parent, quota)).transpose()?;
    Ok(Event {
        revision: source.revision, operation_id: source.operation_id,
        intent_digest: source.intent_digest, attestation: source.attestation,
        payload, scope, causal_parent,
    })
}

/// Logical inline map/set payload, never an estimate of opaque allocator nodes.
pub(in crate::core) fn retain_entry<T>(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    let bytes = std::mem::size_of::<T>() as u64;
    let Some(io) = quota.work().as_deref_mut() else { return Ok(()); };
    io.admit_owned(bytes)?;
    io.retain_owned(bytes)?;
    quota.adopt(bytes)
}

pub(in crate::core) fn is_native(io: &dyn HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { io.original_native_work().is_some() }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = io; false }
}

pub(in crate::core) fn admit_buffer_allocation(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work.as_deref_mut().filter(|io| is_native(&**io)) {
        io.original_native_work().expect("original allocation issuer")
            .admit(acyclic_fs::WorkCounters {
                allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
            }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    }
    let _ = work;
    Ok(())
}

pub(in crate::core) fn begin_buffer_allocation(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work.as_deref_mut().filter(|io| is_native(&**io)) {
        io.original_native_work().expect("original allocation issuer")
            .charge(acyclic_fs::WorkCounters {
                allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
            }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    }
    let _ = work;
    Ok(())
}
