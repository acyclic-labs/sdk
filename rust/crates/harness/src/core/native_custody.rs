//! Native custody follows live reducer components, not query or replay lifetime.
use super::*;
use crate::store::history::HistoryIo;

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
    crate::resources::ProviderRef::new_with_work(namespace, family, version, quota.work())
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
pub(crate) fn copy_reference(
    source: &EventReference, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<EventReference> {
    Ok(EventReference { authority: copy_authority(&source.authority, quota)?, revision: source.revision })
}
pub(crate) fn copy_dependency(
    source: &ExtensionDependency, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ExtensionDependency> {
    Ok(ExtensionDependency { name: copy_text(&source.name, quota)?, version: source.version })
}
pub(crate) fn copy_configuration(
    source: &ExtensionConfiguration, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ExtensionConfiguration> {
    let extension = copy_dependency(&source.extension, quota)?;
    let content = copy_file(&source.content, quota)?;
    Ok(ExtensionConfiguration { extension, content, schema_digest: source.schema_digest })
}
pub(crate) fn copy_extension_record(
    source: &ExtensionRecord, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ExtensionRecord> {
    let name = copy_text(&source.name, quota)?;
    let content = copy_file(&source.content, quota)?;
    Ok(ExtensionRecord {
        name, version: source.version, schema_digest: source.schema_digest,
        implementation_digest: source.implementation_digest, fork_policy: source.fork_policy, content,
    })
}
pub(crate) fn copy_status(
    source: &EffectStatus, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<EffectStatus> {
    Ok(match source {
        EffectStatus::Planned => EffectStatus::Planned,
        EffectStatus::Dispatched => EffectStatus::Dispatched,
        EffectStatus::Indeterminate => EffectStatus::Indeterminate,
        EffectStatus::Succeeded { result } => EffectStatus::Succeeded { result: copy_file(result, quota)? },
        EffectStatus::Failed { message } => EffectStatus::Failed { message: copy_text(message, quota)? },
    })
}
pub(crate) fn copy_effect(
    source: &EffectState, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<EffectState> {
    let provider = copy_text(&source.provider, quota)?;
    let effect_kind = copy_text(&source.effect_kind, quota)?;
    let request = copy_file(&source.request, quota)?;
    let result_schema = copy_file(&source.result_schema, quota)?;
    let status = copy_status(&source.status, quota)?;
    let attempts = copy_copy_vector(&source.attempts, quota)?;
    Ok(EffectState {
        provider, guarantee: source.guarantee, effect_kind, request,
        request_digest: source.request_digest, result_schema,
        result_schema_digest: source.result_schema_digest, status, attempts,
    })
}
pub(crate) fn copy_value(
    source: &Value, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Value> {
    if quota.work().is_none() { return Ok(source.clone()); }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if quota.work().as_deref().is_some_and(|observer| observer.original_native_work().is_some()) {
        let observer = quota.work().as_deref_mut().expect("native Value copy observer");
        let remaining = observer.remaining()?;
        crate::contract::json_owned_value_shape_bounded(
            source,
            crate::contract::JsonBounds {
                maximum_bytes: u64::MAX,
                maximum_items: remaining.items_examined,
                maximum_depth: 256,
            },
            crate::contract::JsonValueSchema::None,
            &mut |charge| observer.codec_charge(charge),
        )?;
    }
    copy_value_inner(source, quota)
}

fn copy_value_inner(
    source: &Value, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Value> {
    quota.work().as_deref_mut().expect("value copy observer")
        .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    Ok(match source {
        Value::Null => Value::Null,
        Value::Bool(value) => Value::Bool(*value),
        Value::Number(value) => Value::Number(value.clone()),
        Value::String(value) => Value::String(copy_text(value, quota)?),
        Value::Array(values) => Value::Array(copy_vector(values, quota, copy_value_inner)?),
        Value::Object(values) => {
            let bytes = values.len().checked_mul(std::mem::size_of::<(String, Value)>())
                .ok_or_else(|| Error::Invalid("JSON object ownership overflows".into()))? as u64;
            let io = quota.work().as_deref_mut().expect("JSON object copy observer");
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            let mut output = serde_json::Map::new();
            for (key, value) in values {
                let key = copy_text(key, quota)?;
                let value = copy_value_inner(value, quota)?;
                output.insert(key, value);
            }
            Value::Object(output)
        }
    })
}

macro_rules! physical_clone_factory {
    ($name:ident, $ty:ty, $copy:path, $source:ident) => {
        pub(crate) fn $name(
            $source: &$ty, work: &mut Option<&mut dyn HistoryIo>,
        ) -> Result<($ty, u64)> {
            if work.is_none() { return Ok(($source.clone(), 0)); }
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let output = $copy($source, &mut quota)?;
            let bytes = quota.retained();
            quota.transfer();
            Ok((output, bytes))
        }
    };
}
pub(crate) use physical_clone_factory;
physical_clone_factory!(native_effect_clone_with_custody, EffectState, copy_effect, source);
physical_clone_factory!(native_value_clone_with_custody, Value, copy_value, source);
pub(crate) fn copy_boxed<T>(
    source: &T, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    copy: impl FnOnce(&T, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
) -> Result<Box<T>> {
    if quota.work().is_none() { return Ok(Box::new(copy(source, quota)?)); }
    let bytes = std::mem::size_of::<T>() as u64;
    if bytes != 0 {
        admit_buffer_allocation(quota.work())?;
        let io = quota.work().as_deref_mut().expect("component Box observer");
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    let value = copy(source, quota)?;
    if bytes != 0 { begin_buffer_allocation(quota.work())?; }
    Ok(Box::new(value))
}
pub(super) fn copy_ticket(
    source: &InteractionTicket, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<InteractionTicket> {
    Ok(InteractionTicket {
        id: source.id, kind: source.kind, request: copy_file(&source.request, quota)?,
        deadline_unix_ms: source.deadline_unix_ms,
        approval: source.approval.as_ref().map(|approval| crate::interaction::ApprovalBinding {
            operation_id: approval.operation_id, action_digest: approval.action_digest,
        }),
    })
}
pub(crate) fn copy_interaction_outcome(
    source: &InteractionOutcome, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<InteractionOutcome> {
    Ok(match source {
        InteractionOutcome::Answered { answer } => InteractionOutcome::Answered {
            answer: copy_boxed(answer.as_ref(), quota, copy_file)?,
        },
        InteractionOutcome::Approved => InteractionOutcome::Approved,
        InteractionOutcome::Declined => InteractionOutcome::Declined,
        InteractionOutcome::Cancelled => InteractionOutcome::Cancelled,
        InteractionOutcome::Expired => InteractionOutcome::Expired,
        InteractionOutcome::Denied => InteractionOutcome::Denied,
        InteractionOutcome::Indeterminate { operation_id } =>
            InteractionOutcome::Indeterminate { operation_id: *operation_id },
    })
}
pub(super) fn copy_resolution(
    source: &InteractionResolution, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<InteractionResolution> {
    let outcome = copy_interaction_outcome(&source.outcome, quota)?;
    let detail = source.detail.as_ref().map(|file| copy_file(file, quota)).transpose()?;
    Ok(InteractionResolution {
        id: source.id, expected_version: source.expected_version, outcome, detail,
    })
}
physical_clone_factory!(native_interaction_ticket_clone_with_custody, InteractionTicket, copy_ticket, source);
physical_clone_factory!(native_interaction_resolution_clone_with_custody, InteractionResolution, copy_resolution, source);
physical_clone_factory!(native_interaction_outcome_clone_with_custody, InteractionOutcome, copy_interaction_outcome, source);
pub(crate) fn copy_capabilities(
    source: &Capabilities, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Capabilities> {
    if quota.work().is_none() { return Ok(source.clone()); }
    let slots = source.iter().count().checked_mul(std::mem::size_of::<String>())
        .ok_or_else(|| Error::Invalid("capability slots overflow".into()))? as u64;
    let io = quota.work().as_deref_mut().expect("capability copy observer");
    io.admit_owned(slots)?;
    io.retain_owned(slots)?;
    quota.adopt(slots)?;
    let mut source = source.iter();
    let mut failure = None;
    let output = Capabilities::new(std::iter::from_fn(|| {
        if failure.is_some() { return None; }
        let value = source.next()?;
        let copied = (|| {
            let observer = quota.work().as_deref_mut().expect("capability copy observer");
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            copy_text(value, quota)
        })();
        match copied {
            Ok(text) => Some(text),
            Err(error) => { failure = Some(error); None }
        }
    }));
    if let Some(error) = failure { drop(output); return Err(error); }
    Ok(output)
}
pub(super) fn copy_recorded_scope(
    source: &RecordedScope, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<RecordedScope> {
    let id = copy_text(&source.id, quota)?;
    let issuer = copy_text(&source.issuer, quota)?;
    let capabilities = copy_capabilities(&source.capabilities, quota)?;
    Ok(RecordedScope { id, issuer, capabilities, agent: source.agent })
}
pub(super) fn copy_observation(
    source: &EffectAttestation, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<EffectAttestation> {
    Ok(EffectAttestation {
        provider: copy_text(&source.provider, quota)?, effect_id: source.effect_id,
        attempt_id: source.attempt_id, request_digest: source.request_digest,
        guarantee: source.guarantee, result_schema_digest: source.result_schema_digest,
        status: copy_status(&source.status, quota)?, proof: source.proof,
    })
}
pub(super) fn copy_seed(
    source: &ForkSeed, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ForkSeed> {
    let (seed, bytes) = source.copy(quota.work())?;
    quota.adopt(bytes)?;
    Ok(seed)
}
pub(super) fn copy_receipt(
    source: &ProjectMergeReceipt, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ProjectMergeReceipt> {
    let (receipt, bytes) = source.clone_with_work_and_custody(quota.work())?;
    quota.adopt(bytes)?;
    Ok(receipt)
}
pub(super) fn copy_message(
    source: &ConversationMessage, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ConversationMessage> {
    source.clone_in_quota(quota)
}
pub(super) fn copy_selection(
    source: &ModelContextSelection, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ModelContextSelection> {
    Ok(ModelContextSelection {
        conversation_revision: source.conversation_revision,
        message_ids: copy_copy_vector(&source.message_ids, quota)?,
        checkpoint: source.checkpoint.as_ref().map(|file| copy_file(file, quota)).transpose()?,
    })
}
pub(super) fn copy_payload(
    source: &EventPayload, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<EventPayload> {
    Ok(match source {
        EventPayload::LifecycleTransitioned { from, to, reason } => EventPayload::LifecycleTransitioned {
            from: *from, to: *to, reason: reason.as_deref().map(|text| copy_text(text, quota)).transpose()?,
        },
        EventPayload::Custom { record } => EventPayload::Custom { record: copy_extension_record(record, quota)? },
        EventPayload::ExtensionStateMigrated { migration } => EventPayload::ExtensionStateMigrated {
            migration: ExtensionStateMigration {
                previous: copy_reference(&migration.previous, quota)?,
                record: copy_extension_record(&migration.record, quota)?,
            },
        },
        EventPayload::ExtensionsSelected { previous, selected, configurations } => EventPayload::ExtensionsSelected {
            previous: copy_vector(previous, quota, copy_dependency)?,
            selected: copy_vector(selected, quota, copy_dependency)?,
            configurations: copy_vector(configurations, quota, copy_configuration)?,
        },
        EventPayload::ExtensionConfigured { previous, record } => EventPayload::ExtensionConfigured {
            previous: previous.as_ref().map(|value| copy_configuration(value, quota)).transpose()?,
            record: copy_configuration(record, quota)?,
        },
        EventPayload::EffectPlanned { effect_id, provider, guarantee, effect_kind, request,
            request_digest, result_schema, result_schema_digest } => EventPayload::EffectPlanned {
            effect_id: *effect_id, provider: copy_text(provider, quota)?, guarantee: *guarantee,
            effect_kind: copy_text(effect_kind, quota)?, request: copy_file(request, quota)?,
            request_digest: *request_digest, result_schema: copy_file(result_schema, quota)?,
            result_schema_digest: *result_schema_digest,
        },
        EventPayload::EffectDispatched { effect_id, attempt_id } =>
            EventPayload::EffectDispatched { effect_id: *effect_id, attempt_id: *attempt_id },
        EventPayload::EffectResolved { observation } =>
            EventPayload::EffectResolved { observation: copy_observation(observation, quota)? },
        EventPayload::ForkPublished { seed } =>
            EventPayload::ForkPublished { seed: copy_boxed(seed.as_ref(), quota, copy_seed)? },
        EventPayload::ProjectMergePublished { receipt } =>
            EventPayload::ProjectMergePublished { receipt: copy_boxed(receipt.as_ref(), quota, copy_receipt)? },
        EventPayload::ConversationBound { agent } => EventPayload::ConversationBound { agent: *agent },
        EventPayload::ConversationMessageAppended { message } =>
            EventPayload::ConversationMessageAppended { message: copy_boxed(message.as_ref(), quota, copy_message)? },
        EventPayload::ModelContextSelected { selection } =>
            EventPayload::ModelContextSelected { selection: copy_selection(selection, quota)? },
        EventPayload::InteractionOpened { ticket } =>
            EventPayload::InteractionOpened { ticket: copy_ticket(ticket, quota)? },
        EventPayload::InteractionResolved { resolution } =>
            EventPayload::InteractionResolved { resolution: copy_resolution(resolution, quota)? },
    })
}
pub(super) fn copy_event(
    source: &Event, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Event> {
    Ok(Event {
        revision: source.revision, operation_id: source.operation_id, intent_digest: source.intent_digest,
        scope: copy_recorded_scope(&source.scope, quota)?,
        causal_parent: source.causal_parent.as_ref().map(|parent| copy_reference(parent, quota)).transpose()?,
        payload: copy_payload(&source.payload, quota)?, attestation: source.attestation,
    })
}
physical_clone_factory!(native_event_clone_with_custody, Event, copy_event, source);
physical_clone_factory!(native_effect_attestation_clone_with_custody, EffectAttestation, copy_observation, source);

/// Logical inline map/set payload, never an estimate of opaque allocator nodes.
pub(super) fn retain_entry<T>(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    let bytes = std::mem::size_of::<T>() as u64;
    let Some(io) = quota.work().as_deref_mut() else { return Ok(()); };
    io.admit_owned(bytes)?;
    io.retain_owned(bytes)?;
    quota.adopt(bytes)
}

pub(super) fn copy_reducer(
    source: &Reducer, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Reducer> {
    let native = work.as_deref().is_some_and(is_native);
    let conversation = if native { None } else { Some(source.conversation.copy(work, None)?.0) };
    let mut portable = None;
    let mut quota = crate::wire_codec::CodecQuota::new(if native { work } else { &mut portable }, 0)?;
    let authority = copy_authority(&source.authority, &mut quota)?;
    let verifier = source.authority_verifier.copy(quota.work())?;
    quota.adopt(verifier.logical_owned_bytes()?)?;
    quota.transfer();
    let mut output = Reducer::new_with_preclaimed_work(authority, verifier, source.schemas.clone(), quota.work())?;
    macro_rules! component {
        ($key:expr, $copy:expr) => {{
            let (value, retired) = output.native_custody.produce($key, quota.work(), $copy)?;
            drop(retired);
            value
        }};
    }
    output.active_extensions = component!(NativeComponentRef::ActiveExtensions,
        |quota| copy_vector(&source.active_extensions, quota, copy_dependency));
    output.active_configurations = component!(NativeComponentRef::ActiveConfigurations,
        |quota| copy_vector(&source.active_configurations, quota, copy_configuration));
    for (name, (reference, record)) in &source.extension_states {
        let entry = component!(NativeComponentRef::ExtensionState(name), |quota| {
            retain_entry::<(String, (EventReference, ExtensionRecord))>(quota)?;
            Ok((copy_text(name, quota)?, (copy_reference(reference, quota)?, copy_extension_record(record, quota)?)))
        });
        output.extension_states.insert(entry.0, entry.1);
    }
    for (extension, record) in &source.configured_extensions {
        let entry = component!(NativeComponentRef::ExtensionConfiguration(extension), |quota| {
            retain_entry::<(ExtensionDependency, ExtensionConfiguration)>(quota)?;
            Ok((copy_dependency(extension, quota)?, copy_configuration(record, quota)?))
        });
        output.configured_extensions.insert(entry.0, entry.1);
    }
    let events = component!(NativeComponentRef::EventBuffer,
        |quota| reserve_copy_vector::<Event>(source.events.len(), quota));
    output.events = VecDeque::from(events);
    for event in &source.events {
        let copied = component!(NativeComponentRef::Event(event.revision), |quota| copy_event(event, quota));
        output.events.push_back(copied);
    }
    for (operation, revision) in &source.operation_positions {
        component!(NativeComponentRef::OperationPosition(*operation), |quota| {
            retain_entry::<(OperationId, u64)>(quota)?;
            Ok(())
        });
        output.operation_positions.insert(*operation, *revision);
    }
    for (id, effect) in &source.effects {
        let mut copied = component!(NativeComponentRef::EffectAction(*id), |quota| {
            retain_entry::<(EffectId, EffectState)>(quota)?;
            Ok(EffectState {
                provider: copy_text(&effect.provider, quota)?, guarantee: effect.guarantee,
                effect_kind: copy_text(&effect.effect_kind, quota)?, request: copy_file(&effect.request, quota)?,
                request_digest: effect.request_digest, result_schema: copy_file(&effect.result_schema, quota)?,
                result_schema_digest: effect.result_schema_digest, status: EffectStatus::Planned, attempts: Vec::new(),
            })
        });
        copied.status = component!(NativeComponentRef::EffectStatus(*id), |quota| copy_status(&effect.status, quota));
        copied.attempts = component!(NativeComponentRef::EffectAttempts(*id), |quota| copy_copy_vector(&effect.attempts, quota));
        output.effects.insert(*id, copied);
    }
    output.indexed_terminal_effects = VecDeque::from(component!(NativeComponentRef::TerminalEffectBuffer,
        |quota| copy_iter(source.indexed_terminal_effects.len(), source.indexed_terminal_effects.iter(), quota,
            |id, quota| {
                if let Some(io) = quota.work().as_deref_mut() {
                    io.copy_bytes(std::mem::size_of::<EffectId>() as u64)?;
                }
                Ok(*id)
            })));
    for (authority, seed) in &source.forks {
        let entry = component!(NativeComponentRef::Fork(authority), |quota| {
            retain_entry::<(Authority, ForkSeed)>(quota)?;
            Ok((copy_authority(authority, quota)?, copy_seed(seed, quota)?))
        });
        output.forks.insert(entry.0, entry.1);
    }
    for (name, operation) in &source.published_merges {
        let entry = component!(NativeComponentRef::Merge(name, operation), |quota| {
            retain_entry::<(String, Vec<u8>)>(quota)?;
            Ok((copy_text(name, quota)?, copy_copy_vector(operation, quota)?))
        });
        output.published_merges.insert(entry);
    }
    if let Some(conversation) = conversation {
        output.conversation = conversation;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native {
        for message in &source.conversation.messages {
            let retired = output.native_custody.install(NativeComponentCustody::prepare(
                NativeComponentRef::ConversationMessage(message.sequence), 0, quota.work(),
            )?, quota.work())?;
            drop(retired);
        }
        let (conversation, remaining) = source.conversation.copy(
            quota.work(), Some(&mut |sequence, produced, work| {
                output.native_custody.absorb_produced(NativeComponentRef::ConversationMessage(sequence),
                    produced, work.as_deref_mut().expect("message producing observer"))
            }),
        )?;
        output.conversation = conversation;
        if remaining != 0 {
            output.native_custody.absorb_produced(NativeComponentRef::Base, remaining,
                quota.work().as_deref_mut().expect("conversation remainder observer"))?;
        }
    }
    if let Some(file) = &source.latest_context_checkpoint {
        output.latest_context_checkpoint = Some(component!(NativeComponentRef::ContextCheckpoint,
            |quota| copy_file(file, quota)));
    }
    for (id, (ticket, resolution)) in &source.interactions {
        let ticket = component!(NativeComponentRef::InteractionTicket(*id), |quota| {
            retain_entry::<(uuid::Uuid, (InteractionTicket, Option<InteractionResolution>))>(quota)?;
            copy_ticket(ticket, quota)
        });
        let resolution = resolution.as_ref().map(|resolution| -> Result<InteractionResolution> {
            Ok(component!(NativeComponentRef::InteractionResolution(*id), |quota| copy_resolution(resolution, quota)))
        }).transpose()?;
        output.interactions.insert(*id, (ticket, resolution));
    }
    output.revision = source.revision;
    output.lifecycle = source.lifecycle;
    output.active_extension_revision = source.active_extension_revision;
    output.effects_archived = source.effects_archived;
    output.resident_event_limit = source.resident_event_limit;
    output.resident_terminal_effect_limit = source.resident_terminal_effect_limit;
    Ok(output)
}
pub(super) fn owned_sum(parts: impl IntoIterator<Item = u64>) -> Result<u64> {
    parts.into_iter().try_fold(0_u64, |sum, bytes| sum.checked_add(bytes)
        .ok_or_else(|| Error::Invalid("component ownership overflows".into())))
}
pub(super) fn owned_buffer<T>(capacity: usize) -> Result<u64> {
    capacity.checked_mul(std::mem::size_of::<T>()).map(|bytes| bytes as u64)
        .ok_or_else(|| Error::Invalid("component buffer ownership overflows".into()))
}
pub(super) fn owned_vector<T>(values: &Vec<T>, mut item: impl FnMut(&T) -> Result<u64>) -> Result<u64> {
    values.iter().try_fold(owned_buffer::<T>(values.capacity())?, |sum, value|
        owned_sum([sum, item(value)?]))
}
pub(super) fn owned_record(record: &ExtensionRecord) -> Result<u64> {
    owned_sum([record.name.capacity() as u64, record.content.logical_owned_bytes()?])
}
pub(super) fn owned_configuration(record: &ExtensionConfiguration) -> Result<u64> {
    owned_sum([record.extension.name.capacity() as u64, record.content.logical_owned_bytes()?])
}
pub(super) fn owned_status(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        _ => Ok(0),
    }
}
pub(super) fn owned_effect_action(effect: &EffectState) -> Result<u64> {
    owned_sum([effect.provider.capacity() as u64, effect.effect_kind.capacity() as u64,
        effect.request.logical_owned_bytes()?, effect.result_schema.logical_owned_bytes()?])
}
pub(super) fn owned_effect(effect: &EffectState) -> Result<u64> {
    owned_sum([owned_effect_action(effect)?, owned_status(&effect.status)?,
        owned_buffer::<EffectAttemptId>(effect.attempts.capacity())?])
}
pub(super) fn owned_ticket(ticket: &InteractionTicket) -> Result<u64> {
    ticket.request.logical_owned_bytes()
}
pub(super) fn owned_resolution(resolution: &InteractionResolution) -> Result<u64> {
    let answer = match &resolution.outcome {
        InteractionOutcome::Answered { answer } =>
            owned_sum([std::mem::size_of::<FileRef>() as u64, answer.logical_owned_bytes()?])?,
        _ => 0,
    };
    owned_sum([answer, resolution.detail.as_ref().map(FileRef::logical_owned_bytes).transpose()?.unwrap_or(0)])
}
pub(super) fn owned_message(message: &ConversationMessage) -> Result<u64> {
    let attachments = match &message.attachments {
        crate::conversation::ReferencedAttachments::Inline { items } => owned_vector(items, |item|
            owned_sum([item.file.logical_owned_bytes()?, item.label.as_ref().map_or(0, |text| text.capacity() as u64)]))?,
        crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => manifest.logical_owned_bytes()?,
    };
    let mut bytes = owned_sum([message.content.logical_owned_bytes()?, attachments,
        message.tool_call_id.as_ref().map_or(0, |text| text.capacity() as u64)])?;
    for (key, file) in &message.extensions {
        bytes = owned_sum([bytes, std::mem::size_of::<(String, FileRef)>() as u64,
            key.capacity() as u64, file.logical_owned_bytes()?])?;
    }
    Ok(bytes)
}
fn owned_revision(revision: &crate::fork::ResourceRevision) -> Result<u64> {
    use crate::fork::ResourceRevision as R;
    match revision {
        R::History(reference) => reference.as_resource().logical_owned_bytes(),
        R::Context(reference) => reference.as_resource().logical_owned_bytes(),
        R::Process(reference) => reference.as_resource().logical_owned_bytes(),
        R::Artifact(reference) => reference.as_resource().logical_owned_bytes(),
        R::SharedVolume(volume) => volume.logical_owned_bytes(),
        R::Project { volume, generation } => owned_sum([
            volume.logical_owned_bytes()?, generation.as_resource().logical_owned_bytes()?]),
        R::PrivateVolume { volume, generation, paths } => owned_sum([
            volume.logical_owned_bytes()?, generation.as_resource().logical_owned_bytes()?,
            owned_vector(paths, |path| Ok(path.capacity() as u64))?]),
        R::Extension { name, reference, .. } => owned_sum([
            name.capacity() as u64, reference.logical_owned_bytes()?]),
    }
}
fn owned_capture(capture: &crate::fork::Capture) -> Result<u64> {
    match capture {
        crate::fork::Capture::Captured(resource) =>
            owned_sum([owned_revision(&resource.source)?, owned_revision(&resource.revision)?]),
        crate::fork::Capture::Unsupported(reason) => Ok(reason.capacity() as u64),
        _ => Ok(0),
    }
}
pub(super) fn owned_seed(seed: &ForkSeed) -> Result<u64> {
    let mut bytes = owned_sum([
        seed.parent.id.capacity() as u64, seed.child.id.capacity() as u64,
        seed.authorized_source.as_ref().map(|source| source.logical_owned_bytes()).transpose()?.unwrap_or(0),
        owned_buffer::<AgentId>(seed.attached_agents.capacity())?,
        owned_vector(&seed.resources, |resource| owned_sum([
            owned_revision(&resource.source)?, owned_revision(&resource.revision)?]))?,
        owned_vector(&seed.omissions, |omission| owned_sum([
            owned_revision(&omission.selection.revision)?, owned_capture(&omission.outcome)?]))?,
        seed.child_private_volume.logical_owned_bytes()?,
        seed.child_private_generation.as_resource().logical_owned_bytes()?,
        seed.child_result_volume.as_ref().map(VolumeRef::logical_owned_bytes).transpose()?.unwrap_or(0),
        seed.child_result_generation.as_ref().map(|generation| generation.as_resource().logical_owned_bytes())
            .transpose()?.unwrap_or(0),
        owned_vector(&seed.inherited_context, FileRef::logical_owned_bytes)?,
        owned_vector(&seed.attachment_manifests, FileRef::logical_owned_bytes)?,
        owned_vector(&seed.shared_grants, |grant| owned_sum([
            grant.volume.logical_owned_bytes()?, owned_buffer::<VolumeOperation>(grant.operations.len())?]))?,
        owned_vector(&seed.reference_grants, |grant| owned_sum([
            grant.file.logical_owned_bytes()?,
            grant.attachment_manifest.as_ref().map(FileRef::logical_owned_bytes).transpose()?.unwrap_or(0)]))?,
    ])?;
    if let Some(summary) = &seed.summary {
        bytes = owned_sum([bytes, summary.selection.checkpoint.logical_owned_bytes()?,
            summary.context.logical_owned_bytes()?,
            owned_vector(&summary.payloads, FileRef::logical_owned_bytes)?,
            owned_vector(&summary.references, FileRef::logical_owned_bytes)?])?;
    }
    if let Some(boundary) = &seed.boundary {
        bytes = owned_sum([bytes, boundary.provider.logical_owned_bytes()?, boundary.evidence.capacity() as u64])?;
    }
    Ok(bytes)
}
pub(super) fn owned_receipt(receipt: &ProjectMergeReceipt) -> Result<u64> {
    owned_sum([
        receipt.child.id.capacity() as u64, receipt.source_project.logical_owned_bytes()?,
        receipt.target_project.logical_owned_bytes()?,
        receipt.source_generation.as_resource().logical_owned_bytes()?,
        receipt.expected_target_generation.as_resource().logical_owned_bytes()?,
        receipt.result_generation.as_resource().logical_owned_bytes()?,
        receipt.provider_operation_id.capacity() as u64,
        receipt.provider_proof.provider.logical_owned_bytes()?,
        receipt.provider_proof.format.capacity() as u64,
        value_owned_bytes(&receipt.provider_proof.statement)?, owned_message(&receipt.notice)?,
    ])
}
fn owned_payload(payload: &EventPayload) -> Result<u64> {
    match payload {
        EventPayload::LifecycleTransitioned { reason, .. } => Ok(reason.as_ref().map_or(0, |text| text.capacity() as u64)),
        EventPayload::Custom { record } => owned_record(record),
        EventPayload::ExtensionStateMigrated { migration } =>
            owned_sum([migration.previous.authority.id.capacity() as u64, owned_record(&migration.record)?]),
        EventPayload::ExtensionsSelected { previous, selected, configurations } => owned_sum([
            owned_vector(previous, |extension| Ok(extension.name.capacity() as u64))?,
            owned_vector(selected, |extension| Ok(extension.name.capacity() as u64))?,
            owned_vector(configurations, owned_configuration)?]),
        EventPayload::ExtensionConfigured { previous, record } => owned_sum([
            previous.as_ref().map(owned_configuration).transpose()?.unwrap_or(0), owned_configuration(record)?]),
        EventPayload::EffectPlanned { provider, effect_kind, request, result_schema, .. } => owned_sum([
            provider.capacity() as u64, effect_kind.capacity() as u64,
            request.logical_owned_bytes()?, result_schema.logical_owned_bytes()?]),
        EventPayload::EffectResolved { observation } =>
            owned_sum([observation.provider.capacity() as u64, owned_status(&observation.status)?]),
        EventPayload::ForkPublished { seed } =>
            owned_sum([std::mem::size_of::<ForkSeed>() as u64, owned_seed(seed)?]),
        EventPayload::ProjectMergePublished { receipt } =>
            owned_sum([std::mem::size_of::<ProjectMergeReceipt>() as u64, owned_receipt(receipt)?]),
        EventPayload::ConversationMessageAppended { message } =>
            owned_sum([std::mem::size_of::<ConversationMessage>() as u64, owned_message(message)?]),
        EventPayload::ModelContextSelected { selection } => owned_sum([
            owned_buffer::<uuid::Uuid>(selection.message_ids.capacity())?,
            selection.checkpoint.as_ref().map(FileRef::logical_owned_bytes).transpose()?.unwrap_or(0)]),
        EventPayload::InteractionOpened { ticket } => ticket.request.logical_owned_bytes(),
        EventPayload::InteractionResolved { resolution } => owned_resolution(resolution),
        EventPayload::EffectDispatched { .. } | EventPayload::ConversationBound { .. } => Ok(0),
    }
}
/// Actual existing payload capacities and known inline entry slots; used only to reconcile the original finite decoder and explicit cold restoration.
pub(crate) fn event_owned_bytes(event: &Event) -> Result<u64> {
    owned_sum([event.scope.logical_owned_bytes()?,
        event.causal_parent.as_ref().map_or(0, |reference| reference.authority.id.capacity() as u64),
        owned_payload(&event.payload)?])
}

    pub(crate) fn value_owned_bytes(value: &Value) -> Result<u64> {
        let overflow = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "JSON ownership overflows" } }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            { Error::Invalid("JSON ownership overflows".into()) }
        };
        match value {
            Value::String(text) => Ok(text.capacity() as u64),
            Value::Array(values) => {
                let slots = values.capacity()
                    .checked_mul(std::mem::size_of::<Value>())
                    .ok_or_else(overflow)? as u64;
                values.iter().try_fold(slots, |bytes,value| {
                    bytes.checked_add(value_owned_bytes(value)?).ok_or_else(overflow)
                })
            }
            Value::Object(values) => {
                let slots = values.len().checked_mul(std::mem::size_of::<(String,Value)>())
                    .ok_or_else(overflow)? as u64;
                values.iter().try_fold(slots, |bytes,(key,value)| {
                    let owned = value_owned_bytes(value)?;
                    bytes.checked_add(key.capacity() as u64)
                        .and_then(|bytes| bytes.checked_add(owned)).ok_or_else(overflow)
                })
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => Ok(0),
        }
    }

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum NativeComponentKey {
    Base, Authority, Event(u64), OperationPosition(OperationId), EventBuffer, TerminalEffectBuffer,
    EffectAction(EffectId), EffectStatus(EffectId), EffectAttempts(EffectId),
    ExtensionState(String), ExtensionConfiguration(ExtensionDependency),
    ActiveExtensions, ActiveConfigurations, Fork(Authority), Merge(String, Vec<u8>),
    ConversationMessage(u64), ContextCheckpoint,
    InteractionTicket(uuid::Uuid), InteractionResolution(uuid::Uuid),
}

#[derive(Clone, Copy)]
pub(super) enum NativeComponentRef<'a> {
    Base, Authority, Event(u64), OperationPosition(OperationId), EventBuffer, TerminalEffectBuffer,
    EffectAction(EffectId), EffectStatus(EffectId), EffectAttempts(EffectId),
    ExtensionState(&'a str), ExtensionConfiguration(&'a ExtensionDependency),
    ActiveExtensions, ActiveConfigurations, Fork(&'a Authority), Merge(&'a str, &'a [u8]),
    ConversationMessage(u64), ContextCheckpoint,
    InteractionTicket(uuid::Uuid), InteractionResolution(uuid::Uuid),
}
impl NativeComponentRef<'_> {
    fn matches(self, key: &NativeComponentKey) -> bool {
        match (self,key) {
            (Self::Base, NativeComponentKey::Base) | (Self::Authority, NativeComponentKey::Authority)
            | (Self::EventBuffer, NativeComponentKey::EventBuffer)
            | (Self::TerminalEffectBuffer, NativeComponentKey::TerminalEffectBuffer)
            | (Self::ActiveExtensions, NativeComponentKey::ActiveExtensions)
            | (Self::ActiveConfigurations, NativeComponentKey::ActiveConfigurations)
            | (Self::ContextCheckpoint, NativeComponentKey::ContextCheckpoint) => true,
            (Self::Event(a),NativeComponentKey::Event(b))
            | (Self::ConversationMessage(a),NativeComponentKey::ConversationMessage(b)) => a == *b,
            (Self::OperationPosition(a),NativeComponentKey::OperationPosition(b)) => a == *b,
            (Self::EffectAction(a),NativeComponentKey::EffectAction(b))
            | (Self::EffectStatus(a),NativeComponentKey::EffectStatus(b))
            | (Self::EffectAttempts(a),NativeComponentKey::EffectAttempts(b)) => a == *b,
            (Self::ExtensionState(a),NativeComponentKey::ExtensionState(b)) => a == b,
            (Self::ExtensionConfiguration(a),NativeComponentKey::ExtensionConfiguration(b)) => a == b,
            (Self::Fork(a),NativeComponentKey::Fork(b)) => a == b,
            (Self::Merge(a,b),NativeComponentKey::Merge(c,d)) => a == c && b == d,
            (Self::InteractionTicket(a),NativeComponentKey::InteractionTicket(b))
            | (Self::InteractionResolution(a),NativeComponentKey::InteractionResolution(b)) => a == *b,
            _ => false,
        }
    }
    fn copy(self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<NativeComponentKey> {
        Ok(match self {
            Self::Base => NativeComponentKey::Base,
            Self::Authority => NativeComponentKey::Authority,
            Self::Event(v) => NativeComponentKey::Event(v),
            Self::OperationPosition(v) => NativeComponentKey::OperationPosition(v),
            Self::EventBuffer => NativeComponentKey::EventBuffer,
            Self::TerminalEffectBuffer => NativeComponentKey::TerminalEffectBuffer,
            Self::EffectAction(v) => NativeComponentKey::EffectAction(v),
            Self::EffectStatus(v) => NativeComponentKey::EffectStatus(v),
            Self::EffectAttempts(v) => NativeComponentKey::EffectAttempts(v),
            Self::ExtensionState(v) => NativeComponentKey::ExtensionState(copy_text(v, quota)?),
            Self::ExtensionConfiguration(v) => NativeComponentKey::ExtensionConfiguration(copy_dependency(v, quota)?),
            Self::ActiveExtensions => NativeComponentKey::ActiveExtensions,
            Self::ActiveConfigurations => NativeComponentKey::ActiveConfigurations,
            Self::Fork(v) => NativeComponentKey::Fork(copy_authority(v, quota)?),
            Self::Merge(a,b) => NativeComponentKey::Merge(copy_text(a, quota)?, copy_copy_vector(b, quota)?),
            Self::ConversationMessage(v) => NativeComponentKey::ConversationMessage(v),
            Self::ContextCheckpoint => NativeComponentKey::ContextCheckpoint,
            Self::InteractionTicket(v) => NativeComponentKey::InteractionTicket(v),
            Self::InteractionResolution(v) => NativeComponentKey::InteractionResolution(v),
        })
    }
}

#[derive(Default)]
pub(super) struct NativeComponentCustody {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    components: Vec<Component>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    buffer: Option<acyclic_fs::NativeAllocation>,
}
impl std::fmt::Debug for NativeComponentCustody {
    fn fmt(&self,f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeComponentCustody").finish_non_exhaustive()
    }
}
impl PartialEq for NativeComponentCustody { fn eq(&self, _: &Self) -> bool { true } }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type Component = (NativeComponentKey, acyclic_fs::NativeAllocation, Option<acyclic_fs::NativeAllocation>);
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type Component = ();

impl NativeComponentCustody {
    pub(super) fn produce<T>(
        &mut self, key: NativeComponentRef<'_>, work: &mut Option<&mut dyn HistoryIo>,
        copy: impl FnOnce(&mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
    ) -> Result<(T, Option<Component>)> {
        let retired = self.install(Self::prepare(key, 0, work)?, work)?;
        let native = work.as_deref().is_some_and(is_native);
        let mut portable = None;
        let result = (|| {
            let mut quota = crate::wire_codec::CodecQuota::new(if native { work } else { &mut portable }, 0)?;
            let value = copy(&mut quota)?;
            let produced = quota.retained();
            quota.transfer();
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native && let Err(error) = self.absorb_produced(
                key, produced, quota.work().as_deref_mut().expect("component producing observer"),
            ) {
                drop(value);
                return Err(error);
            }
            let _ = produced;
            Ok(value)
        })();
        match result {
            Ok(value) => Ok((value, retired)),
            Err(error) => { drop(self.rollback(key, retired)); Err(error) }
        }
    }

    pub(super) fn install(&mut self, component: Option<Component>, work: &mut Option<&mut dyn HistoryIo>) -> Result<Option<Component>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(component) = component {
            if let Some(index) = self.components.iter().position(|(key, _, _)| key == &component.0) {
                return Ok(Some(std::mem::replace(&mut self.components[index], component)));
            }
            let maximum = self.components.capacity().checked_mul(2).unwrap_or(usize::MAX).max(2);
            if let Err(error) = if self.components.len() == self.components.capacity() { self.reserve(maximum, work) } else { Ok(()) } {
                let (key, allocation, key_allocation) = component;
                drop(key);
                drop(key_allocation);
                return Err(match error {
                    Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(allocation) },
                    error => error,
                });
            }
            self.components.push(component);
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = (component, work);
        Ok(None)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reserve(&mut self, maximum: usize, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        if maximum <= self.components.capacity() { return Ok(()); }
        let copied = Self::metadata_bytes(self.components.len())?;
        work.as_deref().expect("native table observer").admit_copy(copied)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let buffer;
        let mut replacement = reserve_copy_vector::<Component>(maximum, &mut quota)?;
        let produced = quota.retained();
        buffer = quota.work().as_deref_mut().expect("native table issuer").take_native_custody(produced)?;
        quota.transfer();
        quota.work().as_deref_mut().expect("native table observer").copy_bytes(copied)?;
        let old = self.buffer.replace(buffer);
        replacement.append(&mut self.components);
        drop(std::mem::replace(&mut self.components, replacement));
        drop(old);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn absorb_produced(
        &mut self, key: NativeComponentRef<'_>, bytes: u64, io: &mut dyn HistoryIo,
    ) -> Result<()> {
        let mut incoming = io.take_native_custody(bytes)?;
        let (_, allocation, _) = self.components.iter_mut().find(|(current, _, _)| key.matches(current))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "native component copy destination is absent",
            })?;
        allocation.absorb(&mut incoming).map_err(|cause| Error::NativeWork {
            cause, custody: Some(incoming),
        })
    }
    pub(super) fn metadata_bytes(count: usize) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let slot = std::mem::size_of::<Component>();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let slot = std::mem::size_of::<NativeComponentKey>();
        count.checked_mul(slot).map(|bytes| bytes as u64)
            .ok_or_else(|| Error::Invalid("native component metadata overflows".into()))
    }
    pub(super) fn prepare(key: NativeComponentRef<'_>, payload: u64,
        work: &mut Option<&mut dyn HistoryIo>) -> Result<Option<Component>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut() {
            if io.original_native_work().is_some() {
                let mut work = Some(&mut *io as &mut dyn HistoryIo);
                let mut quota = crate::wire_codec::CodecQuota::new(&mut work, payload)?;
                let key_allocation;
                let allocation;
                let owned_key = key.copy(&mut quota)?;
                let produced_key_bytes = quota.retained() - payload;
                quota.transfer();
                let io = quota.work().as_deref_mut().expect("native component issuer");
                key_allocation = io.take_native_custody(produced_key_bytes)?;
                allocation = io.take_native_custody(payload)?;
                return Ok(Some((owned_key, allocation, Some(key_allocation))));
            }
        }
        let _ = (key,payload,work);
        Ok(None)
    }
    pub(super) fn remove(&mut self,key: &NativeComponentKey) {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(index) = self.components.iter().position(|(current,_,_)| current == key) {
            let (key,allocation,key_allocation) = self.components.swap_remove(index);
            drop(key);
            drop(allocation);
            drop(key_allocation);
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = key;
    }
    pub(super) fn rollback(&mut self, key: NativeComponentRef<'_>, retired: Option<Component>) -> Option<Component> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(index) = self.components.iter().position(|(current, _, _)| key.matches(current)) {
            return Some(match retired {
                Some(retired) => std::mem::replace(&mut self.components[index], retired),
                None => self.components.swap_remove(index),
            });
        }
        let _ = (key, retired);
        None
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn transfer_effect(&mut self, id: EffectId, io: &mut dyn HistoryIo) -> Result<u64> {
        let mut bytes = 0_u64;
        for key in [NativeComponentKey::EffectAction(id), NativeComponentKey::EffectStatus(id),
            NativeComponentKey::EffectAttempts(id)] {
            if let Some(index) = self.components.iter().position(|(current,_,_)| current == &key) {
                let (source_key,allocation,key_allocation) = self.components.swap_remove(index);
                drop(source_key);
                drop(key_allocation);
                bytes = bytes.checked_add(allocation.bytes())
                    .ok_or_else(|| Error::Invalid("effect custody transfer overflows".into()))?;
                io.absorb_native_custody(allocation)?;
            }
        }
        Ok(bytes)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn original_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        self.components.first().map(|(_,allocation,_)| allocation.work())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn from_reducer(
        reducer: &Reducer, data: &mut acyclic_fs::NativeAllocation,
        metadata: &mut acyclic_fs::NativeAllocation, holder: &mut Self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let mut count = holder.components.len();
        Self::visit(reducer, |_,_| {
            count = count.checked_add(1)
                .ok_or_else(|| Error::Invalid("native component count overflows".into()))?;
            Ok(())
        })?;
        if count > holder.components.capacity() {
            holder.reserve(count, work)?;
        }
        Self::visit(reducer, |key,bytes| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut key_allocation;
            let owned_key = key.copy(&mut quota)?;
            let produced = quota.retained();
            key_allocation = quota.work().as_deref_mut().expect("cold component issuer")
                .take_native_custody(produced)?;
            quota.transfer();
            let inline = match key {
                NativeComponentRef::ExtensionConfiguration(_) =>
                    std::mem::size_of::<(ExtensionDependency, ExtensionConfiguration)>() as u64,
                NativeComponentRef::Fork(_) =>
                    std::mem::size_of::<(Authority, ForkSeed)>() as u64,
                _ => 0,
            };
            let payload = bytes.checked_sub(inline).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "cold component inline slots exceed payload",
            })?;
            if inline != 0 {
                let mut entry = metadata.split_off(inline)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                key_allocation.absorb(&mut entry)
                    .map_err(|cause| Error::NativeWork { cause, custody: Some(entry) })?;
            }
            let split = if matches!(key,NativeComponentRef::OperationPosition(_)) {
                metadata.split_off(bytes)
            } else { data.split_off(payload) };
            let claim = match split {
                Ok(claim) => claim,
                Err(cause) => {
                    drop(owned_key);
                    return Err(Error::NativeWork { cause, custody: Some(key_allocation) });
                }
            };
            holder.components.push((owned_key, claim, Some(key_allocation)));
            Ok(())
        })?;
        Ok(())
    }
    fn visit(reducer: &Reducer, mut f: impl FnMut(NativeComponentRef<'_>,u64)->Result<()>) -> Result<()> {
        use NativeComponentRef as K;
        f(K::Authority, reducer.authority.id.capacity() as u64)?;
        for event in &reducer.events { f(K::Event(event.revision), event_owned_bytes(event)?)?; }
        for operation in reducer.operation_positions.keys() {
            f(K::OperationPosition(*operation), std::mem::size_of::<(OperationId, u64)>() as u64)?;
        }
        f(K::EventBuffer, owned_buffer::<Event>(reducer.events.capacity())?)?;
        f(K::TerminalEffectBuffer, owned_buffer::<EffectId>(reducer.indexed_terminal_effects.capacity())?)?;
        for (id, effect) in &reducer.effects {
            f(K::EffectAction(*id), owned_sum([std::mem::size_of::<(EffectId, EffectState)>() as u64,
                owned_effect_action(effect)?])?)?;
            f(K::EffectStatus(*id), owned_status(&effect.status)?)?;
            f(K::EffectAttempts(*id), owned_buffer::<EffectAttemptId>(effect.attempts.capacity())?)?;
        }
        for (key, (reference, record)) in &reducer.extension_states {
            f(K::ExtensionState(key), owned_sum([
                std::mem::size_of::<(String, (EventReference, ExtensionRecord))>() as u64,
                key.capacity() as u64, reference.authority.id.capacity() as u64, owned_record(record)?])?)?;
        }
        for (key, record) in &reducer.configured_extensions {
            f(K::ExtensionConfiguration(key), owned_sum([
                std::mem::size_of::<(ExtensionDependency, ExtensionConfiguration)>() as u64,
                key.name.capacity() as u64, owned_configuration(record)?])?)?;
        }
        f(K::ActiveExtensions, owned_vector(&reducer.active_extensions,
            |extension| Ok(extension.name.capacity() as u64))?)?;
        f(K::ActiveConfigurations, owned_vector(&reducer.active_configurations, owned_configuration)?)?;
        for (key, seed) in &reducer.forks {
            f(K::Fork(key), owned_sum([std::mem::size_of::<(Authority, ForkSeed)>() as u64,
                key.id.capacity() as u64, owned_seed(seed)?])?)?;
        }
        for (name, operation) in &reducer.published_merges {
            f(K::Merge(name, operation), owned_sum([std::mem::size_of::<(String, Vec<u8>)>() as u64,
                name.capacity() as u64, operation.capacity() as u64])?)?;
        }
        for message in &reducer.conversation.messages {
            f(K::ConversationMessage(message.sequence), owned_message(message)?)?;
        }
        if let Some(checkpoint) = &reducer.latest_context_checkpoint {
            f(K::ContextCheckpoint, checkpoint.logical_owned_bytes()?)?;
        }
        for (id, (ticket, resolution)) in &reducer.interactions {
            f(K::InteractionTicket(*id), owned_sum([
                std::mem::size_of::<(uuid::Uuid, (InteractionTicket, Option<InteractionResolution>))>() as u64,
                owned_ticket(ticket)?])?)?;
            if let Some(resolution) = resolution {
                f(K::InteractionResolution(*id), owned_resolution(resolution)?)?;
            }
        }
        Ok(())
    }
}

pub(super) fn is_native(io: &dyn HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { io.original_native_work().is_some() }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = io; false }
}

pub(super) fn admit_buffer_allocation(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
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

pub(super) fn begin_buffer_allocation(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn admission_from_owned_value(
    value: Value, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(ExtensionAdmission, u64)> {
    fn invalid() -> Error {
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "invalid owned extension admission field",
        }
    }
    fn object(value: Value, count: usize) -> Result<serde_json::Map<String,Value>> {
        let Value::Object(fields) = value else { return Err(invalid()) };
        if fields.len() != count { return Err(invalid()); }
        Ok(fields)
    }
    fn field(fields: &mut serde_json::Map<String,Value>, name: &str) -> Result<Value> {
        fields.remove(name).ok_or_else(invalid)
    }
    fn text(value: Value) -> Result<String> {
        let Value::String(text) = value else { return Err(invalid()) };
        Ok(text)
    }
    fn number(value: Value) -> Result<u64> {
        value.as_u64().ok_or_else(invalid)
    }
    fn dependency(value: Value, _work: &mut Option<&mut dyn HistoryIo>) -> Result<ExtensionDependency> {
        let mut fields = object(value, 2)?;
        let name = text(field(&mut fields, "name")?)?;
        let version = u32::try_from(number(field(&mut fields, "version")?)?).map_err(|_| invalid())?;
        Ok(ExtensionDependency { name, version })
    }
    fn configuration(value: Value, work: &mut Option<&mut dyn HistoryIo>) -> Result<ExtensionConfiguration> {
        let mut fields = object(value, 3)?;
        let extension = dependency(field(&mut fields, "extension")?, work)?;
        let Value::Array(bytes) = field(&mut fields, "schema_digest")? else { return Err(invalid()) };
        if bytes.len() != 32 { return Err(invalid()); }
        let mut schema_digest = [0_u8;32];
        for (output, value) in schema_digest.iter_mut().zip(bytes) {
            *output = u8::try_from(number(value)?).map_err(|_| invalid())?;
        }
        let content = FileRef::from_value(field(&mut fields, "content")?, work)?;
        Ok(ExtensionConfiguration { extension, schema_digest, content })
    }
    fn vector<T>(
        value: Value, work: &mut Option<&mut dyn HistoryIo>,
        mut decode: impl FnMut(Value, &mut Option<&mut dyn HistoryIo>) -> Result<T>,
    ) -> Result<(Vec<T>, u64)> {
        let Value::Array(values) = value else { return Err(invalid()) };
        let count = values.len();
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut output = reserve_copy_vector(count, &mut quota)?;
        for value in values { output.push(decode(value, quota.work())?); }
        let added = quota.retained();
        quota.transfer();
        Ok((output, added))
    }
    work.as_deref().expect("original owned admission observer").check_cancellation()?;
    let mut fields = object(value, 3)?;
    let mut source = object(field(&mut fields, "source")?, 2)?;
    let mut authority = object(field(&mut source, "authority")?, 2)?;
    let kind = match text(field(&mut authority, "kind")?)?.as_str() {
        "agent" => AggregateKind::Agent, "conversation" => AggregateKind::Conversation,
        "session" => AggregateKind::Session, "turn" => AggregateKind::Turn,
        "task" => AggregateKind::Task, _ => return Err(invalid()),
    };
    let source = EventReference {
        authority: Authority { kind, id: text(field(&mut authority, "id")?)? },
        revision: number(field(&mut source, "revision")?)?,
    };
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let admission = (|| -> Result<ExtensionAdmission> {
        let (selected, selected_bytes) = vector(field(&mut fields, "selected")?, quota.work(), dependency)?;
        quota.adopt(selected_bytes)?;
        let (configurations, configuration_bytes) = vector(field(&mut fields, "configurations")?, quota.work(), configuration)?;
        quota.adopt(configuration_bytes)?;
        ExtensionAdmission::from_parts_with_work(source, selected, configurations, quota.work())
    })()?;
    let added = quota.retained();
    quota.transfer();
    Ok((admission, added))
}
