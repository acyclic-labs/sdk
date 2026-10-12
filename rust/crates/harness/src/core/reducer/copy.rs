//! Copying the live reducer projection.
use super::*;
pub(in crate::core) fn copy_reducer(
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
            let (value, retired) = output.native_custody.produce(&$key, quota.work(), $copy)?;
            drop(retired);
            value
        }};
    }
    (output.active_extensions, output.active_configurations) = component!(NativeComponentKey::ActiveSelection,
        |quota| {
            let selected = copy_vector(&source.active_extensions, quota, copy_dependency)?;
            let configurations = copy_vector(&source.active_configurations, quota, copy_configuration)?;
            Ok((selected, configurations))
        });
    for (name, (reference, record)) in &source.extension_states {
        let entry = component!(NativeComponentKey::ExtensionState(Cow::Borrowed(name)), |quota| {
            retain_entry::<(String, (EventReference, ExtensionRecord))>(quota)?;
            Ok((copy_text(name, quota)?, (copy_reference(reference, quota)?, copy_extension_record(record, quota)?)))
        });
        output.extension_states.insert(entry.0, entry.1);
    }
    for (extension, record) in &source.configured_extensions {
        let entry = component!(NativeComponentKey::ExtensionConfiguration(Cow::Borrowed(extension)), |quota| {
            retain_entry::<(ExtensionDependency, ExtensionConfiguration)>(quota)?;
            Ok((copy_dependency(extension, quota)?, copy_configuration(record, quota)?))
        });
        output.configured_extensions.insert(entry.0, entry.1);
    }
    let events = component!(NativeComponentKey::EventBuffer,
        |quota| reserve_copy_vector::<Event>(source.events.len(), quota));
    output.events = VecDeque::from(events);
    for event in &source.events {
        let copied = component!(NativeComponentKey::Event(event.revision), |quota| copy_event(event, quota));
        output.events.push_back(copied);
    }
    for (id, effect) in &source.effects {
        let mut copied = component!(NativeComponentKey::EffectAction(*id), |quota| {
            retain_entry::<(EffectId, EffectState)>(quota)?;
            Ok(EffectState {
                provider: copy_text(&effect.provider, quota)?, guarantee: effect.guarantee,
                effect_kind: copy_text(&effect.effect_kind, quota)?, request: copy_file(&effect.request, quota)?,
                request_digest: effect.request_digest, result_schema: copy_file(&effect.result_schema, quota)?,
                result_schema_digest: effect.result_schema_digest, status: EffectStatus::Planned, attempts: Vec::new(),
            })
        });
        copied.status = component!(NativeComponentKey::EffectStatus(*id), |quota| copy_status(&effect.status, quota));
        copied.attempts = component!(NativeComponentKey::EffectAttempts(*id), |quota| copy_copy_vector(&effect.attempts, quota));
        output.effects.insert(*id, copied);
    }
    output.indexed_terminal_effects = VecDeque::from(component!(NativeComponentKey::TerminalEffectBuffer,
        |quota| copy_iter(source.indexed_terminal_effects.len(), source.indexed_terminal_effects.iter(), quota,
            |id, quota| {
                if let Some(io) = quota.work().as_deref_mut() {
                    io.copy_bytes(std::mem::size_of::<EffectId>() as u64)?;
                }
                Ok(*id)
            })));
    for (authority, seed) in &source.forks {
        let entry = component!(NativeComponentKey::Fork(Cow::Borrowed(authority)), |quota| {
            retain_entry::<(Authority, ForkSeed)>(quota)?;
            Ok((copy_authority(authority, quota)?, copy_seed(seed, quota)?))
        });
        output.forks.insert(entry.0, entry.1);
    }
    for (name, operation) in &source.published_merges {
        let entry = component!(NativeComponentKey::Merge(Cow::Borrowed(name), Cow::Borrowed(operation)), |quota| {
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
                &NativeComponentKey::ConversationMessage(message.sequence), 0, quota.work(),
            )?, quota.work())?;
            drop(retired);
        }
        let (conversation, remaining) = source.conversation.copy(
            quota.work(), Some(&mut |sequence, produced, work| {
                output.native_custody.absorb_produced(&NativeComponentKey::ConversationMessage(sequence),
                    produced, work.as_deref_mut().expect("message producing observer"))
            }),
        )?;
        output.conversation = conversation;
        if remaining != 0 {
            output.native_custody.absorb_produced(&NativeComponentKey::Base, remaining,
                quota.work().as_deref_mut().expect("conversation remainder observer"))?;
        }
    }
    if let Some(file) = &source.latest_context_checkpoint {
        output.latest_context_checkpoint = Some(component!(NativeComponentKey::ContextCheckpoint,
            |quota| copy_file(file, quota)));
    }
    for (id, (ticket, resolution)) in &source.interactions {
        let ticket = component!(NativeComponentKey::InteractionTicket(*id), |quota| {
            retain_entry::<(uuid::Uuid, (InteractionTicket, Option<InteractionResolution>))>(quota)?;
            copy_ticket(ticket, quota)
        });
        let resolution = resolution.as_ref().map(|resolution| -> Result<InteractionResolution> {
            Ok(component!(NativeComponentKey::InteractionResolution(*id), |quota| copy_resolution(resolution, quota)))
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
