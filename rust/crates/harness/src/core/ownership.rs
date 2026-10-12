//! Heap capacities retained by the reducer.
use super::*;
pub(crate) fn owned_sum(parts: impl IntoIterator<Item = u64>) -> Result<u64> {
    parts.into_iter().try_fold(0_u64, |sum, bytes| {
        sum.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("component ownership overflows".into()))
    })
}
pub(crate) fn owned_buffer<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .map(|bytes| bytes as u64)
        .ok_or_else(|| Error::Invalid("component buffer ownership overflows".into()))
}
pub(crate) fn owned_vector<T>(
    values: &Vec<T>,
    mut item: impl FnMut(&T) -> Result<u64>,
) -> Result<u64> {
    values
        .iter()
        .try_fold(owned_buffer::<T>(values.capacity())?, |sum, value| {
            owned_sum([sum, item(value)?])
        })
}
pub(in crate::core) fn owned_record(record: &ExtensionRecord) -> Result<u64> {
    owned_sum([
        record.name.capacity() as u64,
        record.content.logical_owned_bytes()?,
    ])
}
pub(in crate::core) fn owned_configuration(record: &ExtensionConfiguration) -> Result<u64> {
    owned_sum([
        record.extension.name.capacity() as u64,
        record.content.logical_owned_bytes()?,
    ])
}
pub(in crate::core) fn owned_status(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        _ => Ok(0),
    }
}
pub(in crate::core) fn owned_effect_action(effect: &EffectState) -> Result<u64> {
    owned_sum([
        effect.provider.capacity() as u64,
        effect.effect_kind.capacity() as u64,
        effect.request.logical_owned_bytes()?,
        effect.result_schema.logical_owned_bytes()?,
    ])
}
pub(in crate::core) fn owned_effect(effect: &EffectState) -> Result<u64> {
    owned_sum([
        owned_effect_action(effect)?,
        owned_status(&effect.status)?,
        owned_buffer::<EffectAttemptId>(effect.attempts.capacity())?,
    ])
}
pub(in crate::core) fn owned_ticket(ticket: &InteractionTicket) -> Result<u64> {
    ticket.request.logical_owned_bytes()
}
pub(in crate::core) fn owned_resolution(resolution: &InteractionResolution) -> Result<u64> {
    let answer = match &resolution.outcome {
        InteractionOutcome::Answered { answer } => owned_sum([
            std::mem::size_of::<FileRef>() as u64,
            answer.logical_owned_bytes()?,
        ])?,
        _ => 0,
    };
    owned_sum([
        answer,
        resolution
            .detail
            .as_ref()
            .map(FileRef::logical_owned_bytes)
            .transpose()?
            .unwrap_or(0),
    ])
}
pub(in crate::core) fn owned_message(message: &ConversationMessage) -> Result<u64> {
    let attachments = match &message.attachments {
        crate::conversation::ReferencedAttachments::Inline { items } => {
            owned_vector(items, |item| {
                owned_sum([
                    item.file.logical_owned_bytes()?,
                    item.label.as_ref().map_or(0, |text| text.capacity() as u64),
                ])
            })?
        }
        crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
            manifest.logical_owned_bytes()?
        }
    };
    let mut bytes = owned_sum([
        message.content.logical_owned_bytes()?,
        attachments,
        message
            .tool_call_id
            .as_ref()
            .map_or(0, |text| text.capacity() as u64),
    ])?;
    for (key, file) in &message.extensions {
        bytes = owned_sum([
            bytes,
            std::mem::size_of::<(String, FileRef)>() as u64,
            key.capacity() as u64,
            file.logical_owned_bytes()?,
        ])?;
    }
    Ok(bytes)
}
pub(crate) fn owned_revision(revision: &crate::fork::ResourceRevision) -> Result<u64> {
    use crate::fork::ResourceRevision as R;
    match revision {
        R::History(reference) => reference.as_resource().logical_owned_bytes(),
        R::Context(reference) => reference.as_resource().logical_owned_bytes(),
        R::Process(reference) => reference.as_resource().logical_owned_bytes(),
        R::Artifact(reference) => reference.as_resource().logical_owned_bytes(),
        R::SharedVolume(volume) => volume.logical_owned_bytes(),
        R::Project { volume, generation } => owned_sum([
            volume.logical_owned_bytes()?,
            generation.as_resource().logical_owned_bytes()?,
        ]),
        R::PrivateVolume {
            volume,
            generation,
            paths,
        } => owned_sum([
            volume.logical_owned_bytes()?,
            generation.as_resource().logical_owned_bytes()?,
            owned_vector(paths, |path| Ok(path.capacity() as u64))?,
        ]),
        R::Extension {
            name, reference, ..
        } => owned_sum([name.capacity() as u64, reference.logical_owned_bytes()?]),
    }
}
pub(crate) fn owned_capture(capture: &crate::fork::Capture) -> Result<u64> {
    match capture {
        crate::fork::Capture::Captured(resource) => owned_sum([
            owned_revision(&resource.source)?,
            owned_revision(&resource.revision)?,
        ]),
        crate::fork::Capture::Unsupported(reason) => Ok(reason.capacity() as u64),
        _ => Ok(0),
    }
}
pub(crate) fn owned_seed(seed: &ForkSeed) -> Result<u64> {
    let mut bytes = owned_sum([
        seed.parent.id.capacity() as u64,
        seed.child.id.capacity() as u64,
        seed.authorized_source
            .as_ref()
            .map(|source| source.logical_owned_bytes())
            .transpose()?
            .unwrap_or(0),
        owned_buffer::<AgentId>(seed.attached_agents.capacity())?,
        owned_vector(&seed.resources, |resource| {
            owned_sum([
                owned_revision(&resource.source)?,
                owned_revision(&resource.revision)?,
            ])
        })?,
        owned_vector(&seed.omissions, |omission| {
            owned_sum([
                owned_revision(&omission.selection.revision)?,
                owned_capture(&omission.outcome)?,
            ])
        })?,
        seed.child_private_volume.logical_owned_bytes()?,
        seed.child_private_generation
            .as_resource()
            .logical_owned_bytes()?,
        seed.child_result_volume
            .as_ref()
            .map(VolumeRef::logical_owned_bytes)
            .transpose()?
            .unwrap_or(0),
        seed.child_result_generation
            .as_ref()
            .map(|generation| generation.as_resource().logical_owned_bytes())
            .transpose()?
            .unwrap_or(0),
        owned_vector(&seed.inherited_context, FileRef::logical_owned_bytes)?,
        owned_vector(&seed.attachment_manifests, FileRef::logical_owned_bytes)?,
        owned_vector(&seed.shared_grants, |grant| {
            owned_sum([
                grant.volume.logical_owned_bytes()?,
                owned_buffer::<VolumeOperation>(grant.operations.len())?,
            ])
        })?,
        owned_vector(&seed.reference_grants, |grant| {
            owned_sum([
                grant.file.logical_owned_bytes()?,
                grant
                    .attachment_manifest
                    .as_ref()
                    .map(FileRef::logical_owned_bytes)
                    .transpose()?
                    .unwrap_or(0),
            ])
        })?,
    ])?;
    if let Some(summary) = &seed.summary {
        bytes = owned_sum([
            bytes,
            summary.selection.checkpoint.logical_owned_bytes()?,
            summary.context.logical_owned_bytes()?,
            owned_vector(&summary.payloads, FileRef::logical_owned_bytes)?,
            owned_vector(&summary.references, FileRef::logical_owned_bytes)?,
        ])?;
    }
    if let Some(boundary) = &seed.boundary {
        bytes = owned_sum([
            bytes,
            boundary.provider.logical_owned_bytes()?,
            boundary.evidence.capacity() as u64,
        ])?;
    }
    Ok(bytes)
}
pub(in crate::core) fn owned_receipt(receipt: &ProjectMergeReceipt) -> Result<u64> {
    owned_sum([
        receipt.child.id.capacity() as u64,
        receipt.source_project.logical_owned_bytes()?,
        receipt.target_project.logical_owned_bytes()?,
        receipt
            .source_generation
            .as_resource()
            .logical_owned_bytes()?,
        receipt
            .expected_target_generation
            .as_resource()
            .logical_owned_bytes()?,
        receipt
            .result_generation
            .as_resource()
            .logical_owned_bytes()?,
        receipt.provider_operation_id.capacity() as u64,
        receipt.provider_proof.provider.logical_owned_bytes()?,
        receipt.provider_proof.format.capacity() as u64,
        json_value_logical_owned_bytes(&receipt.provider_proof.statement)?,
        owned_message(&receipt.notice)?,
    ])
}
pub(in crate::core) fn owned_payload(payload: &EventPayload) -> Result<u64> {
    match payload {
        EventPayload::LifecycleTransitioned { reason, .. } => {
            Ok(reason.as_ref().map_or(0, |text| text.capacity() as u64))
        }
        EventPayload::Custom { record } => owned_record(record),
        EventPayload::ExtensionStateMigrated { migration } => owned_sum([
            migration.previous.authority.id.capacity() as u64,
            owned_record(&migration.record)?,
        ]),
        EventPayload::ExtensionsSelected {
            previous,
            selected,
            configurations,
        } => owned_sum([
            owned_vector(previous, |extension| Ok(extension.name.capacity() as u64))?,
            owned_vector(selected, |extension| Ok(extension.name.capacity() as u64))?,
            owned_vector(configurations, owned_configuration)?,
        ]),
        EventPayload::ExtensionConfigured { previous, record } => owned_sum([
            previous
                .as_ref()
                .map(owned_configuration)
                .transpose()?
                .unwrap_or(0),
            owned_configuration(record)?,
        ]),
        EventPayload::EffectPlanned {
            provider,
            effect_kind,
            request,
            result_schema,
            ..
        } => owned_sum([
            provider.capacity() as u64,
            effect_kind.capacity() as u64,
            request.logical_owned_bytes()?,
            result_schema.logical_owned_bytes()?,
        ]),
        EventPayload::EffectResolved { observation } => owned_sum([
            observation.provider.capacity() as u64,
            owned_status(&observation.status)?,
        ]),
        EventPayload::ForkPublished { seed } => {
            owned_sum([std::mem::size_of::<ForkSeed>() as u64, owned_seed(seed)?])
        }
        EventPayload::ProjectMergePublished { receipt } => owned_sum([
            std::mem::size_of::<ProjectMergeReceipt>() as u64,
            owned_receipt(receipt)?,
        ]),
        EventPayload::ConversationMessageAppended { message } => owned_sum([
            std::mem::size_of::<ConversationMessage>() as u64,
            owned_message(message)?,
        ]),
        EventPayload::ModelContextSelected { selection } => owned_sum([
            owned_buffer::<uuid::Uuid>(selection.message_ids.capacity())?,
            selection
                .checkpoint
                .as_ref()
                .map(FileRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
        ]),
        EventPayload::InteractionOpened { ticket } => ticket.request.logical_owned_bytes(),
        EventPayload::InteractionResolved { resolution } => owned_resolution(resolution),
        EventPayload::EffectDispatched { .. } | EventPayload::ConversationBound { .. } => Ok(0),
    }
}
/// Actual existing payload capacities and known inline entry slots; used only to reconcile the original finite decoder and explicit cold restoration.
pub(crate) fn event_owned_bytes(event: &Event) -> Result<u64> {
    owned_sum([
        event.scope.logical_owned_bytes()?,
        event
            .causal_parent
            .as_ref()
            .map_or(0, |reference| reference.authority.id.capacity() as u64),
        owned_payload(&event.payload)?,
    ])
}

impl NativeComponentCustody {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn from_reducer(
        reducer: &Reducer,
        data: &mut acyclic_fs::NativeAllocation,
        holder: &mut Self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let mut count = holder.components.len();
        reducer.projection.visit(
            &reducer.authority,
            reducer.events.iter(),
            reducer.events.capacity(),
            |_, _| {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("native component count overflows".into()))?;
                Ok(())
            },
        )?;
        if count > holder.components.capacity() {
            holder.reserve(count, work)?;
        }
        reducer.projection.visit(
            &reducer.authority,
            reducer.events.iter(),
            reducer.events.capacity(),
            |key, bytes| {
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let key_allocation;
                let owned_key = key.copy(&mut quota)?;
                let produced = quota.retained();
                key_allocation = quota
                    .work()
                    .as_deref_mut()
                    .expect("cold component issuer")
                    .take_native_custody(produced)?;
                quota.transfer();
                let claim = match data.split_off(bytes) {
                    Ok(claim) => claim,
                    Err(cause) => {
                        drop(owned_key);
                        return Err(Error::NativeWork {
                            cause,
                            custody: Some(key_allocation),
                        });
                    }
                };
                holder.components.push((owned_key, claim, key_allocation));
                Ok(())
            },
        )?;
        Ok(())
    }
}

impl<B> snapshot::Projection<ConversationState, B> {
    pub(in crate::core) fn visit<'a>(
        &self,
        authority: &Authority,
        events: impl Iterator<Item = &'a Event>,
        event_capacity: usize,
        mut f: impl FnMut(NativeComponentKey<'_>, u64) -> Result<()>,
    ) -> Result<()> {
        use NativeComponentKey as K;
        f(K::Authority, authority.id.capacity() as u64)?;
        for event in events {
            f(K::Event(event.revision), event_owned_bytes(event)?)?;
        }
        f(K::EventBuffer, owned_buffer::<Event>(event_capacity)?)?;
        f(
            K::TerminalEffectBuffer,
            owned_buffer::<EffectId>(self.indexed_terminal_effects.capacity())?,
        )?;
        for (id, effect) in &self.effects {
            f(
                K::EffectAction(*id),
                owned_sum([
                    std::mem::size_of::<(EffectId, EffectState)>() as u64,
                    owned_effect_action(effect)?,
                ])?,
            )?;
            f(K::EffectStatus(*id), owned_status(&effect.status)?)?;
            f(
                K::EffectAttempts(*id),
                owned_buffer::<EffectAttemptId>(effect.attempts.capacity())?,
            )?;
        }
        for (key, (reference, record)) in &self.extension_states {
            f(
                K::ExtensionState(Cow::Borrowed(key)),
                owned_sum([
                    std::mem::size_of::<(String, (EventReference, ExtensionRecord))>() as u64,
                    key.capacity() as u64,
                    reference.authority.id.capacity() as u64,
                    owned_record(record)?,
                ])?,
            )?;
        }
        for (key, record) in &self.configured_extensions {
            f(
                K::ExtensionConfiguration(Cow::Borrowed(key)),
                owned_sum([
                    std::mem::size_of::<(ExtensionDependency, ExtensionConfiguration)>() as u64,
                    key.name.capacity() as u64,
                    owned_configuration(record)?,
                ])?,
            )?;
        }
        f(
            K::ActiveSelection,
            owned_sum([
                owned_vector(&self.active_extensions, |extension| {
                    Ok(extension.name.capacity() as u64)
                })?,
                owned_vector(&self.active_configurations, owned_configuration)?,
            ])?,
        )?;
        for (key, seed) in &self.forks {
            f(
                K::Fork(Cow::Borrowed(key)),
                owned_sum([
                    std::mem::size_of::<(Authority, ForkSeed)>() as u64,
                    key.id.capacity() as u64,
                    owned_seed(seed)?,
                ])?,
            )?;
        }
        for (name, operation) in &self.published_merges {
            f(
                K::Merge(Cow::Borrowed(name), Cow::Borrowed(operation)),
                owned_sum([
                    std::mem::size_of::<(String, Vec<u8>)>() as u64,
                    name.capacity() as u64,
                    operation.capacity() as u64,
                ])?,
            )?;
        }
        for message in &self.conversation.messages {
            f(
                K::ConversationMessage(message.sequence),
                owned_message(message)?,
            )?;
        }
        if let Some(checkpoint) = &self.latest_context_checkpoint {
            f(K::ContextCheckpoint, checkpoint.logical_owned_bytes()?)?;
        }
        for (id, (ticket, resolution)) in &self.interactions {
            f(
                K::InteractionTicket(*id),
                owned_sum([
                    std::mem::size_of::<(
                        uuid::Uuid,
                        (InteractionTicket, Option<InteractionResolution>),
                    )>() as u64,
                    owned_ticket(ticket)?,
                ])?,
            )?;
            if let Some(resolution) = resolution {
                f(K::InteractionResolution(*id), owned_resolution(resolution)?)?;
            }
        }
        Ok(())
    }
}
