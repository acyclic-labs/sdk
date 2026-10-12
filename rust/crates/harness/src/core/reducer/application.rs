//! reducer application semantics.
use super::*;
impl Reducer {
    #[allow(clippy::too_many_lines, reason = "one arm per event")]
    pub(in crate::core) fn apply_payload_with_work(
        &mut self,
        payload: &EventPayload,
        revision: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match payload {
            EventPayload::LifecycleTransitioned { from, to, .. } => {
                if *from != self.projection.lifecycle {
                    return Err(Error::Conflict("lifecycle predecessor mismatch".into()));
                }
                validate_lifecycle(*from, *to)?;
                self.projection.lifecycle = *to;
            }
            EventPayload::Custom { record } => {
                record.validate()?;
                self.schemas.pinned_binding(
                    &record.name,
                    record.version,
                    Some((
                        record.schema_digest,
                        record.implementation_digest,
                        record.fork_policy,
                    )),
                    work,
                )?;
                if self
                    .projection
                    .extension_states
                    .get(&record.name)
                    .is_some_and(|(_, current)| current.version != record.version)
                {
                    return Err(Error::Conflict(
                        "extension state version change lacks migration".into(),
                    ));
                }
                self.replace_extension_state_with_work(record, revision, work)?;
            }
            EventPayload::ExtensionStateMigrated { migration } => {
                migration.validate()?;
                let ExtensionStateMigration { previous, record } = migration;
                self.schemas.pinned_binding(
                    &record.name,
                    record.version,
                    Some((
                        record.schema_digest,
                        record.implementation_digest,
                        record.fork_policy,
                    )),
                    work,
                )?;
                let (source, current) = self
                    .projection
                    .extension_states
                    .get(&record.name)
                    .ok_or_else(|| Error::NotFound(format!("extension state {}", record.name)))?;
                if source != previous
                    || previous.authority != self.authority
                    || current.version == record.version
                {
                    return Err(Error::Conflict(
                        "extension migration does not match retained state".into(),
                    ));
                }
                self.replace_extension_state_with_work(record, revision, work)?;
            }
            EventPayload::ExtensionsSelected {
                previous,
                selected,
                configurations,
            } => {
                if self.authority.kind != AggregateKind::Agent
                    || !matches!(
                        self.projection.lifecycle,
                        LifecycleState::Pending | LifecycleState::Waiting
                    )
                    || previous != &self.projection.active_extensions
                    || !self.selected_configuration_matches_with_work(
                        selected,
                        configurations,
                        work,
                    )?
                    || selected.iter().any(|extension| {
                        self.projection
                            .extension_states
                            .get(&extension.name)
                            .is_some_and(|(_, state)| state.version != extension.version)
                    })
                {
                    return Err(Error::Conflict(
                        "extension activation does not match pinned agent state".into(),
                    ));
                }
                let (next, _retired) = self.native_custody.produce(
                    &NativeComponentKey::ActiveSelection,
                    work,
                    |quota| {
                        let selected =
                            copying::copy_vector(selected, quota, copying::copy_dependency)?;
                        let configurations = copying::copy_vector(
                            configurations,
                            quota,
                            copying::copy_configuration,
                        )?;
                        Ok((selected, configurations))
                    },
                )?;
                self.projection.active_extension_revision = Some(revision);
                (
                    self.projection.active_extensions,
                    self.projection.active_configurations,
                ) = next;
            }
            EventPayload::ExtensionConfigured { previous, record } => {
                if self.authority.kind != AggregateKind::Agent
                    || !matches!(
                        self.projection.lifecycle,
                        LifecycleState::Pending | LifecycleState::Waiting
                    )
                    || self.projection.configured_extensions.get(&record.extension)
                        != previous.as_ref()
                    || {
                        record.validate()?;
                        self.schemas
                            .pinned_binding(
                                &record.extension.name,
                                record.extension.version,
                                None,
                                work,
                            )?
                            .configuration_schema_digest
                            != Some(record.schema_digest)
                    }
                {
                    return Err(Error::Conflict(
                        "extension configuration does not match pinned agent state".into(),
                    ));
                }
                let (entry, _retired) = self.native_custody.produce(
                    &NativeComponentKey::ExtensionConfiguration(Cow::Borrowed(&record.extension)),
                    work,
                    |quota| {
                        copying::retain_entry::<(ExtensionDependency, ExtensionConfiguration)>(
                            quota,
                        )?;
                        let key = copying::copy_dependency(&record.extension, quota)?;
                        let record = copying::copy_configuration(record, quota)?;
                        Ok((key, record))
                    },
                )?;
                if work.as_deref().is_some_and(copying::is_native) {
                    drop(
                        self.projection
                            .configured_extensions
                            .remove_entry(&record.extension),
                    );
                }
                self.projection
                    .configured_extensions
                    .insert(entry.0, entry.1);
            }
            EventPayload::EffectPlanned {
                effect_id,
                provider,
                guarantee,
                effect_kind,
                request,
                request_digest,
                result_schema,
                result_schema_digest,
            } => {
                if provider.trim().is_empty() || effect_kind.trim().is_empty() {
                    return Err(Error::Invalid(
                        "effect provider and kind must be non-empty".into(),
                    ));
                }
                request.validate(work)?;
                if request.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("effect request must be JSON content".into()));
                }
                if self.projection.effects.contains_key(effect_id) {
                    return Err(Error::Conflict("effect identity already exists".into()));
                }
                if *request_digest
                    != effect_request_digest_with_work(
                        provider,
                        *guarantee,
                        effect_kind,
                        request,
                        work,
                    )?
                {
                    return Err(Error::Invalid("effect request digest mismatch".into()));
                }
                result_schema.validate(work)?;
                if result_schema.descriptor().media_type() != "application/schema+json"
                    || *result_schema_digest != *result_schema.descriptor().sha256()
                {
                    return Err(Error::Invalid(
                        "effect result schema digest mismatch".into(),
                    ));
                }
                let (effect, _retired) = self.native_custody.produce(
                    &NativeComponentKey::EffectAction(*effect_id),
                    work,
                    |quota| {
                        copying::retain_entry::<(EffectId, EffectState)>(quota)?;
                        Ok(EffectState {
                            provider: copying::copy_text(provider, quota)?,
                            guarantee: *guarantee,
                            effect_kind: copying::copy_text(effect_kind, quota)?,
                            request: copying::copy_file(request, quota)?,
                            request_digest: *request_digest,
                            result_schema: copying::copy_file(result_schema, quota)?,
                            result_schema_digest: *result_schema_digest,
                            status: EffectStatus::Planned,
                            attempts: Vec::new(),
                        })
                    },
                )?;
                self.projection.effects.insert(*effect_id, effect);
            }
            EventPayload::EffectDispatched {
                effect_id,
                attempt_id,
            } => {
                let effect = self
                    .projection
                    .effects
                    .get_mut(effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                validate_effect_dispatch(effect, attempt_id)?;
                if work.as_deref().is_some_and(copying::is_native)
                    && effect.attempts.len() == effect.attempts.capacity()
                {
                    let maximum = effect
                        .attempts
                        .capacity()
                        .max(1)
                        .checked_mul(2)
                        .ok_or_else(|| {
                            Error::Invalid("effect attempt capacity overflows".into())
                        })?;
                    let copied = effect
                        .attempts
                        .len()
                        .checked_mul(std::mem::size_of::<EffectAttemptId>())
                        .ok_or_else(|| {
                            Error::Invalid("effect attempt move bytes overflow".into())
                        })? as u64;
                    work.as_deref()
                        .expect("attempt copy observer")
                        .admit_copy(copied)?;
                    let (mut replacement, retired) = self.native_custody.produce(
                        &NativeComponentKey::EffectAttempts(*effect_id),
                        work,
                        |quota| copying::reserve_copy_vector::<EffectAttemptId>(maximum, quota),
                    )?;
                    if let Err(error) = work
                        .as_deref_mut()
                        .expect("attempt copy observer")
                        .copy_bytes(copied)
                    {
                        drop(replacement);
                        drop(
                            self.native_custody
                                .rollback(&NativeComponentKey::EffectAttempts(*effect_id), retired),
                        );
                        return Err(error);
                    }
                    replacement.append(&mut effect.attempts);
                    let old_payload = std::mem::replace(&mut effect.attempts, replacement);
                    drop(old_payload);
                    drop(retired);
                }
                effect.attempts.push(*attempt_id);
                effect.status = EffectStatus::Dispatched;
                self.native_custody
                    .remove(&NativeComponentKey::EffectStatus(*effect_id));
            }
            EventPayload::EffectResolved { observation } => {
                let effect = self
                    .projection
                    .effects
                    .get_mut(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                self.authority_verifier.verify_effect(observation, work)?;
                validate_effect_observation(effect, observation)?;
                let (status, _retired) = self.native_custody.produce(
                    &NativeComponentKey::EffectStatus(observation.effect_id),
                    work,
                    |quota| copying::copy_status(&observation.status, quota),
                )?;
                effect.status = status;
            }
            EventPayload::ForkPublished { seed } => {
                self.validate_extension_fork_with_work(seed, work)?;
                self.require_fresh_fork_with_work(seed, work)?;
                if let Some(observer) = work.as_deref_mut().filter(|io| !copying::is_native(&**io))
                {
                    let reservation = inventory::source_inventory(seed)?
                        .checked_add(seed.child.id.len() as u64)
                        .and_then(|bytes| {
                            bytes.checked_add(std::mem::size_of::<(Authority, ForkSeed)>() as u64)
                        })
                        .ok_or_else(|| Error::Invalid("fork map clone quota overflows".into()))?;
                    observer.admit_owned(reservation)?;
                    observer.retain_owned(reservation)?;
                }
                let (entry, _retired) = self.native_custody.produce(
                    &NativeComponentKey::Fork(Cow::Borrowed(&seed.child)),
                    work,
                    |quota| {
                        copying::retain_entry::<(Authority, ForkSeed)>(quota)?;
                        let child = copying::copy_authority(&seed.child, quota)?;
                        let seed = copy_seed(seed, quota)?;
                        Ok((child, seed))
                    },
                )?;
                self.projection.forks.insert(entry.0, entry.1);
            }
            EventPayload::ProjectMergePublished { receipt } => {
                self.require_conversation()?;
                receipt.validate_shape(work)?;
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let (key, key_owned) = Self::merge_key_with_work(receipt, quota.work())?;
                quota.adopt(key_owned)?;
                if self.projection.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if quota.work().as_deref().is_some_and(copying::is_native) {
                    copying::retain_entry::<(String, Vec<u8>)>(&mut quota)?;
                }
                let retired = self.native_custody.install(
                    NativeComponentCustody::prepare(
                        &NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)),
                        0,
                        quota.work(),
                    )?,
                    quota.work(),
                )?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if quota.work().as_deref().is_some_and(copying::is_native) {
                    let produced = quota.retained();
                    quota.transfer();
                    if let Err(error) = self.native_custody.absorb_produced(
                        &NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)),
                        produced,
                        quota
                            .work()
                            .as_deref_mut()
                            .expect("merge producing observer"),
                    ) {
                        let incoming = self.native_custody.rollback(
                            &NativeComponentKey::Merge(
                                Cow::Borrowed(&key.0),
                                Cow::Borrowed(&key.1),
                            ),
                            retired,
                        );
                        drop(key);
                        drop(incoming);
                        return Err(error);
                    }
                }
                if let Err(error) =
                    self.append_conversation_message_with_work(&receipt.notice, quota.work())
                {
                    let incoming = self.native_custody.rollback(
                        &NativeComponentKey::Merge(Cow::Borrowed(&key.0), Cow::Borrowed(&key.1)),
                        retired,
                    );
                    drop(key);
                    drop(incoming);
                    return Err(error);
                }
                self.projection.published_merges.insert(key);
                drop(retired);
            }
            EventPayload::ConversationBound { agent } => {
                self.require_conversation()?;
                self.projection.conversation.bind(*agent)?;
            }
            EventPayload::ConversationMessageAppended { message } => {
                self.require_conversation()?;
                self.append_conversation_message_with_work(message, work)?;
            }
            EventPayload::ModelContextSelected { selection } => {
                self.require_conversation()?;
                selection.validate(&self.projection.conversation)?;
                if let Some(checkpoint) = &selection.checkpoint {
                    let (checkpoint, _retired) = self.native_custody.produce(
                        &NativeComponentKey::ContextCheckpoint,
                        work,
                        |quota| copying::copy_file(checkpoint, quota),
                    )?;
                    self.projection.latest_context_checkpoint = Some(checkpoint);
                }
            }
            EventPayload::InteractionOpened { ticket } => {
                self.require_bound_conversation_if_applicable()?;
                ticket.validate(work)?;
                if self.projection.interactions.contains_key(&ticket.id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                let (ticket, _retired) = self.native_custody.produce(
                    &NativeComponentKey::InteractionTicket(ticket.id),
                    work,
                    |quota| {
                        copying::retain_entry::<(
                            uuid::Uuid,
                            (InteractionTicket, Option<InteractionResolution>),
                        )>(quota)?;
                        copying::copy_ticket(ticket, quota)
                    },
                )?;
                self.projection
                    .interactions
                    .insert(ticket.id, (ticket, None));
            }
            EventPayload::InteractionResolved { resolution } => {
                let (ticket, prior) = self
                    .projection
                    .interactions
                    .get_mut(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                let (resolution, _retired) = self.native_custody.produce(
                    &NativeComponentKey::InteractionResolution(resolution.id),
                    work,
                    |quota| copying::copy_resolution(resolution, quota),
                )?;
                *prior = Some(resolution);
            }
        }
        Ok(())
    }
}
