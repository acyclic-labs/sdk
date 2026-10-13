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
                self.replace_extension_state(record, revision);
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
                self.replace_extension_state(record, revision);
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
                self.projection.active_extension_revision = Some(revision);
                self.projection.active_extensions = selected.clone();
                self.projection.active_configurations = configurations.clone();
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
                self.projection
                    .configured_extensions
                    .insert(record.extension.clone(), record.clone());
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
                let effect = EffectState {
                    provider: provider.clone(),
                    guarantee: *guarantee,
                    effect_kind: effect_kind.clone(),
                    request: request.clone(),
                    request_digest: *request_digest,
                    result_schema: result_schema.clone(),
                    result_schema_digest: *result_schema_digest,
                    status: EffectStatus::Planned,
                    attempts: Vec::new(),
                };
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
                effect.attempts.push(*attempt_id);
                effect.status = EffectStatus::Dispatched;
            }
            EventPayload::EffectResolved { observation } => {
                let effect = self
                    .projection
                    .effects
                    .get_mut(&observation.effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
                self.authority_verifier.verify_effect(observation, work)?;
                validate_effect_observation(effect, observation)?;
                effect.status = observation.status.clone();
            }
            EventPayload::ForkPublished { seed } => {
                self.validate_extension_fork_with_work(seed, work)?;
                self.require_fresh_fork_with_work(seed, work)?;
                if let Some(observer) = work.as_deref_mut().filter(|io| !copying::is_native(&**io))
                {
                    let reservation = owned_bytes(seed)?
                        .checked_add(seed.child.id.len() as u64)
                        .and_then(|bytes| {
                            bytes.checked_add(std::mem::size_of::<(Authority, ForkSeed)>() as u64)
                        })
                        .ok_or_else(|| Error::Invalid("fork map clone quota overflows".into()))?;
                    observer.admit_owned(reservation)?;
                    observer.retain_owned(reservation)?;
                }
                self.projection
                    .forks
                    .insert(seed.child.clone(), seed.as_ref().clone());
            }
            EventPayload::ProjectMergePublished { receipt } => {
                self.require_conversation()?;
                receipt.validate_shape(work)?;
                let key = Self::merge_key(receipt, work)?;
                if self.projection.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                self.append_conversation_message_with_work(&receipt.notice, work)?;
                self.projection.published_merges.insert(key);
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
                    self.projection.latest_context_checkpoint = Some(checkpoint.clone());
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
                self.projection
                    .interactions
                    .insert(ticket.id, (ticket.clone(), None));
            }
            EventPayload::InteractionResolved { resolution } => {
                let (ticket, prior) = self
                    .projection
                    .interactions
                    .get_mut(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                *prior = Some(resolution.clone());
            }
        }
        Ok(())
    }
}
