//! reducer transition semantics.
use super::*;
impl Reducer {
    #[allow(clippy::too_many_lines, reason = "one arm per action")]
    pub(in crate::core) fn transition_with_work(
        &self,
        action: &Action,
        archived_effect: Option<(EffectId, &EffectState)>,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<EventPayload> {
        match action {
            Action::TransitionLifecycle { to, reason } => {
                validate_lifecycle(self.projection.lifecycle, *to)?;
                Ok(EventPayload::LifecycleTransitioned {
                    from: self.projection.lifecycle,
                    to: *to,
                    reason: reason
                        .as_deref()
                        .map(|text| copying::copy_text(text, quota))
                        .transpose()?,
                })
            }
            Action::AppendCustom {
                schema,
                version,
                content,
            } => {
                if self
                    .projection
                    .extension_states
                    .get(schema)
                    .is_some_and(|(_, current)| current.version != *version)
                {
                    return Err(Error::Conflict(
                        "extension state version change requires migration".into(),
                    ));
                }
                content.validate(quota.work())?;
                if content.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("extension content must be JSON".into()));
                }
                let binding = self
                    .schemas
                    .pinned_binding(schema, *version, None, quota.work())?;
                let record = ExtensionRecord {
                    name: copying::copy_text(schema, quota)?,
                    version: *version,
                    schema_digest: binding.schema_digest,
                    implementation_digest: binding.implementation_digest,
                    fork_policy: binding.fork_policy,
                    content: copying::copy_file(content, quota)?,
                };
                record.validate()?;
                Ok(EventPayload::Custom { record })
            }
            Action::MigrateExtensionState {
                name,
                previous,
                to_version,
                content,
            } => {
                let (source, current) = self
                    .projection
                    .extension_states
                    .get(name)
                    .ok_or_else(|| Error::NotFound(format!("extension state {name}")))?;
                if source != previous
                    || previous.authority != self.authority
                    || current.version == *to_version
                {
                    return Err(Error::Conflict(
                        "extension migration source or target is not exact".into(),
                    ));
                }
                let binding = self
                    .schemas
                    .pinned_binding(name, *to_version, None, quota.work())?;
                let record = ExtensionRecord {
                    name: copying::copy_text(name, quota)?,
                    version: *to_version,
                    schema_digest: binding.schema_digest,
                    implementation_digest: binding.implementation_digest,
                    fork_policy: binding.fork_policy,
                    content: copying::copy_file(content, quota)?,
                };
                record.validate()?;
                Ok(EventPayload::ExtensionStateMigrated {
                    migration: ExtensionStateMigration {
                        previous: copying::copy_reference(previous, quota)?,
                        record,
                    },
                })
            }
            Action::SelectExtensions { roots } => {
                if self.authority.kind != AggregateKind::Agent {
                    return Err(Error::Invalid(
                        "extension selection requires an agent aggregate".into(),
                    ));
                }
                if !matches!(
                    self.projection.lifecycle,
                    LifecycleState::Pending | LifecycleState::Waiting
                ) {
                    return Err(Error::Conflict(
                        "extension selection requires a pending or waiting agent".into(),
                    ));
                }
                let (selected, produced) = self.schemas.activation_order(roots, quota.work())?;
                quota.adopt(produced)?;
                for extension in &selected {
                    if self
                        .projection
                        .extension_states
                        .get(&extension.name)
                        .is_some_and(|(_, state)| state.version != extension.version)
                    {
                        return Err(Error::Conflict(format!(
                            "extension {} state must migrate before activation",
                            extension.name
                        )));
                    }
                }
                let configurations = self.selected_configurations_in_quota(&selected, quota)?;
                Ok(EventPayload::ExtensionsSelected {
                    previous: copying::copy_vector(
                        &self.projection.active_extensions,
                        quota,
                        copying::copy_dependency,
                    )?,
                    selected,
                    configurations,
                })
            }
            Action::ConfigureExtension { extension, content } => {
                if self.authority.kind != AggregateKind::Agent {
                    return Err(Error::Invalid(
                        "extension configuration requires an agent aggregate".into(),
                    ));
                }
                if !matches!(
                    self.projection.lifecycle,
                    LifecycleState::Pending | LifecycleState::Waiting
                ) {
                    return Err(Error::Conflict(
                        "extension configuration requires a pending or waiting agent".into(),
                    ));
                }
                let record = self
                    .schemas
                    .configuration_record(extension, content, quota)?;
                Ok(EventPayload::ExtensionConfigured {
                    previous: self
                        .projection
                        .configured_extensions
                        .get(extension)
                        .map(|record| copying::copy_configuration(record, quota))
                        .transpose()?,
                    record,
                })
            }
            Action::PlanEffect {
                effect_id,
                provider,
                guarantee,
                effect_kind,
                request,
                result_schema,
            } => {
                if self.projection.effects.contains_key(effect_id) {
                    return Err(Error::Conflict("effect identity already exists".into()));
                }
                if provider.trim().is_empty() || effect_kind.trim().is_empty() {
                    return Err(Error::Invalid(
                        "effect provider and kind must be non-empty".into(),
                    ));
                }
                request.validate(quota.work())?;
                if request.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("effect request must be JSON content".into()));
                }
                let request_digest = effect_request_digest_with_work(
                    provider,
                    *guarantee,
                    effect_kind,
                    request,
                    quota.work(),
                )?;
                result_schema.validate(quota.work())?;
                if result_schema.descriptor().media_type() != "application/schema+json" {
                    return Err(Error::Invalid(
                        "effect result schema must be JSON Schema content".into(),
                    ));
                }
                let result_schema_digest = *result_schema.descriptor().sha256();
                Ok(EventPayload::EffectPlanned {
                    effect_id: *effect_id,
                    provider: copying::copy_text(provider, quota)?,
                    guarantee: *guarantee,
                    effect_kind: copying::copy_text(effect_kind, quota)?,
                    request: copying::copy_file(request, quota)?,
                    request_digest,
                    result_schema: copying::copy_file(result_schema, quota)?,
                    result_schema_digest,
                })
            }
            Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            } => {
                let effect = self.transition_effect(effect_id, archived_effect)?;
                validate_effect_dispatch(effect, attempt_id)?;
                Ok(EventPayload::EffectDispatched {
                    effect_id: *effect_id,
                    attempt_id: *attempt_id,
                })
            }
            Action::ResolveEffect { observation } => {
                if !observation.status.is_terminal() {
                    return Err(Error::Invalid(
                        "effect resolution must be terminal or indeterminate".into(),
                    ));
                }
                let effect = self.transition_effect(&observation.effect_id, archived_effect)?;
                self.authority_verifier
                    .verify_effect(observation, quota.work())?;
                validate_effect_observation(effect, observation)?;
                Ok(EventPayload::EffectResolved {
                    observation: copying::copy_observation(observation, quota)?,
                })
            }
            Action::PublishFork { seed } => {
                self.validate_extension_fork_with_work(seed, quota.work())?;
                Ok(EventPayload::ForkPublished {
                    seed: copying::copy_boxed(seed.as_ref(), quota, copy_seed)?,
                })
            }
            Action::PublishProjectMerge { receipt } => {
                self.require_conversation()?;
                self.require_bound_conversation_if_applicable()?;
                receipt.validate_shape(quota.work())?;
                let (key, key_owned) = Self::merge_key_with_work(receipt, quota.work())?;
                if self.projection.published_merges.contains(&key) {
                    return Err(Error::Conflict("project join was already published".into()));
                }
                drop(key);
                if let Some(io) = quota.work().as_deref_mut() {
                    io.release_owned(key_owned)?;
                }
                self.projection
                    .conversation
                    .validate_append(&receipt.notice, &mut None)?;
                copying::retain_entry::<ProjectMergeReceipt>(quota)?;
                let receipt = receipt.copy(quota)?;
                Ok(EventPayload::ProjectMergePublished {
                    receipt: Box::new(receipt),
                })
            }
            Action::BindConversation { agent } => {
                self.require_conversation()?;
                if self.projection.conversation.agent.is_some()
                    || !self.projection.conversation.messages.is_empty()
                {
                    return Err(Error::Conflict("conversation is already bound".into()));
                }
                Ok(EventPayload::ConversationBound { agent: *agent })
            }
            Action::AppendConversationMessage { message } => {
                self.require_conversation()?;
                self.projection
                    .conversation
                    .validate_append(message, &mut None)?;
                Ok(EventPayload::ConversationMessageAppended {
                    message: copying::copy_boxed(message.as_ref(), quota, copying::copy_message)?,
                })
            }
            Action::SelectModelContext { selection } => {
                self.require_conversation()?;
                selection.validate(&self.projection.conversation)?;
                Ok(EventPayload::ModelContextSelected {
                    selection: copying::copy_selection(selection, quota)?,
                })
            }
            Action::OpenInteraction { ticket } => {
                self.require_bound_conversation_if_applicable()?;
                ticket.validate(quota.work())?;
                if self.projection.interactions.contains_key(&ticket.id) {
                    return Err(Error::Conflict(
                        "interaction identity already exists".into(),
                    ));
                }
                Ok(EventPayload::InteractionOpened {
                    ticket: copying::copy_ticket(ticket, quota)?,
                })
            }
            Action::ResolveInteraction { resolution } => {
                let (ticket, prior) = self
                    .projection
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                validate_interaction_transition(ticket, prior.as_ref(), resolution)?;
                Ok(EventPayload::InteractionResolved {
                    resolution: copying::copy_resolution(resolution, quota)?,
                })
            }
        }
    }
}
