//! reducer admission semantics.
use super::*;
impl Reducer {
    pub(crate) fn verify_command_scope_with_work(
        &self,
        command: &Command,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.authority_verifier
            .verify_audience(&self.authority, work)?;
        let key = command.idempotency_key.as_str();
        if key.is_empty() || key.chars().any(char::is_control) {
            return Err(Error::Invalid("idempotency key is invalid".into()));
        }
        self.authority_verifier.verify(&command.scope, work)
    }

    pub(crate) fn verify_retained_command_with_work(
        &self,
        command: &Command,
        event: &Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.verify_command_scope_with_work(command, work)?;
        self.authority_verifier.verify_event(event, work)?;
        if event.operation_id != command.operation_id
            || event.intent_digest
                != crate::contract::canonical_json_digest_with_work(
                    command,
                    crate::store::default_projection_read_limits().maximum_bytes,
                    work,
                )?
        {
            return Err(Error::Conflict(
                "operation identity is already bound to another intent".into(),
            ));
        }
        Ok(())
    }

    /// Applies one deterministic command.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "command ownership ends at application"
    )]
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.reducer.apply",
            level = "debug",
            skip_all,
            fields(rev = self.revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub fn apply(&mut self, command: Command) -> Result<ApplyResult> {
        crate::obs::outcome(match self.plan(&command) {
            Ok(ApplyResult::Applied { event }) => self
                .apply_committed(event.clone(), &mut None)
                .map(|_| ApplyResult::Applied { event }),
            planned => planned,
        })
    }

    /// Plans a command without advancing authoritative state.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.reducer.plan",
            level = "debug",
            skip_all,
            fields(rev = self.revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub fn plan(&self, command: &Command) -> Result<ApplyResult> {
        crate::obs::outcome(
            self.plan_with_migration_boundary_with_work(command, false, false, None, &mut None)
                .map(|(result, _)| result),
        )
    }

    /// Returns the original planner's already-retained output allowance.
    pub(crate) fn plan_indexed_with_retained_quota(
        &self,
        command: &Command,
        migration_verified: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if migration_verified && !matches!(&command.action, Action::MigrateExtensionState { .. }) {
            return crate::obs::outcome(Err(Error::Invalid(
                "verified migration planner requires a migration action".into(),
            )));
        }
        crate::obs::outcome(self.plan_with_migration_boundary_with_work(
            command,
            migration_verified,
            true,
            None,
            work,
        ))
    }

    /// Selects only fresh effect transitions whose original projection may have been retired.
    pub(crate) fn archived_effect_for_command(&self, command: &Command) -> Option<EffectId> {
        let effect_id = match &command.action {
            Action::MarkEffectDispatched { effect_id, .. } => *effect_id,
            Action::ResolveEffect { observation } => observation.effect_id,
            _ => return None,
        };
        (self.projection.effects_archived && !self.projection.effects.contains_key(&effect_id))
            .then_some(effect_id)
    }

    pub(crate) fn plan_with_archived_effect(
        &self,
        command: &Command,
        effect_id: EffectId,
        effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if self.archived_effect_for_command(command) != Some(effect_id)
            || !settled_effect(&effect.status)
        {
            return Err(Error::Storage(
                "archived effect does not match terminal planning target".into(),
            ));
        }
        crate::obs::outcome(self.plan_with_migration_boundary_with_work(
            command,
            false,
            true,
            Some((effect_id, effect)),
            work,
        ))
    }

    pub(in crate::core) fn plan_with_migration_boundary_with_work(
        &self,
        command: &Command,
        migration_verified: bool,
        identity_checked: bool,
        archived_effect: Option<(EffectId, &EffectState)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        self.native_custody.require_work(
            work,
            "native reducer planning requires explicit native work",
        )?;
        self.verify_command_scope_with_work(command, work)?;
        let intent = crate::contract::canonical_json_digest_with_work(
            command,
            crate::store::default_projection_read_limits().maximum_bytes,
            work,
        )?;
        if let Some(event) = self.event_for_operation(command.operation_id) {
            if event.intent_digest == intent {
                return event.replay(work);
            }
            return Err(Error::Conflict(
                "operation identity is already bound to another intent".into(),
            ));
        }
        if !identity_checked
            && (self.archived_through_revision() != 0 || self.projection.effects_archived)
        {
            return Err(Error::Unsupported(
                "operation identity requires authoritative archive lookup".into(),
            ));
        }
        if command.expected_revision != self.revision {
            return Err(Error::Conflict(format!(
                "expected revision {}, found {}",
                command.expected_revision, self.revision
            )));
        }
        self.authorize(
            (&command.scope.id, &command.scope.capabilities),
            command.scope.agent(),
            command.operation_id,
            command.action.admission(),
            archived_effect,
            work,
        )?;
        match &command.action {
            Action::MigrateExtensionState { content, .. } => {
                ContentGrant::authorize_volume(
                    &self.authority_verifier,
                    &command.scope,
                    content.volume(),
                    VolumeOperation::Write,
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    work,
                )?;
                if !migration_verified {
                    return Err(Error::Unsupported(
                        "extension migration requires an executing content-admission host".into(),
                    ));
                }
            }
            Action::BindConversation { agent }
                if command.scope.agent().is_some_and(|acting| acting != *agent) =>
            {
                return Err(Error::Unauthorized(
                    "conversation binding does not match the authenticated agent".into(),
                ));
            }
            Action::PublishFork { seed } => self.require_fresh_fork_with_work(seed, work)?,
            _ => {}
        }
        validate_causal_parent_with_work(
            &self.authority,
            self.revision,
            command.causal_parent.as_ref(),
            work,
        )?;
        let revision = next_revision(self.revision)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let payload = self.transition_with_work(&command.action, archived_effect, &mut quota)?;
        let mut event = Event {
            revision,
            operation_id: command.operation_id,
            intent_digest: intent,
            scope: RecordedScope {
                id: copying::copy_text(&command.scope.id, &mut quota)?,
                issuer: copying::copy_text(&command.scope.issuer, &mut quota)?,
                capabilities: copying::copy_capabilities(&command.scope.capabilities, &mut quota)?,
                agent: command.scope.agent,
            },
            attestation: [0; 32],
            causal_parent: command
                .causal_parent
                .as_ref()
                .map(|parent| copying::copy_reference(parent, &mut quota))
                .transpose()?,
            payload,
        };
        event.attestation = self.authority_verifier.attest_event(&event, quota.work())?;
        let retained_quota = quota.retained();
        quota.transfer();
        Ok((ApplyResult::Applied { event }, retained_quota))
    }

    pub(in crate::core) fn transition_effect<'a>(
        &'a self,
        effect_id: &EffectId,
        archived: Option<(EffectId, &'a EffectState)>,
    ) -> Result<&'a EffectState> {
        self.projection
            .effects
            .get(effect_id)
            .or_else(|| {
                archived
                    .filter(|(id, _)| id == effect_id)
                    .map(|(_, state)| state)
            })
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))
    }

    /// Authorizes one transition identically when planned and when committed.
    pub(in crate::core) fn authorize(
        &self,
        scope: (&str, &Capabilities),
        agent: Option<AgentId>,
        operation_id: OperationId,
        (_, capability, subject): (&str, &str, Subject<'_>),
        archived_effect: Option<(EffectId, &EffectState)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        require_capability(scope, capability)?;
        match subject {
            Subject::None => Ok(()),
            Subject::PlannedEffect => require_capability(scope, capability::EFFECT_PLAN),
            Subject::Effect(effect_id) => {
                let effect = self.transition_effect(effect_id, archived_effect)?;
                require_capability(scope, &capability::effect_provider(&effect.provider))
            }
            Subject::Interaction(resolution) => {
                validate_interaction_operation(resolution, operation_id)?;
                let (ticket, _) = self
                    .projection
                    .interactions
                    .get(&resolution.id)
                    .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
                require_capability(scope, &ticket.responder_grant())
            }
            Subject::Fork(seed) => {
                if self.projection.conversation.agent.is_none()
                    || agent != self.projection.conversation.agent
                {
                    return Err(Error::Unauthorized(
                        "fork must be published by the parent agent".into(),
                    ));
                }
                let validation = seed.validate(work);
                if work.is_some() {
                    validation?;
                } else if validation.is_err() {
                    return Err(Error::Invalid(
                        "fork manifest is not bound to its operation and parent revision".into(),
                    ));
                }
                if seed.operation_id != operation_id
                    || seed.parent != self.authority
                    || seed.parent_revision != self.revision
                {
                    return Err(Error::Invalid(
                        "fork manifest is not bound to its operation and parent revision".into(),
                    ));
                }
                let ownership = self.validate_fork_reference_ownership(seed, None, work);
                if work.is_some() {
                    ownership?;
                } else if ownership.is_err() {
                    return Err(Error::Invalid(
                        "fork manifest is not bound to its operation and parent revision".into(),
                    ));
                }
                if let Some(io) = work.as_deref() {
                    io.check_cancellation()?;
                }
                for resource in &seed.resources {
                    if let crate::fork::ResourceRevision::Project { volume, .. } = &resource.source
                    {
                        Self::require_volume_operation(
                            scope,
                            volume,
                            VolumeOperation::Read,
                            crate::store::default_projection_read_limits().maximum_bytes,
                            work,
                        )?;
                    }
                    if let crate::fork::ResourceRevision::PrivateVolume { volume, .. } =
                        &resource.source
                    {
                        if volume.owner()
                            != &VolumeOwner::Agent(agent.ok_or_else(|| {
                                Error::Unauthorized("scratch fork has no parent agent".into())
                            })?)
                        {
                            return Err(Error::Unauthorized(
                                "scratch is not owned by the direct parent".into(),
                            ));
                        }
                        Self::require_volume_operation(
                            scope,
                            volume,
                            VolumeOperation::Read,
                            crate::store::default_projection_read_limits().maximum_bytes,
                            work,
                        )?;
                    }
                }
                Ok(())
            }
            Subject::Merge(receipt) => {
                if receipt.operation_id != operation_id
                    || agent != self.projection.conversation.agent
                {
                    return Err(Error::Unauthorized(
                        "merge receipt is not bound to the parent agent and operation".into(),
                    ));
                }
                receipt.target_project.require_writer(agent, &None)?;
                let maximum_bytes = if work.as_deref().is_some_and(copying::is_native) {
                    acyclic_stream::MAX_RECORD_BYTES as u64
                } else {
                    u64::MAX
                };
                Self::require_volume_operation(
                    scope,
                    &receipt.target_project,
                    VolumeOperation::Write,
                    maximum_bytes,
                    work,
                )?;
                Self::require_volume_operation(
                    scope,
                    &receipt.source_project,
                    VolumeOperation::Read,
                    maximum_bytes,
                    work,
                )?;
                Ok(())
            }
        }
    }

    pub(in crate::core) fn require_volume_operation(
        scope: (&str, &Capabilities),
        volume: &VolumeRef,
        operation: VolumeOperation,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let digest = crate::contract::canonical_json_digest_with_work(volume, maximum_bytes, work)?;
        let hex = blake3::Hash::from_bytes(digest).to_hex();
        let prefix = match operation {
            VolumeOperation::Read => "volume:read:",
            VolumeOperation::Write => "volume:write:",
        };
        let mut label = [0_u8; 77];
        label[..prefix.len()].copy_from_slice(prefix.as_bytes());
        label[prefix.len()..prefix.len() + 64].copy_from_slice(hex.as_bytes());
        require_capability(
            scope,
            std::str::from_utf8(&label[..prefix.len() + 64]).expect("hex capability"),
        )
    }

    /// Applies an event only after its canonical Stream append commits.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.reducer.apply_committed",
            level = "trace",
            skip_all,
            fields(rev = event.revision)
        )
    )]
    pub fn apply_committed(
        &mut self,
        event: Event,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ApplyResult> {
        self.native_custody.require_work(
            work,
            "native reducer mutation requires explicit native work",
        )?;
        self.authority_verifier
            .verify_audience(&self.authority, work)?;
        if let Some(existing) = self.event_for_operation(event.operation_id) {
            if existing == &event {
                return existing.replay(work).map(|(result, _)| result);
            }
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "operation identity is already bound to another event",
            ));
        }
        let expected = next_revision(self.revision)?;
        if event.revision != expected {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(copying::is_native) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "committed revision does not match the exact successor",
                });
            }
            return Err(Error::Conflict(format!(
                "expected committed revision {expected}, found {}",
                event.revision
            )));
        }
        self.authority_verifier.verify_event(&event, work)?;
        self.authorize(
            (&event.scope.id, &event.scope.capabilities),
            event.scope.agent(),
            event.operation_id,
            event.payload.admission(),
            None,
            work,
        )?;
        validate_causal_parent_with_work(
            &self.authority,
            self.revision,
            event.causal_parent.as_ref(),
            work,
        )?;
        if let Some(observer) = work.as_deref_mut().filter(|io| !copying::is_native(*io)) {
            let reservation = event_owned_bytes(&event)?;
            if matches!(&event.payload, EventPayload::ForkPublished { .. }) {
                observer.admit_allocation(std::mem::size_of::<ForkSeed>() as u64)?;
            }
            observer.admit_owned(reservation)?;
            observer.retain_owned(reservation)?;
        }
        let (cached, retired_event) = self.native_custody.produce(
            &NativeComponentKey::Event(event.revision),
            work,
            |quota| copying::copy_event(&event, quota),
        )?;
        let applied = Self::reserve_deque_buffer(
            &mut self.native_custody,
            NativeComponentKey::EventBuffer,
            &mut self.events,
            work,
        )
        .and_then(|()| self.apply_payload_with_work(&event.payload, event.revision, work));
        if let Err(error) = applied {
            drop(cached);
            drop(
                self.native_custody
                    .rollback(&NativeComponentKey::Event(event.revision), retired_event),
            );
            return Err(error);
        }
        self.revision = event.revision;
        self.events.push_back(cached);
        drop(retired_event);
        self.trim_resident_events();
        Ok(ApplyResult::Applied { event })
    }
}
