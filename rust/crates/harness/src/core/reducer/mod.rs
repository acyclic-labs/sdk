//! reducer mod semantics.
use super::*;
mod admission;
mod application;
mod copy;
mod effects;
mod fork;
mod transition;
/// Restorable deterministic reducer state for one authority.
#[derive(Debug, PartialEq)]
pub struct Reducer {
    pub(in crate::core) authority: Authority,
    pub(in crate::core) authority_verifier: AuthorityVerifier,
    pub(in crate::core) schemas: SchemaRegistry,
    pub(in crate::core) revision: u64,
    pub(in crate::core) projection: snapshot::Projection<ConversationState, ()>,
    pub(in crate::core) events: VecDeque<Event>,
    pub(in crate::core) resident_event_limit: usize,
    pub(in crate::core) resident_terminal_effect_limit: usize,
    pub(in crate::core) native_custody: NativeComponentCustody,
}

impl Reducer {
    /// Creates an empty aggregate reducer.
    #[must_use]
    pub fn new(
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Self {
        Self {
            authority,
            authority_verifier,
            schemas,
            revision: 0,
            projection: snapshot::Projection::default(),
            events: VecDeque::new(),
            resident_event_limit: usize::MAX,
            resident_terminal_effect_limit: usize::MAX,
            native_custody: NativeComponentCustody::default(),
        }
    }

    pub(in crate::core) fn replace_extension_state_with_work(
        &mut self,
        record: &ExtensionRecord,
        revision: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let (entry, _retired) = self.native_custody.produce(
            &NativeComponentKey::ExtensionState(Cow::Borrowed(&record.name)),
            work,
            |quota| {
                copying::retain_entry::<(String, (EventReference, ExtensionRecord))>(quota)?;
                let key = copying::copy_text(&record.name, quota)?;
                let authority = copying::copy_authority(&self.authority, quota)?;
                let record = copying::copy_extension_record(record, quota)?;
                Ok((
                    key,
                    (
                        EventReference {
                            authority,
                            revision,
                        },
                        record,
                    ),
                ))
            },
        )?;
        if work.as_deref().is_some_and(copying::is_native) {
            drop(self.projection.extension_states.remove_entry(&record.name));
        }
        self.projection.extension_states.insert(entry.0, entry.1);
        Ok(())
    }

    pub(in crate::core) fn grow_buffer<T>(
        custody: &mut NativeComponentCustody,
        key: NativeComponentKey<'_>,
        length: usize,
        capacity: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<(Vec<T>, Option<native_custody::Component>)>> {
        if !work.as_deref().is_some_and(copying::is_native) || length < capacity {
            return Ok(None);
        }
        let capacity = capacity
            .max(1)
            .checked_mul(2)
            .ok_or_else(|| Error::Invalid("component buffer capacity overflows".into()))?;
        let copied = owned_buffer::<T>(length)?;
        work.as_deref()
            .expect("native buffer observer")
            .admit_copy(copied)?;
        let (replacement, retired) = custody.produce(&key, work, |quota| {
            copying::reserve_copy_vector::<T>(capacity, quota)
        })?;
        if let Err(error) = work
            .as_deref_mut()
            .expect("native buffer observer")
            .copy_bytes(copied)
        {
            drop(replacement);
            drop(custody.rollback(&key, retired));
            return Err(error);
        }
        Ok(Some((replacement, retired)))
    }

    pub(in crate::core) fn reserve_deque_buffer<T>(
        custody: &mut NativeComponentCustody,
        key: NativeComponentKey<'_>,
        payload: &mut VecDeque<T>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some((replacement, retired)) =
            Self::grow_buffer(custody, key, payload.len(), payload.capacity(), work)?
        {
            let mut replacement = VecDeque::from(replacement);
            replacement.append(payload);
            drop(std::mem::replace(payload, replacement));
            drop(retired);
        }
        Ok(())
    }
    pub(in crate::core) fn append_conversation_message_with_work(
        &mut self,
        message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.projection
            .conversation
            .validate_append(message, work)?;
        let (message, retired) = self.native_custody.produce(
            &NativeComponentKey::ConversationMessage(message.sequence),
            work,
            |quota| copying::copy_message(message, quota),
        )?;
        let sequence = message.sequence;
        if let Err(error) = self.projection.conversation.append(message, work) {
            drop(
                self.native_custody
                    .rollback(&NativeComponentKey::ConversationMessage(sequence), retired),
            );
            return Err(error);
        }
        drop(retired);
        Ok(())
    }

    pub(in crate::core) fn merge_key_with_work(
        receipt: &ProjectMergeReceipt,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<((String, Vec<u8>), u64)> {
        if !work.as_deref().is_some_and(copying::is_native) {
            return Ok((
                (
                    receipt.target_project.storage_name(work)?,
                    receipt.provider_operation_id.clone(),
                ),
                0,
            ));
        }
        crate::executor::json::json_byte_length(&receipt.target_project, acyclic_stream::MAX_RECORD_BYTES as u64, work).map(|_| ())?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let name = receipt.target_project.storage_name(quota.work())?;
        quota.adopt(name.capacity() as u64)?;
        let operation = copying::copy_copy_vector(&receipt.provider_operation_id, &mut quota)?;
        let owned = quota.retained();
        quota.transfer();
        Ok(((name, operation), owned))
    }

    /// Returns this reducer's authority.
    #[must_use]
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    pub(crate) fn event_verifier(&self) -> &AuthorityVerifier {
        &self.authority_verifier
    }

    /// Returns the current gapless revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns this aggregate's installed extension registry.
    #[must_use]
    pub const fn schemas(&self) -> &SchemaRegistry {
        &self.schemas
    }

    /// Authenticates and checks the exact migration boundary before a host reads old state or stages new bytes.
    pub(crate) fn preflight_extension_migration(
        &self,
        scope: &Scope,
        expected_revision: u64,
        previous: &EventReference,
        name: &str,
        to_version: u32,
        destination: &VolumeRef,
    ) -> Result<()> {
        self.authority_verifier
            .verify_audience(&self.authority, &mut None)?;
        self.authority_verifier.verify(scope, &mut None)?;
        require_capability(
            (&scope.id, &scope.capabilities),
            capability::EXTENSION_MIGRATE,
        )?;
        ContentGrant::authorize_volume(
            &self.authority_verifier,
            scope,
            destination,
            VolumeOperation::Write,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut None,
        )?;
        if expected_revision != self.revision {
            return Err(Error::Conflict(
                "extension migration revision changed".into(),
            ));
        }
        let (source, current) = self
            .projection
            .extension_states
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("extension state {name}")))?;
        if source != previous
            || previous.authority != self.authority
            || current.version == to_version
        {
            return Err(Error::Conflict(
                "extension migration source or target is not exact".into(),
            ));
        }
        self.schemas
            .pinned_binding(name, to_version, None, &mut None)?;
        Ok(())
    }

    /// Returns the fixed aggregate lifecycle projection.
    #[must_use]
    pub const fn lifecycle(&self) -> LifecycleState {
        self.projection.lifecycle
    }

    /// Dependency-first extension versions selected for new agent admissions.
    #[must_use]
    pub fn active_extensions(&self) -> &[ExtensionDependency] {
        &self.projection.active_extensions
    }

    /// Latest retained state event for one namespace.
    #[must_use]
    pub fn extension_state(&self, name: &str) -> Option<&(EventReference, ExtensionRecord)> {
        self.projection.extension_states.get(name)
    }

    /// Exact configuration revisions pinned by the most recent activation.
    #[must_use]
    pub fn active_configurations(&self) -> &[ExtensionConfiguration] {
        &self.projection.active_configurations
    }

    /// Captures the last committed activation for immutable task admission.
    pub fn extension_admission(&self) -> Result<Option<ExtensionAdmission>> {
        if self.authority.kind != AggregateKind::Agent {
            return Err(Error::Invalid(
                "extension admission requires an agent aggregate".into(),
            ));
        }
        let Some(revision) = self.projection.active_extension_revision else {
            return Ok(None);
        };
        let admission = ExtensionAdmission {
            source: EventReference {
                authority: self.authority.clone(),
                revision,
            },
            selected: self.projection.active_extensions.clone(),
            configurations: self.projection.active_configurations.clone(),
        };
        admission.validate(&mut None)?;
        Ok(Some(admission))
    }

    pub(in crate::core) fn selected_configurations_in_quota(
        &self,
        selected: &[ExtensionDependency],
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Vec<ExtensionConfiguration>> {
        let mut count = 0;
        for extension in selected {
            if self
                .schemas
                .pinned_binding(&extension.name, extension.version, None, quota.work())?
                .configuration_schema
                .is_some()
            {
                count += 1;
            }
        }
        let mut configurations = copying::reserve_copy_vector(count, quota)?;
        for extension in selected {
            let binding = self.schemas.pinned_binding(
                &extension.name,
                extension.version,
                None,
                quota.work(),
            )?;
            if binding.configuration_schema.is_some() {
                let record = self
                    .projection
                    .configured_extensions
                    .get(extension)
                    .ok_or_else(|| {
                        Error::Conflict(format!(
                            "extension {}@{} is not configured",
                            extension.name, extension.version
                        ))
                    })?;
                configurations.push(copying::copy_configuration(record, quota)?);
            }
        }
        Ok(configurations)
    }

    pub(in crate::core) fn selected_configuration_matches_with_work(
        &self,
        selected: &[ExtensionDependency],
        configurations: &[ExtensionConfiguration],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (order, produced) = self.schemas.activation_order(selected, quota.work())?;
        quota.adopt(produced)?;
        if order.as_slice() != selected {
            return Ok(false);
        }
        let mut configurations = configurations.iter();
        for extension in selected {
            let binding = self.schemas.pinned_binding(
                &extension.name,
                extension.version,
                None,
                quota.work(),
            )?;
            if binding.configuration_schema.is_some() {
                let expected = self
                    .projection
                    .configured_extensions
                    .get(extension)
                    .ok_or_else(|| {
                        Error::Conflict(format!(
                            "extension {}@{} is not configured",
                            extension.name, extension.version
                        ))
                    })?;
                if configurations.next() != Some(expected) {
                    return Ok(false);
                }
            }
        }
        Ok(configurations.next().is_none())
    }

    /// Returns the built-in conversation projection for conversation aggregates.
    #[must_use]
    pub fn conversation(&self) -> Option<&ConversationState> {
        (self.authority.kind == AggregateKind::Conversation)
            .then_some(&self.projection.conversation)
    }

    /// Enumerates context selections in the resident event suffix, in admission order.
    pub fn resident_context_selections(&self) -> impl Iterator<Item = &ModelContextSelection> {
        self.events.iter().filter_map(|event| match &event.payload {
            EventPayload::ModelContextSelected { selection } => Some(selection),
            _ => None,
        })
    }

    /// Latest owner-published canonical base, maintained by selection replay.
    #[must_use]
    pub fn latest_context_checkpoint(&self) -> Option<&FileRef> {
        self.projection.latest_context_checkpoint.as_ref()
    }

    /// Returns the exact selection committed for one operation, even after later conversation messages changed the aggregate's current tail.
    #[must_use]
    pub fn context_selection_for_operation(
        &self,
        operation_id: OperationId,
    ) -> Option<&ModelContextSelection> {
        self.event_for_operation(operation_id)
            .and_then(|event| match &event.payload {
                EventPayload::ModelContextSelected { selection } => Some(selection),
                _ => None,
            })
    }

    /// Exact aggregate revision committed for a stable operation identity.
    #[must_use]
    pub fn operation_revision(&self, operation_id: OperationId) -> Option<u64> {
        self.event_for_operation(operation_id)
            .map(|event| event.revision)
    }

    pub(in crate::core) fn event_for_operation(&self, operation_id: OperationId) -> Option<&Event> {
        self.events
            .iter()
            .rev()
            .find(|event| event.operation_id == operation_id)
    }

    /// Returns the exact admitted request and its optional terminal resolution.
    #[must_use]
    pub fn interaction(
        &self,
        id: &uuid::Uuid,
    ) -> Option<&(InteractionTicket, Option<InteractionResolution>)> {
        self.projection.interactions.get(id)
    }

    pub(crate) fn set_resident_event_limit(&mut self, maximum: usize) -> Result<()> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "resident event allowance must be positive".into(),
            ));
        }
        self.resident_event_limit = maximum;
        self.trim_resident_events();
        Ok(())
    }

    pub(in crate::core) fn trim_resident_events(&mut self) {
        while self.events.len() > self.resident_event_limit {
            if let Some(event) = self.events.pop_front() {
                let revision = event.revision;
                drop(event);
                self.native_custody
                    .remove(&NativeComponentKey::Event(revision));
            }
        }
    }

    /// Last revision no longer available through the synchronous resident event reader.
    #[must_use]
    pub fn archived_through_revision(&self) -> u64 {
        self.events
            .front()
            .map_or(self.revision, |event| event.revision - 1)
    }

    /// Returns a bounded page strictly after `revision`.
    pub fn events_after(&self, revision: u64, limit: usize) -> Result<Vec<Event>> {
        if revision > self.revision {
            return Err(Error::Invalid("cursor is beyond the aggregate head".into()));
        }
        if revision < self.archived_through_revision() {
            return Err(Error::Unsupported(
                "cursor requires authoritative archive reader".into(),
            ));
        }
        Ok(self
            .events
            .iter()
            .filter(|event| event.revision > revision)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Returns a child only after its publication event committed.
    #[must_use]
    pub fn fork(&self, child: &Authority) -> Option<&ForkSeed> {
        self.projection.forks.get(child)
    }

    pub(in crate::core) fn require_conversation(&self) -> Result<()> {
        if self.authority.kind != AggregateKind::Conversation {
            return Err(Error::Invalid(
                "conversation command targets another aggregate".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::core) fn require_bound_conversation_if_applicable(&self) -> Result<()> {
        if self.authority.kind == AggregateKind::Conversation
            && self.projection.conversation.agent.is_none()
        {
            return Err(Error::Conflict(
                "conversation is not bound to an agent".into(),
            ));
        }
        Ok(())
    }
}
