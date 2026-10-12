//! Authenticated reducer checkpoints and bounded wire decoding.
use super::*;
mod capture;
mod entries;
mod restoration;
#[cfg(test)]
mod tests;

/// Issuer-authenticated state accelerator; Stream events remain authoritative.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot<C = ConversationState> {
    /// Snapshot format version. Earlier replay-based formats are unsupported.
    pub format_version: u32,
    /// Aggregate represented by the snapshot.
    pub authority: Authority,
    /// Last included event revision.
    pub revision: u64,
    /// Retained canonical event cache, also covered by the attestation.
    pub events: Vec<Event>,
    projection: Projection<C>,
    /// Canonical digest of the complete state and retained cache.
    pub state_digest: [u8; 32],
    /// Domain-separated issuer proof over the digest and aggregate identity.
    pub attestation: [u8; 32],
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[serde(skip)]
    native_allocation: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::core) struct Projection<
    C = ConversationState,
    B = Vec<(ExtensionDependency, [u8; 32])>,
> {
    pub(in crate::core) lifecycle: LifecycleState,
    pub(in crate::core) active_extensions: Vec<ExtensionDependency>,
    pub(in crate::core) active_extension_revision: Option<u64>,
    pub(in crate::core) extension_states: BTreeMap<String, (EventReference, ExtensionRecord)>,
    #[serde(with = "entries")]
    pub(in crate::core) configured_extensions:
        BTreeMap<ExtensionDependency, ExtensionConfiguration>,
    pub(in crate::core) active_configurations: Vec<ExtensionConfiguration>,
    pub(in crate::core) effects: BTreeMap<EffectId, EffectState>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    pub(in crate::core) indexed_terminal_effects: VecDeque<EffectId>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(in crate::core) effects_archived: bool,
    #[serde(with = "entries")]
    pub(in crate::core) forks: BTreeMap<Authority, ForkSeed>,
    pub(in crate::core) published_merges: BTreeSet<(String, Vec<u8>)>,
    pub(in crate::core) conversation: C,
    pub(in crate::core) latest_context_checkpoint: Option<FileRef>,
    pub(in crate::core) interactions:
        BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
    pub(in crate::core) bindings: B,
}

fn is_false(value: &bool) -> bool {
    !*value
}
impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.format_version == other.format_version
            && self.authority == other.authority
            && self.revision == other.revision
            && self.events == other.events
            && self.projection == other.projection
            && self.state_digest == other.state_digest
            && self.attestation == other.attestation
    }
}
impl Snapshot {
    pub(crate) const LOGICAL_SCHEMA_SLOT_BYTES: u64 = {
        let sizes = [
            std::mem::size_of::<Reducer>(),
            std::mem::size_of::<Snapshot>(),
            std::mem::size_of::<snapshot::Projection>(),
            std::mem::size_of::<Event>(),
            std::mem::size_of::<super::EventPayload>(),
            std::mem::size_of::<super::EffectStatus>(),
            std::mem::size_of::<EffectState>(),
            std::mem::size_of::<super::ExtensionBinding>(),
            std::mem::size_of::<super::ExtensionAdmission>(),
            std::mem::size_of::<super::Command>(),
            std::mem::size_of::<super::Action>(),
            std::mem::size_of::<super::Scope>(),
            std::mem::size_of::<super::RecordedScope>(),
            std::mem::size_of::<crate::Capabilities>(),
            std::mem::size_of::<super::ExtensionStateMigration>(),
            std::mem::size_of::<ExtensionRecord>(),
            std::mem::size_of::<ExtensionConfiguration>(),
            std::mem::size_of::<ExtensionDependency>(),
            std::mem::size_of::<Authority>(),
            std::mem::size_of::<EventReference>(),
            std::mem::size_of::<ForkSeed>(),
            std::mem::size_of::<crate::fork::ForkSelection>(),
            std::mem::size_of::<crate::fork::ForkOmission>(),
            std::mem::size_of::<crate::fork::CapturedResource>(),
            std::mem::size_of::<crate::fork::ResourceRevision>(),
            std::mem::size_of::<crate::fork::SharedGrant>(),
            std::mem::size_of::<crate::fork::ReferenceGrant>(),
            std::mem::size_of::<crate::fork::SummaryForkCapture>(),
            std::mem::size_of::<crate::fork::AttestedBoundary>(),
            std::mem::size_of::<crate::merge::ProjectMergeReceipt>(),
            std::mem::size_of::<crate::conversation::ConversationMessage>(),
            std::mem::size_of::<ConversationState>(),
            std::mem::size_of::<crate::conversation::ModelContextSelection>(),
            std::mem::size_of::<FileRef>(),
            std::mem::size_of::<crate::conversation::VolumeRef>(),
            std::mem::size_of::<crate::conversation::FileDescriptor>(),
            std::mem::size_of::<InteractionTicket>(),
            std::mem::size_of::<InteractionResolution>(),
            std::mem::size_of::<crate::resources::ResourceRef>(),
            std::mem::size_of::<crate::resources::ProviderRef>(),
            std::mem::size_of::<(String, serde_json::Value)>(),
            std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<crate::runtime::TaskAdmissionRecord>(),
            std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>(),
            std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>(),
            std::mem::size_of::<crate::resource_preparation::ResourceCreationOrigin>(),
            std::mem::size_of::<crate::workflow::WorkflowCommand>(),
            std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>(),
        ];
        let mut maximum = 0;
        let mut index = 0;
        while index < sizes.len() {
            if sizes[index] > maximum {
                maximum = sizes[index];
            }
            index += 1;
        }
        maximum as u64
    };
    fn digest_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<[u8; 32]> {
        crate::contract::canonical_json_digest_with_work(
            &(
                self.format_version,
                &self.authority,
                self.revision,
                &self.events,
                &self.projection,
            ),
            crate::store::default_projection_read_limits().maximum_bytes,
            work,
        )
    }
    fn verify(
        &self,
        verifier: &AuthorityVerifier,
        schemas: &SchemaRegistry,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if self.format_version != 6 {
            return Err(Error::Unsupported(format!(
                "snapshot format {}",
                self.format_version
            )));
        }
        verifier.verify_audience(&self.authority, work)?;
        if self.digest_with_work(work)? != self.state_digest {
            return Err(Error::Invalid("snapshot digest mismatch".into()));
        }
        if verifier.canonical_proof(
            b"harness/v6/reducer-checkpoint\0",
            &(&verifier.id, &verifier.audience, self.state_digest),
            work,
        )? != self.attestation
        {
            return Err(Error::Unauthorized(
                "snapshot admission attestation is invalid".into(),
            ));
        }
        for (index, id) in self.projection.indexed_terminal_effects.iter().enumerate() {
            if self
                .projection
                .indexed_terminal_effects
                .iter()
                .take(index)
                .any(|prior| prior == id)
                || !self
                    .projection
                    .effects
                    .get(id)
                    .is_some_and(|effect| settled_effect(&effect.status))
            {
                return Err(Error::Invalid(
                    "snapshot indexed terminal effects are invalid".into(),
                ));
            }
        }
        let mut previous = None;
        for (index, event) in self.events.iter().enumerate() {
            verifier.verify_event(event, work)?;
            for prior in &self.events[..index] {
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                }
                if prior.operation_id == event.operation_id {
                    return Err(Error::Invalid("snapshot event suffix is invalid".into()));
                }
            }
            if event.revision == 0
                || event.revision > self.revision
                || previous
                    .is_some_and(|revision: u64| revision.checked_add(1) != Some(event.revision))
            {
                return Err(Error::Invalid("snapshot event suffix is invalid".into()));
            }
            previous = Some(event.revision);
        }
        if previous.unwrap_or(0) != self.revision {
            return Err(Error::Invalid(
                "snapshot event suffix does not reach its head".into(),
            ));
        }
        if self
            .projection
            .active_extension_revision
            .is_some_and(|revision| revision == 0 || revision > self.revision)
        {
            return Err(Error::Invalid(
                "snapshot extension activation revision is invalid".into(),
            ));
        }
        for (extension, expected) in &self.projection.bindings {
            let binding = schemas.pinned_binding(&extension.name, extension.version, None, work)?;
            if crate::contract::canonical_json_digest_with_work(
                binding,
                crate::store::default_projection_read_limits().maximum_bytes,
                work,
            )? != *expected
            {
                return Err(Error::Conflict(
                    "snapshot extension binding mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}

impl<C, B> Projection<C, B> {
    pub(in crate::core) fn copy<N, D>(&self, conversation: D, bindings: N) -> Projection<D, N> {
        Projection {
            lifecycle: self.lifecycle,
            active_extensions: self.active_extensions.clone(),
            active_extension_revision: self.active_extension_revision,
            extension_states: self.extension_states.clone(),
            configured_extensions: self.configured_extensions.clone(),
            active_configurations: self.active_configurations.clone(),
            effects: self.effects.clone(),
            indexed_terminal_effects: self.indexed_terminal_effects.clone(),
            effects_archived: self.effects_archived,
            forks: self.forks.clone(),
            published_merges: self.published_merges.clone(),
            conversation,
            latest_context_checkpoint: self.latest_context_checkpoint.clone(),
            interactions: self.interactions.clone(),
            bindings,
        }
    }
    pub(in crate::core) fn with_bindings<N>(self, bindings: N) -> Projection<C, N> {
        Projection {
            lifecycle: self.lifecycle,
            active_extensions: self.active_extensions,
            active_extension_revision: self.active_extension_revision,
            extension_states: self.extension_states,
            configured_extensions: self.configured_extensions,
            active_configurations: self.active_configurations,
            effects: self.effects,
            indexed_terminal_effects: self.indexed_terminal_effects,
            effects_archived: self.effects_archived,
            forks: self.forks,
            published_merges: self.published_merges,
            conversation: self.conversation,
            latest_context_checkpoint: self.latest_context_checkpoint,
            interactions: self.interactions,
            bindings,
        }
    }
}
impl Default for Projection<ConversationState, ()> {
    fn default() -> Self {
        Self {
            lifecycle: LifecycleState::Pending,
            active_extensions: Vec::new(),
            active_extension_revision: None,
            extension_states: BTreeMap::new(),
            configured_extensions: BTreeMap::new(),
            active_configurations: Vec::new(),
            effects: BTreeMap::new(),
            indexed_terminal_effects: VecDeque::new(),
            effects_archived: false,
            forks: BTreeMap::new(),
            published_merges: BTreeSet::new(),
            conversation: ConversationState::default(),
            latest_context_checkpoint: None,
            interactions: BTreeMap::new(),
            bindings: (),
        }
    }
}

impl Snapshot {
    pub(super) fn require_work(&self, work: &Option<&mut dyn HistoryIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_allocation.is_some() && !work.as_deref().is_some_and(copying::is_native) {
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "native snapshot requires explicit native work",
            ));
        }
        let _ = work;
        Ok(())
    }
    /// Pure capacity witness for splitting a predecoded outer DTO's original guard.
    pub(super) fn fields_owned_bytes(&self) -> Result<u64> {
        let mut bytes = owned_vector(&self.projection.bindings, |(extension, _)| {
            Ok(extension.name.capacity() as u64)
        })?;
        self.projection.visit(
            &self.authority,
            self.events.iter(),
            self.events.capacity(),
            |key, owned| {
                if !matches!(key, NativeComponentKey::ConversationMessage(_)) {
                    bytes = owned_sum([bytes, owned])?;
                }
                Ok(())
            },
        )?;
        Ok(bytes)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn predecoded_native_owned_bytes(&self) -> Result<u64> {
        let conversation = &self.projection.conversation;
        ownership::owned_sum([
            self.fields_owned_bytes()?,
            conversation.native_unclaimed_message_buffer_bytes()?,
            conversation.native_unclaimed_archived_prefix_bytes()?,
            conversation.native_unclaimed_cache_bytes()?,
            conversation
                .messages()
                .iter()
                .try_fold(0, |bytes, message| {
                    ownership::owned_sum([bytes, ownership::owned_message(message)?])
                })?,
        ])
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_custody(
        &mut self,
        allocation: acyclic_fs::NativeAllocation,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.native_allocation = Some(allocation);
        self.adopt_unclaimed_native_conversation_custody(work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn adopt_unclaimed_native_conversation_custody(
        &mut self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let bytes = self
            .projection
            .conversation
            .native_unclaimed_message_buffer_bytes()?;
        if bytes != 0 {
            let shell = self
                .native_allocation
                .as_mut()
                .expect("installed snapshot custody")
                .split_off(bytes)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            self.projection
                .conversation
                .install_native_message_buffer(shell);
        }
        let archive_bytes = self
            .projection
            .conversation
            .native_unclaimed_archived_prefix_bytes()?;
        if archive_bytes != 0 {
            let archive = self
                .native_allocation
                .as_mut()
                .expect("installed snapshot custody")
                .split_off(archive_bytes)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            self.projection
                .conversation
                .install_native_archived_prefix(archive);
        }
        let cache_bytes = self
            .projection
            .conversation
            .native_unclaimed_cache_bytes()?;
        if cache_bytes != 0 {
            let cache = self
                .native_allocation
                .as_mut()
                .expect("installed snapshot custody")
                .split_off(cache_bytes)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            self.projection
                .conversation
                .install_native_preclaimed_caches(cache, work)?;
        }
        Ok(())
    }

    /// Decodes one admitted checkpoint and its existing conversation cache.
    pub fn decode(
        bytes: &[u8],
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut admitted = 0;
        let admission = crate::contract::snapshot_json_admission(
            bytes,
            crate::contract::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 256,
            },
            &mut |charge| {
                if let crate::contract::JsonCodecCharge::Admission {
                    maximum_owned_bytes,
                    ..
                } = charge
                {
                    admitted = maximum_owned_bytes;
                }
                match quota.work().as_deref_mut() {
                    Some(io) => io.codec_charge(charge),
                    None => Ok(()),
                }
            },
        );
        quota.adopt(admitted)?;
        let (_, conversation_allowance) = admission?;
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        decoder.disable_recursion_limit();
        let decoded =
            Snapshot::<crate::conversation::ConversationStateWire>::deserialize(&mut decoder)
                .and_then(|value| decoder.end().map(|()| value));
        let wire = match decoded {
            Ok(snapshot) => snapshot,
            Err(source) => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if quota.work().as_deref().is_some_and(copying::is_native) {
                    let bytes = quota.retained();
                    let custody = quota
                        .work()
                        .as_deref_mut()
                        .expect("native decoder observer")
                        .take_native_custody(bytes)?;
                    quota.transfer();
                    return Err(Error::NativeDecode {
                        code: crate::wire::ErrorCode::Storage,
                        source,
                        custody: Some(custody),
                    });
                }
                return Err(Error::Storage(source.to_string()));
            }
        };
        quota.relinquish(conversation_allowance)?;
        let Snapshot {
            format_version,
            authority,
            revision,
            events,
            projection,
            state_digest,
            attestation,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                native_allocation: _,
        } = wire;
        let Projection {
            conversation,
            lifecycle,
            active_extensions,
            active_extension_revision,
            extension_states,
            configured_extensions,
            active_configurations,
            effects,
            indexed_terminal_effects,
            effects_archived,
            forks,
            published_merges,
            latest_context_checkpoint,
            interactions,
            bindings,
        } = projection;
        let (conversation, retained) =
            ConversationState::from_wire(conversation, maximum_bytes, quota.work())?;
        quota.adopt(retained)?;
        let mut snapshot = Snapshot {
            format_version,
            authority,
            revision,
            events,
            state_digest,
            attestation,
            projection: Projection {
                conversation,
                lifecycle,
                active_extensions,
                active_extension_revision,
                extension_states,
                configured_extensions,
                active_configurations,
                effects,
                indexed_terminal_effects,
                effects_archived,
                forks,
                published_merges,
                latest_context_checkpoint,
                interactions,
                bindings,
            },
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_allocation: None,
        };
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if quota.work().as_deref().is_some_and(copying::is_native) {
            quota.replace(snapshot.predecoded_native_owned_bytes()?)?;
            let bytes = quota.retained();
            let allocation = quota
                .work()
                .as_deref_mut()
                .expect("native decoder observer")
                .take_native_custody(bytes)?;
            quota.transfer();
            snapshot.install_native_custody(allocation, quota.work())?;
        }
        quota.transfer();
        Ok(snapshot)
    }
}
