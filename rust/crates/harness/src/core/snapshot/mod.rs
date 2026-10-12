//! Authenticated reducer checkpoints and bounded wire decoding.
use super::*;
mod capture;
mod entries;
mod restoration;
#[cfg(test)]
mod tests;

/// Versioned, issuer-authenticated accelerator; Stream events remain authoritative.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
        crate::contract::canonical_json_digest(
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
            if crate::contract::canonical_json_digest(
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
