//! Authenticated checkpoints of the reducer projection.
use super::*;
mod capture;
mod restoration;
mod native;
#[cfg(test)]
mod tests;

/// Versioned, issuer-authenticated accelerator; Stream events remain authoritative.
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
pub(super) struct Projection<C = ConversationState> {
    lifecycle: LifecycleState,
    active_extensions: Vec<ExtensionDependency>,
    active_extension_revision: Option<u64>,
    extension_states: BTreeMap<String, (EventReference, ExtensionRecord)>,
    configured_extensions: Vec<(ExtensionDependency, ExtensionConfiguration)>,
    active_configurations: Vec<ExtensionConfiguration>,
    effects: BTreeMap<EffectId, EffectState>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    indexed_terminal_effects: VecDeque<EffectId>,
    #[serde(default, skip_serializing_if = "is_false")]
    effects_archived: bool,
    forks: Vec<(Authority, ForkSeed)>,
    published_merges: BTreeSet<(String, Vec<u8>)>,
    conversation: C,
    latest_context_checkpoint: Option<FileRef>,
    interactions: BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
    bindings: Vec<(ExtensionDependency, [u8; 32])>,
}

fn is_false(value: &bool) -> bool { !*value }
impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.format_version == other.format_version && self.authority == other.authority
            && self.revision == other.revision && self.events == other.events
            && self.projection == other.projection && self.state_digest == other.state_digest
            && self.attestation == other.attestation
    }
}
impl Snapshot {
    pub(crate) const LOGICAL_SCHEMA_SLOT_BYTES: u64 = super::inventory::CloneInventory::SLOT;
    fn digest_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<[u8; 32]> {
        crate::contract::canonical_json_digest_with_work(&(
            self.format_version, &self.authority, self.revision, &self.events, &self.projection,
        ), crate::store::default_projection_read_limits().maximum_bytes, work)
    }
    fn verify(&self, verifier: &AuthorityVerifier, schemas: &SchemaRegistry,
        work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        let bytes = ownership::owned_buffer::<OperationId>(self.events.len())?;
        let mut scratch = crate::wire_codec::CodecQuota::new(work, bytes)?;
        if self.format_version != 6 { return Err(Error::Unsupported(format!("snapshot format {}", self.format_version))); }
        verifier.verify_audience(&self.authority, scratch.work())?;
        if self.digest_with_work(scratch.work())? != self.state_digest { return Err(Error::Invalid("snapshot digest mismatch".into())); }
        if verifier.canonical_proof(b"harness/v6/reducer-checkpoint\0",
            &(&verifier.id, &verifier.audience, self.state_digest), scratch.work())? != self.attestation {
            return Err(Error::Unauthorized("snapshot admission attestation is invalid".into()));
        }
        for (index, id) in self.projection.indexed_terminal_effects.iter().enumerate() {
            if self.projection.indexed_terminal_effects.iter().take(index).any(|prior| prior == id)
                || !self.projection.effects.get(id).is_some_and(|effect| settled_effect(&effect.status)) {
                return Err(Error::Invalid("snapshot indexed terminal effects are invalid".into()));
            }
        }
        let mut identities = BTreeSet::new();
        let mut previous = None;
        for event in &self.events {
            verifier.verify_event(event, scratch.work())?;
            if event.revision == 0 || event.revision > self.revision
                || previous.is_some_and(|revision: u64| revision.checked_add(1) != Some(event.revision))
                || !identities.insert(event.operation_id) {
                return Err(Error::Invalid("snapshot event suffix is invalid".into()));
            }
            previous = Some(event.revision);
        }
        if previous.unwrap_or(0) != self.revision { return Err(Error::Invalid("snapshot event suffix does not reach its head".into())); }
        if self.projection.active_extension_revision.is_some_and(|revision| revision == 0 || revision > self.revision) {
            return Err(Error::Invalid("snapshot extension activation revision is invalid".into()));
        }
        for (extension, expected) in &self.projection.bindings {
            let binding = schemas.pinned_binding(&extension.name, extension.version, None, scratch.work())?;
            if crate::contract::canonical_json_digest_with_work(binding,
                crate::store::default_projection_read_limits().maximum_bytes, scratch.work())? != *expected {
                return Err(Error::Conflict("snapshot extension binding mismatch".into()));
            }
        }
        Ok(())
    }
}
