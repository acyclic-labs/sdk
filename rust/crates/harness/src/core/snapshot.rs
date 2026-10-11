//! Authenticated restoration of the reducer projection, without transition replay.

use super::{
    Authority, AuthorityVerifier, EffectState, Event, EventReference, ExtensionConfiguration,
    ExtensionDependency, ExtensionRecord, LifecycleState, Reducer, SchemaRegistry,
};
use crate::{
    EffectId, Error, Result,
    conversation::{ConversationState, FileRef},
    fork::ForkSeed,
    interaction::{InteractionResolution, InteractionTicket},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use crate::store::history::HistoryIo;

fn canonical_digest_with_work<T: Serialize>(
    value: &T,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<[u8; 32]> {
    if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
    let canonical = crate::store::history::canonical_bytes_with_work(
        value,
        crate::store::default_projection_read_limits().maximum_bytes,
        work,
    )?;
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes(canonical.len() as u64)?;
    }
    let digest = *blake3::hash(&canonical).as_bytes();
    let capacity = canonical.capacity() as u64;
    drop(canonical);
    if let Some(work) = work.as_deref_mut() { work.release_bytes(capacity)?; }
    Ok(digest)
}

// Borrowed closed-schema inventory. These conservative DTO slot reservations
// are quota custody, not an exact allocator/node/RSS measurement.
#[derive(Default)]
struct CloneInventory<'w> {
    bytes: u64,
    work: Option<&'w mut dyn HistoryIo>,
    failure: Option<Error>,
    depth: u16,
}

// A zero-allocation serde carrier; recover the original owner/native failure
// below rather than formatting it into a new serde diagnostic.
#[derive(Debug)]
struct InventoryError;
impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("closed clone inventory rejected") }
}
impl std::error::Error for InventoryError {}
impl serde::ser::Error for InventoryError {
    fn custom<T: std::fmt::Display>(_: T) -> Self { Self }
}

impl CloneInventory<'_> {
    fn visit(&mut self) -> std::result::Result<(), InventoryError> {
        if let Some(work) = self.work.as_deref_mut() {
            if let Err(error) = work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }) {
                self.failure = Some(error);
                return Err(InventoryError);
            }
        }
        Ok(())
    }
    fn add(&mut self, bytes: u64) -> std::result::Result<(), InventoryError> {
        self.visit()?;
        self.bytes = self.bytes.checked_add(bytes).ok_or(InventoryError)?;
        Ok(())
    }
    const SLOT: u64 = {
        let sizes = [
            std::mem::size_of::<Reducer>(), std::mem::size_of::<Snapshot>(),
            std::mem::size_of::<Projection>(), std::mem::size_of::<Event>(),
            std::mem::size_of::<super::EventPayload>(), std::mem::size_of::<super::EffectStatus>(),
            std::mem::size_of::<EffectState>(), std::mem::size_of::<super::ExtensionBinding>(),
            std::mem::size_of::<super::ExtensionAdmission>(), std::mem::size_of::<super::Command>(),
            std::mem::size_of::<super::Action>(), std::mem::size_of::<super::Scope>(),
            std::mem::size_of::<super::RecordedScope>(), std::mem::size_of::<crate::Capabilities>(),
            std::mem::size_of::<super::ExtensionStateMigration>(), std::mem::size_of::<ExtensionRecord>(),
            std::mem::size_of::<ExtensionConfiguration>(), std::mem::size_of::<ExtensionDependency>(),
            std::mem::size_of::<Authority>(), std::mem::size_of::<EventReference>(),
            std::mem::size_of::<ForkSeed>(), std::mem::size_of::<crate::fork::ForkSelection>(),
            std::mem::size_of::<crate::fork::ForkOmission>(), std::mem::size_of::<crate::fork::CapturedResource>(),
            std::mem::size_of::<crate::fork::ResourceRevision>(), std::mem::size_of::<crate::fork::SharedGrant>(),
            std::mem::size_of::<crate::fork::ReferenceGrant>(), std::mem::size_of::<crate::fork::SummaryForkCapture>(),
            std::mem::size_of::<crate::fork::AttestedBoundary>(), std::mem::size_of::<crate::merge::ProjectMergeReceipt>(),
            std::mem::size_of::<crate::conversation::ConversationMessage>(), std::mem::size_of::<ConversationState>(),
            std::mem::size_of::<crate::conversation::ModelContextSelection>(), std::mem::size_of::<FileRef>(),
            std::mem::size_of::<crate::conversation::VolumeRef>(), std::mem::size_of::<crate::conversation::FileDescriptor>(),
            std::mem::size_of::<InteractionTicket>(), std::mem::size_of::<InteractionResolution>(),
            std::mem::size_of::<crate::resources::ResourceRef>(), std::mem::size_of::<crate::resources::ProviderRef>(),
            std::mem::size_of::<(String, serde_json::Value)>(), std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<crate::runtime::TaskAdmissionRecord>(),
            std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>(),
            std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>(),
            std::mem::size_of::<crate::resource_preparation::ResourceCreationOrigin>(),
            std::mem::size_of::<crate::workflow::WorkflowCommand>(),
            std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>(),
        ];
        let mut maximum = 0;
        let mut index = 0;
        while index < sizes.len() { if sizes[index] > maximum { maximum = sizes[index]; } index += 1; }
        maximum as u64
    };
    fn slot(&mut self) -> std::result::Result<(), InventoryError> { self.add(Self::SLOT) }
    fn enter(&mut self) -> std::result::Result<(), InventoryError> {
        self.visit()?;
        if self.work.is_some() && self.depth >= 256 { return Err(InventoryError); }
        self.depth = self.depth.checked_add(1).ok_or(InventoryError)?;
        Ok(())
    }
    fn leave(&mut self) { self.depth -= 1; }
}

macro_rules! inventory_scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method(self, _: $ty) -> std::result::Result<(), Self::Error> { self.visit() }
    )*};
}
impl<'a, 'w> serde::Serializer for &'a mut CloneInventory<'w> {
    type Ok = ();
    type Error = InventoryError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;
    inventory_scalar!(serialize_bool(bool), serialize_i8(i8), serialize_i16(i16),
        serialize_i32(i32), serialize_i64(i64), serialize_i128(i128), serialize_u8(u8),
        serialize_u16(u16), serialize_u32(u32), serialize_u64(u64), serialize_u128(u128),
        serialize_f32(f32), serialize_f64(f64), serialize_char(char));
    fn serialize_str(self, value: &str) -> std::result::Result<(), Self::Error> { self.add(value.len() as u64) }
    fn serialize_bytes(self, value: &[u8]) -> std::result::Result<(), Self::Error> { self.add(value.len() as u64) }
    fn serialize_none(self) -> std::result::Result<(), Self::Error> { self.visit() }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> std::result::Result<(), Self::Error> { self.visit()?; value.serialize(self) }
    fn serialize_unit(self) -> std::result::Result<(), Self::Error> { self.visit() }
    fn serialize_unit_struct(self, _: &'static str) -> std::result::Result<(), Self::Error> { self.visit() }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> std::result::Result<(), Self::Error> { self.visit() }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, value: &T) -> std::result::Result<(), Self::Error> { self.visit()?; value.serialize(self) }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, value: &T) -> std::result::Result<(), Self::Error> { self.slot()?; value.serialize(self) }
    fn serialize_seq(self, _: Option<usize>) -> std::result::Result<Self, Self::Error> { self.enter()?; Ok(self) }
    fn serialize_tuple(self, _: usize) -> std::result::Result<Self, Self::Error> { self.enter()?; Ok(self) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> std::result::Result<Self, Self::Error> { self.enter()?; Ok(self) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> std::result::Result<Self, Self::Error> { self.slot()?; self.enter()?; Ok(self) }
    fn serialize_map(self, _: Option<usize>) -> std::result::Result<Self, Self::Error> { self.enter()?; Ok(self) }
    fn serialize_struct(self, _: &'static str, _: usize) -> std::result::Result<Self, Self::Error> { self.slot()?; self.enter()?; Ok(self) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> std::result::Result<Self, Self::Error> { self.slot()?; self.enter()?; Ok(self) }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, value: &T) -> std::result::Result<(), Self::Error> {
        struct Count<'a, 'w>(&'a mut CloneInventory<'w>);
        impl std::fmt::Write for Count<'_, '_> {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                self.0.add(value.len() as u64).map_err(|_| std::fmt::Error)
            }
        }
        std::fmt::write(&mut Count(self), format_args!("{value}")).map_err(|_| InventoryError)
    }
}
macro_rules! inventory_sequence {
    ($trait:ident, $method:ident) => {
        impl serde::ser::$trait for &mut CloneInventory<'_> {
            type Ok = ();
            type Error = InventoryError;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> std::result::Result<(), Self::Error> {
                self.slot()?; value.serialize(&mut **self)
            }
            fn end(self) -> std::result::Result<(), Self::Error> { self.leave(); Ok(()) }
        }
    };
}
inventory_sequence!(SerializeSeq, serialize_element);
inventory_sequence!(SerializeTuple, serialize_element);
inventory_sequence!(SerializeTupleStruct, serialize_field);
inventory_sequence!(SerializeTupleVariant, serialize_field);
impl serde::ser::SerializeMap for &mut CloneInventory<'_> {
    type Ok = ();
    type Error = InventoryError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> std::result::Result<(), Self::Error> { self.slot()?; key.serialize(&mut **self) }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> std::result::Result<(), Self::Error> { self.slot()?; value.serialize(&mut **self) }
    fn end(self) -> std::result::Result<(), Self::Error> { self.leave(); Ok(()) }
}
macro_rules! inventory_struct {
    ($trait:ident) => {
        impl serde::ser::$trait for &mut CloneInventory<'_> {
            type Ok = ();
            type Error = InventoryError;
            fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> std::result::Result<(), Self::Error> { self.visit()?; value.serialize(&mut **self) }
            fn end(self) -> std::result::Result<(), Self::Error> { self.leave(); Ok(()) }
        }
    };
}
inventory_struct!(SerializeStruct);
inventory_struct!(SerializeStructVariant);

fn source_inventory<T: Serialize + ?Sized>(source: &T) -> Result<u64> {
    let mut inventory = CloneInventory::default();
    source.serialize(&mut inventory).map_err(|_| Error::Invalid("closed clone inventory rejected".into()))?;
    Ok(inventory.bytes)
}
pub(crate) fn event_clone_reservation(event: &Event) -> Result<u64> { source_inventory(event) }
pub(crate) fn scope_clone_owned_bytes(scope: &super::Scope) -> Result<u64> { source_inventory(scope) }
pub(crate) fn fork_seed_clone_reservation(seed: &ForkSeed) -> Result<u64> { source_inventory(seed) }

pub(crate) fn effect_clone_reservation(effect: &EffectState) -> Result<u64> {
    source_inventory(effect)
}
pub(crate) fn command_clone_reservation(command: &super::Command) -> Result<u64> {
    // Cover the original transition result and event construction while both
    // remain live, including their fixed digests and original recorded scope.
    source_inventory(command)?.checked_mul(2)
        .ok_or_else(|| Error::Invalid("command clone reservation overflows".into()))
}

#[cfg(feature = "filesystem")]
pub(crate) fn resource_creation_clone_reservation(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    origin: &crate::resource_preparation::ResourceCreationOrigin,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let mut inventory = CloneInventory { work: work.as_deref_mut().map(|work| work as &mut dyn HistoryIo), ..Default::default() };
    let result = (request, proof, origin).serialize(&mut inventory);
    if let Some(error) = inventory.failure.take() { return Err(error); }
    result.map_err(|_| Error::Invalid("closed creator capture exceeds inventory bounds".into()))?;
    let bytes = inventory.bytes;
    drop(inventory);
    if let Some(work) = work.as_deref_mut() { work.retain_owned(bytes)?; }
    Ok(bytes)
}

/// Versioned, issuer-authenticated accelerator; Stream events remain authoritative.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Snapshot format version. Earlier replay-based formats are unsupported.
    pub format_version: u32,
    /// Aggregate represented by the snapshot.
    pub authority: Authority,
    /// Last included event revision.
    pub revision: u64,
    /// Retained canonical event cache, also covered by the attestation.
    pub events: Vec<Event>,
    projection: Projection,
    /// Canonical digest of the complete state and retained cache.
    pub state_digest: [u8; 32],
    /// Domain-separated issuer proof over the digest and aggregate identity.
    pub attestation: [u8; 32],
}

// Keep configuration and fork maps as ordered entry arrays: their structured
// keys cannot be represented by JSON object keys. UUID-keyed maps remain maps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Projection<C = ConversationState> {
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
    // Rebuild the retry map from the authenticated cache, without applying
    // payloads. This avoids storing every retained Event a second time.
    bindings: Vec<(ExtensionDependency, [u8; 32])>,
}

/// The original snapshot DTO, deferring only its observed conversation rebuild.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotWire {
    format_version: u32,
    authority: Authority,
    revision: u64,
    events: Vec<Event>,
    projection: Projection<crate::conversation::ConversationStateWire>,
    state_digest: [u8; 32],
    attestation: [u8; 32],
}

// Deferring reconstruction removes caches; it never enlarges the guarded DTO.
const _: () = {
    assert!(std::mem::size_of::<SnapshotWire>() <= std::mem::size_of::<Snapshot>());
    assert!(std::mem::size_of::<Projection<crate::conversation::ConversationStateWire>>()
        <= std::mem::size_of::<Projection>());
};

impl SnapshotWire {
    pub(crate) fn into_snapshot_with_work(
        self,
        maximum_encoded_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        let Projection {
            lifecycle, active_extensions, active_extension_revision, extension_states,
            configured_extensions, active_configurations, effects, indexed_terminal_effects,
            effects_archived, forks, published_merges, conversation, latest_context_checkpoint,
            interactions, bindings,
        } = self.projection;
        let (conversation, cache_bytes) = ConversationState::from_wire_with_work(
            conversation, maximum_encoded_bytes, work)?;
        Ok((Snapshot {
            format_version: self.format_version,
            authority: self.authority,
            revision: self.revision,
            events: self.events,
            projection: Projection {
                lifecycle, active_extensions, active_extension_revision, extension_states,
                configured_extensions, active_configurations, effects, indexed_terminal_effects,
                effects_archived, forks, published_merges, conversation, latest_context_checkpoint,
                interactions, bindings,
            },
            state_digest: self.state_digest,
            attestation: self.attestation,
        }, cache_bytes))
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Snapshot {
    pub(crate) const LOGICAL_SCHEMA_SLOT_BYTES: u64 = CloneInventory::SLOT;

    /// One original Serde parse after the closed snapshot admission has succeeded.
    pub(crate) fn decode_admitted_wire(bytes: &[u8]) -> serde_json::Result<SnapshotWire> {
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        decoder.disable_recursion_limit();
        let value = SnapshotWire::deserialize(&mut decoder)?;
        decoder.end()?;
        Ok(value)
    }
}

#[derive(Serialize)]
struct ReducerCloneFields<'a> {
    authority: &'a Authority,
    verifier_id: &'a str,
    verifier_audience: &'a Authority,
    schemas: &'a BTreeMap<(String, u32), super::ExtensionBinding>,
    active_extensions: &'a [ExtensionDependency],
    extension_states: &'a BTreeMap<String, (EventReference, ExtensionRecord)>,
    configured_extensions: &'a BTreeMap<ExtensionDependency, ExtensionConfiguration>,
    active_configurations: &'a [ExtensionConfiguration],
    events: &'a VecDeque<Event>,
    operation_positions: &'a BTreeMap<crate::OperationId, u64>,
    effects: &'a BTreeMap<EffectId, EffectState>,
    indexed_terminal_effects: &'a VecDeque<EffectId>,
    forks: &'a BTreeMap<Authority, ForkSeed>,
    published_merges: &'a BTreeSet<(String, Vec<u8>)>,
    conversation: &'a ConversationState,
    latest_context_checkpoint: &'a Option<FileRef>,
    interactions: &'a BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
}

impl Reducer {
    fn clone_reservation(&self) -> Result<u64> {
        // Registry schema Values, verifier Strings, canonical event deque and
        // retry/cache maps are original owned state, not just the wire view.
        let fields = ReducerCloneFields {
            authority: &self.authority,
            verifier_id: &self.authority_verifier.id,
            verifier_audience: &self.authority_verifier.audience,
            schemas: &self.schemas.schemas,
            active_extensions: &self.active_extensions,
            extension_states: &self.extension_states,
            configured_extensions: &self.configured_extensions,
            active_configurations: &self.active_configurations,
            events: &self.events,
            operation_positions: &self.operation_positions,
            effects: &self.effects,
            indexed_terminal_effects: &self.indexed_terminal_effects,
            forks: &self.forks,
            published_merges: &self.published_merges,
            conversation: &self.conversation,
            latest_context_checkpoint: &self.latest_context_checkpoint,
            interactions: &self.interactions,
        };
        source_inventory(&fields)?
            .checked_add(self.conversation.clone_cache_logical_bytes()?)
            .ok_or_else(|| Error::Invalid("reducer clone reservation overflows".into()))
    }

    /// Keeps logical custody through the original preview and snapshot clones.
    pub(crate) fn publication_snapshot_with_work(
        &self,
        event: &Event,
        maximum_events: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        let preview_reservation = if work.is_some() {
            // Original apply holds the input event, its cache clone and its
            // payload projection clone concurrently (ForkPublished stores seed).
            self.clone_reservation()?.checked_add(
                event_clone_reservation(event)?.checked_mul(3)
                    .ok_or_else(|| Error::Invalid("publication event clone reservation overflows".into()))?,
            ).ok_or_else(|| Error::Invalid("publication preview reservation overflows".into()))?
        } else { 0 };
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.admit_owned(preview_reservation)?;
            observer.retain_owned(preview_reservation)?;
        }
        let result = (|| {
            let mut preview = self.clone();
            preview.apply_committed_with_work(event.clone(), work)?;
            preview.mark_indexed_terminal_effect(event)?;
            preview.snapshot_with_event_limit_with_work(maximum_events, work)
        })();
        if let Some(observer) = work.as_deref_mut() {
            observer.release_owned(preview_reservation)?;
        }
        result
    }

    /// Captures issuer-authenticated state for direct projection restoration.
    /// Closed checkpoint-covered conversation prefixes are represented by their
    /// authenticated logical cut and digest. Terminal projections still need cold retention.
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.snapshot_with_event_limit(self.events.len())
    }

    /// Captures the same authenticated projection with a bounded retry suffix.
    /// A nonempty projection must retain its head event for canonical binding.
    pub fn snapshot_with_event_limit(&self, maximum_events: usize) -> Result<Snapshot> {
        self.snapshot_with_event_limit_with_work(maximum_events, &mut None)
            .map(|(snapshot, _)| snapshot)
    }

    pub(crate) fn snapshot_with_event_limit_with_work(
        &self,
        maximum_events: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        if maximum_events == 0 && self.revision != 0 {
            return Err(Error::Invalid("snapshot must retain its head event".into()));
        }
        let reservation = if work.is_some() {
            // The original map.clone().into_iter().collect() briefly owns both
            // map payload and destination vector slots during capture.
            self.clone_reservation()?.checked_mul(2)
                .ok_or_else(|| Error::Invalid("snapshot clone reservation overflows".into()))?
        } else { 0 };
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.admit_owned(reservation)?;
            observer.retain_owned(reservation)?;
        }
        let result = (|| {
        let conversation = self.checkpointed_conversation()?;
        let projection = Projection {
            lifecycle: self.lifecycle,
            active_extensions: self.active_extensions.clone(),
            active_extension_revision: self.active_extension_revision,
            extension_states: self.extension_states.clone(),
            configured_extensions: self.configured_extensions.clone().into_iter().collect(),
            active_configurations: self.active_configurations.clone(),
            effects: self.effects.clone(),
            indexed_terminal_effects: self.indexed_terminal_effects.clone(),
            effects_archived: self.effects_archived,
            forks: self.forks.clone().into_iter().collect(),
            published_merges: self.published_merges.clone(),
            conversation,
            latest_context_checkpoint: self.latest_context_checkpoint.clone(),
            interactions: self.interactions.clone(),
            bindings: registry_bindings_with_work(&self.schemas, work)?,
        };
        let mut snapshot = Snapshot {
            format_version: 6,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self
                .events
                .iter()
                .skip(self.events.len().saturating_sub(maximum_events))
                .cloned()
                .collect(),
            projection,
            state_digest: [0; 32],
            attestation: [0; 32],
        };
        snapshot.state_digest = snapshot.digest_with_work(work)?;
        snapshot.attestation = self.authority_verifier.attest_snapshot_with_work(&snapshot, work)?;
        Ok(snapshot)
        })();
        if result.is_err() {
            if let Some(observer) = work.as_deref_mut() { observer.release_owned(reservation)?; }
        }
        result.map(|snapshot| (snapshot, reservation))
    }

    fn resident_checkpoint_selection(&self) -> Option<&crate::conversation::ModelContextSelection> {
        self.events
            .iter()
            .rev()
            .find_map(|event| match &event.payload {
                super::EventPayload::ModelContextSelected { selection }
                    if selection.checkpoint.as_ref() == self.latest_context_checkpoint.as_ref()
                        && selection.checkpoint.is_some() =>
                {
                    Some(selection)
                }
                _ => None,
            })
    }

    fn checkpointed_conversation(&self) -> Result<ConversationState> {
        let checkpoint_selection = self.resident_checkpoint_selection();
        match checkpoint_selection {
            Some(selection) => self.conversation.checkpointed_suffix(selection),
            None => Ok(self.conversation.clone()),
        }
    }

    pub(crate) fn compact_conversation_projection(&mut self) -> Result<()> {
        let selection = self.resident_checkpoint_selection();
        let Some(selection) = selection else {
            return Ok(());
        };
        if selection.conversation_revision <= self.conversation.resident_after_sequence()
            || self.conversation.unresolved_user().is_some()
        {
            return Ok(());
        }
        self.conversation = self.conversation.checkpointed_suffix(selection)?;
        Ok(())
    }

    /// Restores authenticated state without invoking any historical transition.
    /// Original registry bindings must still be installed exactly; additional
    /// bindings may be installed without invalidating the checkpoint.
    pub fn restore(
        snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Result<Self> {
        Self::restore_with_work(snapshot, authority_verifier, schemas, &mut None)
    }

    pub(crate) fn restore_with_work(
        snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        snapshot.verify(&authority_verifier, &schemas, work)?;
        let operation_positions = snapshot
            .events
            .iter()
            .map(|event| (event.operation_id, event.revision))
            .collect();
        let projection = snapshot.projection;
        Ok(Self {
            authority: snapshot.authority,
            authority_verifier,
            schemas,
            revision: snapshot.revision,
            lifecycle: projection.lifecycle,
            active_extensions: projection.active_extensions,
            active_extension_revision: projection.active_extension_revision,
            extension_states: projection.extension_states,
            configured_extensions: projection.configured_extensions.into_iter().collect(),
            active_configurations: projection.active_configurations,
            events: snapshot.events.into(),
            resident_event_limit: usize::MAX,
            operation_positions,
            effects: projection.effects,
            indexed_terminal_effects: projection.indexed_terminal_effects,
            effects_archived: projection.effects_archived,
            resident_terminal_effect_limit: usize::MAX,
            forks: projection.forks.into_iter().collect(),
            published_merges: projection.published_merges,
            conversation: projection.conversation,
            latest_context_checkpoint: projection.latest_context_checkpoint,
            interactions: projection.interactions,
        })
    }
}

impl Snapshot {
    fn digest_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<[u8; 32]> {
        canonical_digest_with_work(&(
            self.format_version,
            &self.authority,
            self.revision,
            &self.events,
            &self.projection,
        ), work)
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
        verifier.verify_audience(&self.authority)?;
        if self.digest_with_work(work)? != self.state_digest {
            return Err(Error::Invalid("snapshot digest mismatch".into()));
        }
        if verifier.attest_snapshot_with_work(self, work)? != self.attestation {
            return Err(Error::Unauthorized(
                "snapshot admission attestation is invalid".into(),
            ));
        }
        let eligible: BTreeSet<_> = self.projection.indexed_terminal_effects.iter().collect();
        if eligible.len() != self.projection.indexed_terminal_effects.len()
            || eligible.iter().any(|id| {
                !self
                    .projection
                    .effects
                    .get(id)
                    .is_some_and(|state| super::settled_effect(&state.status))
            })
        {
            return Err(Error::Invalid(
                "snapshot indexed terminal effects are invalid".into(),
            ));
        }
        let mut identities = BTreeSet::new();
        let mut previous = None;
        for event in &self.events {
            verifier.verify_event_with_work(event, work)?;
            if event.revision == 0
                || event.revision > self.revision
                || previous
                    .is_some_and(|revision: u64| revision.checked_add(1) != Some(event.revision))
                || !identities.insert(event.operation_id)
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
            let binding = schemas.pinned_binding(&extension.name, extension.version, None)?;
            if canonical_digest_with_work(binding, work)? != *expected {
                return Err(Error::Conflict(
                    "snapshot extension binding mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}

impl AuthorityVerifier {
    fn attest_snapshot_with_work(
        &self,
        snapshot: &Snapshot,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<[u8; 32]> {
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        let canonical = crate::store::history::canonical_bytes_with_work(
            &(&self.id, &self.audience, snapshot.state_digest),
            crate::store::default_projection_read_limits().maximum_bytes,
            work,
        )?;
        if let Some(work) = work.as_deref_mut() {
            work.hashed_bytes(b"harness/v6/reducer-checkpoint\0".len() as u64 + 8 + canonical.len() as u64)?;
        }
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(b"harness/v6/reducer-checkpoint\0");
        hasher.update(&(canonical.len() as u64).to_le_bytes());
        hasher.update(&canonical);
        let attestation = *hasher.finalize().as_bytes();
        let capacity = canonical.capacity() as u64;
        drop(canonical);
        if let Some(work) = work.as_deref_mut() { work.release_bytes(capacity)?; }
        Ok(attestation)
    }
}

fn registry_bindings_with_work(
    schemas: &SchemaRegistry,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<(ExtensionDependency, [u8; 32])>> {
    schemas
        .schemas
        .iter()
        .map(|((name, version), binding)| {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.copy_bytes(name.len() as u64)?;
            }
            Ok((
                ExtensionDependency {
                    name: name.clone(),
                    version: *version,
                },
                canonical_digest_with_work(binding, work)?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AggregateKind, AuthorityIssuer, ExtensionForkPolicy};
    use serde_json::json;

    fn fixture() -> (Reducer, AuthorityIssuer) {
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "checkpoint-agent".into(),
        };
        let issuer = AuthorityIssuer::new("checkpoint-owner", [23; 32], authority.clone());
        (
            Reducer::new(authority, issuer.verifier(), SchemaRegistry::new()),
            issuer,
        )
    }

    #[test]
    fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
        let (reducer, issuer) = fixture();
        let original = reducer.snapshot()?;
        let mut forged = original.clone();
        forged.projection.lifecycle = LifecycleState::Completed;
        forged.state_digest = forged.digest_with_work(&mut None)?;
        assert!(matches!(
            Reducer::restore(forged, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        let mut forged_cache = original;
        forged_cache.revision = 50;
        forged_cache.state_digest = forged_cache.digest_with_work(&mut None)?;
        assert!(matches!(
            Reducer::restore(forged_cache, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
        let (reducer, issuer) = fixture();
        let snapshot = reducer.snapshot()?;
        for other in [
            AuthorityIssuer::new("checkpoint-owner", [24; 32], reducer.authority.clone()),
            AuthorityIssuer::new("other-owner", [23; 32], reducer.authority),
            AuthorityIssuer::new(
                "checkpoint-owner",
                [23; 32],
                Authority {
                    kind: AggregateKind::Agent,
                    id: "other-agent".into(),
                },
            ),
        ] {
            assert!(matches!(
                Reducer::restore(snapshot.clone(), other.verifier(), SchemaRegistry::new()),
                Err(Error::Unauthorized(_))
            ));
        }
        let mut old = snapshot;
        old.format_version = 2;
        assert!(matches!(
            Reducer::restore(old, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

    #[test]
    fn restore_pins_complete_installed_bindings_and_allows_new_namespaces() -> Result<()> {
        let (mut reducer, issuer) = fixture();
        reducer.schemas.register_configured(
            "example.pinned",
            1,
            json!({"type":"object"}),
            [8; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object"})),
        )?;
        let snapshot = reducer.snapshot()?;
        assert!(matches!(
            Reducer::restore(snapshot.clone(), issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        for edit in 0..5 {
            let mut changed = reducer.schemas.clone();
            let binding = changed
                .schemas
                .get_mut(&("example.pinned".into(), 1))
                .ok_or_else(|| Error::Storage("fixture binding missing".into()))?;
            match edit {
                0 => binding.schema = json!({"type":"string"}),
                1 => binding.implementation_digest = [9; 32],
                2 => binding.fork_policy = ExtensionForkPolicy::Reset,
                3 => {
                    binding.dependencies.insert(ExtensionDependency {
                        name: "example.dependency".into(),
                        version: 1,
                    });
                }
                _ => binding.configuration_schema = Some(json!({"type":"string"})),
            }
            assert!(matches!(
                Reducer::restore(snapshot.clone(), issuer.verifier(), changed),
                Err(Error::Conflict(_))
            ));
        }
        let mut expanded = reducer.schemas.clone();
        expanded.register(
            "example.additional",
            1,
            json!({"type":"string"}),
            [9; 32],
            ExtensionForkPolicy::Reset,
        )?;
        let restored = Reducer::restore(snapshot, issuer.verifier(), expanded.clone())?;
        reducer.schemas = expanded;
        assert_eq!(restored, reducer);
        Ok(())
    }
}
