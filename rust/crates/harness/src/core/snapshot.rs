//! Authenticated restoration of the reducer projection, without transition replay.
//! Native captures and deep copies require explicit pre-funded work and own their
//! reservations after the query ends. Restoration moves the original authenticated
//! fields into live component custody; the private wire is still parsed only once.

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

// A borrowed source-field walk, not JSON sizing or a cloned/encoded tree.
// Only the closed reducer/snapshot/event and preparation schemas use this witness.
// Reservations are logical quota custody, never allocator/RSS observations.
#[derive(Default)]
struct CloneInventory<'w> {
    bytes: u64,
    work: Option<&'w mut dyn HistoryIo>,
    failure: Option<Error>,
    depth: u16,
    slots: u64,
    slot_bytes: Option<u64>,
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
    fn slot(&mut self) -> std::result::Result<(), InventoryError> {
        self.slots = self.slots.checked_add(1).ok_or(InventoryError)?;
        self.add(self.slot_bytes.unwrap_or(Self::SLOT))
    }
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

pub(crate) fn source_inventory<T: Serialize + ?Sized>(source: &T) -> Result<u64> {
    let mut inventory = CloneInventory::default();
    source.serialize(&mut inventory).map_err(|_| Error::Invalid("closed clone inventory rejected".into()))?;
    Ok(inventory.bytes)
}
pub(crate) fn scope_clone_owned_bytes(scope: &super::Scope) -> Result<u64> { source_inventory(scope) }
pub(crate) fn fork_seed_clone_reservation(seed: &ForkSeed) -> Result<u64> { source_inventory(seed) }

fn preparation_inventory_error(
    work: &Option<&mut dyn HistoryIo>, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

/// Closed logical ownership allowance, not allocator/copy counters or a new claim.
pub(crate) fn resource_preparation_owned_bytes(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.items(1)?;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
        let child = &request.child;
        let mut bytes = 0u64;
        macro_rules! owned_add {
            ($owned:expr) => {
                bytes = bytes.checked_add($owned).ok_or_else(|| preparation_inventory_error(
                    work, "resource preparation ownership inventory overflows"))?;
            };
        }
        owned_add!(request.provider.logical_owned_bytes()?);
        owned_add!(request.creation_grant.capacity() as u64);
        owned_add!(child.operation_id.capacity() as u64);
        for text in [&child.task.name, &child.task.version, &child.machine.name, &child.machine.version] {
            owned_add!(text.capacity() as u64);
        }
        for value in [&request.payload, &child.input, &child.input_schema, &child.output_schema] {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.items(1)?;
            }
            owned_add!(super::native_custody::value_owned_bytes(value)?);
        }
        owned_add!(child.grants.logical_owned_bytes()?);
        if let Some(policy) = &child.policy {
            owned_add!(policy.name.capacity() as u64);
            owned_add!(policy.version.capacity() as u64);
        }
        if let Some(extensions) = &child.extensions {
            owned_add!(extensions.logical_owned_bytes()?);
        }
        if let Some(execution) = &child.execution {
            owned_add!(execution.provider.name.capacity() as u64);
            owned_add!(execution.provider.version.capacity() as u64);
            owned_add!(execution.build.as_resource().logical_owned_bytes()?);
            if let Some(environment) = &execution.environment {
                owned_add!(environment.as_resource().logical_owned_bytes()?);
            }
        }
        owned_add!((request.resources.capacity() as u64)
            .checked_mul(std::mem::size_of::<crate::resources::ResourceRef>() as u64)
            .ok_or_else(|| preparation_inventory_error(
                work, "resource preparation ownership inventory overflows"))?);
        for reference in &request.resources {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.items(1)?;
            }
            owned_add!(reference.logical_owned_bytes()?);
        }
        owned_add!((child.resource_proofs.capacity() as u64)
            .checked_mul(std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>() as u64)
            .ok_or_else(|| preparation_inventory_error(
                work, "resource preparation ownership inventory overflows"))?);
        for proof in &child.resource_proofs {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.items(1)?;
            }
            owned_add!(proof.provider.logical_owned_bytes()?);
            owned_add!(proof.resource.logical_owned_bytes()?);
            owned_add!(proof.evidence.logical_owned_bytes()?);
        }
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        return Ok(bytes);
    }
    // The request root contains its admission and all optional inline fields.
    // Existing reachable slots plus the actual preparation proof type close the
    // collection inventory; four slots per live element cover Vec minimum/growth.
    let slot = CloneInventory::SLOT
        .max(std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>() as u64)
        .max(std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>() as u64);
    let mut inventory = CloneInventory { slot_bytes: Some(slot), ..CloneInventory::default() };
    request.serialize(&mut inventory).map_err(|_| preparation_inventory_error(
        work, "resource preparation ownership inventory overflows"))?;
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.items(inventory.slots)?;
    }
    inventory.bytes.checked_mul(4).ok_or_else(|| preparation_inventory_error(
        work, "resource preparation ownership inventory overflows"))
}

/// Prefunds the original proof Clone; its Vec shell belongs to the caller.
pub(crate) fn resource_creation_proof_clone_reservation(
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.items(1)?;
    }
    let mut bytes = 0u64;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut recipe = (0, 0);
    for provider in [&proof.provider, proof.resource.provider(), proof.evidence.provider()] {
        for text in [provider.namespace(), provider.family(), provider.version()] {
            bytes = bytes.checked_add(text.len() as u64).ok_or_else(|| preparation_inventory_error(
                work, "resource creation proof clone inventory overflows"))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            super::native_custody::clone_work_add(
                &mut recipe, super::native_custody::clone_work_string(text))?;
        }
    }
    for reference in [&proof.resource, &proof.evidence] {
        bytes = bytes.checked_add(reference.key().len() as u64)
            .and_then(|bytes| bytes.checked_add(reference.version().map_or(0, |value| value.len() as u64)))
            .ok_or_else(|| preparation_inventory_error(
                work, "resource creation proof clone inventory overflows"))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            super::native_custody::clone_work_add(&mut recipe,
                super::native_custody::clone_work_copy_vec::<u8>(reference.key().len())?)?;
            if let Some(version) = reference.version() {
                super::native_custody::clone_work_add(
                    &mut recipe, super::native_custody::clone_work_string(version))?;
            }
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let intrinsic = work.as_deref().is_some_and(|work| work.original_native_work().is_some());
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let intrinsic = false;
    if !intrinsic {
        if let Some(observer) = work.as_deref_mut() { observer.admit_copy(bytes)?; }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    super::native_custody::admit_clone_work(recipe, work)?;
    let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    super::native_custody::begin_clone_work(recipe, quota.work())?;
    if !intrinsic {
        if let Some(observer) = quota.work().as_deref_mut() { observer.copy_bytes(bytes)?; }
    }
    quota.transfer();
    Ok(bytes)
}

pub(crate) fn event_clone_reservation(event: &Event) -> Result<u64> {
    source_inventory(event)
}

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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
    // Last: every owned wire/projection buffer is destroyed before its issuer's
    // native reservation. This is not serialized or part of state equality.
    #[serde(skip)]
    native_custody: SnapshotCustody,
}

#[derive(Default)]
struct SnapshotCustody {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    allocation: Option<acyclic_fs::NativeAllocation>,
}

impl std::fmt::Debug for SnapshotCustody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SnapshotCustody")
    }
}

impl PartialEq for SnapshotCustody {
    fn eq(&self, _: &Self) -> bool { true }
}

// Keep configuration and fork maps as ordered entry arrays: their structured
// keys cannot be represented by JSON object keys. UUID-keyed maps remain maps.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl Projection {
    fn clone_work_recipe(&self) -> Result<(u64, u64)> {
        use super::native_custody::*;
        let mut recipe = (0, 0);
        clone_work_add(&mut recipe, clone_work_container(self.active_extensions.len()))?;
        for extension in &self.active_extensions {
            clone_work_add(&mut recipe, clone_work_string(&extension.name))?;
        }
        for (name, (reference, record)) in &self.extension_states {
            clone_work_add(&mut recipe, clone_work_string(name))?;
            clone_work_add(&mut recipe, clone_work_string(&reference.authority.id))?;
            clone_work_add(&mut recipe, extension_record_clone_work(record)?)?;
        }
        clone_work_add(&mut recipe, clone_work_container(self.configured_extensions.len()))?;
        for (extension, configuration) in &self.configured_extensions {
            clone_work_add(&mut recipe, clone_work_string(&extension.name))?;
            clone_work_add(&mut recipe, extension_configuration_clone_work(configuration)?)?;
        }
        clone_work_add(&mut recipe, clone_work_container(self.active_configurations.len()))?;
        for configuration in &self.active_configurations {
            clone_work_add(&mut recipe, extension_configuration_clone_work(configuration)?)?;
        }
        for effect in self.effects.values() {
            clone_work_add(&mut recipe, effect_clone_work(effect)?)?;
        }
        clone_work_add(&mut recipe, clone_work_copy_vec::<EffectId>(self.indexed_terminal_effects.len())?)?;
        clone_work_add(&mut recipe, clone_work_container(self.forks.len()))?;
        for (authority, seed) in &self.forks {
            clone_work_add(&mut recipe, clone_work_string(&authority.id))?;
            clone_work_add(&mut recipe, fork_seed_clone_work(seed)?)?;
        }
        for (name, operation) in &self.published_merges {
            clone_work_add(&mut recipe, clone_work_string(name))?;
            clone_work_add(&mut recipe, clone_work_copy_vec::<u8>(operation.len())?)?;
        }
        if let Some(checkpoint) = &self.latest_context_checkpoint {
            clone_work_add(&mut recipe, checkpoint.clone_work_recipe()?)?;
        }
        for (ticket, resolution) in self.interactions.values() {
            clone_work_add(&mut recipe, interaction_ticket_clone_work(ticket)?)?;
            if let Some(resolution) = resolution {
                clone_work_add(&mut recipe, interaction_resolution_clone_work(resolution)?)?;
            }
        }
        clone_work_add(&mut recipe, clone_work_container(self.bindings.len()))?;
        for (extension, _) in &self.bindings {
            clone_work_add(&mut recipe, clone_work_string(&extension.name))?;
        }
        // Conversation owns its separate original cache/shell/payload clone phase.
        // UUID keys, inline proofs and opaque BTree nodes are not String copies.
        Ok(recipe)
    }
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
            native_custody: SnapshotCustody::default(),
        }, cache_bytes))
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Snapshot {
    pub(crate) const LOGICAL_SCHEMA_SLOT_BYTES: u64 = CloneInventory::SLOT;

    /// Deep-copies ordinary state. Native state requires an explicit work ledger.
    pub fn try_clone(&self) -> Result<Self> {
        self.clone_with_work(&mut None)
    }

    /// Deep-copies into independently admitted custody on the supplied ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_native(&self, native: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        let mut observer = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.clone_with_work(&mut Some(&mut observer))
    }

    pub(crate) fn clone_with_work(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.allocation.is_some()
            && !work.as_deref().is_some_and(|work| work.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native snapshot cloning requires explicit native work",
            });
        }
        if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
        let reservation = if work.is_some() {
            let payload = source_inventory(self)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
                let conversation = &self.projection.conversation;
                let archive = conversation.native_archived_prefix_bytes()?;
                let buffers = (conversation.messages().len() as u64)
                    .checked_mul(std::mem::size_of::<crate::conversation::ConversationMessage>() as u64)
                    .and_then(|shell| shell.checked_add(archive))
                    .ok_or_else(|| Error::Invalid("snapshot conversation clone slots overflow".into()))?;
                payload.checked_sub(buffers)
                    .ok_or_else(|| Error::Invalid("snapshot conversation clone exceeds source slots".into()))?
            } else {
                payload.checked_add(self.projection.conversation.clone_cache_logical_bytes()?)
                    .ok_or_else(|| Error::Invalid("snapshot clone reservation overflows".into()))?
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            {
                payload.checked_add(self.projection.conversation.clone_cache_logical_bytes()?)
                    .ok_or_else(|| Error::Invalid("snapshot clone reservation overflows".into()))?
            }
        } else { 0 };
        let mut quota = crate::wire_codec::CodecQuota::new(work, reservation)?;
        let mut native_custody = SnapshotCustody::default();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(observer) = quota.work().as_deref_mut()
            .filter(|observer| observer.original_native_work().is_some())
        {
            native_custody.allocation = Some(observer.take_native_custody(reservation)?);
            quota.transfer();
        }
        let conversation = self.projection.conversation.clone_with_work(quota.work())?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if quota.work().as_deref().is_some_and(|work| work.original_native_work().is_some()) {
            use super::native_custody::*;
            let mut recipe = self.projection.clone_work_recipe()?;
            clone_work_add(&mut recipe, clone_work_string(&self.authority.id))?;
            clone_work_add(&mut recipe, clone_work_container(self.events.len()))?;
            for event in &self.events { clone_work_add(&mut recipe, event_clone_work(event)?)?; }
            admit_clone_work(recipe, quota.work())?;
            begin_clone_work(recipe, quota.work())?;
        }
        let copied = Self {
            format_version: self.format_version,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self.events.clone(),
            projection: Projection {
                lifecycle: self.projection.lifecycle,
                active_extensions: self.projection.active_extensions.clone(),
                active_extension_revision: self.projection.active_extension_revision,
                extension_states: self.projection.extension_states.clone(),
                configured_extensions: self.projection.configured_extensions.clone(),
                active_configurations: self.projection.active_configurations.clone(),
                effects: self.projection.effects.clone(),
                indexed_terminal_effects: self.projection.indexed_terminal_effects.clone(),
                effects_archived: self.projection.effects_archived,
                forks: self.projection.forks.clone(),
                published_merges: self.projection.published_merges.clone(),
                conversation,
                latest_context_checkpoint: self.projection.latest_context_checkpoint.clone(),
                interactions: self.projection.interactions.clone(),
                bindings: self.projection.bindings.clone(),
            },
            state_digest: self.state_digest,
            attestation: self.attestation,
            native_custody,
        };
        // Legacy HistoryIo keeps its established caller-owned quota policy.
        quota.transfer();
        Ok(copied)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_custody(
        &mut self, allocation: acyclic_fs::NativeAllocation,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.native_custody.allocation = Some(allocation);
        let bytes = self.projection.conversation.messages.capacity() as u64
            * std::mem::size_of::<crate::conversation::ConversationMessage>() as u64;
        if bytes != 0 {
            let shell = self.native_custody.allocation.as_mut().expect("installed snapshot custody")
                .split_off(bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.projection.conversation.install_native_message_buffer(shell);
        }
        let archive_bytes = self.projection.conversation.native_archived_prefix_bytes()?;
        if archive_bytes != 0 {
            let archive = self.native_custody.allocation.as_mut().expect("installed snapshot custody")
                .split_off(archive_bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.projection.conversation.install_native_archived_prefix(archive);
        }
        let cache_bytes = self.projection.conversation.native_unclaimed_cache_bytes()?;
        if cache_bytes != 0 {
            let cache = self.native_custody.allocation.as_mut().expect("installed snapshot custody")
                .split_off(cache_bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.projection.conversation.install_native_preclaimed_caches(cache, work)?;
        }
        Ok(())
    }

    /// One original Serde parse after the closed snapshot admission has succeeded.
    pub(crate) fn decode_admitted_wire(bytes: &[u8]) -> serde_json::Result<SnapshotWire> {
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        decoder.disable_recursion_limit();
        let value = SnapshotWire::deserialize(&mut decoder)?;
        decoder.end()?;
        Ok(value)
    }

    pub(crate) fn decode_with_work(
        bytes: &[u8], maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut admitted_owned = 0;
        let admission = if let Some(observer) = custody.work().as_deref_mut() {
            crate::contract::snapshot_json_admission(
                &bytes,
                crate::contract::JsonBounds {
                    maximum_bytes,
                    maximum_items: maximum_bytes,
                    maximum_depth: 256,
                },
                &mut |charge| {
                    let owned = match charge {
                        crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } => Some(maximum_owned_bytes),
                        _ => None,
                    };
                    observer.codec_charge(charge)?;
                    if let Some(owned) = owned { admitted_owned = owned; }
                    Ok(())
                },
            ).map(Some)
        } else { Ok(None) };
        // Adoption happens even when the original guard reports a later
        // error, so every successful prior claim is released by the same RAII.
        custody.adopt(admitted_owned)?;
        let admission = admission?;
        let mut snapshot: Snapshot = if let Some(admission) = admission {
            // One original Serde parse moves the conversation's wire fields,
            // then the same rebuild observes its actual original digest work.
            let wire = match Snapshot::decode_admitted_wire(&bytes) {
                Ok(wire) => wire,
                Err(source) => {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    {
                        let retained = custody.retained();
                        if let Some(observer) = custody.work().as_deref_mut()
                            .filter(|observer| observer.original_native_work().is_some())
                        {
                            let allocation = observer.take_native_custody(retained)?;
                            custody.transfer();
                            return Err(Error::NativeDecode {
                                code: crate::wire::ErrorCode::Storage, source,
                                custody: Some(allocation),
                            });
                        }
                    }
                    return Err(Error::Storage(source.to_string()));
                }
            };
            let future_cache = admission.conversation_cache_bytes
                .checked_add(admission.conversation_canonical_scratch_bytes)
                .ok_or_else(|| Error::Storage("snapshot cache allowance overflows".into()))?;
            custody.relinquish(future_cache)?;
            let (snapshot, cache_bytes) = wire.into_snapshot_with_work(
                maximum_bytes, custody.work())?;
            custody.adopt(cache_bytes)?;
            snapshot
        } else {
            crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?
        };
        // Transfer the original once-parsed fields' actual reservation to the
        // snapshot itself; held snapshots outlive this query and reducer prunes.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let retained = custody.retained();
            if let Some(observer) = custody.work().as_deref_mut()
                .filter(|observer| observer.original_native_work().is_some())
            {
                let allocation = observer.take_native_custody(retained)?;
                custody.transfer();
                snapshot.install_native_custody(allocation, custody.work())?;
            }
        }
        custody.transfer();
        Ok(snapshot)
    }
}

#[derive(Serialize)]
struct ReducerCloneFields<'a> {
    authority: &'a Authority,
    verifier_id: &'a str,
    verifier_audience: &'a Authority,
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn snapshot_capture_clone_work(&self, maximum_events: usize) -> Result<(u64, u64)> {
        use super::native_custody::*;
        let mut recipe = clone_work_string(&self.authority.id);
        let retained = self.events.len().min(maximum_events);
        clone_work_add(&mut recipe, clone_work_container(retained))?;
        for event in self.events.iter().skip(self.events.len() - retained) {
            clone_work_add(&mut recipe, event_clone_work(event)?)?;
        }
        clone_work_add(&mut recipe, clone_work_container(self.active_extensions.len()))?;
        for extension in &self.active_extensions {
            clone_work_add(&mut recipe, clone_work_string(&extension.name))?;
        }
        for (name, (reference, record)) in &self.extension_states {
            clone_work_add(&mut recipe, clone_work_string(name))?;
            clone_work_add(&mut recipe, clone_work_string(&reference.authority.id))?;
            clone_work_add(&mut recipe, extension_record_clone_work(record)?)?;
        }
        clone_work_add(&mut recipe, clone_work_container(self.configured_extensions.len()))?;
        for (extension, configuration) in &self.configured_extensions {
            clone_work_add(&mut recipe, clone_work_string(&extension.name))?;
            clone_work_add(&mut recipe, extension_configuration_clone_work(configuration)?)?;
        }
        clone_work_add(&mut recipe, clone_work_container(self.active_configurations.len()))?;
        for configuration in &self.active_configurations {
            clone_work_add(&mut recipe, extension_configuration_clone_work(configuration)?)?;
        }
        for effect in self.effects.values() {
            clone_work_add(&mut recipe, effect_clone_work(effect)?)?;
        }
        clone_work_add(&mut recipe, clone_work_copy_vec::<EffectId>(self.indexed_terminal_effects.len())?)?;
        clone_work_add(&mut recipe, clone_work_container(self.forks.len()))?;
        for (authority, seed) in &self.forks {
            clone_work_add(&mut recipe, clone_work_string(&authority.id))?;
            clone_work_add(&mut recipe, fork_seed_clone_work(seed)?)?;
        }
        for (name, operation) in &self.published_merges {
            clone_work_add(&mut recipe, clone_work_string(name))?;
            clone_work_add(&mut recipe, clone_work_copy_vec::<u8>(operation.len())?)?;
        }
        if let Some(checkpoint) = &self.latest_context_checkpoint {
            clone_work_add(&mut recipe, checkpoint.clone_work_recipe()?)?;
        }
        for (ticket, resolution) in self.interactions.values() {
            clone_work_add(&mut recipe, interaction_ticket_clone_work(ticket)?)?;
            if let Some(resolution) = resolution {
                clone_work_add(&mut recipe, interaction_resolution_clone_work(resolution)?)?;
            }
        }
        // Bindings and conversation have already completed their own guarded,
        // fallible phases before this original infallible projection clone.
        Ok(recipe)
    }

    fn clone_reservation(&self) -> Result<u64> {
        // Verifier Strings, canonical events and retry/cache maps are original
        // owned state. The immutable shared schema Arc is not a deep copy.
        let fields = ReducerCloneFields {
            authority: &self.authority,
            verifier_id: &self.authority_verifier.id,
            verifier_audience: &self.authority_verifier.audience,
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let intrinsic = work.as_deref().is_some_and(|work| work.original_native_work().is_some());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let intrinsic = false;
        let preview_reservation = if work.is_some() {
            // The preview's own deep clone has intrinsic native custody.
            // Legacy observed work preserves its original caller-owned claim.
            let reducer = if intrinsic { 0 } else { self.clone_reservation()? };
            reducer.checked_add(event_clone_reservation(event)?.checked_mul(3)
                .ok_or_else(|| Error::Invalid("publication event clone reservation overflows".into()))?)
                .ok_or_else(|| Error::Invalid("publication preview reservation overflows".into()))?
        } else { 0 };
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.admit_owned(preview_reservation)?;
            observer.retain_owned(preview_reservation)?;
        }
        let result = (|| {
            let mut preview = self.clone_with_work(work)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if intrinsic {
                let recipe = super::native_custody::event_clone_work(event)?;
                super::native_custody::admit_clone_work(recipe, work)?;
                super::native_custody::begin_clone_work(recipe, work)?;
            }
            preview.apply_committed_with_work(event.clone(), work)?;
            preview.mark_indexed_terminal_effect_with_work(event, work)?;
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


    /// Captures an independently owned native snapshot on the supplied ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn snapshot_native(
        &self,
        maximum_events: usize,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Snapshot> {
        let mut observer = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.snapshot_with_event_limit_with_work(maximum_events, &mut Some(&mut observer))
            .map(|(snapshot, _)| snapshot)
    }
    pub(crate) fn snapshot_with_event_limit_with_work(
        &self,
        maximum_events: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.original_work().is_some()
            && !work.as_deref().is_some_and(|work| work.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native snapshot capture requires explicit native work",
            });
        }
        if maximum_events == 0 && self.revision != 0 {
            return Err(Error::Invalid("snapshot must retain its head event".into()));
        }
        let reservation = if work.is_some() {
            // The original map.clone().into_iter().collect() briefly owns both
            // map payload and destination vector slots during capture.
            self.clone_reservation()?.checked_mul(2)
                .ok_or_else(|| Error::Invalid("snapshot clone reservation overflows".into()))?
        } else { 0 };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let reservation = if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
            let shell = (self.conversation.messages().len() as u64)
                .checked_mul(std::mem::size_of::<crate::conversation::ConversationMessage>() as u64)
                .ok_or_else(|| Error::Invalid("snapshot conversation shell slots overflow".into()))?;
            let archive = self.conversation.native_archived_prefix_bytes()?;
            let intrinsic_conversation = self.conversation.clone_cache_logical_bytes()?
                .checked_add(shell)
                .and_then(|bytes| bytes.checked_add(archive))
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or_else(|| Error::Invalid("snapshot intrinsic conversation reservation overflows".into()))?;
            reservation.checked_sub(intrinsic_conversation)
                .ok_or_else(|| Error::Invalid("snapshot intrinsic conversation exceeds source slots".into()))?
        } else { reservation };
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.admit_owned(reservation)?;
            observer.retain_owned(reservation)?;
        }
        let mut native_custody = SnapshotCustody::default();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(observer) = work.as_deref_mut()
            .filter(|observer| observer.original_native_work().is_some())
        {
            native_custody.allocation = Some(observer.take_native_custody(reservation)?);
        }
        let result = (|| {
        let conversation = self.checkpointed_conversation(work)?;
        let bindings = registry_bindings_with_work(&self.schemas, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
            let recipe = self.snapshot_capture_clone_work(maximum_events)?;
            super::native_custody::admit_clone_work(recipe, work)?;
            super::native_custody::begin_clone_work(recipe, work)?;
        }
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
            bindings,
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
            native_custody,
        };
        snapshot.state_digest = snapshot.digest_with_work(work)?;
        snapshot.attestation = self.authority_verifier.attest_snapshot_with_work(&snapshot, work)?;
        Ok(snapshot)
        })();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let intrinsic = work.as_deref().is_some_and(|work| work.original_native_work().is_some());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let intrinsic = false;
        if result.is_err() && !intrinsic {
            if let Some(observer) = work.as_deref_mut() { observer.release_owned(reservation)?; }
        }
        result.map(|snapshot| (snapshot, if intrinsic { 0 } else { reservation }))
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

    fn checkpointed_conversation(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<ConversationState> {
        let checkpoint_selection = self.resident_checkpoint_selection();
        match checkpoint_selection {
            Some(selection) => self.conversation.checkpointed_suffix_with_work(selection, work),
            None => self.conversation.clone_with_work(work),
        }
    }

    pub(crate) fn compact_conversation_projection(
        &mut self, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let selection = self.resident_checkpoint_selection();
        let Some(selection) = selection else {
            return Ok(());
        };
        if selection.conversation_revision <= self.conversation.resident_after_sequence()
            || self.conversation.unresolved_user().is_some()
        {
            return Ok(());
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
            use super::native_custody::{NativeComponentCustody as Holder,
                NativeComponentKey as Key, NativeComponentRef as Ref, PreparedComponent};
            let count = self.conversation.messages().len();
            let bytes = (count as u64).checked_mul(std::mem::size_of::<(u64, PreparedComponent)>() as u64)
                .ok_or_else(|| Error::Invalid("conversation retirement slots overflow".into()))?;
            work.as_deref_mut().expect("native retirement observer").admit_allocation(bytes)?;
            let mut slots = crate::wire_codec::CodecQuota::new(work, bytes)?;
            let mut prepared = Vec::new();
            prepared.try_reserve_exact(count)
                .map_err(|cause| slots.work().as_deref().expect("native retirement observer")
                    .allocation_failure(cause))?;
            for message in self.conversation.messages() {
                let claim = Holder::prepare(Ref::ConversationMessage(message.sequence),
                    source_inventory(message)?, slots.work())?;
                prepared.push((message.sequence, claim));
            }
            let replacement = self.conversation.checkpointed_suffix_with_work(selection, slots.work())?;
            // The prepared claims cover the genuinely new copied messages;
            // original claims remain installed until original data is destroyed.
            self.conversation = replacement;
            for (sequence, claim) in prepared {
                if self.conversation.messages().binary_search_by_key(&sequence, |message| message.sequence).is_ok() {
                    drop(claim.install(&mut self.native_custody, slots.work())?);
                } else {
                    self.native_custody.remove(&Key::ConversationMessage(sequence));
                }
            }
            return Ok(());
        }
        self.conversation = self.conversation.checkpointed_suffix_with_work(selection, work)?;
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

    /// Restores original authenticated fields with explicitly admitted native custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn restore_native(
        snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut observer = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let metadata = authority_verifier.logical_owned_bytes()?
            .checked_add(schemas.logical_owned_bytes()?)
            .ok_or_else(|| Error::Invalid("restore input metadata custody overflows".into()))?;
        observer.admit_owned(metadata)?;
        observer.retain_owned(metadata)?;
        Self::restore_with_work(snapshot, authority_verifier, schemas, &mut Some(&mut observer))
    }

    pub(crate) fn restore_with_work(
        mut snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if snapshot.native_custody.allocation.is_some()
            && !work.as_deref().is_some_and(|work| work.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native snapshot restoration requires explicit native work",
            });
        }
        snapshot.verify(&authority_verifier, &schemas, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let intrinsic = work.as_deref().is_some_and(|work| work.original_native_work().is_some());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let intrinsic = false;
        // The original decoded fields retain their issuing ledger. New map and
        // retry slots are pre-admitted before any original restore constructor.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if intrinsic && snapshot.native_custody.allocation.is_none() {
            let required = source_inventory(&snapshot)?
                .checked_add(snapshot.projection.conversation.native_unclaimed_cache_bytes()?)
                .ok_or_else(|| Error::Invalid("snapshot input custody overflows".into()))?;
            let observer = work.as_deref_mut().expect("native restoration observer");
            observer.admit_owned(required)?;
            observer.retain_owned(required)?;
            let allocation = observer.take_native_custody(required)?;
            snapshot.install_native_custody(allocation, work)?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut metadata_allocation = if intrinsic {
            let bytes = snapshot.restoration_metadata_reservation()?;
            let observer = work.as_deref_mut().expect("native restoration metadata observer");
            observer.admit_owned(bytes)?;
            observer.retain_owned(bytes)?;
            Some(observer.take_native_custody(bytes)?)
        } else { None };
        let mut restored_custody = super::native_custody::NativeComponentCustody::default();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let base = if intrinsic {
            let bytes = authority_verifier.logical_owned_bytes()?
                .checked_add(schemas.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("restore input metadata custody overflows".into()))?;
            super::native_custody::PreparedComponent::preclaimed(
                super::native_custody::NativeComponentKey::Base,
                work.as_deref_mut().expect("native restore observer").take_native_custody(bytes)?,
            )
        } else {
            super::native_custody::NativeComponentCustody::prepare(
                super::native_custody::NativeComponentRef::Base, 0, work)?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let base = super::native_custody::NativeComponentCustody::prepare(
            super::native_custody::NativeComponentRef::Base, 0, work)?;
        drop(base.install(&mut restored_custody, work)?);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut allocation = snapshot.native_custody.allocation.take();
        let operation_positions = snapshot
            .events
            .iter()
            .map(|event| (event.operation_id, event.revision))
            .collect();
        let projection = snapshot.projection;
        let mut reducer = Self {
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
            native_custody: super::native_custody::NativeComponentCustody::default(),
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(allocation) = allocation.as_mut() {
            super::native_custody::NativeComponentCustody::from_reducer(
                &reducer, allocation,
                metadata_allocation.as_mut().expect("native restoration metadata"),
                &mut restored_custody, work)?;
        }
        reducer.native_custody = restored_custody;
        Ok(reducer)
    }
}

impl Snapshot {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn restoration_metadata_reservation(&self) -> Result<u64> {
        use super::native_custody::{NativeComponentCustody as Holder, NativeComponentRef as Key};
        // Newly allocated ownership metadata belongs to the explicit restore
        // operation, not the original decoded payload's independent ledger.
        let mut bytes = 0_u64;
        let mut components = 1_usize; // The separately admitted verifier/schema Base.
        let mut metadata = |key| -> Result<()> {
            let key_bytes = match key {
                Key::ExtensionState(name) => name.len() as u64,
                Key::ExtensionConfiguration(extension) => extension.name.len() as u64,
                Key::Fork(authority) => authority.id.len() as u64,
                Key::Merge(name, operation) => (name.len() as u64).checked_add(operation.len() as u64)
                    .ok_or_else(|| Error::Invalid("snapshot merge metadata overflows".into()))?,
                Key::OperationPosition(_) => std::mem::size_of::<(crate::OperationId, u64)>() as u64,
                _ => 0,
            };
            bytes = bytes.checked_add(key_bytes)
                .ok_or_else(|| Error::Invalid("snapshot restoration metadata overflows".into()))?;
            components = components.checked_add(1)
                .ok_or_else(|| Error::Invalid("snapshot restoration component count overflows".into()))?;
            Ok(())
        };
        metadata(Key::Authority)?;
        for event in &self.events {
            metadata(Key::Event(event.revision))?;
            metadata(Key::OperationPosition(event.operation_id))?;
        }
        metadata(Key::EventBuffer)?;
        metadata(Key::TerminalEffectBuffer)?;
        for id in self.projection.effects.keys() {
            metadata(Key::EffectAction(*id))?;
            metadata(Key::EffectStatus(*id))?;
            metadata(Key::EffectAttempts(*id))?;
        }
        for name in self.projection.extension_states.keys() { metadata(Key::ExtensionState(name))?; }
        for (extension, _) in &self.projection.configured_extensions {
            metadata(Key::ExtensionConfiguration(extension))?;
        }
        metadata(Key::ActiveExtensions)?;
        metadata(Key::ActiveConfigurations)?;
        for (authority, _) in &self.projection.forks { metadata(Key::Fork(authority))?; }
        for (name, operation) in &self.projection.published_merges {
            metadata(Key::Merge(name, operation))?;
        }
        for message in self.projection.conversation.messages() {
            metadata(Key::ConversationMessage(message.sequence))?;
        }
        if self.projection.latest_context_checkpoint.is_some() { metadata(Key::ContextCheckpoint)?; }
        for (id, (_, resolution)) in &self.projection.interactions {
            metadata(Key::InteractionTicket(*id))?;
            if resolution.is_some() { metadata(Key::InteractionResolution(*id))?; }
        }
        bytes.checked_add(Holder::metadata_bytes(components)?)
            .ok_or_else(|| Error::Invalid("snapshot restoration metadata buffer overflows".into()))
    }

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
        let scratch_bytes = if work.is_some() {
            (self.events.len() as u64).checked_add(self.projection.indexed_terminal_effects.len() as u64)
                .and_then(|items| items.checked_mul(CloneInventory::SLOT))
                .ok_or_else(|| Error::Invalid("snapshot verification slots overflow".into()))?
        } else { 0 };
        // Original validation sets are temporary source containers, not part of
        // the restored state; their claim ends only after both sets are dropped.
        let mut scratch = crate::wire_codec::CodecQuota::new(work, scratch_bytes)?;
        if self.format_version != 6 {
            return Err(Error::Unsupported(format!(
                "snapshot format {}",
                self.format_version
            )));
        }
        verifier.verify_audience(&self.authority)?;
        if self.digest_with_work(scratch.work())? != self.state_digest {
            return Err(Error::Invalid("snapshot digest mismatch".into()));
        }
        if verifier.attest_snapshot_with_work(self, scratch.work())? != self.attestation {
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
            verifier.verify_event_with_work(event, scratch.work())?;
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
            if canonical_digest_with_work(binding, scratch.work())? != *expected {
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
    let count = schemas.schemas.as_deref().map_or(0, BTreeMap::len);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
        let recipe = super::native_custody::clone_work_container(count);
        super::native_custody::admit_clone_work(recipe, work)?;
        super::native_custody::begin_clone_work(recipe, work)?;
    }
    let mut bindings = Vec::with_capacity(count);
    for ((name, version), binding) in schemas.schemas.as_deref().into_iter().flat_map(BTreeMap::iter) {
        let digest = canonical_digest_with_work(binding, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let intrinsic = work.as_deref().is_some_and(|work| work.original_native_work().is_some());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let intrinsic = false;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if intrinsic {
            let recipe = super::native_custody::clone_work_string(name);
            super::native_custody::admit_clone_work(recipe, work)?;
            super::native_custody::begin_clone_work(recipe, work)?;
        }
        if !intrinsic {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.copy_bytes(name.len() as u64)?;
            }
        }
        bindings.push((ExtensionDependency { name: name.clone(), version: *version }, digest));
    }
    Ok(bindings)
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 4096,
            peak_allocation_bytes: 4_194_304,
            bytes_copied: 4_194_304,
            bytes_encoded: 4_194_304,
            bytes_hashed: 4_194_304,
            source_bytes_read: 4_194_304,
            items_examined: 4_194_304,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn populated_fixture() -> Result<(Reducer, AuthorityIssuer)> {
        let (mut reducer, issuer) = fixture();
        for (index, to) in [LifecycleState::Active, LifecycleState::Completed].into_iter().enumerate() {
            reducer.apply(super::super::Command {
                operation_id: crate::OperationId::from_bytes([index as u8 + 1; 16]),
                idempotency_key: crate::IdempotencyKey::new(format!("snapshot-source-{index}"))?,
                expected_revision: index as u64,
                scope: issuer.root("snapshot-source", crate::Capabilities::new(["lifecycle:manage"])),
                causal_parent: None,
                action: super::super::Action::TransitionLifecycle {
                    to, reason: Some(format!("{index}:{}", "owned canonical lifecycle reason".repeat(128))),
                },
            })?;
        }
        Ok((reducer, issuer))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn conversation_fixture() -> Result<(Reducer, AuthorityIssuer)> {
        use crate::conversation::{ConversationMessage, FileDescriptor, MessageKind,
            VolumeClass, VolumeOwner, VolumeRef};
        let authority = Authority { kind: AggregateKind::Conversation, id: "snapshot-conversation".into() };
        let issuer = AuthorityIssuer::new("snapshot-conversation-owner", [24; 32], authority.clone());
        let mut reducer = Reducer::new(authority, issuer.verifier(), SchemaRegistry::new());
        let agent = crate::AgentId::from_bytes([9; 16]);
        let content = FileRef::new(
            VolumeRef::new(crate::resources::ProviderRef::new("test", "filesystem", "2")?,
                "snapshot-content", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?,
            "conversation/input.json", "generation-1",
            FileDescriptor::from_bytes(br#"{"text":"owned conversation clone input"}"#, "application/json")?,
            "input.json",
        )?;
        let actions = [
            super::super::Action::BindConversation { agent },
            super::super::Action::AppendConversationMessage { message: Box::new(ConversationMessage {
                id: uuid::Uuid::from_bytes([10; 16]), sequence: 1, kind: MessageKind::User,
                content, attachments: Vec::new().into(), reply_to: None,
                tool_call_id: None, extensions: BTreeMap::new(),
            }) },
        ];
        for (index, action) in actions.into_iter().enumerate() {
            reducer.apply(super::super::Command {
                operation_id: crate::OperationId::from_bytes([index as u8 + 11; 16]),
                idempotency_key: crate::IdempotencyKey::new(format!("snapshot-conversation-{index}"))?,
                expected_revision: index as u64,
                scope: issuer.root("snapshot-conversation",
                    crate::Capabilities::new(["conversation:bind", "conversation:append"])),
                causal_parent: None, action,
            })?;
        }
        Ok((reducer, issuer))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_cold_wire_restores_with_independent_metadata_after_source_cancellation() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let (reducer, issuer) = populated_fixture()?;
        let bytes = crate::contract::canonical_json_bytes(&reducer.snapshot()?)?;
        let source_token = CancellationToken::new();
        let source = NativeMaterializationWork::new(native_budget(), &source_token).expect("source decoder ledger");
        let mut io = crate::store::history::native_work_io(&source).expect("original decoder observer");
        let snapshot = Snapshot::decode_with_work(&bytes, 4_194_304, &mut Some(&mut io))?;
        assert_eq!(io.into_custody().bytes(), 0);
        source_token.cancel();
        let restore_token = CancellationToken::new();
        let restore = NativeMaterializationWork::new(native_budget(), &restore_token).expect("independent restore ledger");
        let restored = Reducer::restore_native(snapshot, issuer.verifier(), SchemaRegistry::new(), &restore)?;
        assert_eq!(restored, reducer);
        assert!(restore.receipt().allocation_operations > 0);
        drop(restored);
        restore.admit_retained(native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_original_cold_decoder_preserves_digest_and_issuer_mac_rejection() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let (reducer, issuer) = populated_fixture()?;
        let original = reducer.snapshot()?;
        for forged_digest in [false, true] {
            let mut forged = original.try_clone()?;
            if forged_digest {
                forged.projection.lifecycle = LifecycleState::Pending;
                forged.state_digest = forged.digest_with_work(&mut None)?;
            } else {
                forged.attestation[0] ^= 1;
            }
            let bytes = crate::contract::canonical_json_bytes(&forged)?;
            let token = CancellationToken::new();
            let source = NativeMaterializationWork::new(native_budget(), &token).expect("cold decoder ledger");
            let mut io = crate::store::history::native_work_io(&source).expect("original decoder observer");
            let decoded = Snapshot::decode_with_work(&bytes, 4_194_304, &mut Some(&mut io))?;
            assert_eq!(io.into_custody().bytes(), 0);
            assert!(matches!(
                Reducer::restore_native(decoded, issuer.verifier(), SchemaRegistry::new(), &source),
                Err(Error::Unauthorized(_))
            ));
            source.admit_retained(native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let mut forged = original;
        forged.projection.lifecycle = LifecycleState::Pending;
        let bytes = crate::contract::canonical_json_bytes(&forged)?;
        let token = CancellationToken::new();
        let source = NativeMaterializationWork::new(native_budget(), &token).expect("digest rejection ledger");
        let mut io = crate::store::history::native_work_io(&source).expect("original decoder observer");
        let decoded = Snapshot::decode_with_work(&bytes, 4_194_304, &mut Some(&mut io))?;
        drop(io);
        assert!(matches!(Reducer::restore_native(decoded, issuer.verifier(), SchemaRegistry::new(), &source),
            Err(Error::Invalid(_))));
        source.admit_retained(native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_held_snapshot_and_independent_clone_keep_real_payload_after_reducer_prune() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let (reducer, issuer) = populated_fixture()?;
        let token_a = CancellationToken::new();
        let token_b = CancellationToken::new();
        let token_c = CancellationToken::new();
        let a = NativeMaterializationWork::new(native_budget(), &token_a).expect("state ledger");
        let b = NativeMaterializationWork::new(native_budget(), &token_b).expect("snapshot ledger");
        let c = NativeMaterializationWork::new(native_budget(), &token_c).expect("clone ledger");
        let mut state = Reducer::restore_native(reducer.snapshot()?, issuer.verifier(), SchemaRegistry::new(), &a)?;
        let held = state.snapshot_native(2, &b)?;
        let cloned = held.clone_native(&c)?;
        assert_eq!(held, cloned);
        assert!(!held.events.is_empty());
        let old_head = held.events.last().expect("canonical head").revision;
        state.set_resident_event_limit(1)?;
        assert_eq!(state.events.len(), 1);
        assert_eq!(held.events.len(), 2);
        assert_eq!(cloned.events.last().expect("independent head").revision, old_head);
        let available = native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        assert!(b.admit_retained(available).is_err());
        assert!(c.admit_retained(available).is_err());
        drop(state);
        a.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        drop(held);
        b.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(c.admit_retained(available).is_err(), "the independent real deep copy remains alive");
        drop(cloned);
        c.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_snapshot_clone_denials_preserve_source_and_drop_partial_owned_copy() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let (reducer, _) = conversation_fixture()?;
        let source_token = CancellationToken::new();
        let source = NativeMaterializationWork::new(native_budget(), &source_token).expect("source ledger");
        let snapshot = reducer.snapshot_native(2, &source)?;
        let first_phase_copies = snapshot.projection.conversation.native_clone_copy_recipe()?.1;
        assert!(first_phase_copies > 0);
        assert!(matches!(snapshot.try_clone(), Err(Error::NativeBoundary { .. })));
        let source_receipt = source.receipt();
        for counter in ["bytes_copied", "allocation_operations", "mid-clone-bytes"] {
            let mut allowance = native_budget();
            if counter == "bytes_copied" { allowance.bytes_copied = 0; }
            if counter == "allocation_operations" { allowance.allocation_operations = 0; }
            // Permit the actual message/cache phase, then deny the separate
            // original event-cache/projection clone on the same cumulative ledger.
            if counter == "mid-clone-bytes" { allowance.bytes_copied = first_phase_copies; }
            let token = CancellationToken::new();
            let denied = NativeMaterializationWork::new(allowance, &token).expect("original denial ledger");
            let prefix = denied.receipt();
            let error = snapshot.clone_native(&denied).expect_err("real source clone must preflight native work");
            let Error::NativeWork { cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: denied_counter, ..
            }), .. } = error else { panic!("original typed clone denial") };
            assert_eq!(denied_counter, if counter == "mid-clone-bytes" { "bytes_copied" } else { counter });
            if counter == "mid-clone-bytes" {
                assert_eq!(denied.receipt().bytes_copied - prefix.bytes_copied, first_phase_copies);
            } else {
                assert_eq!(denied.receipt().bytes_copied, prefix.bytes_copied);
            }
            assert_eq!(source.receipt(), source_receipt);
            assert_eq!(snapshot.events.len(), 2);
            denied.admit_retained(allowance.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        Ok(())
    }

    #[test]
    fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
        let (reducer, issuer) = fixture();
        let original = reducer.snapshot()?;
        let mut forged = original.clone_with_work(&mut None)?;
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
                Reducer::restore(snapshot.clone_with_work(&mut None)?, other.verifier(), SchemaRegistry::new()),
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
            Reducer::restore(snapshot.clone_with_work(&mut None)?, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        for edit in 0..5 {
            let mut changed = reducer.schemas.clone();
            let binding = changed
                .schemas.as_mut().map(std::sync::Arc::make_mut)
                .and_then(|schemas| schemas.get_mut(&("example.pinned".into(), 1)))
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
                Reducer::restore(snapshot.clone_with_work(&mut None)?, issuer.verifier(), changed),
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
