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

#[derive(Default)]
struct CloneInventory<'w> {
    bytes: Option<u64>,
    work: Option<&'w mut dyn HistoryIo>,
    failure: Option<Error>,
    depth: u16,
    slots: u64,
    slot_bytes: Option<u64>,
}

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
        if let Some(total) = self.bytes.as_mut() {
            *total = total.checked_add(bytes).ok_or(InventoryError)?;
        }
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
        if self.bytes.is_none() { return self.visit(); }
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
    let mut inventory = CloneInventory { bytes: Some(0), ..Default::default() };
    source.serialize(&mut inventory).map_err(|_| Error::Invalid("closed clone inventory rejected".into()))?;
    Ok(inventory.bytes.expect("size inventory"))
}

/// Inspects the closed SDK admission DTOs before their canonical comparison.
pub(crate) fn source_metadata_inspection_with_work<T: Serialize + ?Sized>(
    source: &T, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if work.is_none() { return Ok(()); }
    let failure = {
        let mut inventory = CloneInventory {
            work: work.as_deref_mut().map(|observer| observer as &mut dyn HistoryIo),
            ..Default::default()
        };
        match source.serialize(&mut inventory) {
            Ok(()) => return Ok(()),
            Err(_) => inventory.failure.take(),
        }
    };
    match failure {
        Some(error) => Err(error),
        None => Err(preparation_inventory_error(work, "closed admission metadata inspection rejected")),
    }
}
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
        observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
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
        for text in [&child.task.name, &child.task.version, &child.machine.name, &child.machine.version] {
            owned_add!(text.capacity() as u64);
        }
        for value in [&request.payload, &child.input, &child.input_schema, &child.output_schema] {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
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
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
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
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            owned_add!(proof.provider.logical_owned_bytes()?);
            owned_add!(proof.resource.logical_owned_bytes()?);
            owned_add!(proof.evidence.logical_owned_bytes()?);
        }
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        return Ok(bytes);
    }
    let slot = CloneInventory::SLOT
        .max(std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>() as u64)
        .max(std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>() as u64);
    let mut inventory = CloneInventory { bytes: Some(0), slot_bytes: Some(slot), ..Default::default() };
    request.serialize(&mut inventory).map_err(|_| preparation_inventory_error(
        work, "resource preparation ownership inventory overflows"))?;
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: inventory.slots })?;
    }
    inventory.bytes.expect("size inventory").checked_mul(4).ok_or_else(|| preparation_inventory_error(
        work, "resource preparation ownership inventory overflows"))
}

/// Copies the concrete proof fields physically; the caller owns its Vec shell.
pub(crate) fn resource_creation_proof_clone_with_custody(
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(crate::resource_preparation::ResourceCreationProofRef, u64)> {
    if work.is_none() { return Ok((proof.clone(), 0)); }
    fn reference(
        source: &crate::resources::ResourceRef,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<crate::resources::ResourceRef> {
        let provider = super::copy_provider(source.provider(), quota)?;
        let key = super::copy_copy_vector(source.key(), quota)?;
        let version = match source.version() {
            Some(version) => Some(super::native_custody::copy_text(version, quota)?),
            None => None,
        };
        crate::resources::ResourceRef::new_with_work(source.kind(), provider, key, version, quota.work())
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    quota.work().as_deref_mut().expect("proof copy observer").check_cancellation()?;
    let provider = super::copy_provider(&proof.provider, &mut quota)?;
    let resource = reference(&proof.resource, &mut quota)?;
    let evidence = reference(&proof.evidence, &mut quota)?;
    let copied = crate::resource_preparation::ResourceCreationProofRef {
        provider, preparation: proof.preparation, resource, evidence,
    };
    let retained = quota.retained();
    quota.transfer();
    Ok((copied, retained))
}

#[cfg(feature = "filesystem")]
pub(crate) fn resource_creation_clone_reservation(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    origin: &crate::resource_preparation::ResourceCreationOrigin,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let mut inventory = CloneInventory { bytes: Some(0), work: work.as_deref_mut().map(|work| work as &mut dyn HistoryIo), ..Default::default() };
    let result = (request, proof, origin).serialize(&mut inventory);
    if let Some(error) = inventory.failure.take() { return Err(error); }
    result.map_err(|_| Error::Invalid("closed creator capture exceeds inventory bounds".into()))?;
    let bytes = inventory.bytes.expect("size inventory");
    drop(inventory);
    if let Some(work) = work.as_deref_mut() { work.retain_owned(bytes)?; }
    Ok(bytes)
}

/// Versioned, issuer-authenticated accelerator; Stream events remain authoritative.
#[derive(Debug, Serialize, Deserialize)]
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[serde(skip)]
    native_allocation: Option<acyclic_fs::NativeAllocation>,
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.format_version == other.format_version && self.authority == other.authority
            && self.revision == other.revision && self.events == other.events
            && self.projection == other.projection && self.state_digest == other.state_digest
            && self.attestation == other.attestation
    }
}

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
    bindings: Vec<(ExtensionDependency, [u8; 32])>,
}

fn snapshot_map_entry<T>(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    if quota.work().is_none() { return Ok(()); }
    let bytes = std::mem::size_of::<T>() as u64;
    let observer = quota.work().as_deref_mut().expect("snapshot component observer");
    observer.admit_owned(bytes)?;
    observer.retain_owned(bytes)?;
    quota.adopt(bytes)
}

fn copy_snapshot_states(
    source: &BTreeMap<String, (EventReference, ExtensionRecord)>,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<BTreeMap<String, (EventReference, ExtensionRecord)>> {
    let mut output = BTreeMap::new();
    for (name, (reference, record)) in source {
        snapshot_map_entry::<(String, (EventReference, ExtensionRecord))>(quota)?;
        let name = super::native_custody::copy_text(name, quota)?;
        let reference = super::native_custody::copy_reference(reference, quota)?;
        let record = super::native_custody::copy_extension_record(record, quota)?;
        output.insert(name, (reference, record));
    }
    Ok(output)
}

fn copy_snapshot_effects(
    source: &BTreeMap<EffectId, EffectState>,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<BTreeMap<EffectId, EffectState>> {
    let mut output = BTreeMap::new();
    for (id, effect) in source {
        snapshot_map_entry::<(EffectId, EffectState)>(quota)?;
        let (effect, retained) = super::native_effect_clone_with_custody(effect, quota.work())?;
        quota.adopt(retained)?;
        output.insert(*id, effect);
    }
    Ok(output)
}

fn copy_snapshot_merges(
    source: &BTreeSet<(String, Vec<u8>)>,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<BTreeSet<(String, Vec<u8>)>> {
    let mut output = BTreeSet::new();
    for (name, operation) in source {
        snapshot_map_entry::<(String, Vec<u8>)>(quota)?;
        let name = super::native_custody::copy_text(name, quota)?;
        let operation = super::copy_copy_vector(operation, quota)?;
        output.insert((name, operation));
    }
    Ok(output)
}

fn copy_snapshot_interactions(
    source: &BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>> {
    let mut output = BTreeMap::new();
    for (id, (ticket, resolution)) in source {
        snapshot_map_entry::<(uuid::Uuid, (InteractionTicket, Option<InteractionResolution>))>(quota)?;
        let (ticket, retained) = super::native_interaction_ticket_clone_with_custody(ticket, quota.work())?;
        quota.adopt(retained)?;
        let resolution = match resolution {
            Some(resolution) => {
                let (resolution, retained) = super::native_interaction_resolution_clone_with_custody(
                    resolution, quota.work())?;
                quota.adopt(retained)?;
                Some(resolution)
            }
            None => None,
        };
        output.insert(*id, (ticket, resolution));
    }
    Ok(output)
}

fn copy_snapshot_terminal_effects(
    source: &VecDeque<EffectId>, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<VecDeque<EffectId>> {
    if quota.work().is_none() { return Ok(source.clone()); }
    let bytes = source.len().checked_mul(std::mem::size_of::<EffectId>())
        .ok_or_else(|| preparation_inventory_error(quota.work(), "terminal effect copy size overflows"))? as u64;
    quota.work().as_deref().expect("snapshot terminal copy observer").admit_copy(bytes)?;
    let mut output = super::reserve_copy_vector::<EffectId>(source.len(), quota)?;
    for slice in [source.as_slices().0, source.as_slices().1] {
        if !slice.is_empty() {
            quota.work().as_deref_mut().expect("snapshot terminal copy observer")
                .copy_bytes((slice.len() * std::mem::size_of::<EffectId>()) as u64)?;
            output.extend_from_slice(slice);
        }
    }
    Ok(VecDeque::from(output))
}

macro_rules! copy_projection {
    ($destination:expr, $source:expr, $quota:expr) => {{
        let destination = &mut $destination;
        let source = &$source;
        let quota = &mut $quota;
        destination.active_extensions = super::copy_vector(&source.active_extensions, quota, super::native_custody::copy_dependency)?;
        destination.extension_states = copy_snapshot_states(&source.extension_states, quota)?;
        destination.configured_extensions = super::reserve_copy_vector(source.configured_extensions.len(), quota)?;
        for (extension, configuration) in source.configured_extensions.iter() {
            destination.configured_extensions.push((
                super::native_custody::copy_dependency(extension, quota)?,
                super::copy_configuration(configuration, quota)?,
            ));
        }
        destination.active_configurations = super::copy_vector(&source.active_configurations, quota, super::copy_configuration)?;
        destination.effects = copy_snapshot_effects(&source.effects, quota)?;
        destination.indexed_terminal_effects = copy_snapshot_terminal_effects(&source.indexed_terminal_effects, quota)?;
        destination.forks = super::reserve_copy_vector(source.forks.len(), quota)?;
        for (authority, seed) in source.forks.iter() {
            let authority = super::copy_authority(authority, quota)?;
            let seed = super::native_custody::copy_seed(seed, quota)?;
            destination.forks.push((authority, seed));
        }
        destination.published_merges = copy_snapshot_merges(&source.published_merges, quota)?;
        destination.latest_context_checkpoint = source.latest_context_checkpoint.as_ref()
            .map(|file| super::copy_file(file, quota)).transpose()?;
        destination.interactions = copy_snapshot_interactions(&source.interactions, quota)?;
    }};
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
        let (conversation, cache_bytes) = ConversationState::from_wire(
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_allocation: None,
        }, cache_bytes))
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Snapshot {
    pub(crate) const LOGICAL_SCHEMA_SLOT_BYTES: u64 = CloneInventory::SLOT;

    /// Copies authenticated state using the caller's ledger.
    pub fn copy(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_allocation.is_some()
            && !work.as_deref().is_some_and(|work| work.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native snapshot cloning requires explicit native work",
            });
        }
        if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
        let native = work.as_deref().is_some_and(super::native_custody::is_native);
        let reservation = if work.is_some() && !native {
            source_inventory(self)?.checked_add(self.projection.conversation.clone_cache_logical_bytes()?)
                .ok_or_else(|| Error::Invalid("snapshot clone reservation overflows".into()))?
        } else { 0 };
        let mut quota = crate::wire_codec::CodecQuota::new(work, reservation)?;
        let mut copied = Self::empty(self.authority.kind, self.revision, self.projection.lifecycle,
            self.projection.active_extension_revision, self.projection.effects_archived, quota.work())?;
        let (conversation, retained) = self.projection.conversation.copy(quota.work(), None)?;
        copied.projection.conversation = conversation;
        if native { quota.adopt(retained)?; }
        let mut ordinary_work = None;
        let mut ordinary_quota = crate::wire_codec::CodecQuota::from_retained(&mut ordinary_work, 0);
        let mut fields = if native { &mut quota } else { &mut ordinary_quota };
        copied.authority = super::native_custody::copy_authority(&self.authority, fields)?;
        copied.events = super::copy_iter(self.events.len(), self.events.iter(), fields, |event, quota| {
            let (event, retained) = super::native_event_clone_with_custody(event, quota.work())?;
            quota.adopt(retained)?;
            Ok(event)
        })?;
        copy_projection!(copied.projection, self.projection, fields);
        copied.projection.bindings = super::native_custody::copy_vector(
            &self.projection.bindings, fields, |(extension, digest), quota| {
                Ok((super::native_custody::copy_dependency(extension, quota)?, *digest))
            })?;
        copied.format_version = self.format_version;
        copied.state_digest = self.state_digest;
        copied.attestation = self.attestation;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native { copied.adopt_intrinsic_output(&mut quota)?; }
        quota.transfer();
        Ok(copied)
    }

    fn empty(
        authority_kind: super::AggregateKind, revision: u64, lifecycle: LifecycleState,
        active_extension_revision: Option<u64>, effects_archived: bool,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_allocation = work.as_deref_mut().filter(|io| io.original_native_work().is_some())
            .map(|io| io.take_native_custody(0)).transpose()?;
        let _ = work;
        Ok(Self {
            format_version: 6,
            authority: Authority { kind: authority_kind, id: String::new() },
            revision,
            events: Vec::new(),
            projection: Projection {
                lifecycle, active_extensions: Vec::new(), active_extension_revision,
                extension_states: BTreeMap::new(), configured_extensions: Vec::new(),
                active_configurations: Vec::new(), effects: BTreeMap::new(),
                indexed_terminal_effects: VecDeque::new(), effects_archived,
                forks: Vec::new(), published_merges: BTreeSet::new(),
                conversation: ConversationState::default(), latest_context_checkpoint: None,
                interactions: BTreeMap::new(), bindings: Vec::new(),
            },
            state_digest: [0; 32],
            attestation: [0; 32],
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_allocation,
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn adopt_intrinsic_output(&mut self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
        let retained = quota.retained();
        let mut incoming = quota.work().as_deref_mut().expect("native snapshot observer")
            .take_native_custody(retained)?;
        quota.transfer();
        self.native_allocation.as_mut().expect("installed native snapshot owner")
            .absorb(&mut incoming).map_err(|cause| Error::NativeWork { cause, custody: Some(incoming) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_custody(
        &mut self, allocation: acyclic_fs::NativeAllocation,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.native_allocation = Some(allocation);
        self.adopt_unclaimed_native_conversation_custody(work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn adopt_unclaimed_native_conversation_custody(
        &mut self, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let bytes = self.projection.conversation.native_unclaimed_message_buffer_bytes()?;
        if bytes != 0 {
            let shell = self.native_allocation.as_mut().expect("installed snapshot custody")
                .split_off(bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.projection.conversation.install_native_message_buffer(shell);
        }
        let archive_bytes = self.projection.conversation.native_unclaimed_archived_prefix_bytes()?;
        if archive_bytes != 0 {
            let archive = self.native_allocation.as_mut().expect("installed snapshot custody")
                .split_off(archive_bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.projection.conversation.install_native_archived_prefix(archive);
        }
        let cache_bytes = self.projection.conversation.native_unclaimed_cache_bytes()?;
        if cache_bytes != 0 {
            let cache = self.native_allocation.as_mut().expect("installed snapshot custody")
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

    /// Decodes authenticated state and rebuilds its conversation cache.
    pub fn decode(
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
        custody.adopt(admitted_owned)?;
        let admission = admission?;
        let mut snapshot: Snapshot = if let Some(admission) = admission {
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
            custody.relinquish(admission.1)?;
            let (snapshot, cache_bytes) = wire.into_snapshot_with_work(
                maximum_bytes, custody.work())?;
            custody.adopt(cache_bytes)?;
            snapshot
        } else {
            crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?
        };
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

impl Reducer {
    fn clone_reservation(&self) -> Result<u64> {
        let inventory = source_inventory(&(
            &self.authority, &self.authority_verifier.id, &self.authority_verifier.audience,
            &self.active_extensions, &self.extension_states, &self.configured_extensions,
            &self.active_configurations, &self.events, &self.operation_positions,
            &self.effects, &self.indexed_terminal_effects, &self.forks, &self.published_merges,
            &self.conversation, &self.latest_context_checkpoint, &self.interactions,
        ))?;
        let cache = self.conversation.clone_cache_logical_bytes()?;
        // The tuple's sixteen field slots replace one reducer slot.
        inventory.checked_sub(15 * CloneInventory::SLOT)
            .and_then(|bytes| bytes.checked_add(cache))
            .ok_or_else(|| Error::Invalid("reducer clone reservation overflows".into()))
    }

    /// Captures the projection after one committed event.
    pub(crate) fn publication_snapshot_with_work(
        &self, event: &Event, maximum_events: usize, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        let native = work.as_deref().is_some_and(super::native_custody::is_native);
        let reservation = if work.is_some() && !native {
            self.clone_reservation()?.checked_add(source_inventory(event)?.checked_mul(3)
                .ok_or_else(|| Error::Invalid("publication event clone reservation overflows".into()))?)
                .ok_or_else(|| Error::Invalid("publication preview reservation overflows".into()))?
        } else { 0 };
        if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
        let mut quota = crate::wire_codec::CodecQuota::new(work, reservation)?;
        let result = (|| {
            let mut preview = self.copy(quota.work())?;
            let input = if native {
                let (input, retained) = super::native_event_clone_with_custody(event, quota.work())?;
                quota.adopt(retained)?;
                input
            } else { event.clone() };
            preview.apply_committed(input, quota.work())?;
            preview.mark_indexed_terminal_effect_with_work(event, quota.work())?;
            preview.snapshot(maximum_events, quota.work())
        })();
        if !native { quota.relinquish(reservation)?; }
        result
    }

    /// Captures authenticated state and a bounded retry suffix.
    pub fn snapshot(
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
        let native = work.as_deref().is_some_and(super::native_custody::is_native);
        let reservation = if work.is_some() && !native {
            self.clone_reservation()?.checked_mul(2)
                .ok_or_else(|| Error::Invalid("snapshot clone reservation overflows".into()))?
        } else { 0 };
        if let Some(observer) = work.as_deref() { observer.check_cancellation()?; }
        let mut quota = crate::wire_codec::CodecQuota::new(work, reservation)?;
        let mut snapshot = Snapshot::empty(self.authority.kind, self.revision, self.lifecycle,
            self.active_extension_revision, self.effects_archived, quota.work())?;
        let (conversation, retained) = match self.resident_checkpoint_selection() {
            Some(selection) => self.conversation.checkpointed_suffix(selection, quota.work(), None)?,
            None => self.conversation.copy(quota.work(), None)?,
        };
        snapshot.projection.conversation = conversation;
        if native { quota.adopt(retained)?; }
        {
            let mut ordinary_work = None;
            let mut ordinary_quota = crate::wire_codec::CodecQuota::from_retained(&mut ordinary_work, 0);
            let mut fields = if native { &mut quota } else { &mut ordinary_quota };
            snapshot.authority = super::native_custody::copy_authority(&self.authority, fields)?;
            let retained_events = self.events.len().min(maximum_events);
            snapshot.events = super::copy_iter(retained_events,
                self.events.iter().skip(self.events.len() - retained_events), fields, |event, quota| {
                    let (event, retained) = super::native_event_clone_with_custody(event, quota.work())?;
                    quota.adopt(retained)?;
                    Ok(event)
                })?;
            copy_projection!(snapshot.projection, self, fields);
        }
        let count = self.schemas.schemas.as_deref().map_or(0, BTreeMap::len);
        snapshot.projection.bindings = if native { super::reserve_copy_vector(count, &mut quota)? }
            else { Vec::with_capacity(count) };
        for ((name, version), binding) in self.schemas.schemas.as_deref().into_iter().flat_map(BTreeMap::iter) {
            let digest = crate::contract::canonical_json_digest_with_work(binding, crate::store::default_projection_read_limits().maximum_bytes, quota.work())?;
            let name = if native { super::native_custody::copy_text(name, &mut quota)? } else {
                if let Some(observer) = quota.work().as_deref_mut() {
                    observer.check_cancellation()?;
                    observer.copy_bytes(name.len() as u64)?;
                }
                name.clone()
            };
            snapshot.projection.bindings.push((ExtensionDependency { name, version: *version }, digest));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native { snapshot.adopt_intrinsic_output(&mut quota)?; }
        snapshot.state_digest = snapshot.digest_with_work(quota.work())?;
        snapshot.attestation = self.authority_verifier.attest_snapshot_with_work(&snapshot, quota.work())?;
        quota.transfer();
        Ok((snapshot, reservation))
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
            use super::native_custody::{NativeComponentKey as Key, Component};
            let count = self.conversation.messages().len();
            let mut slots = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut prepared = super::reserve_copy_vector::<(u64, Option<Component>)>(
                count, &mut slots)?;
            for message in self.conversation.messages() {
                prepared.push((message.sequence, None));
            }
            let mut next = 0;
            let (replacement, _) = self.conversation.checkpointed_suffix(
                selection, slots.work(), Some(&mut |sequence, produced, work| {
                    while next < prepared.len() && prepared[next].0 < sequence { next += 1; }
                    if next == prepared.len() || prepared[next].0 != sequence {
                        return Err(preparation_inventory_error(work, "conversation copied component identity is invalid"));
                    }
                    let allocation = work.as_deref_mut().expect("native retirement observer")
                        .take_native_custody(produced)?;
                    prepared[next].1 = Some((Key::ConversationMessage(sequence), allocation, None));
                    next += 1;
                    Ok(())
                }))?;
            self.conversation = replacement;
            for (sequence, claim) in prepared {
                if let Some(claim) = claim {
                    drop(self.native_custody.install(Some(claim), slots.work())?);
                } else {
                    self.native_custody.remove(&Key::ConversationMessage(sequence));
                }
            }
            return Ok(());
        }
        self.conversation = self.conversation.checkpointed_suffix(selection, work, None).map(|(state, _)| state)?;
        Ok(())
    }

    /// Restores authenticated state without invoking any historical transition.
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
        let metadata = authority_verifier.logical_owned_bytes()?;
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
        if snapshot.native_allocation.is_some()
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if intrinsic && snapshot.native_allocation.is_none() {
            let required = snapshot.predecoded_native_owned_bytes()?;
            let observer = work.as_deref_mut().expect("native restoration observer");
            observer.admit_owned(required)?;
            observer.retain_owned(required)?;
            let allocation = observer.take_native_custody(required)?;
            snapshot.install_native_custody(allocation, work)?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if intrinsic {
            snapshot.adopt_unclaimed_native_conversation_custody(work)?;
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
            let bytes = authority_verifier.logical_owned_bytes()?;
            Some((super::native_custody::NativeComponentKey::Base,
                work.as_deref_mut().expect("native restore observer").take_native_custody(bytes)?, None))
        } else {
            super::native_custody::NativeComponentCustody::prepare(
                &super::native_custody::NativeComponentKey::Base, 0, work)?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let base = super::native_custody::NativeComponentCustody::prepare(
            &super::native_custody::NativeComponentKey::Base, 0, work)?;
        drop(restored_custody.install(base, work)?);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let retired_vector_bytes = if intrinsic {
            super::native_custody::owned_sum([
                super::native_custody::owned_buffer::<(ExtensionDependency, ExtensionConfiguration)>(
                    snapshot.projection.configured_extensions.capacity())?,
                super::native_custody::owned_buffer::<(Authority, ForkSeed)>(
                    snapshot.projection.forks.capacity())?,
            ])?
        } else { 0 };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut allocation = snapshot.native_allocation.take();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _retired_vector_backing = if let Some(source) = allocation.as_mut() {
            match source.split_off(retired_vector_bytes) {
                Ok(retired) => Some(retired),
                Err(cause) => return Err(Error::NativeWork { cause, custody: allocation.take() }),
            }
        } else { None };
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
        super::native_custody::owned_sum([
            super::native_custody::owned_buffer::<(crate::OperationId, u64)>(self.events.len())?,
            super::native_custody::owned_buffer::<(ExtensionDependency, ExtensionConfiguration)>(
                self.projection.configured_extensions.len())?,
            super::native_custody::owned_buffer::<(Authority, ForkSeed)>(self.projection.forks.len())?,
        ])
    }

    /// Pure capacity witness for splitting a predecoded outer DTO's original guard.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn predecoded_native_owned_bytes(&self) -> Result<u64> {
        use super::native_custody::*;
        let projection = &self.projection;
        let mut bytes = owned_sum([
            self.authority.id.capacity() as u64,
            owned_vector(&self.events, super::event_owned_bytes)?,
            owned_vector(&projection.active_extensions, |extension| Ok(extension.name.capacity() as u64))?,
            owned_vector(&projection.configured_extensions, |(extension, configuration)| {
                owned_sum([extension.name.capacity() as u64, owned_configuration(configuration)?])
            })?,
            owned_vector(&projection.active_configurations, owned_configuration)?,
            owned_buffer::<EffectId>(projection.indexed_terminal_effects.capacity())?,
            owned_vector(&projection.forks, |(authority, seed)| {
                owned_sum([authority.id.capacity() as u64, owned_seed(seed)?])
            })?,
            owned_vector(&projection.bindings, |(extension, _)| Ok(extension.name.capacity() as u64))?,
            projection.conversation.native_unclaimed_message_buffer_bytes()?,
            projection.conversation.native_unclaimed_archived_prefix_bytes()?,
            projection.conversation.native_unclaimed_cache_bytes()?,
        ])?;
        for (name, (reference, record)) in &projection.extension_states {
            bytes = owned_sum([bytes,
                std::mem::size_of::<(String, (EventReference, ExtensionRecord))>() as u64,
                name.capacity() as u64, reference.authority.id.capacity() as u64, owned_record(record)?])?;
        }
        for effect in projection.effects.values() {
            bytes = owned_sum([bytes, std::mem::size_of::<(EffectId, EffectState)>() as u64,
                owned_effect(effect)?])?;
        }
        for (name, operation) in &projection.published_merges {
            bytes = owned_sum([bytes, std::mem::size_of::<(String, Vec<u8>)>() as u64,
                name.capacity() as u64, operation.capacity() as u64])?;
        }
        for message in projection.conversation.messages() {
            bytes = owned_sum([bytes, owned_message(message)?])?;
        }
        if let Some(file) = &projection.latest_context_checkpoint {
            bytes = owned_sum([bytes, file.logical_owned_bytes()?])?;
        }
        for (ticket, resolution) in projection.interactions.values() {
            bytes = owned_sum([bytes,
                std::mem::size_of::<(uuid::Uuid, (InteractionTicket, Option<InteractionResolution>))>() as u64,
                owned_ticket(ticket)?,
                resolution.as_ref().map_or(Ok(0), owned_resolution)?])?;
        }
        Ok(bytes)
    }

    /// Adopts a genuine predecoded payload claim without issuing native work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn adopt_predecoded_native_custody(
        mut self, allocation: acyclic_fs::NativeAllocation,
    ) -> Result<Self> {
        let incoming = allocation;
        if self.native_allocation.is_some() {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(incoming),
            });
        }
        self.native_allocation = Some(incoming);
        let required = self.predecoded_native_owned_bytes()?;
        if self.native_allocation.as_ref().expect("installed predecoded custody").bytes() < required {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: self.native_allocation.take(),
            });
        }
        Ok(self)
    }

    fn digest_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<[u8; 32]> {
        crate::contract::canonical_json_digest_with_work(&(
            self.format_version,
            &self.authority,
            self.revision,
            &self.events,
            &self.projection,
        ), crate::store::default_projection_read_limits().maximum_bytes, work)
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
        let mut scratch = crate::wire_codec::CodecQuota::new(work, scratch_bytes)?;
        if self.format_version != 6 {
            return Err(Error::Unsupported(format!(
                "snapshot format {}",
                self.format_version
            )));
        }
        verifier.verify_audience(&self.authority, scratch.work())?;
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
            verifier.verify_event(event, scratch.work())?;
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
            let binding = schemas.pinned_binding(&extension.name, extension.version, None, &mut None)?;
            if crate::contract::canonical_json_digest_with_work(binding, crate::store::default_projection_read_limits().maximum_bytes, scratch.work())? != *expected {
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
        if let Some(work) = work.as_deref_mut() { work.release_owned(capacity)?; }
        Ok(attestation)
    }
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
            Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
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
                scope: issuer.issue("snapshot-source", &crate::Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof"),
                causal_parent: None,
                action: super::super::Action::TransitionLifecycle {
                    to, reason: Some(format!("{index}:{}", "owned canonical lifecycle reason".repeat(128))),
                },
            })?;
        }
        Ok((reducer, issuer))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_original_cold_decoder_preserves_digest_and_issuer_mac_rejection() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let (reducer, issuer) = populated_fixture()?;
        let original = reducer.snapshot(usize::MAX, &mut None)?.0;
        for forged_digest in [false, true] {
            let mut forged = original.copy(&mut None)?;
            if forged_digest {
                forged.projection.lifecycle = LifecycleState::Pending;
                forged.state_digest = forged.digest_with_work(&mut None)?;
            } else {
                forged.attestation[0] ^= 1;
            }
            let bytes = crate::contract::canonical_json_bytes(&forged)?;
            let token = CancellationToken::new();
            let source = NativeMaterializationWork::new(native_budget(), &token).expect("cold decoder ledger");
            let mut io = crate::store::history::native_work_io(&source).expect("cold decoder observer");
            let decoded = Snapshot::decode(&bytes, 4_194_304, &mut Some(&mut io))?;
            drop(io);
            assert!(matches!(
                Reducer::restore_native(decoded, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), &source),
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
        let decoded = Snapshot::decode(&bytes, 4_194_304, &mut Some(&mut io))?;
        drop(io);
        assert!(matches!(Reducer::restore_native(decoded, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), &source),
            Err(Error::Invalid(_))));
        source.admit_retained(native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[test]
    fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
        let (reducer, issuer) = fixture();
        let original = reducer.snapshot(usize::MAX, &mut None)?.0;
        let mut forged = original.copy(&mut None)?;
        forged.projection.lifecycle = LifecycleState::Completed;
        forged.state_digest = forged.digest_with_work(&mut None)?;
        assert!(matches!(
            Reducer::restore(forged, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        let mut forged_cache = original;
        forged_cache.revision = 50;
        forged_cache.state_digest = forged_cache.digest_with_work(&mut None)?;
        assert!(matches!(
            Reducer::restore(forged_cache, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
        let (reducer, issuer) = fixture();
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
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
                Reducer::restore(snapshot.copy(&mut None)?, other.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
                Err(Error::Unauthorized(_))
            ));
        }
        let mut old = snapshot;
        old.format_version = 2;
        assert!(matches!(
            Reducer::restore(old, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
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
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
        assert!(matches!(
            Reducer::restore(snapshot.copy(&mut None)?, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
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
                Reducer::restore(snapshot.copy(&mut None)?, issuer.verifier(&mut None).expect("verifier copy"), changed),
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
        let restored = Reducer::restore(snapshot, issuer.verifier(&mut None).expect("verifier copy"), expanded.clone())?;
        reducer.schemas = expanded;
        assert_eq!(restored, reducer);
        Ok(())
    }
}
