//! Finite metadata inspection and clone admission.
use super::*;
#[derive(Default)]
pub(super) struct CloneInventory<'w> {
    pub(super) bytes: Option<u64>,
    pub(super) work: Option<&'w mut dyn HistoryIo>,
    pub(super) failure: Option<Error>,
    pub(super) depth: u16,
    pub(super) slots: u64,
    pub(super) slot_bytes: Option<u64>,
}

#[derive(Debug)]
pub(super) struct InventoryError;
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
    pub(super) const SLOT: u64 = {
        let sizes = [
            std::mem::size_of::<Reducer>(), std::mem::size_of::<Snapshot>(),
            std::mem::size_of::<snapshot::Projection>(), std::mem::size_of::<Event>(),
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
        None => Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Invalid, "closed admission metadata inspection rejected")),
    }
}
