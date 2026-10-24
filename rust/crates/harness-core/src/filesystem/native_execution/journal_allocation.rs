use acyclic_fs::{
    MaterializationEdit, MaterializationJournal, MaterializationPreimage,
    MaterializationPreparationSelection,
};
use std::mem::size_of;

pub(crate) fn decode_owned_bytes(slots: u64, input: u64, scratch: u64) -> Option<u64> {
    // This closed DTO has no Value, flattened maps, or untagged enums. Its
    // non-byte vector elements require a container, even in struct-as-array
    // form. Four slots per container cover initial capacity and simultaneous
    // old/new growth. Slice decoding has no sequence size hint; a resident
    // Value can preallocate before validating its elements, so retain the
    // older per-item bound for that path. Opaque bytes need no DTO slots.
    // Four input lengths cover owned strings and their decoder scratch.
    let slot = [
        size_of::<MaterializationEdit>(),
        size_of::<MaterializationPreimage>(),
        size_of::<MaterializationPreparationSelection>(),
        size_of::<String>(),
    ]
    .into_iter()
    .max()? as u64;
    slots
        .checked_mul(slot)?
        .checked_add(input.checked_mul(4)?)?
        .checked_add(scratch)?
        .checked_add(size_of::<MaterializationJournal>() as u64)
}
