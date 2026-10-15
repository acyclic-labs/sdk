use acyclic_fs::{
    MaterializationEdit, MaterializationJournal, MaterializationPreimage,
    MaterializationPreparationSelection,
};
use std::mem::size_of;

pub(crate) fn decode_owned_bytes(items: u64, input: u64, scratch: u64) -> Option<u64> {
    // This closed DTO has no Value, flattened maps, or untagged enums. Its
    // non-byte vector elements each require at least three JSON items, covering
    // the old and new vector capacities during growth. Byte vectors need less.
    // Four input lengths cover owned strings and their decoder scratch.
    let slot = [
        size_of::<MaterializationEdit>(),
        size_of::<MaterializationPreimage>(),
        size_of::<MaterializationPreparationSelection>(),
        size_of::<String>(),
    ]
    .into_iter()
    .max()? as u64;
    items
        .checked_mul(slot)?
        .checked_add(input.checked_mul(4)?)?
        .checked_add(scratch)?
        .checked_add(size_of::<MaterializationJournal>() as u64)
}
