//! Admitted native vector allocation shared by the contract's codecs.
//!
//! Every physical vector is admitted before allocation and remains funded with
//! the result, so a codec's work is proportional to its bounded input.

use std::sync::Arc;

/// Allocates an admitted vector of `length` values.
pub fn vector<T>(
    length: usize,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    allocation: &mut acyclic_native_runtime::ReadAllocation,
) -> std::io::Result<Vec<T>> {
    let bytes = length
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(std::io::ErrorKind::OutOfMemory)?;
    let mut claim = acyclic_native_runtime::ReadAllocation::new(meter, bytes)?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(length)
        .map_err(|error| meter.allocation_failed(error))?;
    let excess = value
        .capacity()
        .saturating_sub(length)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(std::io::ErrorKind::OutOfMemory)?;
    claim.reconcile_capacity(
        bytes
            .checked_add(excess)
            .ok_or(std::io::ErrorKind::OutOfMemory)?,
    )?;
    allocation.absorb(&mut claim)?;
    Ok(value)
}

/// Copies `source` into an admitted vector, charging its copy.
pub fn copy_vector<T: Clone>(
    source: &[T],
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    allocation: &mut acyclic_native_runtime::ReadAllocation,
) -> std::io::Result<Vec<T>> {
    let mut output = vector(source.len(), meter, allocation)?;
    let bytes = std::mem::size_of_val(source) as u64;
    meter.admit_codec(0, bytes, 0)?;
    output.extend_from_slice(source);
    meter.record_codec(0, bytes, 0)?;
    Ok(output)
}
