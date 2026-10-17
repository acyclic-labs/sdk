// Frozen capture_reserve from 96790de61, independent of the candidate.
use super::*;

pub(super) fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    meter
        .with_original_leaf(|work| work.check())
        .map_err(capture_demand_error)?;
    let wanted = values
        .len()
        .checked_add(additional)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    if wanted <= values.capacity() {
        return Ok(());
    }
    let size = std::mem::size_of::<T>();
    let old = values
        .capacity()
        .checked_mul(size)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    let copied = values
        .len()
        .checked_mul(size)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    let grown = wanted.max(values.capacity().saturating_mul(2).max(4));
    let capacity = match grown.checked_mul(size) {
        Some(bytes)
            if bytes as u64
                <= meter
                    .with_original_leaf(|work| work.remaining())
                    .map_err(capture_demand_error)?
                    .peak_allocation_bytes =>
        {
            grown
        }
        _ => wanted,
    };
    let mut replacement = capture_vec(capacity, meter)?;
    capture_charge(
        meter,
        crate::materializer::NativeResourceCredit::Copy.delta(copied as u64),
    )?;
    replacement.append(values);
    let old_values = std::mem::replace(values, replacement);
    drop(old_values);
    meter
        .with_original_leaf(|work| work.release(old as u64))
        .map_err(capture_demand_error)
}
