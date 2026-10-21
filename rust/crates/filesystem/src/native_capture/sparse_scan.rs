use super::*;

pub(super) fn block_has_data(
    block: &[u8],
    meter: &NativeMaterializationWork,
) -> Result<bool, CaptureError> {
    if block.is_empty() {
        return Ok(false);
    }
    // Lock order: original leaf (50) precedes resource allowances.
    // No I/O, owner retirement or demand re-entry while the leaf stays locked.
    meter
        .with_original_leaf(|work| {
            let items = |count| crate::materializer::NativeResourceCredit::Items.delta(count);
            let maximum = work.item_charge(items(block.len() as u64));
            let (maximum, length) = if work.reserve(maximum).is_ok() {
                (maximum, block.len())
            } else {
                // Admit only the remaining prefix so a tight budget still
                // accepts an early match without inspecting a refused byte.
                let length = block
                    .len()
                    .min(usize::try_from(work.remaining()?.items_examined).unwrap_or(usize::MAX));
                let maximum = work.item_charge(items(length as u64));
                work.reserve(maximum)?;
                (maximum, length)
            };
            let found = block.iter().take(length).position(|byte| *byte != 0);
            let examined = found.map_or(length, |index| index + 1);
            let actual = work.item_charge(items(examined as u64));
            work.complete(maximum, actual)?;
            work.check()?;
            if found.is_none() && length < block.len() {
                work.charge(items(1))?;
            }
            Ok(found.is_some())
        })
        .map_err(capture_demand_error)
}

#[cfg(test)]
#[cfg(acyclic_fs_loom)]
#[path = "sparse_scan_loom.rs"]
mod model_checking;
#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
#[path = "sparse_scan_tests.rs"]
mod tests;
