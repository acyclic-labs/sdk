use super::*;

pub(super) fn admit_patch_plan(
    old_size: u64,
    mutations: &[ExtentMutation],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(u64, usize), FsError> {
    let mut size = old_size;
    let mut raw_count = 0_usize;
    for mutation in mutations {
        charge_items(accounting, 1)?;
        match mutation {
            ExtentMutation::Replace {
                offset,
                length,
                kind,
                extend,
            } => {
                if *length == 0 {
                    return Err(extent_error(ExtentMutationError::EmptyRange));
                }
                validate_kind(kind, *length).map_err(extent_error)?;
                let end = offset
                    .checked_add(*length)
                    .ok_or_else(|| extent_error(ExtentMutationError::RangeOverflow))?;
                if end > size && !extend {
                    return Err(extent_error(ExtentMutationError::OutsideFile));
                }
                raw_count = raw_count
                    .checked_add(usize::from(*offset > size) + 1)
                    .ok_or(WorkError::Overflow)?;
                size = size.max(end);
            }
            ExtentMutation::Resize { logical_bytes } => {
                raw_count = raw_count
                    .checked_add(usize::from(*logical_bytes != size))
                    .ok_or(WorkError::Overflow)?;
                size = *logical_bytes;
            }
        }
    }
    Ok((size, raw_count))
}

pub(super) fn charge_items(
    accounting: &mut KernelAccounting<'_>,
    count: u64,
) -> Result<(), FsError> {
    accounting.charge(WorkCounters {
        items_examined: count,
        ..WorkCounters::UNCHARGED
    })
}

pub(super) fn normalize_raw_patches(
    raw: &[Patch],
    final_size: u64,
    accounting: &mut KernelAccounting<'_>,
    sort: impl FnOnce(&mut Owned<Vec<u64>>, &mut KernelAccounting<'_>) -> Result<(), FsError>,
) -> Result<Owned<Vec<Patch>>, FsError> {
    let capacity = raw.len().checked_mul(2).ok_or(WorkError::Overflow)?;
    let mut coordinates = Owned::<Vec<u64>>::allocate(capacity, accounting)?;
    for patch in raw {
        charge_items(accounting, 1)?;
        let start = patch.offset.min(final_size);
        let end = patch.end.min(final_size);
        if start < end {
            coordinates.value.extend([start, end]);
        }
    }
    if coordinates.value.is_empty() {
        coordinates.retire(accounting)?;
        return Owned::allocate(0, accounting);
    }
    sort(&mut coordinates, accounting)?;
    charge_items(accounting, usize_to_u64(coordinates.value.len()))?;
    coordinates.value.dedup();
    let intervals = coordinates.value.len().saturating_sub(1);
    let mut assignments = Owned::<Vec<Option<usize>>>::allocate(intervals, accounting)?;
    charge_items(accounting, usize_to_u64(intervals))?;
    assignments.value.resize(intervals, None);
    let mut parents = Owned::<Vec<usize>>::allocate(intervals + 1, accounting)?;
    charge_items(accounting, usize_to_u64(intervals + 1))?;
    parents.value.extend(0..=intervals);
    let result = (|| {
        for (patch_index, patch) in raw.iter().enumerate().rev() {
            charge_items(accounting, 1)?;
            let start = patch.offset.min(final_size);
            let end = patch.end.min(final_size);
            if start >= end {
                continue;
            }
            let left = coordinate_index(&coordinates.value, start, accounting)?;
            let right = coordinate_index(&coordinates.value, end, accounting)?;
            let mut interval = find_next(&mut parents.value, left, accounting)?;
            while interval < right {
                charge_items(accounting, 1)?;
                *assignments
                    .value
                    .get_mut(interval)
                    .ok_or(WorkError::Overflow)? = Some(patch_index);
                let successor = find_next(&mut parents.value, interval + 1, accounting)?;
                *parents.value.get_mut(interval).ok_or(WorkError::Overflow)? = successor;
                interval = successor;
            }
        }
        let mut result = Owned::<Vec<Patch>>::allocate(intervals, accounting)?;
        for (interval, assignment) in assignments.value.iter().enumerate() {
            charge_items(accounting, 1)?;
            let Some(patch_index) = assignment else {
                continue;
            };
            let source = raw.get(*patch_index).ok_or(WorkError::Overflow)?;
            let offset = *coordinates.value.get(interval).ok_or(WorkError::Overflow)?;
            let end = *coordinates
                .value
                .get(interval + 1)
                .ok_or(WorkError::Overflow)?;
            let candidate = Patch {
                offset,
                end,
                kind: offset_kind(&source.kind, offset - source.offset).map_err(extent_error)?,
            };
            if let Some(previous) = result.value.last_mut()
                && patch_continues(previous, &candidate).map_err(extent_error)?
            {
                previous.end = candidate.end;
            } else {
                result.value.push(candidate);
            }
        }
        Ok(result)
    })();
    parents.retire(accounting)?;
    assignments.retire(accounting)?;
    coordinates.retire(accounting)?;
    result
}

pub(super) fn sort_patch_coordinates(
    coordinates: &mut Owned<Vec<u64>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    // Each nonempty patch contributes start < end. One remaining pair is
    // already ordered and needs neither radix passes nor their scratch buffer.
    if coordinates.value.len() <= 2 {
        return Ok(());
    }
    sort_all_coordinates(coordinates, accounting)
}

fn sort_all_coordinates(
    coordinates: &mut Owned<Vec<u64>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let mut scratch = Owned::<Vec<u64>>::allocate(coordinates.value.len(), accounting)?;
    let sorted = radix_sort(coordinates, &mut scratch, accounting);
    scratch.retire(accounting)?;
    sorted
}

fn radix_sort(
    values: &mut Owned<Vec<u64>>,
    scratch: &mut Owned<Vec<u64>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    scratch.value.resize(values.value.len(), 0);
    for byte in 0..8_u32 {
        accounting.charge(WorkCounters {
            items_examined: usize_to_u64(values.value.len())
                .checked_mul(2)
                .ok_or(WorkError::Overflow)?,
            bytes_copied: usize_to_u64(
                values
                    .value
                    .len()
                    .checked_mul(size_of::<u64>())
                    .ok_or(WorkError::Overflow)?,
            ),
            ..WorkCounters::UNCHARGED
        })?;
        let shift = byte * 8;
        let mut counts = [0_usize; 256];
        for value in values.value.iter() {
            let count = counts
                .get_mut(usize::from(((value >> shift) & 0xff) as u8))
                .ok_or(WorkError::Overflow)?;
            *count += 1;
        }
        let mut offset = 0;
        for count in &mut counts {
            let current = *count;
            *count = offset;
            offset += current;
        }
        for value in values.value.iter() {
            let count = counts
                .get_mut(usize::from(((value >> shift) & 0xff) as u8))
                .ok_or(WorkError::Overflow)?;
            *scratch.value.get_mut(*count).ok_or(WorkError::Overflow)? = *value;
            *count += 1;
        }
        // Move physical data and its exact original custody together on every pass.
        std::mem::swap(values, scratch);
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub(super) fn coordinate_index(
    values: &[u64],
    target: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<usize, FsError> {
    let found = binary_search_with_accounting(values, accounting, |value| value.cmp(&target))?;
    found.map_err(|_| extent_error(ExtentMutationError::PatchInvariant))
}

pub(super) fn find_next(
    parents: &mut [usize],
    start: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<usize, FsError> {
    let mut root = start;
    while *parents.get(root).ok_or(WorkError::Overflow)? != root {
        charge_items(accounting, 1)?;
        root = *parents.get(root).ok_or(WorkError::Overflow)?;
    }
    let mut current = start;
    while *parents.get(current).ok_or(WorkError::Overflow)? != current {
        charge_items(accounting, 1)?;
        let next = *parents.get(current).ok_or(WorkError::Overflow)?;
        *parents.get_mut(current).ok_or(WorkError::Overflow)? = root;
        current = next;
    }
    Ok(root)
}
