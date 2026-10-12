use super::*;

pub(super) fn owned_size(
    count: usize,
    width: usize,
) -> Result<u64, NativeTreeMaterializationError> {
    u64::try_from(count.checked_mul(width).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| crate::WorkError::Overflow.into())
}
pub(super) fn plan_owned_bytes(
    plan: &MaterializationPlan,
) -> Result<u64, NativeTreeMaterializationError> {
    let mut bytes = owned_size(
        plan.edits.capacity(),
        std::mem::size_of::<MaterializationEdit>(),
    )?;
    for edit in &plan.edits {
        let (a, b) = match edit {
            MaterializationEdit::Install { path, image }
            | MaterializationEdit::SetMetadata { path, image } => {
                (path.capacity(), image.capacity())
            }
            MaterializationEdit::Remove { path } => (path.capacity(), 0),
            MaterializationEdit::Rename { from, to } => (from.capacity(), to.capacity()),
        };
        bytes = bytes
            .checked_add(owned_size(
                a.checked_add(b).ok_or(crate::WorkError::Overflow)?,
                1,
            )?)
            .ok_or(crate::WorkError::Overflow)?;
    }
    if let MaterializationIntent::NativePreparation {
        selection,
        original,
    } = &plan.intent
    {
        bytes = bytes
            .checked_add(original.capacity() as u64)
            .and_then(|bytes| {
                bytes.checked_add(
                    owned_size(
                        selection.capacity(),
                        std::mem::size_of::<MaterializationPreparationSelection>(),
                    )
                    .ok()?,
                )
            })
            .ok_or(crate::WorkError::Overflow)?;
        for selected in selection {
            bytes = bytes
                .checked_add(selected.provider_identity.capacity() as u64)
                .ok_or(crate::WorkError::Overflow)?;
        }
    }
    Ok(bytes)
}
/// Owned capacity of a journal's actual payload.
pub fn journal_owned_bytes(
    journal: &MaterializationJournal,
) -> Result<u64, NativeTreeMaterializationError> {
    let mut bytes = plan_owned_bytes(&journal.plan)?
        .checked_add(owned_size(
            journal.preimages.capacity(),
            std::mem::size_of::<MaterializationPreimage>(),
        )?)
        .ok_or(crate::WorkError::Overflow)?;
    for preimage in &journal.preimages {
        bytes = bytes
            .checked_add(preimage.image.capacity() as u64)
            .ok_or(crate::WorkError::Overflow)?;
    }
    Ok(bytes)
}
pub(super) fn native_vec_push<T>(
    values: &mut Vec<T>,
    value: T,
    allocation: &mut NativeAllocation,
) -> Result<(), NativeTreeMaterializationError> {
    if values.len() == values.capacity() {
        let added = values.capacity().max(1);
        let before = owned_size(values.capacity(), std::mem::size_of::<T>())?;
        let mut growth = allocation.work().claim(
            owned_size(added, std::mem::size_of::<T>())?,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let reserved = values.try_reserve_exact(added);
        let reconciled = growth.reconcile_capacity(
            owned_size(values.capacity(), std::mem::size_of::<T>())?
                .checked_sub(before)
                .ok_or(crate::WorkError::Overflow)?,
        );
        allocation.absorb(&mut growth)?;
        reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
        reconciled?;
    }
    values.push(value);
    Ok(())
}
pub(super) fn copy_string(
    value: &str,
    work: &NativeMaterializationWork,
    allocation: &mut NativeAllocation,
) -> Result<String, NativeTreeMaterializationError> {
    String::from_utf8(copy_image(value.as_bytes(), work, allocation)?)
        .map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}
pub(super) fn copy_image(
    value: &[u8],
    work: &NativeMaterializationWork,
    allocation: &mut NativeAllocation,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let (bytes, mut added) = work.copy_bytes(value)?;
    allocation.absorb(&mut added)?;
    Ok(bytes)
}
pub(super) fn duplicate_native_edit(
    edit: &MaterializationEdit,
    work: &NativeMaterializationWork,
) -> Result<(MaterializationEdit, NativeAllocation), NativeTreeMaterializationError> {
    let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let edit = match edit {
        MaterializationEdit::Install { path, image } => MaterializationEdit::Install {
            path: copy_string(path, work, &mut allocation)?,
            image: copy_image(image, work, &mut allocation)?,
        },
        MaterializationEdit::SetMetadata { path, image } => MaterializationEdit::SetMetadata {
            path: copy_string(path, work, &mut allocation)?,
            image: copy_image(image, work, &mut allocation)?,
        },
        MaterializationEdit::Remove { path } => MaterializationEdit::Remove {
            path: copy_string(path, work, &mut allocation)?,
        },
        MaterializationEdit::Rename { from, to } => MaterializationEdit::Rename {
            from: copy_string(from, work, &mut allocation)?,
            to: copy_string(to, work, &mut allocation)?,
        },
    };
    Ok((edit, allocation))
}

pub(super) fn check_native_plan(
    plan: &MaterializationPlan,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let count = plan
        .edits
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let _allocation = work.claim(
        owned_size(count, std::mem::size_of::<&str>())?,
        u64::from(count != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut paths = Vec::new();
    paths
        .try_reserve_exact(count)
        .map_err(NativeTreeMaterializationError::AllocationFailed)?;
    let valid = validate_plan(
        plan,
        paths,
        || work.charge(NativeResourceCredit::Items.delta(1)),
        |paths| {
            native_sort_by(paths, work, |a, b| {
                Ok(native_compare(a.bytes(), b.bytes(), work)? == std::cmp::Ordering::Greater)
            })
        },
    )?;
    if valid {
        Ok(())
    } else {
        Err(NativeTreeMaterializationError::InvalidLayout)
    }
}
#[cfg(feature = "native-mount")]
pub(super) fn native_absolute_string(
    path: &str,
    work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
    let length = path
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let allocation = work.claim(length as u64, 1, crate::WorkCounters::UNCHARGED)?;
    let delta = NativeResourceCredit::Copy.delta(path.len() as u64);
    work.admit(delta)?;
    let mut value = String::new();
    value
        .try_reserve_exact(length)
        .map_err(NativeTreeMaterializationError::AllocationFailed)?;
    value.push('/');
    value.push_str(path);
    work.spent(delta)?;
    Ok((value, allocation))
}
