use super::*;

pub(crate) fn reserve_buffer<T>(
    values: &mut Vec<T>,
    capacity: usize,
    allocation: &mut NativeAllocation,
) -> Result<(), NativeTreeMaterializationError> {
    crate::kernel::reserve_native_buffer(values, capacity, allocation)
}

pub(super) fn push_buffer<T>(
    values: &mut Vec<T>,
    value: T,
    allocation: &mut NativeAllocation,
) -> Result<(), NativeTreeMaterializationError> {
    if values.len() == values.capacity() {
        let capacity = values
            .capacity()
            .checked_mul(2)
            .and_then(|size| size.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        reserve_buffer(values, capacity, allocation)?;
    }
    values.push(value);
    Ok(())
}

pub(super) fn owned_size(
    count: usize,
    width: usize,
) -> Result<u64, NativeTreeMaterializationError> {
    Ok(crate::foundation::usize_to_u64(
        count.checked_mul(width).ok_or(crate::WorkError::Overflow)?,
    ))
}
pub(super) fn copy_image(
    bytes: &[u8],
    allocation: &mut NativeAllocation,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let (bytes, mut copied) = allocation.work().copy_bytes(bytes)?;
    allocation.absorb(&mut copied)?;
    Ok(bytes)
}
pub(super) fn copy_string(
    text: &str,
    allocation: &mut NativeAllocation,
) -> Result<String, NativeTreeMaterializationError> {
    String::from_utf8(copy_image(text.as_bytes(), allocation)?)
        .map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}
pub(super) fn duplicate_edit(
    edit: &MaterializationEdit,
    allocation: &mut NativeAllocation,
) -> Result<MaterializationEdit, NativeTreeMaterializationError> {
    Ok(match edit {
        MaterializationEdit::Install { path, image } => MaterializationEdit::Install {
            path: copy_string(path, allocation)?,
            image: copy_image(image, allocation)?,
        },
        MaterializationEdit::SetMetadata { path, image } => MaterializationEdit::SetMetadata {
            path: copy_string(path, allocation)?,
            image: copy_image(image, allocation)?,
        },
        MaterializationEdit::Remove { path } => MaterializationEdit::Remove {
            path: copy_string(path, allocation)?,
        },
        MaterializationEdit::Rename { from, to } => MaterializationEdit::Rename {
            from: copy_string(from, allocation)?,
            to: copy_string(to, allocation)?,
        },
    })
}
pub(super) fn plan_owned_bytes(
    plan: &MaterializationPlan,
) -> Result<u64, NativeTreeMaterializationError> {
    let mut bytes = owned_size(
        plan.edits.capacity(),
        std::mem::size_of::<MaterializationEdit>(),
    )?;
    let mut add = |count: usize, width: usize| -> Result<(), NativeTreeMaterializationError> {
        bytes = bytes
            .checked_add(owned_size(count, width)?)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(())
    };
    for edit in &plan.edits {
        match edit {
            MaterializationEdit::Install { path, image }
            | MaterializationEdit::SetMetadata { path, image } => {
                add(path.capacity(), 1)?;
                add(image.capacity(), 1)?;
            }
            MaterializationEdit::Remove { path } => add(path.capacity(), 1)?,
            MaterializationEdit::Rename { from, to } => {
                add(from.capacity(), 1)?;
                add(to.capacity(), 1)?;
            }
        }
    }
    if let MaterializationIntent::GenerationTransition { .. } = &plan.intent {
        add(1, MaterializationIntent::TRANSITION_HEAP_BYTES)?;
    }
    if let MaterializationIntent::NativePreparation {
        selection,
        original,
    } = &plan.intent
    {
        add(
            selection.capacity(),
            std::mem::size_of::<MaterializationPreparationSelection>(),
        )?;
        add(original.capacity(), 1)?;
        for item in selection {
            add(item.provider_identity.capacity(), 1)?;
        }
    }
    Ok(bytes)
}
/// Capacity retained by a journal's owned payloads.
pub fn journal_owned_bytes(
    journal: &MaterializationJournal,
) -> Result<u64, NativeTreeMaterializationError> {
    journal.preimages.iter().try_fold(
        plan_owned_bytes(&journal.plan)?
            .checked_add(owned_size(
                journal.preimages.capacity(),
                std::mem::size_of::<MaterializationPreimage>(),
            )?)
            .ok_or(crate::WorkError::Overflow)?,
        |bytes, image| {
            bytes
                .checked_add(image.image.capacity() as u64)
                .ok_or(crate::WorkError::Overflow.into())
        },
    )
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
    let (paths, _allocation) = crate::native_host::buffer::<&str, _>(count, &mut &*work)?;
    work.charge(NativeResourceCredit::Items.delta(plan.edits.len() as u64))?;
    if validate_plan(plan, paths) {
        Ok(())
    } else {
        Err(NativeTreeMaterializationError::InvalidLayout)
    }
}
