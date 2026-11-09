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
impl NativeMaterializationWork {
    /// Copies a lexical path into admitted capacity.
    pub fn path(
        &self,
        root: &Path,
        leaf: Option<&Path>,
    ) -> Result<(PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        let copied = root
            .as_os_str()
            .len()
            .checked_add(leaf.map_or(0, |leaf| leaf.as_os_str().len()))
            .ok_or(crate::WorkError::Overflow)?;
        let capacity = copied
            .checked_add(usize::from(leaf.is_some()))
            .ok_or(crate::WorkError::Overflow)?;
        let delta = NativeResourceCredit::Copy.delta(copied as u64);
        self.admit(delta)?;
        let mut allocation = self.claim(
            capacity as u64,
            u64::from(capacity != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut path = PathBuf::new();
        path.try_reserve_exact(capacity)
            .map_err(NativeTreeMaterializationError::AllocationFailed)?;
        path.push(root);
        if let Some(leaf) = leaf {
            path.push(leaf);
        }
        self.spent(delta)?;
        allocation.reconcile_capacity(path.capacity() as u64)?;
        Ok((path, allocation))
    }
    /// Copies the admitted prefix before reporting a byte limit.
    pub fn copy_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        let copied = bytes
            .len()
            .min(usize::try_from(self.remaining()?.bytes_copied).unwrap_or(usize::MAX));
        let output = crate::kernel::allocation::writer::native_image(self, |writer| {
            writer.append(bytes.get(..copied).ok_or(crate::WorkError::Overflow)?)
        })?;
        if copied != bytes.len() {
            self.admit(NativeResourceCredit::Copy.delta(1))?;
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(output)
    }
    /// Formats directly into admitted capacity.
    pub fn display<T: std::fmt::Display + ?Sized>(
        &self,
        value: &T,
    ) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
        let (bytes, allocation) =
            crate::kernel::allocation::writer::native_image(self, |writer| {
                std::fmt::write(writer, format_args!("{value}"))
                    .map_err(|_| NativeTreeMaterializationError::InvalidLayout.into())
            })?;
        Ok((
            String::from_utf8(bytes).map_err(|_| NativeTreeMaterializationError::InvalidLayout)?,
            allocation,
        ))
    }
}
