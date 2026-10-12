use super::*;

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
    Ok(String::from_utf8(copy_image(text.as_bytes(), allocation)?).expect("copied UTF-8"))
}
pub(super) fn copy_slice<T, U>(
    values: &[T],
    allocation: &mut NativeAllocation,
    mut copy: impl FnMut(&T, &mut NativeAllocation) -> Result<U, NativeTreeMaterializationError>,
) -> Result<Vec<U>, NativeTreeMaterializationError> {
    let (mut copied, mut capacity) =
        crate::native_host::buffer(values.len(), &mut allocation.work())?;
    allocation.absorb(&mut capacity)?;
    for value in values {
        copied.push(copy(value, allocation)?);
    }
    Ok(copied)
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
        let mut writer = ImageWriter::new(self)?;
        writer.append(bytes)?;
        Ok((writer.bytes, writer.allocation))
    }
    /// Formats directly into admitted capacity.
    pub fn display<T: std::fmt::Display + ?Sized>(
        &self,
        value: &T,
    ) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
        let mut writer = ImageWriter::new(self)?;
        if std::fmt::write(&mut writer, format_args!("{value}")).is_err() {
            return Err(writer
                .failure
                .take()
                .unwrap_or(NativeTreeMaterializationError::InvalidLayout));
        }
        Ok((
            String::from_utf8(writer.bytes).expect("formatted UTF-8"),
            writer.allocation,
        ))
    }
}
pub(super) struct ImageWriter {
    pub(super) bytes: Vec<u8>,
    pub(super) allocation: NativeAllocation,
    failure: Option<NativeTreeMaterializationError>,
}
impl ImageWriter {
    pub(super) fn new(
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeTreeMaterializationError> {
        Ok(Self {
            bytes: Vec::new(),
            allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            failure: None,
        })
    }
    fn append(&mut self, bytes: &[u8]) -> Result<(), NativeTreeMaterializationError> {
        let work = self.allocation.work().clone();
        let copied = bytes
            .len()
            .min(usize::try_from(work.remaining()?.bytes_copied).unwrap_or(usize::MAX));
        if copied == 0 && !bytes.is_empty() {
            work.admit(NativeResourceCredit::Copy.delta(1))?;
        }
        let delta = NativeResourceCredit::Copy.delta(copied as u64);
        let reserved = work.reserve_work(delta)?;
        let length = self
            .bytes
            .len()
            .checked_add(copied)
            .ok_or(crate::WorkError::Overflow)?;
        if length > self.bytes.capacity() {
            self.allocation
                .grow((length - self.bytes.capacity()) as u64)?;
            let attempted = self.bytes.try_reserve_exact(length - self.bytes.len());
            let reconciled = self
                .allocation
                .reconcile_capacity(self.bytes.capacity() as u64);
            attempted.map_err(NativeTreeMaterializationError::AllocationFailed)?;
            reconciled?;
        }
        self.bytes.extend_from_slice(&bytes[..copied]);
        reserved.record_completed(delta)?;
        if copied != bytes.len() {
            work.admit(NativeResourceCredit::Copy.delta(1))?;
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(())
    }
}
impl std::fmt::Write for ImageWriter {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.append(value.as_bytes()).map_err(|error| {
            self.failure = Some(error);
            std::fmt::Error
        })
    }
}
