use super::*;

pub(super) fn check_native_plan(
    plan: &MaterializationPlan,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let count = plan
        .edits
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let mut allocation = work.claim(
        owned_size(count, std::mem::size_of::<&str>())?,
        u64::from(count != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut paths = Vec::new();
    paths
        .try_reserve_exact(count)
        .map_err(NativeTreeMaterializationError::AllocationFailed)?;
    allocation.reconcile_capacity(owned_size(paths.capacity(), std::mem::size_of::<&str>())?)?;
    if validate_plan(plan, paths, || {
        work.charge(NativeResourceCredit::Items.delta(1))
    })? {
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
    work.display(&format_args!("/{path}"))
}
use crate::WorkCounters;

pub(super) fn copy_image(
    image: &[u8],
    allocation: &mut NativeAllocation,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let (image, mut copied) = allocation.work().copy_bytes(image)?;
    allocation.absorb(&mut copied)?;
    Ok(image)
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
/// Actual retained journal payload, excluding the allocation guard itself.
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
            WorkCounters::UNCHARGED,
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
impl NativeMaterializationWork {
    /// Copies a lexical path with admitted capacity.
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
            WorkCounters::UNCHARGED,
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
    /// Copies the admitted prefix before reporting a byte-limit failure.
    pub fn copy_bytes(
        &self,
        source: &[u8],
    ) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        let length = source
            .len()
            .min(usize::try_from(self.remaining()?.bytes_copied).unwrap_or(usize::MAX));
        if length == 0 && !source.is_empty() {
            self.admit(NativeResourceCredit::Copy.delta(1))?;
        }
        let delta = NativeResourceCredit::Copy.delta(length as u64);
        let credit = self.reserve_work(delta)?;
        let mut allocation = self.claim(
            length as u64,
            u64::from(length != 0),
            WorkCounters::UNCHARGED,
        )?;
        let mut output = Vec::new();
        let reserved = output.try_reserve_exact(length);
        let reconciled = allocation.reconcile_capacity(output.capacity() as u64);
        reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
        reconciled?;
        output.extend_from_slice(&source[..length]);
        credit.record_completed(delta)?;
        if length != source.len() {
            self.admit(NativeResourceCredit::Copy.delta(1))?;
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok((output, allocation))
    }
    /// Formats directly into an admitted buffer.
    pub fn display<T: std::fmt::Display + ?Sized>(
        &self,
        value: &T,
    ) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
        let mut writer = ImageWriter {
            bytes: Vec::new(),
            allocation: self.claim(0, 0, WorkCounters::UNCHARGED)?,
            failure: None,
        };
        if std::fmt::write(&mut writer, format_args!("{value}")).is_err() {
            return Err(writer
                .failure
                .take()
                .unwrap_or(NativeTreeMaterializationError::InvalidLayout));
        }
        Ok((
            String::from_utf8(writer.bytes)
                .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?,
            writer.allocation,
        ))
    }
}
pub(super) struct ImageWriter {
    pub(super) bytes: Vec<u8>,
    pub(super) allocation: NativeAllocation,
    pub(super) failure: Option<NativeTreeMaterializationError>,
}
impl std::fmt::Write for ImageWriter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.append(text.as_bytes())
    }
}
impl ImageWriter {
    fn append(&mut self, bytes: &[u8]) -> std::fmt::Result {
        let result = (|| {
            let work = self.allocation.work().clone();
            let copied = bytes
                .len()
                .min(usize::try_from(work.remaining()?.bytes_copied).unwrap_or(usize::MAX));
            if copied == 0 && !bytes.is_empty() {
                work.admit(NativeResourceCredit::Copy.delta(1))?;
            }
            let delta = NativeResourceCredit::Copy.delta(copied as u64);
            let credit = work.reserve_work(delta)?;
            let length = self
                .bytes
                .len()
                .checked_add(copied)
                .ok_or(crate::WorkError::Overflow)?;
            if length > self.bytes.capacity() {
                let before = self.bytes.capacity();
                self.allocation.grow((length - before) as u64)?;
                let reserved = self.bytes.try_reserve_exact(length - self.bytes.len());
                let reconciled = self
                    .allocation
                    .reconcile_capacity(self.bytes.capacity() as u64);
                reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
                reconciled?;
            }
            self.bytes.extend_from_slice(&bytes[..copied]);
            credit.record_completed(delta)?;
            if copied != bytes.len() {
                work.admit(NativeResourceCredit::Copy.delta(1))?;
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            Ok::<_, NativeTreeMaterializationError>(())
        })();
        if let Err(error) = result {
            self.failure = Some(error);
            return Err(std::fmt::Error);
        }
        Ok(())
    }
}
impl std::io::Write for ImageWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(error) = self.allocation.work().charge(crate::WorkCounters {
            bytes_encoded: bytes.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        }) {
            self.failure = Some(error);
            return Err(std::io::ErrorKind::Other.into());
        }
        self.append(bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
