//! Admitted copies of paths, bytes and formatted text on the native ledger.
use super::{NativeAllocation, NativeMaterializationWork, NativeResourceCredit};
use crate::NativeTreeMaterializationError;
use std::path::{Path, PathBuf};

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
