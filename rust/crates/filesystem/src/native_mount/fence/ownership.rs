use super::{MountDestinationGuard, NativeMountError};
use crate::ledger::NativeResourceCredit::WriteOperations;
use crate::native_host::NativeHostWork as _;
use crate::native_ownership::{HASHED_BYTES, NativeOwnershipReceipt, RECEIPT_BYTES};
use std::os::windows::fs::FileExt as _;

impl MountDestinationGuard {
    /// Commit ownership before ProjFS can expose authored state. The receipt
    /// lives in the very file whose exclusive lock outlives that provider.
    pub(in crate::native_mount) fn publish_root_ownership(
        &self,
        identity: crate::NativeRootIdentity,
        volume: crate::VolumeId,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let file = self.file.as_ref().ok_or(NativeMountError::ControlPending)?;
        let parent = self
            .parent_directory
            .as_ref()
            .ok_or(NativeMountError::InvalidDestination)?;
        let fence_identity = crate::native_host::file_identity_and_length(file, &mut &*original)?.0;
        let parent_identity = crate::native_host::directory_identity(parent, &mut &*original)?;
        let encoded = crate::WorkCounters {
            bytes_encoded: RECEIPT_BYTES as u64,
            bytes_hashed: HASHED_BYTES as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        original.admit(encoded)?;
        let (pid, started) = super::super::projfs::current_process_owner(original)?;
        let bytes = NativeOwnershipReceipt {
            root: identity,
            fence: fence_identity,
            parent: parent_identity,
            volume,
            pid,
            started,
        }
        .encode();
        original.record_admitted(encoded)?;
        let mut unwritten = bytes.as_slice();
        while !unwritten.is_empty() {
            let offset = bytes.len() - unwritten.len();
            let written = (&mut &*original).attempt_io(WriteOperations.delta(1), || {
                file.seek_write(unwritten, offset as u64)
            })?;
            if written == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
            }
            unwritten = unwritten
                .get(written..)
                .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        }
        (&mut &*original).attempt_io(WriteOperations.delta(1), || {
            file.set_len(RECEIPT_BYTES as u64)
        })?;
        crate::native_host::sync_file_held(&**file, &mut &*original)?;
        let registry = self
            .registry_file
            .as_ref()
            .ok_or(NativeMountError::InvalidDestination)?;
        crate::native_host::sync_directory_held(&registry.directory, &mut &*original)?;
        crate::native_host::sync_directory_held(parent, &mut &*original)?;
        Ok(())
    }

    pub(in crate::native_mount) fn root_ownership(
        &self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Option<RootOwnership>, NativeMountError> {
        let file = self.file.as_ref().ok_or(NativeMountError::ControlPending)?;
        let (fence_identity, length) =
            crate::native_host::file_identity_and_length(file, &mut &*original)?;
        if length == 0 {
            return Ok(None);
        }
        // A nonempty wrong-length receipt still has to admit its version.
        // Only its fixed header is needed to refuse it; no payload is hashed.
        let read_length = if length == RECEIPT_BYTES as u64 {
            RECEIPT_BYTES
        } else {
            usize::try_from(length.min(16)).map_err(|_| crate::WorkError::Overflow)?
        };
        let mut bytes = [0; RECEIPT_BYTES];
        let read_bytes = bytes
            .get_mut(..read_length)
            .ok_or(crate::WorkError::Overflow)?;
        let mut unread = &mut *read_bytes;
        while !unread.is_empty() {
            let offset = read_length - unread.len();
            let read = (&mut &*original).attempt_io(
                crate::WorkCounters {
                    backend_read_operations: 1,
                    source_bytes_read: unread.len() as u64,
                    ..crate::WorkCounters::UNCHARGED
                },
                || file.seek_read(unread, offset as u64),
            )?;
            if read == 0 {
                return Ok(None);
            }
            unread = std::mem::take(&mut unread)
                .get_mut(read..)
                .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        }
        let receipt = match NativeOwnershipReceipt::decode(read_bytes, || {
            original.charge(crate::WorkCounters {
                bytes_hashed: HASHED_BYTES as u64,
                ..crate::WorkCounters::UNCHARGED
            })
        }) {
            Ok(receipt) => receipt,
            Err(crate::NativeTreeMaterializationError::InvalidLayout) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let parent = self
            .parent_directory
            .as_ref()
            .ok_or(NativeMountError::InvalidDestination)?;
        let parent_identity = crate::native_host::directory_identity(parent, &mut &*original)?;
        // Copying receipt bytes cannot substitute another physical lock file
        // or parent for the one held before provider activation.
        if !receipt.binds(fence_identity, parent_identity) {
            return Ok(None);
        }
        Ok(Some(receipt))
    }
}

/// Evidence read from the original locked physical fence.
pub(in crate::native_mount) type RootOwnership = NativeOwnershipReceipt;

#[cfg(test)]
impl MountDestinationGuard {
    pub(in crate::native_mount) fn owned_root(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::NativeRootIdentity>, NativeMountError> {
        Ok(self.root_ownership(work)?.map(|receipt| receipt.root))
    }
}
