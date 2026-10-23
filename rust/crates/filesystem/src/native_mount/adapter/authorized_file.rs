use super::*;
use crate::access::{Action, PinnedPath};

struct AuthorizedFile {
    file: Arc<dyn MountOpenFile>,
    permissions: PinnedPath,
    policy: Arc<dyn super::handle_policy::HandlePolicy>,
    file_id: FileId,
    created: bool,
    _allocation: crate::NativeAllocation,
}

pub(super) fn authorize_file(
    (file, bytes): (Arc<dyn MountOpenFile>, u64),
    permissions: PinnedPath,
    policy: Arc<dyn super::handle_policy::HandlePolicy>,
    file_id: FileId,
    created: bool,
    work: &crate::NativeMaterializationWork,
) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
    let allocation = work.claim(
        (std::mem::size_of::<AuthorizedFile>() + 2 * std::mem::size_of::<usize>()) as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    Ok((
        Arc::new(AuthorizedFile {
            file,
            permissions,
            policy,
            file_id,
            created,
            _allocation: allocation,
        }),
        bytes,
    ))
}

pub(in crate::native_mount) fn retarget_file(
    old: &dyn MountOpenFile,
    replacement: &dyn MountOpenFile,
    work: &crate::NativeMaterializationWork,
) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
    let old = (old as &dyn std::any::Any)
        .downcast_ref::<AuthorizedFile>()
        .ok_or(MountSourceError::Stale)?;
    let replacement = (replacement as &dyn std::any::Any)
        .downcast_ref::<AuthorizedFile>()
        .ok_or(MountSourceError::Stale)?;
    if old.file_id != replacement.file_id
        || old.permissions.volume() != replacement.permissions.volume()
    {
        return Err(MountSourceError::Stale);
    }
    authorize_file(
        (Arc::clone(&replacement.file), 0),
        old.permissions.clone(),
        old.policy.clone(),
        old.file_id,
        old.created,
        work,
    )
}

impl AuthorizedFile {
    fn require(
        &self,
        action: Action,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError> {
        access::require(
            &self.permissions,
            action,
            access::access_time(self.policy.clock())?,
        )?;
        self.policy.check(&self.permissions, work)
    }

    fn writable(
        &self,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<bool, MountSourceError> {
        let now = access::access_time(self.policy.clock())?;
        if self.created {
            access::require(&self.permissions, Action::Create, now)?;
            self.policy.check(&self.permissions, work)?;
            Ok(true)
        } else {
            let allowed = self.permissions.decide(Action::Write, now).allowed;
            if allowed {
                self.policy.check(&self.permissions, work)?;
            }
            Ok(allowed)
        }
    }

    fn require_write(
        &self,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError> {
        if self.writable(work)? {
            Ok(())
        } else {
            self.require(Action::Write, work)
        }
    }
}

macro_rules! checked {
    ($action:ident, $meter:expr; $($name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> $result {
            self.require(Action::$action, $meter)?;
            let result = self.file.$name($($arg),*);
            #[cfg(test)]
            if super::tests::return_fence::omitted() {
                return result;
            }
            // Retain the original ceiling and expiry, and reject changes to
            // the owner policy while storage was pending.
            self.require(Action::$action, $meter)?;
            result
        })*
    };
}

macro_rules! written {
    ($meter:expr; $($name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> $result {
            self.require_write($meter)?;
            self.file.$name($($arg),*)
        })*
    };
}

impl MountOpenFile for AuthorizedFile {
    fn clone_source(
        &self,
        checkout: &(dyn std::any::Any + Send + Sync),
        work: &crate::NativeMaterializationWork,
    ) -> Result<FileId, MountSourceError> {
        self.require(Action::Read, Some(work))?;
        let result = self.file.clone_source(checkout, work);
        self.require(Action::Read, Some(work))?;
        result
    }

    fn clone_range_from(
        &self,
        source: &dyn MountOpenFile,
        source_offset: u64,
        offset: u64,
        length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.require_write(Some(work))?;
        self.file
            .clone_range_from(source, source_offset, offset, length, work)
    }

    checked! { List, None;
        lookup() -> Result<MountLookup, MountSourceError>;
        list_attributes(cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountAttributePage, MountSourceError>;
    }
    checked! { List, Some(work);
        lookup_with_meter(work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        list_attributes_with_meter(cursor: Option<&[u8]>, maximum_entries: u32, work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    }
    checked! { Read, None;
        read_range(offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError>;
        read_up_to(offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError>;
        seek(offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
    }
    checked! { Read, Some(work);
        read_attribute(name: &[u8], work: &crate::NativeMaterializationWork) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
        read_range_with_meter(offset: u64, length: u32, work: &crate::NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        read_up_to_with_meter(offset: u64, maximum_bytes: u32, work: &crate::NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        seek_with_meter(offset: u64, target: MountSeekTarget, work: &crate::NativeMaterializationWork) -> Result<Option<u64>, MountSourceError>;
        read_sparse_range(offset: u64, length: u32, work: &crate::NativeMaterializationWork) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError>;
    }
    written! { None;
        resize(logical_bytes: u64) -> Result<(), MountSourceError>;
        allocate_range(offset: u64, length: u64, operation: MountRangeAllocation) -> Result<(), MountSourceError>;
    }
    written! { Some(work);
        resize_with_meter(logical_bytes: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        allocate_range_with_meter(offset: u64, length: u64, operation: MountRangeAllocation, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        set_attributes(metadata: FileMetadata, logical_bytes: Option<u64>, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        write_attribute(name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        remove_attribute(name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        write_sparse_range(offset: u64, range: (MountSparseRange, crate::NativeAllocation), work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        if self.writable(None)? {
            self.file.write_range(offset, bytes)
        } else {
            self.require(Action::Append, None)?;
            self.file.append_range(offset, bytes)
        }
    }

    fn write_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        if self.writable(Some(work))? {
            self.file.write_range_with_meter(offset, bytes, work)
        } else {
            self.require(Action::Append, Some(work))?;
            self.file.append_range_with_meter(offset, bytes, work)
        }
    }

    fn append_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        if !self.writable(None)? {
            self.require(Action::Append, None)?;
        }
        self.file.append_range(offset, bytes)
    }

    fn append_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        if !self.writable(Some(work))? {
            self.require(Action::Append, Some(work))?;
        }
        self.file.append_range_with_meter(offset, bytes, work)
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.file.settle(work)
    }
}
