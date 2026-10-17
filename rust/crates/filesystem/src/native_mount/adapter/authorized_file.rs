use super::*;
use crate::access::{Action, PinnedPath};

struct AuthorizedFile {
    file: Arc<dyn MountOpenFile>,
    permissions: PinnedPath,
    file_id: FileId,
    created: bool,
    _allocation: crate::NativeAllocation,
}

pub(in crate::native_mount) fn authorize_file(
    (file, bytes): (Arc<dyn MountOpenFile>, u64),
    permissions: PinnedPath,
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
        old.file_id,
        old.created,
        work,
    )
}

impl AuthorizedFile {
    fn require(&self, action: Action) -> Result<(), MountSourceError> {
        access::require(&self.permissions, action, access::access_time()?)
    }

    fn writable(&self) -> Result<bool, MountSourceError> {
        let now = access::access_time()?;
        if self.created {
            access::require(&self.permissions, Action::Create, now)?;
            Ok(true)
        } else {
            Ok(self.permissions.decide(Action::Write, now).allowed)
        }
    }

    fn require_write(&self) -> Result<(), MountSourceError> {
        if self.writable()? {
            Ok(())
        } else {
            self.require(Action::Write)
        }
    }
}

macro_rules! checked {
    ($action:ident; $($name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> $result {
            self.require(Action::$action)?;
            self.file.$name($($arg),*)
        })*
    };
}

macro_rules! written {
    ($($name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
        $(fn $name(&self, $($arg: $ty),*) -> $result {
            self.require_write()?;
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
        self.require(Action::Read)?;
        self.file.clone_source(checkout, work)
    }

    fn clone_range_from(
        &self,
        source: &dyn MountOpenFile,
        source_offset: u64,
        offset: u64,
        length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.require_write()?;
        self.file
            .clone_range_from(source, source_offset, offset, length, work)
    }

    checked! { List;
        lookup() -> Result<MountLookup, MountSourceError>;
        lookup_with_meter(work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError>;
        list_attributes(cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountAttributePage, MountSourceError>;
        list_attributes_with_meter(cursor: Option<&[u8]>, maximum_entries: u32, work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    }
    checked! { Read;
        read_attribute(name: &[u8], work: &crate::NativeMaterializationWork) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
        read_range(offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError>;
        read_range_with_meter(offset: u64, length: u32, work: &crate::NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        read_up_to(offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError>;
        read_up_to_with_meter(offset: u64, maximum_bytes: u32, work: &crate::NativeMaterializationWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
        seek(offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
        seek_with_meter(offset: u64, target: MountSeekTarget, work: &crate::NativeMaterializationWork) -> Result<Option<u64>, MountSourceError>;
        read_sparse_range(offset: u64, length: u32, work: &crate::NativeMaterializationWork) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError>;
    }
    written! {
        resize(logical_bytes: u64) -> Result<(), MountSourceError>;
        resize_with_meter(logical_bytes: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        allocate_range(offset: u64, length: u64, operation: MountRangeAllocation) -> Result<(), MountSourceError>;
        allocate_range_with_meter(offset: u64, length: u64, operation: MountRangeAllocation, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        set_attributes(metadata: FileMetadata, logical_bytes: Option<u64>, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        write_attribute(name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        remove_attribute(name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
        write_sparse_range(offset: u64, range: (MountSparseRange, crate::NativeAllocation), work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError>;
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        if self.writable()? {
            self.file.write_range(offset, bytes)
        } else {
            self.require(Action::Append)?;
            self.file.append_range(offset, bytes)
        }
    }

    fn write_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        if self.writable()? {
            self.file.write_range_with_meter(offset, bytes, work)
        } else {
            self.require(Action::Append)?;
            self.file.append_range_with_meter(offset, bytes, work)
        }
    }

    fn append_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        if !self.writable()? {
            self.require(Action::Append)?;
        }
        self.file.append_range(offset, bytes)
    }

    fn append_range_with_meter(
        &self,
        offset: u64,
        bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        if !self.writable()? {
            self.require(Action::Append)?;
        }
        self.file.append_range_with_meter(offset, bytes, work)
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.file.settle(work)
    }
}
