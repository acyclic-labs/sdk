use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ContentVersion {
    pub(super) logical_bytes: u64,
    pub(super) modified_ns: MetadataField<i64>,
    pub(super) changed_ns: MetadataField<i64>,
}

impl ContentVersion {
    pub(super) fn of(lookup: &MountLookup) -> Self {
        Self {
            logical_bytes: lookup.node.logical_bytes,
            modified_ns: lookup.metadata.modified_ns,
            changed_ns: lookup.metadata.changed_ns,
        }
    }
}

impl FuseProjection {
    pub(super) fn read_link(
        &self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, NativeMountError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let path = fuse_clone_path(self.core.state()?.path(inode)?, work)?;
        let stamp = source.view_stamp_with_meter(work)?;
        let target = fuse_owned_source_chunk(source.read_link(&path, work)?, work)?;
        self.core
            .state()?
            .admit_attributes(source, inode, stamp, work)?;
        Ok(target)
    }

    pub(super) fn read_handle(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        size: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, NativeMountError> {
        let file = self
            .core
            .state()?
            .file_handle(inode, handle)?
            .open_file
            .clone();
        let bytes = fuse_owned_source_chunk(
            file.file(work)?.read_up_to_with_meter(offset, size, work)?,
            work,
        )?;
        if self.core.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV.into());
        }
        Ok(bytes)
    }
}
