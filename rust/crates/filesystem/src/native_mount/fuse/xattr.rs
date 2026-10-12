use super::*;

impl FuseProjection {
    pub(super) fn write_named_attribute(
        &self,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if position != 0 || flags & !(libc::XATTR_CREATE | libc::XATTR_REPLACE) != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        let mode = match (
            flags & libc::XATTR_CREATE != 0,
            flags & libc::XATTR_REPLACE != 0,
        ) {
            (true, true) => return Err(libc::EINVAL.into()),
            (true, false) => crate::native_mount::MountAttributeWriteMode::Create,
            (false, true) => crate::native_mount::MountAttributeWriteMode::Replace,
            (false, false) => crate::native_mount::MountAttributeWriteMode::Upsert,
        };
        let _names = self.core.names()?;
        self.check_source_write(work)?;
        let target = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.attribute_target(inode, work)?
        };
        let value = fuse_source_chunk(value, work)?;
        self.source_write(work, || {
            Ok(match target {
                AttributeTarget::Handle(open_file) => {
                    open_file
                        .file(work)?
                        .write_attribute(name.as_bytes(), value, mode, work)?
                }
                AttributeTarget::Path(path) => {
                    self.core
                        .source
                        .write_attribute(&path, name.as_bytes(), value, mode, work)?
                }
            })
        })
    }

    pub(super) fn read_named_attribute(
        &self,
        inode: u64,
        name: &OsStr,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, NativeMountError> {
        let _names = self.core.names()?;
        let target =
            self.core
                .state()?
                .attributes_to_read(self.core.source.as_ref(), inode, work)?;
        let value = match target {
            Some(AttributeTarget::Handle(open_file)) => {
                open_file.file(work)?.read_attribute(name.as_bytes(), work)
            }
            Some(AttributeTarget::Path(path)) => {
                self.core
                    .source
                    .read_attribute(&path, name.as_bytes(), work)
            }
            None => Ok(None),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => NativeMountError::from(libc::ENODATA),
            error => error.into(),
        })?
        .ok_or(libc::ENODATA)?;
        fuse_owned_source_chunk(value, work)
    }

    pub(super) fn list_named_attributes(
        &self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<Vec<u8>>, NativeMountError> {
        let _names = self.core.names()?;
        let Some(target) =
            self.core
                .state()?
                .attributes_to_read(self.core.source.as_ref(), inode, work)?
        else {
            return fuse_vec(0, work);
        };
        let mut previous: Option<(
            crate::native_mount::MountAttributePage,
            crate::NativeAllocation,
        )> = None;
        let mut encoded = fuse_vec(0, work)?;
        loop {
            fuse_item(work)?;
            let cursor = previous
                .as_ref()
                .and_then(|(page, _)| page.next_cursor.as_deref());
            let page = match &target {
                AttributeTarget::Handle(open_file) => open_file
                    .file(work)?
                    .list_attributes_with_meter(cursor, ATTRIBUTE_PAGE_SIZE, work),
                AttributeTarget::Path(path) => self.core.source.list_attributes_with_meter(
                    path,
                    cursor,
                    ATTRIBUTE_PAGE_SIZE,
                    work,
                ),
            }?;
            for name in &page.0.names {
                fuse_item(work)?;
                let required = name.len().checked_add(1).ok_or(libc::EOVERFLOW)?;
                if encoded.len().checked_add(required).ok_or(libc::EOVERFLOW)?
                    > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES
                {
                    return Err(libc::E2BIG.into());
                }
                fuse_copy_admit(required as u64, work)?;
                fuse_reserve(&mut encoded, required, work)?;
                encoded.extend_from_slice(name);
                encoded.push(0);
                fuse_copy_record(required as u64, work)?;
            }
            let Some(next) = page.0.next_cursor.as_deref() else {
                return Ok(encoded);
            };
            if cursor == Some(next) {
                return Err(libc::EIO.into());
            }
            previous = Some(page);
        }
    }

    pub(super) fn remove_named_attribute(
        &self,
        inode: u64,
        name: &OsStr,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let _names = self.core.names()?;
        self.check_source_write(work)?;
        let target = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.attribute_target(inode, work)?
        };
        self.source_write(work, || {
            match target {
                AttributeTarget::Handle(open_file) => open_file
                    .file(work)?
                    .remove_attribute(name.as_bytes(), work),
                AttributeTarget::Path(path) => {
                    self.core
                        .source
                        .remove_attribute(&path, name.as_bytes(), work)
                }
            }
            .map_err(|error| match error {
                MountSourceError::NotFound => NativeMountError::from(libc::ENODATA),
                error => error.into(),
            })
        })
    }
}
