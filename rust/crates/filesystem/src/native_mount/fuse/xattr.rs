use super::*;

impl FuseProjection {
    pub(super) fn attributes_to_write(&self, inode: u64, work: &crate::NativeMaterializationWork) -> Result<AttributeTarget, FuseRequestError> {
        self.check_source_write(work)?;
        let state = self.core.state()?;
        state.admit_write()?;
        Ok(state.attribute_target(inode, work)?)
    }

    pub(super) fn attributes_to_read(&self, inode: u64, work: &crate::NativeMaterializationWork) -> Result<Option<AttributeTarget>, FuseRequestError> {
        Ok(self.core.state()?.attributes_to_read(self.core.source.as_ref(), inode, work)?)
    }

    pub(super) fn write_named_attribute(
        &self,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
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
        let target = self.attributes_to_write(inode, work)?;
        let value = fuse_source_chunk(value, work)?;
        self.source_write(work, || match target {
            AttributeTarget::Handle(open_file) => {
                let file = match open_file.file(work) {
                    Ok(file) => file,
                    Err(error) => { DeferredOpenFile::discard_packet(value, work)?; return Err(error.into()); }
                };
                file.write_attribute(name.as_bytes(), value, mode, work)
            }
            AttributeTarget::Path(path) => {
                self.core.source.as_ref()
                    .write_attribute(&path, name.as_bytes(), value, mode, work)
            }
        }.map_err(FuseRequestError::from))
    }

    pub(super) fn read_named_attribute(&self, inode: u64, name: &OsStr, work: &crate::NativeMaterializationWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let _names = self.core.names()?;
        let value = match self.attributes_to_read(inode, work)? {
            Some(AttributeTarget::Handle(open_file)) => open_file.file(work)?.read_attribute(name.as_bytes(), work),
            Some(AttributeTarget::Path(path)) => {
                self.core.source.as_ref().read_attribute(&path, name.as_bytes(), work)
            }
            None => Ok(None),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => FuseRequestError::Errno(libc::ENODATA),
            error => error.into(),
        })?
        .ok_or_else(|| FuseRequestError::Errno(libc::ENODATA))?;
        fuse_owned_source_chunk(value, work)
    }

    pub(super) fn list_named_attributes(
        &self,
        inode: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FuseOwned<Vec<u8>>, FuseRequestError> {
        let _names = self.core.names()?;
        let Some(target) = self.attributes_to_read(inode, work)? else { return Ok(fuse_vec(0, work)?); };
        let mut previous: Option<FuseOwned<crate::native_mount::MountAttributePage>> = None;
        let mut encoded = fuse_vec(0, work)?;
        loop {
            fuse_item(work)?;
            let cursor = previous.as_ref().and_then(|page| page.next_cursor.as_deref());
            let (page, allocation) = match &target {
                AttributeTarget::Handle(open_file) => open_file.file(work)?.list_attributes_with_meter(cursor, ATTRIBUTE_PAGE_SIZE, work),
                AttributeTarget::Path(path) => self.core.source.as_ref().list_attributes_with_meter(path, cursor, ATTRIBUTE_PAGE_SIZE, work),
            }.map_err(FuseRequestError::from)?;
            let page = FuseOwned::native(page, allocation);
            for name in &page.names {
                fuse_item(work)?;
                let required = name.len().checked_add(1).ok_or(libc::EOVERFLOW)?;
                if encoded.len().checked_add(required).ok_or(libc::EOVERFLOW)? > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(libc::E2BIG.into());
                }
                fuse_copy_admit(required as u64, work)?;
                fuse_reserve(&mut encoded, required, work)?;
                encoded.extend_from_slice(name);
                encoded.push(0);
                fuse_copy_record(required as u64, work)?;
            }
            let Some(next) = page.next_cursor.as_deref() else { return Ok(encoded); };
            if cursor == Some(next) { return Err(libc::EIO.into()); }
            previous = Some(page);
        }
    }

    pub(super) fn remove_named_attribute(&self, inode: u64, name: &OsStr, work: &crate::NativeMaterializationWork) -> Result<(), FuseRequestError> {
        let _names = self.core.names()?;
        let target = self.attributes_to_write(inode, work)?;
        self.source_write(work, || match target {
            AttributeTarget::Handle(open_file) => open_file.file(work)?.remove_attribute(name.as_bytes(), work),
            AttributeTarget::Path(path) => self.core.source.as_ref().remove_attribute(&path, name.as_bytes(), work),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => FuseRequestError::Errno(libc::ENODATA),
            error => error.into(),
        }))
    }

}
