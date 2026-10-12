use super::*;

impl FuseProjection {
    pub(super) fn check_source_write(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
        match (
            &self.core.callback_guard,
            self.core.source.native_callback_guard(),
        ) {
            (None, None) => Ok(()),
            (Some(original), Some(current)) if Arc::ptr_eq(original, &current) => {
                crate::native_mount::adapter::CallbackRuntime::existing()?
                    .verify_native_callback_write_guard(original.as_ref(), work)
                    .map_err(FuseRequestError::from)
            }
            _ => Err(NativeMountError::ControlPending.into()),
        }
    }

    pub(super) fn source_write<T>(
        &self,
        work: &crate::NativeMaterializationWork,
        effect: impl FnOnce() -> Result<T, FuseRequestError>,
    ) -> Result<T, FuseRequestError> {
        self.check_source_write(work)?;
        let result = effect();
        if let Err(refusal) = self.check_source_write(work) {
            let cause = match result {
                Err(cause) => NativeMountError::from(cause),
                Ok(_) => NativeMountError::from(refusal),
            };
            let errno = native_callback_errno(&cause);
            self.core
                .retain_handler_failure((cause, work.native_read_meter().lock().work));
            return Err(FuseRequestError::Errno(errno));
        }
        result
    }
}

impl FuseProjection {
    pub(super) fn set_attributes(
        &self,
        inode: u64,
        handle: Option<u64>,
        changes: &AttributeChanges,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(FileAttr, Duration), FuseRequestError> {
        if !changes.is_empty() {
            self.check_source_write(work)?;
        }
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let (facts, open_file) = {
            let state = self.core.state()?;
            state.admit_write().map_err(|_| libc::EOPNOTSUPP)?;
            let open_file = handle
                .map(|handle| state.open_handle(inode, handle, work))
                .transpose()?;
            (state.node_facts(source, inode, handle, work)?, open_file)
        };
        let current = resolve_node(source, facts, work)?;
        let _claim = changes
            .size
            .map(|_| self.core.claim_content(current.lookup.node.file_id, work))
            .transpose()?;
        let resolved = if changes.is_empty() {
            current
        } else {
            let metadata = changes.applied_to(current.lookup.metadata)?;
            let stamp = source.view_stamp_with_meter(work)?;
            match (&open_file, current.through) {
                (Some(open_file), _) => {
                    self.source_write(work, || {
                        open_file
                            .file(work)?
                            .set_attributes(metadata, changes.size, work)
                            .map_err(FuseRequestError::from)
                    })?;
                    Resolved {
                        lookup: open_file
                            .file(work)?
                            .lookup_with_meter(work)
                            .map_err(FuseRequestError::from)?,
                        stamp,
                        through: None,
                    }
                }
                (None, Some(path)) => {
                    self.source_write(work, || {
                        source
                            .set_attributes(&path, metadata, changes.size, work)
                            .map_err(FuseRequestError::from)
                    })?;
                    resolve_node(
                        source,
                        Err(NodeFacts::Names {
                            file_id: Some(current.lookup.node.file_id),
                            paths: {
                                let mut paths = fuse_vec(1, work)?;
                                paths.push(path);
                                paths
                            },
                        }),
                        work,
                    )?
                }
                (None, None) => return Err(libc::ESTALE.into()),
            }
        };
        let mut state = self.core.state()?;
        if !changes.is_empty()
            && let Some(handle) = handle
        {
            state.mark_handle_dirty(inode, handle)?;
        }
        Ok(state.apply(source, inode, &resolved, work)?)
    }
}

impl FuseProjection {
    pub(super) fn write_handle(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        data: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<u32, FuseRequestError> {
        self.check_source_write(work)?;
        let operation = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.open_handle_operation(inode, handle, work)?
        };
        let _operation = operation.lock().map_err(|_| libc::EIO)?;
        let open_file = {
            let mut state = self.core.state()?;
            let file = state
                .files
                .get_mut(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
                .ok_or(libc::ESTALE)?;
            file.dirty = true;
            file.open_file.clone(work)?
        };

        #[cfg(test)]
        pause_test_write();

        let length = u32::try_from(data.len()).map_err(|_| libc::EOVERFLOW)?;
        let bytes = fuse_source_chunk(data, work)?;
        self.source_write(work, || {
            let file = match open_file.file(work) {
                Ok(file) => file,
                Err(error) => {
                    DeferredOpenFile::discard_packet(bytes, work)?;
                    return Err(error.into());
                }
            };
            file.write_range_with_meter(offset, bytes, work)
                .map_err(FuseRequestError::from)
        })?;
        let stamp = self.core.source.as_ref().view_stamp_with_meter(work)?;
        let refreshed = Some(
            open_file
                .file(work)?
                .lookup_with_meter(work)
                .map_err(FuseRequestError::from)?,
        );
        let mut state = self.core.state()?;
        let current = state.files.get(&handle).is_some_and(|file| {
            file.inode == inode
                && Arc::ptr_eq(&file.operation, &operation)
                && file.open_file.same_file(&open_file)
        });
        if current && let Some(lookup) = refreshed {
            state.apply_handle_facts(inode, lookup, stamp);
        }
        Ok(length)
    }

    pub(super) fn flush_handle(
        &self,
        inode: u64,
        handle: u64,
        force: bool,
        release: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
        let authored_flush = {
            let state = self.core.state()?;
            let file = state
                .files
                .get(&handle)
                .filter(|file| file.inode == inode)
                .ok_or(libc::ESTALE)?;
            force || (file.dirty && self.core.source.as_ref().flush_on_handle_close())
        };
        if authored_flush {
            self.check_source_write(work)?;
        }
        let operation = self
            .core
            .state()?
            .open_handle_operation(inode, handle, work)?;
        let _operation = operation.lock().map_err(|_| libc::EIO)?;
        let (should_flush, open_file) = {
            let state = self.core.state()?;
            let file = state
                .files
                .get(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
                .ok_or(libc::ESTALE)?;
            (
                force || (file.dirty && self.core.source.as_ref().flush_on_handle_close()),
                file.open_file.clone(work)?,
            )
        };
        let settled = if force {
            self.source_write(work, || {
                open_file.settle(work).map_err(FuseRequestError::from)
            })
        } else {
            Ok(())
        };
        let flushed = settled.and_then(|()| {
            if should_flush {
                self.source_write(work, || {
                    self.core
                        .source
                        .as_ref()
                        .flush(work)
                        .map_err(FuseRequestError::from)
                })
            } else {
                Ok(())
            }
        });
        let mut state = self.core.state()?;
        let current = state
            .files
            .get(&handle)
            .is_some_and(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation));
        if !current {
            return Err(libc::ESTALE.into());
        }
        if flushed.is_ok()
            && let Some(file) = state.files.get_mut(&handle)
        {
            file.dirty = false;
        }
        if release {
            let discarded = state.discard_file_handle(inode, handle);
            flushed.and(discarded.map_err(FuseRequestError::from))
        } else {
            flushed
        }
    }
}

impl FuseProjection {
    pub(super) fn allocate(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        length: u64,
        mode: i32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), FuseRequestError> {
        self.check_source_write(work)?;
        const KEEP_SIZE: i32 = 0x01;
        const PUNCH_HOLE: i32 = 0x02;
        const ZERO_RANGE: i32 = 0x10;
        let open_file = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.open_handle(inode, handle, work)?
        };
        if length == 0 {
            return Err(libc::EINVAL.into());
        }
        let operation = if mode == (PUNCH_HOLE | KEEP_SIZE) {
            crate::MountRangeAllocation::PunchHole
        } else if mode == ZERO_RANGE || mode == (ZERO_RANGE | KEEP_SIZE) {
            crate::MountRangeAllocation::ZeroRange {
                extend: mode == ZERO_RANGE,
            }
        } else if mode == 0 || mode == KEEP_SIZE {
            crate::MountRangeAllocation::Preallocate {
                keep_size: mode == KEEP_SIZE,
            }
        } else {
            return Err(libc::EOPNOTSUPP.into());
        };
        self.source_write(work, || {
            open_file
                .file(work)?
                .allocate_range_with_meter(offset, length, operation, work)
                .map_err(FuseRequestError::from)
        })?;
        self.core.state()?.mark_handle_dirty(inode, handle)?;
        let stamp = self.core.source.as_ref().view_stamp_with_meter(work)?;
        let refreshed = Some(
            open_file
                .file(work)?
                .lookup_with_meter(work)
                .map_err(FuseRequestError::from)?,
        );
        let mut state = self.core.state()?;
        if let Some(lookup) = refreshed {
            state.apply_handle_facts(inode, lookup, stamp);
        }
        Ok(())
    }

    pub(super) fn seek(
        &self,
        inode: u64,
        handle: u64,
        offset: i64,
        whence: i32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<i64, FuseRequestError> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL.into()),
        };
        let open_file = self.core.state()?.open_handle(inode, handle, work)?;
        let found = open_file
            .file(work)?
            .seek_with_meter(offset, target, work)
            .map_err(FuseRequestError::from)?
            .ok_or(libc::ENXIO)?;
        i64::try_from(found).map_err(|_| libc::EOVERFLOW.into())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the copy_file_range request: two inodes, two handles, two offsets, a length, and flags"
    )]
    pub(super) fn copy_range(
        &self,
        source_inode: u64,
        source_handle: u64,
        source_offset: u64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: u64,
        length: u64,
        flags: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u32, FuseRequestError> {
        self.check_source_write(work)?;
        let (source_open, destination_open) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                state.open_handle(source_inode, source_handle, work)?,
                state.open_handle(destination_inode, destination_handle, work)?,
            )
        };
        u32::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        if flags != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        if length == 0 {
            return Ok(0);
        }
        let (transferred, failure) = copy_open_range(
            self,
            &source_open,
            source_offset,
            &destination_open,
            destination_offset,
            length,
            work,
        )?;
        if let Some(error) = failure {
            self.core
                .record_handler_failure(work, NativeMountError::from(error));
        }
        self.core
            .state()?
            .mark_handle_dirty(destination_inode, destination_handle)?;
        let stamp = self.core.source.as_ref().view_stamp_with_meter(work)?;
        let refreshed = Some(
            destination_open
                .file(work)?
                .lookup_with_meter(work)
                .map_err(FuseRequestError::from)?,
        );
        let mut state = self.core.state()?;
        if let Some(lookup) = refreshed {
            state.apply_handle_facts(destination_inode, lookup, stamp);
        }
        Ok(transferred)
    }
}

pub(super) fn copy_open_range(
    projection: &FuseProjection,
    source_open: &FuseOpenFile,
    source_offset: u64,
    destination_open: &FuseOpenFile,
    destination_offset: u64,
    length: u64,
    work: &crate::NativeMaterializationWork,
) -> Result<(u32, Option<FuseRequestError>), FuseRequestError> {
    projection.check_source_write(work)?;
    let source = projection.core.source.as_ref();
    let source_lookup = source_open
        .file(work)?
        .lookup_with_meter(work)
        .map_err(FuseRequestError::from)?;
    let destination_lookup = destination_open
        .file(work)?
        .lookup_with_meter(work)
        .map_err(FuseRequestError::from)?;
    let source_size = source_lookup.node.logical_bytes;
    let copy_length = length.min(source_size.saturating_sub(source_offset));
    if copy_length == 0 {
        return Ok((0, None));
    }
    if source_lookup.node.link_count != 0 && destination_lookup.node.link_count != 0 {
        match projection.source_write(work, || {
            source
                .clone_range_by_id(
                    source_lookup.node.file_id,
                    source_offset,
                    destination_lookup.node.file_id,
                    destination_offset,
                    copy_length,
                    work,
                )
                .map_err(FuseRequestError::from)
        }) {
            Ok(()) => {
                return Ok((
                    u32::try_from(copy_length).map_err(|_| libc::EOVERFLOW)?,
                    None,
                ));
            }
            Err(FuseRequestError::Native(NativeMountError::Source(
                MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported,
            ))) => {}
            Err(error) => return Err(error),
        }
    }
    let same_open_file = source_open.same_file(destination_open);
    let backwards = same_open_file
        && destination_offset > source_offset
        && destination_offset < source_offset.saturating_add(copy_length);
    let mut transferred = 0_u64;
    let mut failure = None;
    while transferred < copy_length {
        if let Err(error) = projection.check_source_write(work) {
            if transferred == 0 {
                return Err(error);
            }
            failure = Some(error);
            break;
        }
        if let Err(error) = fuse_item(work) {
            if transferred == 0 {
                return Err(error);
            }
            failure = Some(error);
            break;
        }
        let remaining = copy_length - transferred;
        let chunk = remaining.min(u64::from(DETACHED_COPY_CHUNK_BYTES));
        let relative = if backwards {
            remaining - chunk
        } else {
            transferred
        };
        let chunk = u32::try_from(chunk).unwrap_or(DETACHED_COPY_CHUNK_BYTES);
        let sparse = match source_open
            .file(work)
            .and_then(|file| file.read_sparse_range(source_offset + relative, chunk, work))
        {
            Ok(sparse) => sparse,
            Err(error) if transferred == 0 => return Err(error.into()),
            Err(error) => {
                failure = Some(error.into());
                break;
            }
        };
        let copied = sparse.0.logical_bytes;
        if let Err(error) = projection.source_write(work, || {
            destination_open
                .file(work)?
                .write_sparse_range(destination_offset + relative, sparse, work)
                .map_err(FuseRequestError::from)
        }) {
            if transferred == 0 {
                return Err(error);
            }
            failure = Some(error);
            break;
        }
        transferred = transferred.saturating_add(copied);
        if copied < u64::from(chunk) {
            break;
        }
    }
    Ok((
        u32::try_from(transferred).map_err(|_| libc::EOVERFLOW)?,
        failure,
    ))
}
