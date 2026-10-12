use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Lowers every authored kind on the caller's existing allocation context.
    pub(super) async fn compile_authored_mutation_with_accounting(
        &self,
        authored: crate::kernel::Owned<AuthoredMutation>,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, FsError> {
        let crate::kernel::Owned {
            value,
            mut allocation,
            mut bytes,
        } = authored;
        let path_bytes = value.logical_owned_bytes();
        let result = async {
            accounting.check()?;
            let mutation = match value {
                AuthoredMutation::Reidentify { path, file_id } => {
                    let current = lookup_path_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        &self.root,
                        &path,
                        self.volume.config,
                        accounting,
                        cancellation,
                        None,
                    )
                    .await?
                    .record
                    .ok_or(FsError::NotFound)?;
                    if current.kind == FileKind::Directory {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if current.file_id == file_id {
                        return Ok(None);
                    }
                    let (existing, existing_allocation) = lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        self.root.file_table,
                        file_id,
                        DecodeLimits::for_volume(self.volume.config),
                        accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                    )
                    .await?;
                    drop(existing_allocation);
                    if existing.is_some_and(|existing| {
                        existing.kind != current.kind
                            || existing.metadata != current.metadata
                            || existing.payload != current.payload
                    }) {
                        return Err(FsError::FileIdentityCollision);
                    }
                    Mutation::Restore {
                        path,
                        record: FileRecord {
                            file_id,
                            link_count: 1,
                            ..current
                        },
                    }
                }
                AuthoredMutation::CreateFile {
                    path,
                    bytes: content,
                    metadata,
                } => {
                    let file_id = self.authored_file_id(FileKind::Regular, &path, accounting)?;
                    return self
                        .compile_create_file_with_accounting(
                            path,
                            content,
                            metadata,
                            file_id,
                            operations,
                            &mut allocation,
                            &mut bytes,
                            staged_metadata,
                            accounting,
                            cancellation,
                        )
                        .await
                        .map(Some);
                }
                AuthoredMutation::CreateFileFromContent {
                    path,
                    content,
                    metadata,
                    file_id,
                } => {
                    let metadata = self
                        .stage_authored_metadata_with_accounting(
                            metadata,
                            staged_metadata,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    let file_id = match file_id {
                        Some(file_id) => file_id,
                        None => self.authored_file_id(FileKind::Regular, &path, accounting)?,
                    };
                    self.compile_regular_content_with_accounting(
                        path,
                        content,
                        metadata,
                        file_id,
                        operations,
                        &mut allocation,
                        &mut bytes,
                        accounting,
                    )?;
                    return Ok(Some(file_id));
                }
                create @ (AuthoredMutation::CreateDirectory { .. }
                | AuthoredMutation::CreateSymbolicLink { .. }
                | AuthoredMutation::CreateEmptySpecial { .. }
                | AuthoredMutation::CreateDevice { .. }
                | AuthoredMutation::CreateReparsePoint { .. }) => {
                    let (path, kind, payload, metadata) = match create {
                        AuthoredMutation::CreateDirectory { path, metadata } => {
                            let entries = self
                                .stage_empty_tree_with_accounting(accounting, cancellation)
                                .await?;
                            (
                                path,
                                FileKind::Directory,
                                FilePayload::Directory { entries },
                                metadata,
                            )
                        }
                        AuthoredMutation::CreateSymbolicLink {
                            path,
                            target,
                            metadata,
                        } => {
                            let content = self
                                .stage_authored_blob_with_accounting(
                                    target,
                                    accounting,
                                    cancellation,
                                )
                                .await?;
                            (
                                path,
                                FileKind::SymbolicLink,
                                FilePayload::SymbolicLink {
                                    target_bytes: content.logical_bytes,
                                    target: content.root,
                                },
                                metadata,
                            )
                        }
                        AuthoredMutation::CreateEmptySpecial {
                            path,
                            kind,
                            metadata,
                        } => {
                            if !matches!(
                                kind,
                                FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary
                            ) {
                                return Err(FsError::InvalidSpecialKind);
                            }
                            if !kind.is_supported_by_profile(self.volume.config.profile) {
                                return Err(FsError::UnsupportedFileKind);
                            }
                            (path, kind, FilePayload::Empty, metadata)
                        }
                        AuthoredMutation::CreateDevice {
                            path,
                            kind,
                            major,
                            minor,
                            metadata,
                        } => {
                            if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
                                return Err(FsError::InvalidSpecialKind);
                            }
                            if !kind.is_supported_by_profile(self.volume.config.profile) {
                                return Err(FsError::UnsupportedFileKind);
                            }
                            (path, kind, FilePayload::Device { major, minor }, metadata)
                        }
                        AuthoredMutation::CreateReparsePoint {
                            path,
                            payload,
                            metadata,
                        } => {
                            if !FileKind::ReparsePoint
                                .is_supported_by_profile(self.volume.config.profile)
                            {
                                return Err(FsError::UnsupportedFileKind);
                            }
                            let content = self
                                .stage_authored_blob_with_accounting(
                                    payload,
                                    accounting,
                                    cancellation,
                                )
                                .await?;
                            (
                                path,
                                FileKind::ReparsePoint,
                                FilePayload::ReparsePoint {
                                    payload_bytes: content.logical_bytes,
                                    payload: content.root,
                                },
                                metadata,
                            )
                        }
                        _ => unreachable!("record creation kinds are exhausted"),
                    };
                    return self
                        .compile_record_with_accounting(
                            operations,
                            path,
                            kind,
                            payload,
                            metadata,
                            &mut allocation,
                            &mut bytes,
                            staged_metadata,
                            accounting,
                            cancellation,
                        )
                        .await
                        .map(Some);
                }
                AuthoredMutation::Remove {
                    path,
                    expected_file_id,
                } => Mutation::Remove {
                    path,
                    expected_file_id: expected_file_id
                        .map_or(MetadataField::Unavailable, MetadataField::Value),
                },
                AuthoredMutation::Rename {
                    source,
                    destination,
                    replace,
                } => Mutation::Rename {
                    source,
                    destination,
                    replace,
                },
                AuthoredMutation::HardLink {
                    source,
                    destination,
                } => Mutation::Link {
                    source,
                    destination,
                },
                AuthoredMutation::Write {
                    path,
                    offset,
                    bytes: content,
                } => {
                    if content.is_empty() {
                        drop(content);
                        Mutation::ValidateRegular { path }
                    } else {
                        let content = self
                            .stage_authored_blob_with_accounting(content, accounting, cancellation)
                            .await?;
                        Mutation::Write {
                            path,
                            offset,
                            length: content.logical_bytes,
                            content: content.root,
                            content_offset: 0,
                        }
                    }
                }
                AuthoredMutation::WriteFromContent {
                    path,
                    offset,
                    content,
                } => {
                    if content.logical_bytes == 0 {
                        Mutation::ValidateRegular { path }
                    } else {
                        Mutation::Write {
                            path,
                            offset,
                            length: content.logical_bytes,
                            content: content.root,
                            content_offset: 0,
                        }
                    }
                }
                AuthoredMutation::SetMetadata { path, metadata } => {
                    let metadata = self
                        .stage_authored_metadata_with_accounting(
                            metadata,
                            staged_metadata,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    if path.is_root() {
                        drop(path);
                        let remaining = bytes.checked_sub(path_bytes).ok_or(WorkError::Overflow)?;
                        let retired = crate::kernel::split_allocation(&mut allocation, path_bytes)?;
                        bytes = remaining;
                        retire_allocation(retired, path_bytes, accounting)?;
                        Mutation::File {
                            file_id: self.root.root_file_id,
                            mutation: FileMutation::SetMetadata { metadata },
                        }
                    } else {
                        Mutation::SetMetadata { path, metadata }
                    }
                }
                AuthoredMutation::Resize {
                    path,
                    logical_bytes,
                } => Mutation::Resize {
                    path,
                    logical_bytes,
                },
                AuthoredMutation::ZeroRange {
                    path,
                    range,
                    allocated,
                    extend,
                } => Mutation::ZeroRange {
                    path,
                    offset: range.offset,
                    length: range.length,
                    allocated,
                    extend,
                },
                AuthoredMutation::Preallocate {
                    path,
                    range,
                    keep_size,
                } => Mutation::Preallocate {
                    path,
                    offset: range.offset,
                    length: range.length,
                    keep_size,
                },
                AuthoredMutation::CloneRange(request) => Mutation::CloneRange {
                    source: request.source,
                    source_offset: request.source_offset,
                    destination: request.destination,
                    destination_offset: request.destination_offset,
                    length: request.length,
                },
            };
            push_authored_operation(
                operations,
                &mut allocation,
                &mut bytes,
                accounting,
                mutation,
            )?;
            Ok(None)
        }
        .await;
        retire_allocation(allocation, bytes, accounting)?;
        result
    }
}

pub(super) fn push_authored_operation(
    operations: &mut crate::kernel::Owned<Vec<Mutation>>,
    allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
    bytes: &mut u64,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    mutation: Mutation,
) -> Result<(), FsError> {
    if operations.value.len() == operations.value.capacity() {
        return Err(FsError::PendingMutationAllocationFailed);
    }
    accounting.charge(WorkCounters {
        bytes_copied: size_of::<Mutation>() as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let path_bytes = mutation_path_owned_bytes(&mutation);
    let remaining = bytes.checked_sub(path_bytes).ok_or(WorkError::Overflow)?;
    let retained = operations
        .bytes
        .checked_add(path_bytes)
        .ok_or(WorkError::Overflow)?;
    let mut mutation = crate::kernel::Owned::new(
        mutation,
        crate::kernel::split_allocation(allocation, path_bytes)?,
        path_bytes,
    );
    *bytes = remaining;
    let transferred = operations.allocation.absorb(&mut mutation.allocation);
    if let Err(error) = transferred {
        mutation.retire(accounting)?;
        return Err(error);
    }
    operations.value.push(mutation.value);
    operations.bytes = retained;
    Ok(())
}

pub(super) fn clone_authored_path(
    path: &NamespacePath,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<NamespacePath>, FsError> {
    let cancellation = name_cancellation(accounting);
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    let bytes = path.retained_bytes();
    Ok(crate::kernel::Owned::new(path, allocation, bytes))
}

pub(super) fn clone_borrowed_authored_descriptor(
    authored: &AuthoredMutation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<AuthoredMutation>, FsError> {
    use AuthoredMutation::*;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let mut bytes = 0_u64;
    let result = (|| {
        let mut copy_path = |source: &NamespacePath| -> Result<NamespacePath, FsError> {
            let mut path = clone_authored_path(source, accounting)?;
            let Some(total) = bytes.checked_add(path.bytes) else {
                path.retire(accounting)?;
                return Err(WorkError::Overflow.into());
            };
            if let Err(error) = allocation.absorb(&mut path.allocation) {
                path.retire(accounting)?;
                return Err(error);
            }
            bytes = total;
            Ok(path.value)
        };
        Ok::<_, FsError>(match authored {
            Reidentify { path, file_id } => Reidentify {
                path: copy_path(path)?,
                file_id: *file_id,
            },
            CreateFile {
                path,
                bytes,
                metadata,
            } => CreateFile {
                path: copy_path(path)?,
                bytes: bytes.clone(),
                metadata: *metadata,
            },
            CreateFileFromContent {
                path,
                content,
                metadata,
                file_id,
            } => CreateFileFromContent {
                path: copy_path(path)?,
                content: *content,
                metadata: *metadata,
                file_id: *file_id,
            },
            CreateDirectory { path, metadata } => CreateDirectory {
                path: copy_path(path)?,
                metadata: *metadata,
            },
            CreateSymbolicLink {
                path,
                target,
                metadata,
            } => CreateSymbolicLink {
                path: copy_path(path)?,
                target: target.clone(),
                metadata: *metadata,
            },
            CreateEmptySpecial {
                path,
                kind,
                metadata,
            } => CreateEmptySpecial {
                path: copy_path(path)?,
                kind: *kind,
                metadata: *metadata,
            },
            CreateDevice {
                path,
                kind,
                major,
                minor,
                metadata,
            } => CreateDevice {
                path: copy_path(path)?,
                kind: *kind,
                major: *major,
                minor: *minor,
                metadata: *metadata,
            },
            CreateReparsePoint {
                path,
                payload,
                metadata,
            } => CreateReparsePoint {
                path: copy_path(path)?,
                payload: payload.clone(),
                metadata: *metadata,
            },
            Remove {
                path,
                expected_file_id,
            } => Remove {
                path: copy_path(path)?,
                expected_file_id: *expected_file_id,
            },
            Rename {
                source,
                destination,
                replace,
            } => Rename {
                source: copy_path(source)?,
                destination: copy_path(destination)?,
                replace: *replace,
            },
            HardLink {
                source,
                destination,
            } => HardLink {
                source: copy_path(source)?,
                destination: copy_path(destination)?,
            },
            Write {
                path,
                offset,
                bytes,
            } => Write {
                path: copy_path(path)?,
                offset: *offset,
                bytes: bytes.clone(),
            },
            WriteFromContent {
                path,
                offset,
                content,
            } => WriteFromContent {
                path: copy_path(path)?,
                offset: *offset,
                content: *content,
            },
            SetMetadata { path, metadata } => SetMetadata {
                path: copy_path(path)?,
                metadata: *metadata,
            },
            Resize {
                path,
                logical_bytes,
            } => Resize {
                path: copy_path(path)?,
                logical_bytes: *logical_bytes,
            },
            ZeroRange {
                path,
                range,
                allocated,
                extend,
            } => ZeroRange {
                path: copy_path(path)?,
                range: *range,
                allocated: *allocated,
                extend: *extend,
            },
            Preallocate {
                path,
                range,
                keep_size,
            } => Preallocate {
                path: copy_path(path)?,
                range: *range,
                keep_size: *keep_size,
            },
            CloneRange(request) => CloneRange(FileCloneRequest {
                source: copy_path(&request.source)?,
                source_offset: request.source_offset,
                destination: copy_path(&request.destination)?,
                destination_offset: request.destination_offset,
                length: request.length,
            }),
        })
    })();
    match result {
        Ok(value) => Ok(crate::kernel::Owned::new(value, allocation, bytes)),
        Err(error) => {
            crate::kernel::retire_allocation(allocation, bytes, accounting)?;
            Err(error)
        }
    }
}
