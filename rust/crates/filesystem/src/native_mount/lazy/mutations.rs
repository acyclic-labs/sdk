macro_rules! mutation_callbacks {
    () => {
        fn create_file(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
            absent_since: Option<ViewStamp>,
            work: &crate::NativeMaterializationWork,
        ) -> Result<MountLookup, MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            self.promote_parents_locked(path, Some(work))?;
            let created = self
                .authored
                .create_file(path, metadata, absent_since, work)?;
            self.record_rebound(&self.path(path, None)?.0)?;
            Ok(created)
        }

        fn create_directory(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
            work: &crate::NativeMaterializationWork,
        ) -> Result<MountLookup, MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            self.promote_parents_locked(path, Some(work))?;
            let created = self.authored.create_directory(path, metadata, work)?;
            self.record_rebound(&self.path(path, None)?.0)?;
            Ok(created)
        }

        fn create_symbolic_link(
            &self,
            path: &MountPath,
            target: crate::kernel::OwnedSourceChunk,
            metadata: FileMetadata,
            work: &crate::NativeMaterializationWork,
        ) -> Result<MountLookup, MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            self.promote_parents_locked(path, Some(work))?;
            let created = self
                .authored
                .create_symbolic_link(path, target, metadata, work)?;
            self.record_rebound(&self.path(path, None)?.0)?;
            Ok(created)
        }

        fn create_special(
            &self,
            path: &MountPath,
            kind: MountNodeKind,
            device: Option<(u32, u32)>,
            metadata: FileMetadata,
            work: &crate::NativeMaterializationWork,
        ) -> Result<MountLookup, MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            self.promote_parents_locked(path, Some(work))?;
            let created = self
                .authored
                .create_special(path, kind, device, metadata, work)?;
            self.record_rebound(&self.path(path, None)?.0)?;
            Ok(created)
        }

        fn set_attributes(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
            logical_bytes: Option<u64>,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            if let Some(file) = self.detached_for_path(path)? {
                return file.set_attributes(metadata, logical_bytes, work);
            }
            self.promote_locked(path, Some(work))?;
            self.authored
                .set_attributes(path, metadata, logical_bytes, work)
        }

        fn write_attribute(
            &self,
            path: &MountPath,
            name: &[u8],
            value: crate::kernel::OwnedSourceChunk,
            mode: MountAttributeWriteMode,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            if let Some(file) = self.detached_for_path(path)? {
                return file.write_attribute(name, value, mode, work);
            }
            self.promote_locked(path, Some(work))?;
            self.authored.write_attribute(path, name, value, mode, work)
        }

        fn remove_attribute(
            &self,
            path: &MountPath,
            name: &[u8],
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            if let Some(file) = self.detached_for_path(path)? {
                return file.remove_attribute(name, work);
            }
            self.promote_locked(path, Some(work))?;
            self.authored.remove_attribute(path, name, work)
        }

        fn write_range(
            &self,
            path: &MountPath,
            offset: u64,
            bytes: Bytes,
        ) -> Result<(), MountSourceError> {
            self.open_file(path)?.write_range(offset, bytes)
        }

        fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
            self.open_file(path)?.resize(logical_bytes)
        }

        fn allocate_range(
            &self,
            path: &MountPath,
            offset: u64,
            length: u64,
            operation: MountRangeAllocation,
        ) -> Result<(), MountSourceError> {
            self.open_file(path)?
                .allocate_range(offset, length, operation)
        }

        fn clone_range(
            &self,
            source: &MountPath,
            source_offset: u64,
            destination: &MountPath,
            destination_offset: u64,
            length: u64,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, None)?;
            if self.detached_for_path(source)?.is_some()
                || self.detached_for_path(destination)?.is_some()
            {
                return Err(MountSourceError::Unsupported(
                    "range cloning a detached identity requires identity-level clone support"
                        .into(),
                ));
            }
            self.promote_locked(source, None)?;
            self.promote_locked(destination, None)?;
            self.authored.clone_range(
                source,
                source_offset,
                destination,
                destination_offset,
                length,
            )
        }

        fn clone_range_by_id(
            &self,
            source_file_id: FileId,
            source_offset: u64,
            destination_file_id: FileId,
            destination_offset: u64,
            length: u64,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            work.admit(crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork)?;
            let _ = (
                source_file_id,
                source_offset,
                destination_file_id,
                destination_offset,
                length,
            );
            Err(MountSourceError::Unsupported(
                "clone-by-identity requires path-backed promotion in a lazy mount".into(),
            ))
        }

        fn remove(
            &self,
            path: &MountPath,
            expected: Option<FileId>,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            let text = self.path(path, None)?.0;
            let removed_id = if let Some(authored) = self.authored.lookup(path)? {
                if self.removed_identity(&text, authored.node.file_id)? {
                    return Err(MountSourceError::NotFound);
                }
                if expected.is_some_and(|expected| expected != authored.node.file_id) {
                    return Err(MountSourceError::Stale);
                }
                let removed_open = if self.has_open_source_handle(authored.node.file_id)? {
                    let (record, file) =
                        self.removed_open_file(authored.node.file_id, authored.metadata, work)?;
                    Some((record, authored.metadata, file))
                } else {
                    None
                };
                self.authored.remove(path, expected, work)?;
                if let Some((record, metadata, file)) = removed_open {
                    self.record_removed_identity(record, metadata, file)?;
                }
                authored.node.file_id
            } else {
                if self.is_removed(&text)? {
                    return Err(MountSourceError::NotFound);
                }
                let file_id = self.runtime.wait(|| async {
                    let (lookup, _allocation) = self
                        .lazy
                        .lookup(
                            &text,
                            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        )
                        .await
                        .map_err(lazy_error)?;
                    let actual = file_id(&self.lazy, &text, &lookup, Some(work)).await?;
                    if expected.is_some_and(|expected| expected != actual) {
                        return Err(MountSourceError::Stale);
                    }
                    Ok(actual)
                })?;
                if self.has_open_source_handle(file_id)? {
                    self.promote_locked(path, Some(work))?;
                    let metadata = self
                        .authored
                        .lookup(path)?
                        .ok_or(MountSourceError::Stale)?
                        .metadata;
                    let (record, file) = self.removed_open_file(file_id, metadata, work)?;
                    self.authored.remove(path, Some(file_id), work)?;
                    self.record_removed_identity(record, metadata, file)?;
                }
                file_id
            };
            self.record_removed(path, removed_id, work)
        }

        fn rename(
            &self,
            source: &MountPath,
            destination: &MountPath,
            replace: bool,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            let source_text = self.path(source, None)?.0;
            let destination_text = self.path(destination, None)?.0;
            if self.authored.lookup(source)?.is_none() && self.is_removed(&source_text)? {
                return Err(MountSourceError::NotFound);
            }
            if source_text == destination_text {
                if self.authored.lookup(source)?.is_some() {
                    return Ok(());
                }
                return self.runtime.wait(|| async {
                    self.lazy
                        .lookup(
                            &source_text,
                            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        )
                        .await
                        .map_err(lazy_error)?;
                    Ok(())
                });
            }
            if self.authored.lookup(source)?.is_none() {
                let (source_lookup, _allocation) = self.runtime.wait(|| async {
                    self.lazy
                        .lookup(
                            &source_text,
                            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        )
                        .await
                        .map_err(lazy_error)
                })?;
                if matches!(
                    source_lookup,
                    LazyLookup::Source {
                        node: SourceNode {
                            kind: SourceNodeKind::Directory,
                            ..
                        },
                        ..
                    }
                ) {
                    // Its entries resolve through the source by their paths, which
                    // the rename changes: the subtree becomes authored first and
                    // moves as one directory.
                    self.promote_subtree_locked(source, work)?;
                }
            }
            self.promote_locked(source, Some(work))?;
            let source_id = self
                .authored
                .lookup(source)?
                .ok_or(MountSourceError::Stale)?
                .node
                .file_id;
            self.promote_parents_locked(destination, Some(work))?;
            if self.authored.lookup(destination)?.is_none()
                && !self.is_removed(&destination_text)?
            {
                let source_destination = self.runtime.wait(|| async {
                    self.lazy
                        .lookup(
                            &destination_text,
                            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        )
                        .await
                        .map_err(lazy_error)
                });
                match source_destination {
                    Ok(_) if replace => self.promote_locked(destination, Some(work))?,
                    Ok(_) => return Err(MountSourceError::AlreadyExists),
                    Err(MountSourceError::NotFound) => {}
                    Err(error) => return Err(error),
                }
            }
            let replaced_open = if replace {
                if let Some(destination_lookup) = self.authored.lookup(destination)? {
                    let destination_id = destination_lookup.node.file_id;
                    if destination_id != source_id && self.has_open_source_handle(destination_id)? {
                        let (record, file) = self.removed_open_file(
                            destination_id,
                            destination_lookup.metadata,
                            work,
                        )?;
                        Some((record, destination_lookup.metadata, file))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            self.authored.rename(source, destination, replace, work)?;
            if let Some((record, metadata, file)) = replaced_open {
                self.record_removed_identity(record, metadata, file)?;
            }
            self.record_removed(source, source_id, work)?;
            self.record_rebound(&destination_text)
        }

        fn hard_link(
            &self,
            source: &MountPath,
            destination: &MountPath,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let _mutation = self.mutation_lease(None, Some(work))?;
            self.promote_locked(source, Some(work))?;
            self.promote_parents_locked(destination, Some(work))?;
            self.authored.hard_link(source, destination, work)?;
            self.record_rebound(&self.path(destination, None)?.0)
        }

        fn flush(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
            // The authored checkout's policy decides, as at its own native
            // boundary: a manual mount publishes only through sync and unmount.
            let authored = self.authored.shared_checkout();
            if !self
                .runtime
                .wait(|| async { Ok(authored.publishes_at_native_boundary().await) })?
            {
                return Ok(());
            }
            let _mutation = self.mutation_lease(None, Some(work))?;

            self.runtime.wait(|| async {
                self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted, work)
                    .await
            })
        }
    };
}

pub(super) use mutation_callbacks;
