impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Applies one content change at `path` with its time stamp as one
    /// mutation.
    fn change_content(
        &self,
        path: NamespacePath,
        change: ContentChange<NamespacePath>,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .change_content(path, change, &self.cancellation)
                .await
        })
    }
}

impl<A, O> MountFilesystem for CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn supports_posix_named_attributes(&self) -> bool {
        self.profile == FilesystemProfile::Posix
    }

    fn flush_publishes(&self) -> bool {
        // The policy is fixed at construction; an unreadable answer keeps
        // the conservative default.
        self.runtime
            .wait(|| async { Ok(self.checkout.publishes_at_native_boundary().await) })
            .unwrap_or(true)
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.checkout
            .view_gate
            .is_stable()
            .then(|| self.checkout.ledger.stamp())
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        self.checkout.ledger.observe(observer);
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.unchanged_since(&path, file_id, stamp))
    }

    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.path(path).is_ok_and(|path| {
            self.checkout
                .reported_unchanged_since(&path, file_id, stamp)
        })
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.checkout.node_unchanged_since(file_id, stamp)
    }

    fn listing_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.listing_unchanged_since(&path, stamp))
    }

    fn folded_path(&self, path: &MountPath) -> Option<MountPath> {
        self.folds_names().then(|| {
            path.components()
                .iter()
                .fold(MountPath::root(), |folded, component| {
                    folded.child(
                        self.folded_name(component)
                            .unwrap_or_else(|| component.clone()),
                    )
                })
        })
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.checkout.view_gate.generation())
    }

    fn view_is_stable(&self) -> bool {
        self.checkout.view_gate.is_stable()
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        self.acquire_binding_lease(None)
    }

    /// Leases the view for a callback, once every pending creation is
    /// applied: whatever the callback then reads sees them.
    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let lease = self.runtime.wait(|| async {
            Arc::clone(&self.checkout.view_gate)
                .read_for_callback(owner, expected_epoch)
                .await
        })?;
        Ok(Box::new(lease))
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        let mut carrier = NativeDemandCarrier::Borrowed(&mut work);
        self.runtime
            .wait(|| self.lookup_async(path, owner, &mut carrier))
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        CheckoutMountSource::open_file(self, path)
    }

    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        CheckoutMountSource::open_created(self, path, created)
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        CheckoutMountSource::detach_file(self, path)
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            checkout
                .read_symbolic_link(&path, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            checkout
                .read_file_range(
                    &path,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            checkout
                .seek_file_extent(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        CheckoutMountSource::read_directory(self, path, cursor, maximum_entries)
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        _absent_since: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime
            .block_on(
                || {
                    tokio::time::timeout(
                        CALLBACK_TIMEOUT,
                        self.create(path, work, |path| AuthoredMutation::CreateFile {
                            path,
                            metadata,
                            bytes: acyclic_stream::OwnedBytes::new(),
                        }),
                    )
                },
                Some(work.runtime_meter().as_ref()),
            )
            .map_err(|error| MountSourceError::NativeWork(work.runtime_error(error)))?
            .map_err(|_| MountSourceError::Stale)?
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime
            .block_on(
                || {
                    tokio::time::timeout(
                        CALLBACK_TIMEOUT,
                        self.create(path, work, |path| AuthoredMutation::CreateDirectory {
                            path,
                            metadata,
                        }),
                    )
                },
                Some(work.runtime_meter().as_ref()),
            )
            .map_err(|error| MountSourceError::NativeWork(work.runtime_error(error)))?
            .map_err(|_| MountSourceError::Stale)?
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime
            .block_on(
                || {
                    tokio::time::timeout(
                        CALLBACK_TIMEOUT,
                        self.create(path, work, |path| AuthoredMutation::CreateSymbolicLink {
                            path,
                            metadata,
                            target: target.bytes,
                        }),
                    )
                },
                Some(work.runtime_meter().as_ref()),
            )
            .map_err(|error| MountSourceError::NativeWork(work.runtime_error(error)))?
            .map_err(|_| MountSourceError::Stale)?
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        let kind = match (kind, device) {
            (MountNodeKind::Fifo, None) => FileKind::Fifo,
            (MountNodeKind::Socket, None) => FileKind::Socket,
            (MountNodeKind::CharacterDevice, Some(_)) => FileKind::CharacterDevice,
            (MountNodeKind::BlockDevice, Some(_)) => FileKind::BlockDevice,
            _ => {
                return Err(MountSourceError::NativeText {
                    kind: super::NativeSourceTextKind::Unsupported,
                    text: "special-file kind and device identity disagree",
                });
            }
        };
        self.runtime
            .block_on(
                || {
                    tokio::time::timeout(
                        CALLBACK_TIMEOUT,
                        self.create(path, work, |path| match device {
                            Some((major, minor)) => AuthoredMutation::CreateDevice {
                                path,
                                kind,
                                major,
                                minor,
                                metadata,
                            },
                            None => AuthoredMutation::CreateEmptySpecial {
                                path,
                                kind,
                                metadata,
                            },
                        }),
                    )
                },
                Some(work.runtime_meter().as_ref()),
            )
            .map_err(|error| MountSourceError::NativeWork(work.runtime_error(error)))?
            .map_err(|_| MountSourceError::Stale)?
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&path, &self.cancellation).await?;
            let mut authored = vec![AuthoredMutation::SetMetadata {
                path: path.clone(),
                metadata,
            }];
            if let Some(logical_bytes) = logical_bytes {
                authored.push(AuthoredMutation::Resize {
                    path,
                    logical_bytes,
                });
            }
            checkout
                .apply_authored_transaction(authored, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(node), &self.cancellation)
                .await
        })
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            checkout
                .read_named_attribute(&path, &name, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = cursor
            .map(|name| self.posix_attribute_name(name))
            .transpose()?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            let receipt = checkout
                .list_named_attributes(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let names = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.name.class() != AttributeClass::PosixXattr {
                        return Err(MountSourceError::Unsupported(
                            "POSIX mount encountered a non-POSIX named attribute".to_owned(),
                        ));
                    }
                    Ok(entry.name.as_bytes().to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_cursor = has_more.then(|| names.last().cloned()).flatten();
            Ok(MountAttributePage { names, next_cursor })
        })
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&path, &self.cancellation).await?;
            checkout
                .write_named_attribute(
                    path,
                    name,
                    value,
                    mode,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(node), &self.cancellation)
                .await
        })
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&path, &self.cancellation).await?;
            checkout
                .remove_named_attribute(path, name, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(node), &self.cancellation)
                .await
        })
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(path, ContentChange::Write { offset, bytes })
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(path, ContentChange::Resize { logical_bytes })
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(
            path,
            range_allocation(ByteRange { offset, length }, operation),
        )
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.change_content(
            destination,
            ContentChange::CloneFrom {
                source,
                source_offset,
                offset: destination_offset,
                length,
            },
        )
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .change_content(
                    destination_file_id,
                    ContentChange::CloneFrom {
                        source: source_file_id,
                        source_offset,
                        offset: destination_offset,
                        length,
                    },
                    &self.cancellation,
                )
                .await
        })
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = match expected {
                Some(node) => node,
                None => checkout.node_at(&path, &self.cancellation).await?,
            };
            checkout
                .remove(
                    path.clone(),
                    expected,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(ViewChange::Unbound(&path, node), &self.cancellation)
                .await
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let moved = checkout.node_at(&source, &self.cancellation).await?;
            let replaced = checkout
                .node_if_bound(&destination, &self.cancellation)
                .await?;
            checkout
                .rename(
                    source.clone(),
                    destination.clone(),
                    replace,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(
                    ViewChange::Moved {
                        from: &source,
                        to: &destination,
                        moved,
                        replaced,
                    },
                    &self.cancellation,
                )
                .await
        })
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&source, &self.cancellation).await?;
            checkout
                .hard_link(
                    source,
                    destination.clone(),
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(ViewChange::Linked(&destination, node), &self.cancellation)
                .await
        })
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            // A manual checkout publishes nothing here, so it never excludes
            // every callback just to decide that.
            if !self.checkout.publishes_at_native_boundary().await {
                return Ok(());
            }
            let mut checkout = self.checkout.lock().await;
            checkout
                .publish_at_native_boundary(&self.cancellation)
                .await
        })
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let expected_root_identity = capture_root_identity(source_root).map_err(engine_error)?;
        self.capture_host_with_identity(
            source_root,
            paths,
            expected_root_identity,
            HostCaptureScope::ExactPaths,
        )
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_identity(
            source_root,
            std::slice::from_ref(path),
            capture_root_identity(source_root).map_err(engine_error)?,
            HostCaptureScope::Subtree,
        )
    }
}
