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
            if !self.checkout.view_gate.reads_for_callback(owner) {
                self.checkout.settle_pending().await;
            }
            Arc::clone(&self.checkout.view_gate)
                .read_for_callback(owner, expected_epoch)
                .await
        })?;
        Ok(Box::new(lease))
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| self.lookup_async(path, owner))
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        let file_id = self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let record = checkout
                .lookup_no_follow(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .record
                .ok_or(MountSourceError::NotFound)?;
            if record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own native file handles".to_owned(),
                ));
            }
            Ok(record.file_id)
        })?;
        Ok(Arc::new(CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    fn open_created(
        &self,
        _path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let attached = CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id: created.node.file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        };
        Ok(match self.checkout.pending_create(created.node.file_id) {
            Some(pending) => Arc::new(PendingCreatedFile { pending, attached }),
            None => Arc::new(attached),
        })
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        let (file, metadata) = self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            observation.ensure_publication_resolved()?;
            let mut checkout = observation.observer();
            let lookup = checkout
                .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .ok_or(MountSourceError::NotFound)?;
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own detached native handles".to_owned(),
                ));
            }
            let file = checkout
                .detach_regular_file(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            Ok((file, lookup.metadata))
        })?;
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState {
                file,
                metadata,
                mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
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
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
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
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
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
        let path = self.path(path)?;
        let cursor = cursor.map(|bytes| self.logical_name(bytes)).transpose()?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let receipt = checkout
                .list_directory_records(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let entries = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(MountDirectoryEntry {
                        name: native_mount_name(&entry.name)?,
                        node: mount_node(entry.record),
                        metadata: entry.metadata,
                        // A checkout lookup issues no content pin.
                        pin: None,
                    })
                })
                .collect::<Result<Vec<_>, MountSourceError>>()?;
            let next_cursor = has_more
                .then(|| entries.last().map(|entry| entry.name.clone()))
                .flatten();
            Ok(MountDirectoryPage {
                entries,
                next_cursor,
            })
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        // Sampled before the lookup, so it proves what the lookup found.
        let absent_since = self.checkout.ledger.stamp();
        if self.lookup(path)?.is_some() {
            return Err(MountSourceError::AlreadyExists);
        }
        self.create_absent_file(path, metadata, absent_since)
    }

    fn create_absent_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        absent_since: ViewStamp,
    ) -> Result<MountLookup, MountSourceError> {
        let mount_path = path;
        let path = self.path(path)?;
        // A creation waits to be applied, but not for publication to resolve.
        // An uncontended state answers in place, without entering the runtime.
        match self.checkout.state.try_read() {
            Ok(state) => state.ensure_publication_resolved()?,
            Err(_) => self.runtime.wait(|| async {
                self.checkout
                    .state
                    .read()
                    .await
                    .ensure_publication_resolved()
            })?,
        }
        let waiting = self
            .checkout
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        if waiting >= MAXIMUM_GROUPED_CHANGES {
            self.runtime.wait(|| async {
                self.checkout.settle_pending().await;
                Ok(())
            })?;
        }
        let file_id = FileId::new();
        let pending = || {
            Arc::new(PendingCreate {
                path: path.clone(),
                file_id,
                origin: ViewOrigin::current(),
                maximum_bytes: PENDING_CREATE_BYTES.min(self.limits.maximum_read_bytes),
                state: StdMutex::new(PendingState::Pending {
                    metadata: Box::new(metadata),
                    bytes: Vec::new(),
                }),
            })
        };
        if !self.checkout.hold_pending(pending(), Some(absent_since)) {
            // Something rebound the path since the caller looked, or another
            // creation holds it: this lookup decides instead.
            if self.lookup(mount_path)?.is_some() || !self.checkout.hold_pending(pending(), None) {
                return Err(MountSourceError::AlreadyExists);
            }
        }
        Ok(MountLookup {
            node: MountNode {
                file_id,
                kind: MountNodeKind::Regular,
                logical_bytes: 0,
                link_count: 1,
                device: None,
            },
            metadata,
        })
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateDirectory {
                        path: path.clone(),
                        metadata,
                    }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            let file_id = receipt
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or_else(|| {
                    MountSourceError::Engine("create omitted file identity".to_owned())
                })?;
            checkout
                .publish_after_mutation(ViewChange::Bound(&path), &self.cancellation)
                .await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::Directory,
                    logical_bytes: 0,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        let logical_bytes = u64::try_from(target.len()).map_err(|_| {
            MountSourceError::Unsupported("symbolic-link target is too large".to_owned())
        })?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateSymbolicLink {
                        path: path.clone(),
                        target,
                        metadata,
                    }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            let file_id = receipt
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or_else(|| {
                    MountSourceError::Engine("create omitted file identity".to_owned())
                })?;
            checkout
                .publish_after_mutation(ViewChange::Bound(&path), &self.cancellation)
                .await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::SymbolicLink,
                    logical_bytes,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        let (file_kind, mutation) = match (kind, device) {
            (MountNodeKind::Fifo, None) => (
                FileKind::Fifo,
                AuthoredMutation::CreateEmptySpecial {
                    path: path.clone(),
                    kind: FileKind::Fifo,
                    metadata,
                },
            ),
            (MountNodeKind::Socket, None) => (
                FileKind::Socket,
                AuthoredMutation::CreateEmptySpecial {
                    path: path.clone(),
                    kind: FileKind::Socket,
                    metadata,
                },
            ),
            (MountNodeKind::CharacterDevice, Some((major, minor))) => (
                FileKind::CharacterDevice,
                AuthoredMutation::CreateDevice {
                    path: path.clone(),
                    kind: FileKind::CharacterDevice,
                    major,
                    minor,
                    metadata,
                },
            ),
            (MountNodeKind::BlockDevice, Some((major, minor))) => (
                FileKind::BlockDevice,
                AuthoredMutation::CreateDevice {
                    path: path.clone(),
                    kind: FileKind::BlockDevice,
                    major,
                    minor,
                    metadata,
                },
            ),
            _ => {
                return Err(MountSourceError::Unsupported(
                    "special-file kind and device identity disagree".to_owned(),
                ));
            }
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(vec![mutation], boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            let file_id = receipt
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or_else(|| {
                    MountSourceError::Engine("create omitted file identity".to_owned())
                })?;
            checkout
                .publish_after_mutation(ViewChange::Bound(&path), &self.cancellation)
                .await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind,
                    logical_bytes: 0,
                    link_count: 1,
                    device: match file_kind {
                        FileKind::CharacterDevice | FileKind::BlockDevice => device,
                        _ => None,
                    },
                },
                metadata,
            })
        })
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
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
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
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
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
                .change_content_by_id(
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
        self.capture_host_with_identity(source_root, paths, expected_root_identity, HostCaptureScope::ExactPaths)
    }

    fn capture_host_subtree(&self, source_root: &Path, path: &MountPath) -> Result<(), MountSourceError> {
        self.capture_host_with_identity(source_root, std::slice::from_ref(path),
            capture_root_identity(source_root).map_err(engine_error)?, HostCaptureScope::Subtree)
    }
}
