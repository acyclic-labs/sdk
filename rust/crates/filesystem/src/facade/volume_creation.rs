use super::*;

pub(super) const ROOT_FILE_DOMAIN: &[u8] = b"acyclic-fs-root-file-v1\0";
pub(super) const CREATION_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-create-operation-v1\0";
pub(super) const CREATION_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-create-fingerprint-v1\0";

pub(super) struct PreparedCreationCommit {
    pub(super) commit: ProposedCommit,
    pub(super) changes: Option<crate::PublicationChanges>,
    pub(super) hold: crate::PublicationHold,
}

/// Immutable semantics and optional policy published in the first generation.
#[derive(Clone, Copy)]
pub struct VolumeInitialization<'a> {
    /// Owning volume's native name semantics and limits.
    pub config: VolumeConfig,
    /// Owner-validated versioned policy bytes; absence grants no access.
    pub access_policy: Option<&'a [u8]>,
}

impl<'a> VolumeInitialization<'a> {
    /// Select immutable volume semantics and the initial owner policy.
    pub const fn new(config: VolumeConfig, access_policy: Option<&'a [u8]>) -> Self {
        Self {
            config,
            access_policy,
        }
    }
}

/// One named attribute written beside a creation's file in its initial generation.
#[derive(Clone, Debug)]
pub struct CreationAttribute {
    /// The creation file the attribute belongs to, spelled as in the creation.
    pub path: String,
    /// The exact canonical attribute name.
    pub name: AttributeName,
    /// The attribute value.
    pub value: acyclic_stream::OwnedBytes,
}

/// One creation of a volume: the operation a retry repeats, and the files its
/// creation commit publishes in the initial generation. Files belong to a
/// named creation, so only a creation with an operation can hold them.
#[derive(Clone, Debug)]
pub struct CreationOperation<'a> {
    /// The creation's operation identity.
    pub operation_id: OperationId,
    /// Files the creation commit publishes, each a child of the root.
    pub files: &'a [(&'a str, acyclic_stream::OwnedBytes)],
    /// Named attributes written with those files in the same generation.
    pub attributes: Vec<CreationAttribute>,
}

impl<'a> CreationOperation<'a> {
    /// A creation whose initial generation holds exactly `files`.
    pub const fn populated(
        operation_id: OperationId,
        files: &'a [(&'a str, acyclic_stream::OwnedBytes)],
    ) -> Self {
        Self::populated_with_attributes(operation_id, files, Vec::new())
    }

    /// A creation whose initial generation holds exactly `files` and their attributes.
    pub const fn populated_with_attributes(
        operation_id: OperationId,
        files: &'a [(&'a str, acyclic_stream::OwnedBytes)],
        attributes: Vec<CreationAttribute>,
    ) -> Self {
        Self {
            operation_id,
            files,
            attributes,
        }
    }
}

impl From<OperationId> for CreationOperation<'_> {
    /// A creation of an empty initial generation.
    fn from(operation_id: OperationId) -> Self {
        Self::populated(operation_id, &[])
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Creates a fresh volume and its authenticated empty generation using
    /// the supplied standalone budget or original native invocation.
    pub async fn create_volume<W: FsWork>(
        &self,
        config: VolumeConfig,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Volume<A, O>, FsError> {
        self.create_volume_with_id(VolumeId::new(), config, work, cancellation)
            .await
    }
    /// Idempotently creates one caller-selected volume identity.
    pub async fn create_volume_with_id<W: FsWork>(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Volume<A, O>, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .create_volume_operation(
                volume_id,
                VolumeInitialization {
                    config,
                    access_policy: None,
                },
                None,
                &mut accounting,
                cancellation,
                None::<&mut ()>,
            )
            .await
            .map(|(volume, _)| volume);
        work.finish(result, &accounting)
    }
    /// Opens one volume using its authenticated immutable configuration.
    /// Reads and temporary backing use the supplied work source; native work
    /// retains its original admission and cancellation through authority replay.
    pub async fn open_volume<W: FsWork>(
        &self,
        volume_id: VolumeId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Volume<A, O>, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .open_volume_with_accounting(volume_id, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(super) async fn open_volume_with_accounting(
        &self,
        volume_id: VolumeId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Volume<A, O>, FsError> {
        let mut accounting = accounting.allocation_scope()?;
        let config = self
            .read_creation(volume_id, &mut accounting, cancellation)
            .await?
            .0
            .config;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        Ok(Volume {
            fs: self.clone(),
            id: volume_id,
            config,
        })
    }
    /// Restores a volume authority from a fully imported immutable generation.
    pub async fn restore_volume(
        &self,
        manifest: &GenerationExportManifest,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        validate_volume_capabilities(manifest.config, self.inner.capabilities)
            .map_err(OperationFailure::before_work)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let proof = authenticate_generation_export_manifest_async(
            &self.inner.objects,
            manifest,
            closure_limits(manifest.config),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            OperationFailure::new(map_transfer_error(failure.error), *failure.work)
        })?;
        let mut work = proof.work;
        let mut allocations = AllocationLedger::default();
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut allocations,
            &mut work,
            budget,
            cancellation,
        );
        let result = self
            .publish_creation(
                VolumeCreated {
                    volume_id: manifest.volume_id,
                    config: manifest.config,
                    initial_generation_root: manifest.generation_root,
                },
                Some(operation_id),
                &manifest.objects,
                proven_at,
                &mut accounting,
                cancellation,
                None::<&mut ()>,
            )
            .await;
        measured(result, &accounting)
    }
    /// Publishes the creation commit whose initial generation holds exactly
    /// the creation's files: every volume and workspace is created here, an
    /// empty one being the case without files.
    pub(super) async fn create_volume_operation<E, G>(
        &self,
        volume_id: VolumeId,
        initialization: VolumeInitialization<'_>,
        creation: Option<&CreationOperation<'_>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> Result<(Volume<A, O>, ObjectId), E>
    where
        E: From<FsError> + acyclic_stream::ProviderTask,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
    {
        accounting.check()?;
        let config = initialization.config;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let operation_id = creation.as_ref().map(|creation| creation.operation_id);
        let generation_root = self
            .initial_generation_root(
                volume_id,
                initialization,
                creation,
                accounting,
                cancellation,
            )
            .await?;
        let proof = self
            .publication_proof(generation_root, proven_at, volume_id, config, accounting)
            .await?;
        let result = self
            .publish_creation(
                VolumeCreated {
                    volume_id,
                    config,
                    initial_generation_root: generation_root,
                },
                operation_id,
                &proof.objects,
                proven_at,
                accounting,
                cancellation,
                guard,
            )
            .await;
        drop(proof);
        result.map(|volume| (volume, generation_root))
    }
    pub(super) async fn publish_creation<E, G>(
        &self,
        creation: VolumeCreated,
        operation_id: Option<OperationId>,
        closure: &[ObjectId],
        proven_at: u64,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> Result<Volume<A, O>, E>
    where
        E: From<FsError> + acyclic_stream::ProviderTask,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
    {
        let VolumeCreated {
            volume_id, config, ..
        } = creation;
        let prepared = self
            .prepare_creation_commit(
                creation,
                operation_id,
                closure,
                proven_at,
                accounting,
                cancellation,
            )
            .await?;
        accounting.check()?;
        let mut authority = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let result = self
            .inner
            .authority
            .create_first_record(
                volume_authority_id(volume_id),
                prepared.commit,
                &mut authority,
                guard,
            )
            .await;
        let recorded = finish_authority_work(authority);
        if let Some(changes) = prepared.changes {
            changes.retire(accounting)?;
        }
        recorded?;
        drop(prepared.hold);
        let created = result.map_err(|error| match error {
            crate::AuthorityError::Authority(error) => E::from(FsError::from(error)),
            crate::AuthorityError::Owner(error) => error.into(),
        })?;
        if !created {
            return Err(FsError::CreationRejected.into());
        }
        Ok(Volume {
            fs: self.clone(),
            id: volume_id,
            config,
        })
    }

    /// Encodes one volume's first record and holds its closure live until
    /// the commit that carries it is durable.
    pub(super) async fn prepare_creation_commit(
        &self,
        creation: VolumeCreated,
        operation_id: Option<OperationId>,
        closure: &[ObjectId],
        proven_at: u64,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<PreparedCreationCommit, FsError> {
        let volume_id = creation.volume_id;
        accounting.check()?;
        let operation_id = if let Some(id) = operation_id {
            id
        } else {
            accounting.charge(identity_hash_work(CREATION_OPERATION_DOMAIN))?;
            let mut identity = [0; 16];
            identity.copy_from_slice(
                &derived_digest(CREATION_OPERATION_DOMAIN, volume_id).as_bytes()[..16],
            );
            OperationId::from_bytes(identity)
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_volume_created(creation, observe, allocation_error)
            })?;
        let mut commit = creation_commit(operation_id, encoded, accounting)?;
        commit.publication = Some(
            crate::PublicationChanges::initial(
                &self.inner.objects,
                creation,
                accounting,
                cancellation,
            )
            .await?,
        );
        let hold = self
            .flush_publication(
                crate::PublicationScope {
                    root: Some(creation.initial_generation_root),
                    related_root: None,
                    related_objects: &[],
                    objects: closure,
                    proven_at,
                },
                accounting,
                cancellation,
            )
            .await?;
        let changes = commit.publication.clone();
        Ok(PreparedCreationCommit {
            commit,
            changes,
            hold,
        })
    }
    pub(super) async fn read_creation(
        &self,
        volume_id: VolumeId,
        accounting: &mut KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<(VolumeCreated, OperationId), FsError> {
        accounting.check()?;
        let mut work = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let maximum_payload_bytes = work.accounting().remaining()?.authority_bytes_read;
        let result = self
            .inner
            .authority
            .replay(
                volume_authority_id(volume_id),
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: maximum_payload_bytes,
                },
                &mut work,
            )
            .await;
        finish_authority_work(work)?;
        let (records, _allocation) = result.map_err(|error| match error {
            #[cfg(not(target_arch = "wasm32"))]
            AuthorityStoreError::NativeStream(error)
                if error.category() == acyclic_stream::StreamCategory::NotFound =>
            {
                AuthorityStoreError::Missing
            }
            error => error,
        })?;
        decode_first_creation_record(volume_id, &records, accounting)
    }

    /// The initial generation a creation publishes: the empty generation when
    /// it has no files, else that generation's one checkpoint creating each
    /// file. It is a function of the volume, its configuration and the
    /// creation alone, so a retried creation rebuilds exactly the generation
    /// the original published.
    pub(super) async fn initial_generation_root(
        &self,
        volume_id: VolumeId,
        initialization: VolumeInitialization<'_>,
        creation: Option<&CreationOperation<'_>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let config = initialization.config;
        let [.., empty] = self
            .build_empty_generation(volume_id, initialization, accounting, cancellation)
            .await?;
        // Populated files are authored by the creation itself, under the
        // operation a retry repeats.
        let Some(creation) = creation.filter(|creation| !creation.files.is_empty()) else {
            return Ok(empty);
        };
        // The checkout and its transaction run in an admitted heap frame, so
        // no caller's future embeds them.
        let mut frame_allocation = None;
        let (mut future, frame_bytes) = crate::heap_future::in_heap_admitted(
            (self, &mut *accounting),
            |(_, accounting), bytes| {
                frame_allocation = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
                Ok::<_, FsError>(())
            },
            |(this, accounting)| async move {
                this.populate_initial_generation(
                    volume_id,
                    config,
                    empty,
                    creation,
                    accounting,
                    cancellation,
                )
                .await
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        if let Some(mut allocation) = frame_allocation {
            accounting.release_owned(&mut allocation, frame_bytes)?;
        }
        result
    }

    /// Whether a recovered initial generation holds exactly `files`, decided
    /// by reads alone: each file holds its bytes and the root lists nothing
    /// else. The creation commit has no directories to create parents in, so
    /// every file it publishes is a child of the root. A retry with other,
    /// fewer or no files therefore never adopts the creation, and recovery
    /// writes nothing. The caller releases the standalone read custody.
    pub(super) async fn initial_generation_matches(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        generation_root: ObjectId,
        files: &[(&str, acyclic_stream::OwnedBytes)],
        attributes: &[CreationAttribute],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let groups = group_creation_attributes(files, attributes, accounting)?;
        let result = async {
            let volume = Volume {
                fs: self.clone(),
                id: volume_id,
                config,
            };
            let mut checkout = volume
                .unpublished_checkout(generation_root, accounting, cancellation)
                .await?;
            // Repeated paths are never one creation's files: the commit would
            // refuse the second, so a retry naming one twice is rejected here.
            let mut paths =
                crate::kernel::Owned::<Vec<NamespacePath>>::allocate(files.len(), accounting)?;
            for (ordinal, (name, bytes)) in files.iter().enumerate() {
                let (path, mut allocation) = NamespacePath::parse_in_profile(
                    name,
                    config.profile,
                    config.limits,
                    accounting,
                    cancellation,
                )?;
                paths.allocation.absorb(&mut allocation)?;
                paths.bytes = paths
                    .bytes
                    .checked_add(path.retained_bytes())
                    .ok_or(WorkError::Overflow)?;
                let record = match checkout
                    .lookup_content_record(&path, accounting, cancellation)
                    .await
                {
                    Ok(record) => record,
                    Err(FsError::NotFound) => return Ok(false),
                    Err(error) => return Err(error),
                };
                // Each file is a regular child of the root holding its bytes,
                // read in chunks the volume allows.
                let expected: &[u8] = bytes.as_ref();
                let logical_bytes = match record.payload {
                    _ if path.depth() != 1 || record.kind != FileKind::Regular => return Ok(false),
                    FilePayload::InlineRegular(data) if data.as_bytes() == expected => 0,
                    FilePayload::Regular { logical_bytes, .. }
                        if logical_bytes == expected.len() as u64 =>
                    {
                        logical_bytes
                    }
                    _ => return Ok(false),
                };
                let step = crate::kernel::CONTENT_READ_CHUNK_BYTES;
                let mut offset = 0;
                while offset < logical_bytes {
                    let length = step.min(logical_bytes - offset);
                    let range = ByteRange { offset, length };
                    let read = checkout
                        .read_record_range_with_accounting(
                            record,
                            range,
                            range,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    let start = usize::try_from(offset).map_err(|_| WorkError::Overflow)?;
                    let end = usize::try_from(offset + length).map_err(|_| WorkError::Overflow)?;
                    if expected.get(start..end) != Some(read.bytes.as_ref()) {
                        return Ok(false);
                    }
                    offset += length;
                }
                let requested = match groups.by_file.value.get(ordinal).copied().flatten() {
                    Some(group) => {
                        let (_, range) =
                            groups.groups.value.get(group).ok_or(FsError::InvalidDiff)?;
                        groups
                            .ordered
                            .value
                            .get(range.clone())
                            .ok_or(FsError::InvalidDiff)?
                    }
                    None => &[],
                };
                let mut after: Option<crate::kernel::Owned<AttributeName>> = None;
                let mut requested_index = 0;
                let mut matched = true;
                loop {
                    let (listing, allocation) = checkout
                        .list_named_attributes_with_accounting(
                            &path,
                            after.as_ref().map(|name| &name.value),
                            AttributePageSize::new(ATTRIBUTE_PAGE_ENTRIES),
                            None,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    let listing_bytes = crate::foundation::usize_to_u64(
                        listing
                            .entries
                            .capacity()
                            .checked_mul(std::mem::size_of::<crate::kernel::AttributeEntry>())
                            .ok_or(WorkError::Overflow)?,
                    );
                    let listing = crate::kernel::Owned::new(listing, allocation, listing_bytes);
                    for entry in &listing.value.entries {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        let Some(key) = requested.get(requested_index) else {
                            matched = false;
                            break;
                        };
                        if &entry.name != key.name {
                            matched = false;
                            break;
                        }
                        requested_index += 1;
                    }
                    if !matched {
                        listing.retire(accounting)?;
                        break;
                    }
                    let has_more = listing.value.has_more;
                    let next = match listing.value.entries.last() {
                        Some(entry) => {
                            let (name, allocation) =
                                crate::kernel::decode_owned(accounting, |observe| {
                                    entry.name.try_clone(&mut |step| observe(step))
                                })?;
                            let bytes = crate::foundation::usize_to_u64(name.retained_bytes());
                            Some(crate::kernel::Owned::new(name, allocation, bytes))
                        }
                        None => None,
                    };
                    listing.retire(accounting)?;
                    if let Some(previous) = after.take() {
                        previous.retire(accounting)?;
                    }
                    after = next;
                    if !has_more {
                        break;
                    }
                    if after.is_none() {
                        matched = false;
                        break;
                    }
                }
                if let Some(previous) = after.take() {
                    previous.retire(accounting)?;
                }
                if !matched || requested_index != requested.len() {
                    return Ok(false);
                }
                for key in requested {
                    let attribute = attributes.get(key.ordinal).ok_or(FsError::InvalidDiff)?;
                    let actual = checkout
                        .read_named_attribute_with_accounting(
                            &path,
                            &attribute.name,
                            None,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    if actual.map(|chunk| chunk.bytes).as_deref() != Some(attribute.value.as_ref())
                    {
                        return Ok(false);
                    }
                }
                accounting.charge(WorkCounters {
                    bytes_copied: std::mem::size_of::<NamespacePath>() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                paths.value.push(path);
            }
            if !initial_paths_are_distinct(&paths.value, accounting)? {
                return Ok(false);
            }
            paths.retire(accounting)?;
            initial_root_has_file_count(
                &mut checkout,
                files.len(),
                config.limits,
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        let retired = groups.retire(accounting);
        let result = result?;
        retired?;
        Ok(result)
    }

    async fn populate_initial_generation(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        empty: ObjectId,
        creation: &CreationOperation<'_>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let operation_id = creation.operation_id;
        let files = creation.files;
        let attributes = creation.attributes.as_slice();
        let groups = group_creation_attributes(files, attributes, accounting)?;
        let result = async {
            let volume = Volume {
                fs: self.clone(),
                id: volume_id,
                config,
            };
            let mut checkout = volume
                .unpublished_checkout(empty, accounting, cancellation)
                .await?;
            checkout.bind_authored_operation(operation_id);
            let mut operations =
                crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(files.len(), accounting)?;
            let moved = WorkCounters {
                bytes_copied: std::mem::size_of::<AuthoredMutation>() as u64,
                ..WorkCounters::UNCHARGED
            };
            for (ordinal, (name, bytes)) in files.iter().enumerate() {
                let (path, mut allocation) = NamespacePath::parse_in_profile(
                    name,
                    config.profile,
                    config.limits,
                    accounting,
                    cancellation,
                )?;
                let mut metadata = FileMetadata::default();
                if let Some(group) = groups.by_file.value.get(ordinal).copied().flatten() {
                    let (_, range) = groups.groups.value.get(group).ok_or(FsError::InvalidDiff)?;
                    let entries = groups
                        .ordered
                        .value
                        .get(range.clone())
                        .ok_or(FsError::InvalidDiff)?;
                    for key in entries {
                        let attribute = attributes.get(key.ordinal).ok_or(FsError::InvalidDiff)?;
                        metadata = super::attributes::change_attribute(
                            &volume,
                            metadata,
                            &attribute.name,
                            Some((attribute.value.as_ref(), NamedAttributeWriteMode::Create)),
                            accounting,
                            cancellation,
                        )
                        .await?;
                    }
                }
                let retained = path.retained_bytes();
                accounting.admit(moved)?;
                operations.allocation.absorb(&mut allocation)?;
                operations.bytes = operations
                    .bytes
                    .checked_add(retained)
                    .ok_or(WorkError::Overflow)?;
                operations.value.push(AuthoredMutation::CreateFile {
                    file_id: None,
                    path,
                    bytes: bytes.clone(),
                    metadata,
                });
                accounting.record_completed(moved)?;
            }
            checkout
                .apply_authored_transaction_with_accounting(operations, accounting, cancellation)
                .await?
                .retire(accounting)?;
            checkout
                .checkpoint_root_with_accounting(accounting, cancellation)
                .await
        }
        .await;
        let retired = groups.retire(accounting);
        let result = result?;
        retired?;
        Ok(result)
    }

    pub(super) async fn build_empty_generation(
        &self,
        volume_id: VolumeId,
        initialization: VolumeInitialization<'_>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<[ObjectId; 4], FsError> {
        let config = initialization.config;
        let metadata = match initialization.access_policy {
            Some(bytes) => {
                self.initial_policy_metadata(volume_id, config, bytes, accounting, cancellation)
                    .await?
            }
            None => FileMetadata::default(),
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_metadata(metadata, observe, allocation_error)
            })?;
        let metadata = self
            .put_encoded_with_accounting(ObjectKind::Metadata, encoded, accounting, cancellation)
            .await?;
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_tree_page(
                    &TreePage::Leaf(Vec::new()),
                    config.limits.maximum_directory_page_entries,
                    observe,
                    allocation_error,
                )
            })?;
        let tree = self
            .put_encoded_with_accounting(ObjectKind::TreePage, encoded, accounting, cancellation)
            .await?;
        accounting.charge(identity_hash_work(ROOT_FILE_DOMAIN))?;
        let mut identity = [0; 16];
        identity.copy_from_slice(&derived_digest(ROOT_FILE_DOMAIN, volume_id).as_bytes()[..16]);
        let root_file_id = FileId::from_bytes(identity);
        let record = FileRecord {
            bindings: None,
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory { entries: tree },
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_table_leaf_records(
                    std::slice::from_ref(&record),
                    config.limits.maximum_directory_page_entries,
                    None,
                    observe,
                    allocation_error,
                )
            })?;
        let file_table = self
            .put_encoded_with_accounting(
                ObjectKind::FileTablePage,
                encoded,
                accounting,
                cancellation,
            )
            .await?;
        let root = GenerationRoot {
            volume_id,
            root_file_id,
            file_table,
            parents: Vec::new(),
            required_features: 0,
        };
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_generation_root(&root, observe, allocation_error)
            })?;
        let generation_root = self
            .put_encoded_with_accounting(
                ObjectKind::GenerationRoot,
                encoded,
                accounting,
                cancellation,
            )
            .await?;
        Ok([metadata, tree, file_table, generation_root])
    }
}

struct CreationAttributeKey<'a> {
    ordinal: usize,
    path: &'a str,
    name: &'a AttributeName,
}

struct CreationAttributeGroups<'a> {
    ordered: crate::kernel::Owned<Vec<CreationAttributeKey<'a>>>,
    groups: crate::kernel::Owned<Vec<(&'a str, std::ops::Range<usize>)>>,
    by_file: crate::kernel::Owned<Vec<Option<usize>>>,
}

impl CreationAttributeGroups<'_> {
    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let Self {
            ordered,
            groups,
            by_file,
        } = self;
        ordered.retire(accounting)?;
        groups.retire(accounting)?;
        by_file.retire(accounting)
    }
}

/// Groups a creation's attributes by their exact file path once, rejecting
/// attributes on unknown paths and repeated names exactly as fresh creation
/// does. The grouping is the only attribute search creation and recovery use.
fn group_creation_attributes<'a>(
    files: &[(&str, acyclic_stream::OwnedBytes)],
    attributes: &'a [CreationAttribute],
    accounting: &mut KernelAccounting<'_>,
) -> Result<CreationAttributeGroups<'a>, FsError> {
    let mut ordered = crate::kernel::Owned::<Vec<CreationAttributeKey<'a>>>::allocate(
        attributes.len(),
        accounting,
    )?;
    for (ordinal, attribute) in attributes.iter().enumerate() {
        accounting.charge(WorkCounters {
            bytes_copied: std::mem::size_of::<CreationAttributeKey<'_>>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        ordered.value.push(CreationAttributeKey {
            ordinal,
            path: attribute.path.as_str(),
            name: &attribute.name,
        });
    }
    crate::kernel::sort_by_with_accounting(&mut ordered.value, accounting, |left, right| {
        left.path
            .cmp(right.path)
            .then_with(|| left.name.cmp(right.name))
    })?;
    let mut groups = crate::kernel::Owned::<Vec<(&'a str, std::ops::Range<usize>)>>::allocate(
        attributes.len(),
        accounting,
    )?;
    let mut start = 0;
    while let Some(first) = ordered.value.get(start) {
        let path = first.path;
        let mut end = start + 1;
        while let Some(current) = ordered.value.get(end) {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if current.path != path {
                break;
            }
            let previous = ordered.value.get(end - 1).ok_or(FsError::InvalidDiff)?;
            if current.name == previous.name {
                return Err(FsError::CreationRejected);
            }
            end += 1;
        }
        accounting.charge(WorkCounters {
            bytes_copied: std::mem::size_of::<(&str, std::ops::Range<usize>)>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        groups.value.push((path, start..end));
        start = end;
    }
    let mut by_file =
        crate::kernel::Owned::<Vec<Option<usize>>>::allocate(files.len(), accounting)?;
    let mut matched = crate::kernel::Owned::<Vec<bool>>::allocate(groups.value.len(), accounting)?;
    for _ in 0..groups.value.len() {
        accounting.charge(WorkCounters {
            bytes_copied: std::mem::size_of::<bool>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        matched.value.push(false);
    }
    for (name, _) in files {
        let found = match crate::kernel::binary_search_with_accounting(
            &groups.value,
            accounting,
            |group| group.0.cmp(*name),
        )? {
            Ok(index) => Some(index),
            Err(_) => None,
        };
        if let Some(index) = found {
            let slot = matched.value.get_mut(index).ok_or(FsError::InvalidDiff)?;
            *slot = true;
        }
        accounting.charge(WorkCounters {
            bytes_copied: std::mem::size_of::<Option<usize>>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        by_file.value.push(found);
    }
    let mut all_matched = true;
    for matched in &matched.value {
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        all_matched &= *matched;
    }
    matched.retire(accounting)?;
    if !all_matched {
        return Err(FsError::CreationRejected);
    }
    Ok(CreationAttributeGroups {
        ordered,
        groups,
        by_file,
    })
}

pub(super) fn derived_digest(domain: &[u8], volume_id: VolumeId) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&volume_id.into_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) fn identity_hash_work(domain: &[u8]) -> WorkCounters {
    WorkCounters {
        bytes_hashed: u64::try_from(domain.len()).unwrap_or(u64::MAX) + 16,
        bytes_copied: 16,
        ..WorkCounters::default()
    }
}

pub(super) fn creation_fingerprint(payload: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CREATION_FINGERPRINT_DOMAIN);
    hasher.update(payload);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) fn creation_commit(
    operation_id: OperationId,
    encoded: (Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation),
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ProposedCommit, FsError> {
    accounting.check()?;
    let hashing = WorkCounters {
        bytes_hashed: (CREATION_FINGERPRINT_DOMAIN.len() as u64)
            .checked_add(encoded.0.len() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::default()
    };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&encoded.0);
    accounting.record_completed(hashing)?;
    let bytes = encoded.0.capacity() as u64;
    let payload = crate::kernel::allocation::retained::Retained::from_owned(
        crate::kernel::Owned::new(encoded.0, encoded.1, bytes),
        accounting,
    )?
    .into_bytes((), accounting)?;
    Ok(ProposedCommit {
        publication: None,
        operation_id,
        fingerprint,
        payload,
    })
}

pub(super) fn merge_creation_backend_work(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    spent: WorkCounters,
) -> Result<(), FsError> {
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    *work = work.with_backend(spent, allocations.live_bytes())?;
    work.verify(budget)?;
    Ok(())
}

/// Once every requested file is a distinct child of the root, its complete
/// paged entry count proves that the generation contains nothing else.
async fn initial_root_has_file_count<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    count: usize,
    limits: crate::VolumeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let root = NamespacePath::new(Vec::new(), limits)?;
    let mut listed = 0_usize;
    let mut after: Option<LogicalName> = None;
    loop {
        let bound = after
            .as_ref()
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let (mut page, _allocation) = checkout
            .list_directory_with_accounting(
                &root,
                bound,
                limits.maximum_directory_page_entries,
                accounting,
                cancellation,
            )
            .await?;
        listed = listed
            .checked_add(page.entries.len())
            .ok_or(WorkError::Overflow)?;
        if listed > count {
            return Ok(false);
        }
        if !page.has_more {
            return Ok(listed == count);
        }
        let Some(last) = page.entries.pop() else {
            return Ok(false);
        };
        after = Some(last.name);
    }
}

/// Use the kernel's metered sort rather than scanning every preceding path.
/// Ordered retries use linear adjacency scans; other request orders use the
/// same bounded sort as batched namespace operations.
fn initial_paths_are_distinct(
    paths: &[NamespacePath],
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    let mut ordered = crate::kernel::Owned::allocate(paths.len(), accounting)?;
    let distinct = (|| -> Result<bool, FsError> {
        for (ordinal, key) in paths.iter().enumerate() {
            accounting.charge(WorkCounters {
                bytes_copied: std::mem::size_of::<crate::kernel::IndexedKey<'_, NamespacePath>>()
                    as u64,
                ..WorkCounters::UNCHARGED
            })?;
            ordered
                .value
                .push(crate::kernel::IndexedKey { ordinal, key });
        }
        crate::kernel::sort_indexed(&mut ordered.value, accounting)?;
        for adjacent in ordered.value.windows(2) {
            let [previous, current] = adjacent else {
                return Err(FsError::InvalidDiff);
            };
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if previous.key == current.key {
                return Ok(false);
            }
        }
        Ok(true)
    })();
    let retired = ordered.retire(accounting);
    let distinct = distinct?;
    retired?;
    Ok(distinct)
}
