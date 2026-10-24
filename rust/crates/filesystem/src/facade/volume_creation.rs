use super::*;

pub(super) const ROOT_FILE_DOMAIN: &[u8] = b"acyclic-fs-root-file-v1\0";
pub(super) const CREATION_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-create-operation-v1\0";
pub(super) const CREATION_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-create-fingerprint-v1\0";

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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Creates a fresh volume and its authenticated empty generation.
    pub async fn create_volume(
        &self,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        self.create_volume_with_id(VolumeId::new(), config, budget, cancellation)
            .await
    }
    /// Idempotently creates one caller-selected volume identity.
    pub async fn create_volume_with_id(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .create_volume_operation(
                volume_id,
                VolumeInitialization {
                    config,
                    access_policy: None,
                },
                None,
                &[],
                &mut accounting,
                cancellation,
                None::<&mut ()>,
            )
            .await
            .map(|(volume, _)| volume);
        measured(result, &accounting)
    }
    /// Opens one volume using its authenticated immutable configuration.
    pub async fn open_volume(
        &self,
        volume_id: VolumeId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let observed = self
            .read_creation_record(volume_id, budget, cancellation)
            .await?;
        let config = observed.value.0.config;
        validate_volume_capabilities(config, self.inner.capabilities)
            .map_err(|error| OperationFailure::new(error, observed.work))?;
        Ok(OperationReceipt {
            value: Volume {
                fs: self.clone(),
                id: volume_id,
                config,
            },
            work: observed.work,
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
    /// `files`: every volume and workspace is created here, an empty one being
    /// the case without files.
    pub(super) async fn create_volume_operation<E, G>(
        &self,
        volume_id: VolumeId,
        initialization: VolumeInitialization<'_>,
        operation_id: Option<OperationId>,
        files: &[(&str, acyclic_stream::OwnedBytes)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> Result<(Volume<A, O>, ObjectId), E>
    where
        E: From<FsError> + Send,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
    {
        accounting.check()?;
        let config = initialization.config;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let generation_root = self
            .initial_generation_root(
                volume_id,
                initialization,
                operation_id,
                files,
                accounting,
                cancellation,
            )
            .await?;
        let proof = self
            .publication_proof(
                generation_root,
                proven_at,
                volume_id,
                config,
                accounting,
                cancellation,
            )
            .await?;
        let result = self
            .publish_creation(
                VolumeCreated {
                    volume_id,
                    config,
                    initial_generation_root: generation_root,
                },
                operation_id,
                &proof.value.objects,
                proven_at,
                accounting,
                cancellation,
                guard,
            )
            .await;
        proof.retire(accounting)?;
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
        E: From<FsError> + Send,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
    {
        let VolumeCreated {
            volume_id, config, ..
        } = creation;
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
        let capacity = encoded.0.capacity() as u64;
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
                crate::PublicationScope::Closure {
                    objects: closure,
                    proven_at,
                },
                accounting,
                cancellation,
            )
            .await?;
        accounting.check()?;
        let mut authority = authority_work(accounting, cancellation)?;
        let changes = commit.publication.clone();
        let result = self
            .inner
            .authority
            .create_first_record(
                volume_authority_id(volume_id),
                commit,
                &mut authority,
                guard,
            )
            .await;
        let recorded = record_authority_work(accounting, &authority);
        if let Some(changes) = changes {
            changes.retire(accounting)?;
        }
        recorded?;
        if readonly_standalone_live(accounting).is_some() {
            accounting.release_owned(&mut KernelOwnedAllocation::Standalone, capacity)?;
        }
        drop(hold);
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
    pub(crate) async fn read_creation_record<W: FsWork>(
        &self,
        volume_id: VolumeId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(VolumeCreated, OperationId), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_creation(volume_id, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn read_creation(
        &self,
        volume_id: VolumeId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(VolumeCreated, OperationId), FsError> {
        accounting.check()?;
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .replay(
                volume_authority_id(volume_id),
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                },
                &mut work,
            )
            .await;
        record_authority_work(accounting, &work)?;
        let (records, _allocation) = result.map_err(|error| match error {
            #[cfg(not(target_arch = "wasm32"))]
            AuthorityStoreError::NativeStream(acyclic_stream::StreamError::NotFound) => {
                AuthorityStoreError::Missing
            }
            error => error,
        })?;
        decode_first_creation_record(volume_id, &records, accounting)
    }

    /// The initial generation a creation publishes: the empty generation when
    /// `files` is empty, else that generation's one checkpoint creating each
    /// file. It is a function of the volume, its configuration, the creation
    /// operation and `files` alone, so a retried creation rebuilds exactly the
    /// generation the original published.
    pub(super) async fn initial_generation_root(
        &self,
        volume_id: VolumeId,
        initialization: VolumeInitialization<'_>,
        operation_id: Option<OperationId>,
        files: &[(&str, acyclic_stream::OwnedBytes)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let config = initialization.config;
        let [.., empty] = self
            .build_empty_generation(volume_id, initialization, accounting, cancellation)
            .await?;
        if files.is_empty() {
            return Ok(empty);
        }
        // Populated files are authored by the creation itself; only a named
        // creation has an operation that a retry can repeat.
        let operation_id = operation_id.ok_or(FsError::CreationRejected)?;
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
                    operation_id,
                    files,
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
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
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
        for (path, bytes) in files {
            let (path, mut allocation) = NamespacePath::parse_in_profile(
                path,
                config.profile,
                config.limits,
                accounting,
                cancellation,
            )?;
            paths.allocation.absorb(&mut allocation)?;
            accounting.charge(WorkCounters {
                items_examined: paths.value.len() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if paths.value.contains(&path) {
                return Ok(false);
            }
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
            let step = config.limits.maximum_read_bytes.max(1);
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
                if read.bytes.as_ref() != &expected[start..end] {
                    return Ok(false);
                }
                offset += length;
            }
            paths.value.push(path);
        }
        // Each requested file is a distinct child of the root, so the root
        // holds nothing else exactly when it lists no more entries than
        // there are files. It is paged within the volume's page limit.
        let root = NamespacePath::new(Vec::new(), config.limits)?;
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
                    config.limits.maximum_directory_page_entries,
                    accounting,
                    cancellation,
                )
                .await?;
            listed = listed
                .checked_add(page.entries.len())
                .ok_or(WorkError::Overflow)?;
            if listed > files.len() {
                return Ok(false);
            }
            if !page.has_more {
                return Ok(listed == files.len());
            }
            let Some(last) = page.entries.pop() else {
                return Ok(false);
            };
            after = Some(last.name);
        }
    }

    async fn populate_initial_generation(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        empty: ObjectId,
        operation_id: OperationId,
        files: &[(&str, acyclic_stream::OwnedBytes)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
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
        for (path, bytes) in files {
            let (path, mut allocation) = NamespacePath::parse_in_profile(
                path,
                config.profile,
                config.limits,
                accounting,
                cancellation,
            )?;
            let retained = path.retained_bytes();
            accounting.admit(moved)?;
            operations.allocation.absorb(&mut allocation)?;
            operations.bytes = operations
                .bytes
                .checked_add(retained)
                .ok_or(WorkError::Overflow)?;
            operations.value.push(AuthoredMutation::CreateFile {
                path,
                bytes: bytes.clone(),
                metadata: FileMetadata::default(),
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
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = &encoded.1
    {
        if allocation.bytes() != encoded.0.capacity() as u64
            || !accounting
                .native_work()
                .is_some_and(|original| allocation.work().shares_ledger(original))
        {
            return Err(FsError::HandleAllocationFailed);
        }
        let control_bytes = crate::kernel::allocation::bytes_owner_control_bytes::<
            crate::kernel::Owned<Vec<u8>>,
        >()?;
        let mut control = allocation.work().claim(
            control_bytes,
            u64::from((control_bytes) != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        let bytes = encoded.0.capacity() as u64;
        let mut payload = crate::kernel::Owned::new(encoded.0, encoded.1, bytes);
        let KernelOwnedAllocation::NativeOriginal(allocation) = &mut payload.allocation else {
            unreachable!("native encoded allocation was matched before moving");
        };
        allocation.absorb(&mut control)?;
        payload.bytes = allocation.bytes();
        let payload = Bytes::from_owner(payload);
        return Ok(ProposedCommit {
            publication: None,
            operation_id,
            fingerprint,
            payload,
        });
    }
    let owner = WorkCounters {
        allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),
        ..WorkCounters::default()
    };
    accounting.admit(owner)?;
    let payload = Bytes::from(encoded.0);
    accounting.record_completed(owner)?;
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
