use super::*;

pub(super) const STAGED_METADATA_CACHE_ENTRIES: usize = 16;

pub(super) struct AuthoredMetadataCache {
    pub(super) entries: [Option<(FileMetadata, ObjectId)>; STAGED_METADATA_CACHE_ENTRIES],
    pub(super) next: usize,
}

impl AuthoredMetadataCache {

    pub(super) fn new() -> Self {
        Self {
            entries: [None; STAGED_METADATA_CACHE_ENTRIES],
            next: 0,
        }
    }


    pub(super) fn get(&self, metadata: FileMetadata, accounting: &mut KernelAccounting<'_>) -> Result<Option<ObjectId>, FsError> {
        for entry in &self.entries {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if let Some((cached, object)) = entry { if *cached == metadata { return Ok(Some(*object)); } }
        }
        Ok(None)
    }


    pub(super) fn remember(&mut self, metadata: FileMetadata, object: ObjectId) {
        if let Some(slot) = self.entries.get_mut(self.next) {
            *slot = Some((metadata, object));
        }
        self.next = (self.next + 1) % STAGED_METADATA_CACHE_ENTRIES;
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {


    pub(crate) async fn workspace_conflict_metadata(
        &self,
        workspace: &Volume<A, O>,
        record: FileRecord,
    ) -> Result<Vec<u8>, WorkspaceError> {
        let read = self
            .inner
            .objects
            .read(
                record.metadata,
                workspace.config.limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?;
        crate::kernel::decode_file_metadata(&read.value.bytes, DecodeLimits::for_volume(workspace.config), &mut |_| Ok::<_, FsError>(()))
        .map_err(WorkspaceError::engine)?;
        Ok(read.value.bytes.to_vec())
    }


    pub(crate) async fn stage_merge_metadata(
        &self,
        workspace: &Volume<A, O>,
        bytes: &[u8],
    ) -> Result<ObjectId, WorkspaceError> {
        let metadata = crate::kernel::decode_file_metadata(bytes, DecodeLimits::for_volume(workspace.config), &mut |_| Ok::<_, FsError>(())).map_err(WorkspaceError::engine)?;
        let cancellation = CancellationToken::new();
        let encoded = crate::kernel::encode::<FsError>(WorkBudget::UNBOUNDED, Some(&cancellation), 0, |observe| crate::kernel::encode_file_metadata(metadata, observe, FsError::EncodedAllocationFailed)).map_err(|failure| WorkspaceError::engine(failure.error))?;
        let (object, _) = self
            .put_encoded(ObjectKind::Metadata, encoded, WorkCounters::default(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(object)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {


    /// Reads canonical Copy metadata without following the terminal binding.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_metadata_with_native_work(
        &self, path: &NamespacePath, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let record = self.lookup_record_with_native_work(path, work, cancellation).await?.ok_or(FsError::NotFound)?;
        self.read_record_metadata(record, work, cancellation).await
    }


    /// Reads metadata for an already authenticated record.
    pub async fn read_record_metadata<W: FsWork>(
        &self, record: FileRecord, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
                return read_record_metadata_to_file_root_with_accounting(
                    &self.volume, record, &mut accounting, cancellation,
                ).await;
            }
            let (_, _, budget, _) = accounting.standalone_parts().expect("standalone metadata read");
            let metadata = self.volume.fs.inner.objects.read(
                record.metadata, self.volume.config.limits.maximum_object_bytes, budget, cancellation,
            ).await;
            let completed = match &metadata { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            *accounting.standalone_parts().expect("standalone metadata read").1 = completed;
            let metadata = metadata.map_err(|failure| FsError::Object(failure.error))?;
            crate::kernel::decode_file_metadata(&metadata.value, DecodeLimits::for_volume(self.volume.config), &mut |_| Ok::<_, FsError>(())).map_err(Into::into)
        }.await;
        budget.finish(result, &accounting)
    }


    /// Reads complete canonical metadata for one exact path.
    pub async fn read_metadata(
        &self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let resolved = self.resolve_file(path, budget, cancellation).await?;
        let mut work = resolved.work;
        let metadata = self
            .read_record_metadata(resolved.value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, metadata.work)?;
        Ok(FsReceipt {
            value: metadata.value,
            work,
        })
    }
}

pub(super) async fn read_record_metadata_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    record: FileRecord,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileMetadata, FsError> {
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (read, _retained_bytes) = crate::kernel::read_object_with_accounting(
            &volume.fs.inner.objects, record.metadata, volume.config.limits.maximum_object_bytes,
            accounting, cancellation,
        ).await.map_err(|error| match error {
            FsError::BlobRead(BlobReadError::Storage(error)) => match error {
                #[cfg(not(target_arch = "wasm32"))]
                crate::ObjectStoreError::Native(error) => FsError::NativeKernel(error),
                error => FsError::Object(error),
            },
            error => error,
        })?;
        crate::kernel::decode_owned(accounting, |observe| crate::kernel::decode_file_metadata(&read.bytes, DecodeLimits::for_volume(volume.config), &mut |step| observe(step))).map(|(metadata, _)| metadata)
    }.await;
    readonly_cleanup_standalone(accounting, before, 0)?;
    result
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


    pub(super) async fn stage_metadata_with_accounting(
        &self,
        metadata: FileMetadata,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(metadata, observe, allocation_error))?;
        self.volume.fs.put_encoded_with_accounting(
            ObjectKind::Metadata, encoded, accounting, cancellation,
        ).await
    }


    pub(super) async fn stage_authored_metadata_with_accounting(
        &self,
        metadata: FileMetadata,
        staged: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        accounting.check()?;
        if let Some(value) = staged.get(metadata, accounting)? {
            return Ok(value);
        }
        let value = self.stage_metadata_with_accounting(metadata, accounting, cancellation).await?;
        staged.remember(metadata, value);
        Ok(value)
    }


    /// Reads complete canonical metadata by stable file identity.
    pub async fn read_metadata_by_id<W: FsWork>(
        &mut self, file_id: FileId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_metadata_by_id_with_accounting(file_id, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }


    pub(crate) async fn read_metadata_by_id_with_accounting(
        &mut self, file_id: FileId,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let record = self.lookup_file_record_by_id_with_accounting(file_id, accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume, record, accounting, cancellation,
            ).await?;
            if self.tracks_observations() {
                self.observe_base_metadata_ids_with_accounting(
                    std::slice::from_ref(&file_id), accounting, cancellation,
                ).await?;
            }
            Ok(metadata)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }


    /// Replaces complete metadata and optionally logical size as one attached
    pub async fn set_attributes_by_id<W: FsWork>(
        &mut self, file_id: FileId, metadata: FileMetadata, logical_bytes: Option<u64>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            self.set_attributes_by_id_with_accounting(
            file_id, metadata, logical_bytes, &mut accounting, cancellation,
            ).await
        }.await;
        budget.finish(result, &accounting)
    }


    pub(super) async fn set_attributes_by_id_with_accounting(
        &mut self, file_id: FileId, metadata: FileMetadata, logical_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let metadata = self.stage_metadata_with_accounting(metadata, accounting, cancellation).await?;
        let count = 1 + usize::from(logical_bytes.is_some());
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(count, accounting)?;
        let result = (|| -> Result<(), FsError> {
            accounting.admit(WorkCounters { bytes_copied: (count * size_of::<Mutation>()) as u64, ..WorkCounters::UNCHARGED })?;
            operations.value.push(Mutation::File { file_id, mutation: FileMutation::SetMetadata { metadata } });
            if let Some(logical_bytes) = logical_bytes {
                operations.value.push(Mutation::File { file_id, mutation: FileMutation::Resize { logical_bytes } });
            }
            accounting.record_completed(WorkCounters { bytes_copied: (count * size_of::<Mutation>()) as u64, ..WorkCounters::UNCHARGED })
        })();
        if let Err(error) = result { operations.retire(accounting)?; return Err(error); }
        self.mutate_owned_with_accounting(operations, accounting, cancellation).await
    }


    /// Replaces complete metadata and optionally logical size as one path transaction.
    pub async fn set_attributes(
        &mut self,
        path: NamespacePath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                &path, &mut accounting, cancellation, true
            ).await?;
            drop(allocation);
            let file_id = lookup.record.ok_or(FsError::NotFound)?.file_id;
            self.set_attributes_by_id_with_accounting(
                file_id, metadata, logical_bytes, &mut accounting, cancellation,
            ).await
        }.await;
        measured(result, &accounting)
    }


    /// Reads complete canonical metadata for one exact path.
    pub async fn read_metadata<W: FsWork>(
        &mut self, path: &NamespacePath, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_metadata_with_accounting(path, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }


    pub(crate) async fn read_metadata_with_accounting(
        &mut self, path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        self.lookup_no_follow_with_metadata_with_accounting(path, accounting, cancellation).await?
            .map(|lookup| lookup.metadata).ok_or(FsError::NotFound)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {


    /// Replaces complete metadata and optionally logical size as one detached
    pub async fn set_attributes<W: FsWork>(
        &mut self, metadata: FileMetadata, logical_bytes: Option<u64>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.set_attributes_with_accounting(metadata, logical_bytes, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }


    pub(super) async fn set_attributes_with_accounting(
        &mut self, metadata: FileMetadata, logical_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(metadata, observe, allocation_error))?;
        let metadata_id = self.volume.fs.put_encoded_with_accounting(
            ObjectKind::Metadata, encoded, accounting, cancellation,
        ).await?;
        let payload = if let Some(logical_bytes) = logical_bytes {
            crate::kernel::apply_regular_mutation_with_accounting(
                &self.volume.fs.inner.objects, self.record.payload,
                RegularMutation::Resize { logical_bytes }, self.volume.config, accounting, cancellation,
            ).await?.payload
        } else { self.record.payload };
        self.record.metadata = metadata_id;
        self.record.payload = payload;
        Ok(())
    }


    /// Reads complete authenticated metadata for this detached file.
    pub async fn read_metadata<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = read_record_metadata_to_file_root_with_accounting(
            &self.volume, self.record, &mut accounting, cancellation,
        ).await;
        budget.finish(result, &accounting)
    }
}

