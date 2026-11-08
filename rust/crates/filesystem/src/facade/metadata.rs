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

    pub(super) fn get(
        &self,
        metadata: FileMetadata,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<ObjectId>, FsError> {
        for entry in &self.entries {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if let Some((cached, object)) = entry
                && *cached == metadata
            {
                return Ok(Some(*object));
            }
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
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, WorkspaceError> {
        let read = self
            .inner
            .objects
            .read(
                record.metadata,
                workspace.config.limits.maximum_object_bytes,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation),
            )
            .await
            .map_err(WorkspaceError::engine)?;
        crate::kernel::decode_file_metadata(
            &read.bytes,
            DecodeLimits::for_volume(workspace.config),
            &mut |_| Ok::<_, FsError>(()),
        )
        .map_err(WorkspaceError::engine)?;
        Ok(read.bytes.to_vec())
    }

    pub(crate) async fn stage_merge_metadata(
        &self,
        workspace: &Volume<A, O>,
        bytes: &[u8],
    ) -> Result<ObjectId, WorkspaceError> {
        let metadata = crate::kernel::decode_file_metadata(
            bytes,
            DecodeLimits::for_volume(workspace.config),
            &mut |_| Ok::<_, FsError>(()),
        )
        .map_err(WorkspaceError::engine)?;
        self.stage_merge_metadata_value(metadata).await
    }

    pub(crate) async fn stage_merge_metadata_value(
        &self,
        metadata: FileMetadata,
    ) -> Result<ObjectId, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let encoded = crate::kernel::codec::encode_owned(&mut accounting, |observe, error| {
            crate::kernel::encode_file_metadata(metadata, observe, error)
        })
        .map_err(WorkspaceError::engine)?;
        let object = self
            .put_encoded_with_accounting(
                ObjectKind::Metadata,
                encoded,
                &mut accounting,
                &cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(object)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PinnedReader<A, O> {
    /// Reads complete canonical metadata for one exact path.
    pub async fn read_metadata<W: FsWork>(
        &self,
        path: &NamespacePath,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let record = self
                .resolve_file(path, &mut accounting, cancellation)
                .await?;
            crate::kernel::read_file_metadata_with_accounting(
                self.volume.fs.objects(),
                record.metadata,
                DecodeLimits::for_volume(self.volume.config),
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        budget.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    pub(crate) async fn stage_metadata_with_accounting(
        &self,
        metadata: FileMetadata,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_metadata(metadata, observe, allocation_error)
            })?;
        self.volume
            .fs
            .put_encoded_with_accounting(ObjectKind::Metadata, encoded, accounting, cancellation)
            .await
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
        let value = self
            .stage_metadata_with_accounting(metadata, accounting, cancellation)
            .await?;
        staged.remember(metadata, value);
        Ok(value)
    }

    pub(super) async fn set_attributes_with_accounting<F: ContentFile>(
        &mut self,
        file: F,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let file_id = match file.address() {
            ContentAddress::File(id) => id,
            ContentAddress::Path(path) => {
                let lookup = {
                    let (lookup, _allocation) = self
                        .lookup_no_follow_with_accounting(path, accounting, cancellation, true)
                        .await?;
                    lookup
                };
                lookup.record.ok_or(FsError::NotFound)?.file_id
            }
        };
        let metadata = self
            .stage_metadata_with_accounting(metadata, accounting, cancellation)
            .await?;
        let count = 1 + usize::from(logical_bytes.is_some());
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(count, accounting)?;
        let result = (|| -> Result<(), FsError> {
            accounting.admit(WorkCounters {
                bytes_copied: (count * size_of::<Mutation>()) as u64,
                ..WorkCounters::UNCHARGED
            })?;
            operations.value.push(Mutation::File {
                file_id,
                mutation: FileMutation::SetMetadata { metadata },
            });
            if let Some(logical_bytes) = logical_bytes {
                operations.value.push(Mutation::File {
                    file_id,
                    mutation: FileMutation::Resize { logical_bytes },
                });
            }
            accounting.record_completed(WorkCounters {
                bytes_copied: (count * size_of::<Mutation>()) as u64,
                ..WorkCounters::UNCHARGED
            })
        })();
        if let Err(error) = result {
            operations.retire(accounting)?;
            return Err(error);
        }
        self.apply_mutations(operations, accounting, cancellation)
            .await
    }

    /// Replaces metadata and optional logical size atomically by path or file identity.
    pub async fn set_attributes<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            self.set_attributes_with_accounting(
                file,
                metadata,
                logical_bytes,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        budget.finish(result, &accounting)
    }

    /// Reads complete canonical metadata by path or stable file identity.
    pub async fn read_metadata<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_metadata_with_accounting(file, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_metadata_with_accounting<F: ContentFile>(
        &mut self,
        file: F,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let mut accounting = accounting.allocation_scope()?;
        match file.address() {
            ContentAddress::Path(path) => self
                .lookup_no_follow_with_metadata_with_accounting(path, &mut accounting, cancellation)
                .await?
                .map(|lookup| lookup.metadata)
                .ok_or(FsError::NotFound),
            ContentAddress::File(id) => {
                let record = self
                    .lookup_file_record_by_id_with_accounting(id, &mut accounting, cancellation)
                    .await?
                    .ok_or(FsError::NotFound)?;
                let metadata = crate::kernel::read_file_metadata_with_accounting(
                    self.volume.fs.objects(),
                    record.metadata,
                    DecodeLimits::for_volume(self.volume.config),
                    &mut accounting,
                    cancellation,
                )
                .await?;
                if self.tracks_observations() {
                    self.observe_base_metadata_ids_with_accounting(
                        std::slice::from_ref(&id),
                        &mut accounting,
                        cancellation,
                    )
                    .await?;
                }
                Ok(metadata)
            }
        }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> DetachedFile<A, O> {
    /// Replaces complete metadata and optionally logical size as one detached
    pub async fn set_attributes<W: FsWork>(
        &mut self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .set_attributes_with_accounting(metadata, logical_bytes, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn set_attributes_with_accounting(
        &mut self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_metadata(metadata, observe, allocation_error)
            })?;
        let metadata_id = self
            .volume
            .fs
            .put_encoded_with_accounting(ObjectKind::Metadata, encoded, accounting, cancellation)
            .await?;
        let payload = if let Some(logical_bytes) = logical_bytes {
            crate::kernel::apply_regular_mutation_with_accounting(
                &self.volume.fs.inner.objects,
                self.record.payload,
                RegularMutation::Resize { logical_bytes },
                self.volume.config,
                accounting,
                cancellation,
            )
            .await?
            .payload
        } else {
            self.record.payload
        };
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
        let result = crate::kernel::read_file_metadata_with_accounting(
            self.volume.fs.objects(),
            self.record.metadata,
            DecodeLimits::for_volume(self.volume.config),
            &mut accounting,
            cancellation,
        )
        .await;
        budget.finish(result, &accounting)
    }
}
