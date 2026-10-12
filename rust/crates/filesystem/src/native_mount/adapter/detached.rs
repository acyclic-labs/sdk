struct DetachedMountState<A, O> {
    file: DetachedFile<A, O>,
    metadata: FileMetadata,
    mutation_epoch: u64,
}

impl<A, O> DetachedMountState<A, O> {
    /// Applies one content change with its time stamp, as every native
    /// write does, as one detached record transition, then records it: the
    /// change and its record happen together or not at all.
    async fn change_content(
        &mut self,
        change: ContentChange<std::convert::Infallible>,
        ledger: &ViewLedger,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut metadata = self.metadata;
        let stamped = metadata
            .stamp_content_change(content_change_time()?)
            .then_some(metadata);
        self.file
            .change_content(change, stamped, boundary_budget(), cancellation)
            .await
            .map_err(engine_error)?;
        self.metadata = metadata;
        self.record_change(ledger)
    }

    /// Advances the identity's own publication epoch and invalidates every
    /// cached lookup of the identity, under the lock its lookups take.
    fn record_change(&mut self, ledger: &ViewLedger) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.mutation_epoch = self
            .mutation_epoch
            .checked_add(1)
            .ok_or(MountSourceError::Stale)?;
        ledger.record(&ViewChange::Node(self.file.file_id()));
        Ok(())
    }
}

pub(super) struct CheckoutDetachedFile<A, O> {
    state: tokio::sync::Mutex<DetachedMountState<A, O>>,
    ledger: Arc<ViewLedger>,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
}

impl<A, O> CheckoutDetachedFile<A, O> {
    fn attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }

    pub(super) fn snapshot(&self) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            Ok((state.file.record(), state.metadata, state.mutation_epoch))
        })
    }

    /// Reports the detached identity's live state within a caller's callback.
    pub(super) async fn lookup_async(&self) -> Result<MountLookup, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let state = self.state.lock().await;
        Ok(MountLookup {
            node: MountNode {
                file_id: state.file.file_id(),
                kind: MountNodeKind::Regular,
                logical_bytes: state.file.logical_bytes(),
                link_count: 0,
                device: None,
            },
            metadata: state.metadata,
        })
    }
}

impl<A, O> CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Applies one content change to this detached file with its time
    /// stamp as one record transition, and records it.
    fn change_content(
        &self,
        change: ContentChange<std::convert::Infallible>,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .change_content(change, &self.ledger, &self.cancellation)
                .await
        })
    }
}

impl<A, O> MountOpenFile for CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.runtime.wait(|| self.lookup_async())
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .read_range(
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

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            let length = state
                .file
                .logical_bytes()
                .saturating_sub(offset)
                .min(u64::from(maximum_bytes));
            if length == 0 {
                return Ok(Bytes::new());
            }
            state
                .file
                .read_range(
                    ByteRange { offset, length },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .seek(
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

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Write { offset, bytes })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Resize { logical_bytes })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.change_content(range_allocation(ByteRange { offset, length }, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .set_attributes(
                    metadata,
                    logical_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            state.metadata = metadata;
            state.record_change(&self.ledger)
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .read_named_attribute(&name, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let cursor = cursor.map(|name| self.attribute_name(name)).transpose()?;
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            let receipt = state
                .file
                .list_named_attributes(
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
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let receipt = state
                .file
                .write_named_attribute(name, value, mode, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            state.metadata = receipt.value;
            state.record_change(&self.ledger)
        })
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let receipt = state
                .file
                .remove_named_attribute(name, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            state.metadata = receipt.value;
            state.record_change(&self.ledger)
        })
    }
}
