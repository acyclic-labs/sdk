use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ResolvedFile<A, O> {
    /// Reads one logical regular-file range without another namespace lookup.
    pub async fn read_range<W: FsWork>(
        &self,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        self.reader
            .read_record_range(self.record, range, budget, cancellation)
            .await
    }

    /// Reads opaque symbolic-link target bytes without another namespace lookup.
    pub async fn read_symbolic_link<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, FsError> {
        self.reader
            .read_symbolic_link_record(self.record, budget, cancellation)
            .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {
    /// Reads one authenticated record's range, preserving intrinsic byte custody.
    pub async fn read_record_range<W: FsWork>(
        &self,
        record: FileRecord,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_range_record(record, range, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn read_range_record(
        &self,
        record: FileRecord,
        range: ByteRange,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        let standalone = matches!(
            *accounting,
            KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. }
        );
        let before = readonly_standalone_live(accounting);
        let result = async {
            if range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            crate::kernel::read_file_range_with_accounting(
                &self.volume.fs.inner.objects,
                FileRangeRequest {
                    record,
                    range,
                    maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                    limits: DecodeLimits::for_volume(self.volume.config),
                    budget: accounting.remaining()?,
                },
                accounting,
                cancellation,
            )
            .await
        }
        .await;
        let retained = result.as_ref().map_or(0, |read| read.logical_owned_bytes);
        readonly_cleanup_standalone(accounting, before, retained)?;
        result.map_err(|error| {
            if standalone {
                FsError::FileRead(match error {
                    FsError::FileRead(error) => error,
                    FsError::BlobRead(error) => FileRangeReadError::Blob(error),
                    FsError::Work(error) => FileRangeReadError::Work(error),
                    FsError::Cancelled(_) => FileRangeReadError::Cancelled,
                    error => FileRangeReadError::Context(Box::new(error)),
                })
            } else {
                error
            }
        })
    }

    pub(super) async fn read_symbolic_link_record<W: FsWork>(
        &self,
        record: FileRecord,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            read_symbolic_link_record_with_accounting(
                &self.volume,
                record,
                &mut accounting,
                cancellation,
            )
            .await
            .map(|read| read.bytes)
        }
        .await;
        budget.finish(result, &accounting)
    }

    /// Reads one exact logical range without mutating checkout state.
    pub async fn read_file_range<W: FsWork>(
        &self,
        path: &NamespacePath,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            if range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            let record = self
                .resolve_file(path, &mut accounting, cancellation)
                .await?;
            self.read_range_record(record, range, &mut accounting, cancellation)
                .await
        }
        .await;
        budget.finish(result, &accounting)
    }

    /// Reads exact opaque symbolic-link target bytes without following it.
    pub async fn read_symbolic_link<W: FsWork>(
        &self,
        path: &NamespacePath,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let record = self
                .resolve_file(path, &mut accounting, cancellation)
                .await?;
            read_symbolic_link_record_with_accounting(
                &self.volume,
                record,
                &mut accounting,
                cancellation,
            )
            .await
            .map(|read| read.bytes)
        }
        .await;
        budget.finish(result, &accounting)
    }
}

pub(super) async fn read_symbolic_link_record_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    record: FileRecord,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::OwnedSourceChunk, FsError> {
    let FilePayload::SymbolicLink {
        target_bytes,
        target,
    } = record.payload
    else {
        return Err(FsError::NotSymbolicLink);
    };
    if target_bytes > volume.config.limits.maximum_read_bytes {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    accounting.check()?;
    if target_bytes == 0 {
        let bytes =
            crate::kernel::owned_read_bytes((Vec::new(), accounting.claim_owned(0, 0)?).into())?;
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, 0));
        }
        return Ok(crate::kernel::OwnedSourceChunk::claimed(bytes, 0));
    }
    let before = readonly_standalone_live(accounting);
    let result = crate::kernel::read_blob_range_with_accounting(
        &volume.fs.inner.objects,
        target,
        ByteRange {
            offset: 0,
            length: target_bytes,
        },
        DecodeLimits::for_volume(volume.config),
        accounting,
        cancellation,
        true,
    )
    .await;
    let retained = result.as_ref().map_or(0, |read| read.logical_owned_bytes());
    readonly_cleanup_standalone(accounting, before, retained)?;
    result.map(|read| {
        let owned = read.logical_owned_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, owned);
        }
        crate::kernel::OwnedSourceChunk::claimed(read.bytes, owned)
    })
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Reads the same symbolic-link target, preserving producer-owned capacity.
    pub async fn read_symbolic_link<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_symbolic_link_with_accounting(path, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_symbolic_link_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, FsError> {
        let lookup = {
            let (lookup, _allocation) = self
                .lookup_no_follow_with_accounting(path, accounting, cancellation, true)
                .await?;
            lookup
        };
        let record = lookup.record.ok_or(FsError::NotFound)?;
        read_symbolic_link_record_with_accounting(&self.volume, record, accounting, cancellation)
            .await
    }

    /// Reads exact opaque Windows reparse-point bytes without interpreting them.
    pub async fn read_reparse_point(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<acyclic_stream::OwnedBytes> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let lookup = {
                let (lookup, _allocation) = self
                    .lookup_no_follow_with_accounting(path, &mut accounting, cancellation, true)
                    .await?;
                lookup
            };
            let record = lookup.record.ok_or(FsError::NotFound)?;
            let FilePayload::ReparsePoint {
                payload_bytes,
                payload,
            } = record.payload
            else {
                return Err(FsError::NotReparsePoint);
            };
            if payload_bytes > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            if payload_bytes == 0 {
                return Ok(acyclic_stream::OwnedBytes::new());
            }
            crate::kernel::read_blob_range_with_accounting(
                &self.volume.fs.inner.objects,
                payload,
                ByteRange {
                    offset: 0,
                    length: payload_bytes,
                },
                DecodeLimits::for_volume(self.volume.config),
                &mut accounting,
                cancellation,
                true,
            )
            .await
            .map(|read| read.bytes)
        }
        .await;
        measured(result, &accounting)
    }

    /// Reads one authenticated candidate file record by stable identity.
    pub async fn read_file_record_by_id<W: FsWork>(
        &mut self,
        file_id: FileId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRecord, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let before = readonly_standalone_live(&accounting);
        let result = async {
            let record = {
                let (record, _allocation) =
                    crate::kernel::lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects,
                        self.root.file_table,
                        file_id,
                        DecodeLimits::for_volume(self.volume.config),
                        &mut accounting,
                        cancellation,
                        Some(&self.dependencies.data_custody),
                    )
                    .await?;
                record
            };
            let record = record.ok_or(FsError::NotFound)?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [Some(DependencyRegion::FileRecord(file_id)), None],
                    &mut accounting,
                    cancellation,
                )
                .await?;
            }
            Ok(record)
        }
        .await;
        let result = readonly_cleanup_standalone(&mut accounting, before, 0).and(result);
        budget.finish(result, &accounting)
    }

    /// Reads at most `maximum_bytes`, clipping to the current logical EOF.
    pub async fn read_file_up_to<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        offset: u64,
        maximum_bytes: u32,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let requested_range = ByteRange {
                offset,
                length: u64::from(maximum_bytes),
            };
            if requested_range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            let record = self
                .lookup_content_record(&file, &mut accounting, cancellation)
                .await?;
            if record.kind != FileKind::Regular {
                return Err(FsError::FileRead(FileRangeReadError::NotRegular));
            }
            let logical_bytes = match record.payload {
                FilePayload::InlineRegular(data) => data.as_bytes().len() as u64,
                FilePayload::Regular { logical_bytes, .. } => logical_bytes,
                _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
            };
            let length = requested_range
                .length
                .min(logical_bytes.saturating_sub(offset));
            if length == 0 {
                if self.tracks_observations() && requested_range.length != 0 {
                    self.observe_base_regular_range_with_accounting(
                        record.file_id,
                        requested_range,
                        &mut accounting,
                        cancellation,
                    )
                    .await?;
                }
                let bytes = crate::kernel::owned_read_bytes(
                    (Vec::new(), accounting.claim_owned(0, 0)?).into(),
                )?;
                return Ok(FileRangeRead {
                    bytes,
                    logical_owned_bytes: 0,
                    work: accounting.spent(),
                });
            }
            self.read_record_range_with_accounting(
                record,
                ByteRange { offset, length },
                requested_range,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        let result = result.map(|mut value| {
            value.work = accounting.spent();
            value
        });
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_record_range_with_accounting(
        &mut self,
        record: FileRecord,
        range: ByteRange,
        observed_range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let mut read = crate::kernel::read_file_range_with_accounting(
                &self.volume.fs.inner.objects,
                FileRangeRequest {
                    record,
                    range,
                    maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                    limits: DecodeLimits::for_volume(self.volume.config),
                    budget: accounting.remaining()?,
                },
                accounting,
                cancellation,
            )
            .await?;
            if self.tracks_observations() && observed_range.length != 0 {
                self.observe_base_regular_range_with_accounting(
                    record.file_id,
                    observed_range,
                    accounting,
                    cancellation,
                )
                .await?;
            }
            read.work = accounting.spent();
            Ok(read)
        }
        .await;
        let retained = result.as_ref().map_or(0, |read| read.logical_owned_bytes);
        readonly_cleanup_standalone(accounting, before, retained)?;
        result
    }

    /// Reads one exact logical regular-file range without materializing holes.
    pub async fn read_file_range<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_file_range_with_accounting(file, range, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_file_range_with_accounting<F: ContentFile>(
        &mut self,
        file: F,
        range: ByteRange,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let record = self
            .lookup_content_record(&file, accounting, cancellation)
            .await?;
        self.read_record_range_with_accounting(record, range, range, accounting, cancellation)
            .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {
    /// Reads the same detached target while retaining producer-owned capacity.
    pub(crate) async fn read_symbolic_link<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = read_symbolic_link_record_with_accounting(
            &self.volume,
            self.record,
            &mut accounting,
            cancellation,
        )
        .await;
        budget.finish(result, &accounting)
    }

    /// Reads one bounded sparse range without requiring a namespace binding.
    pub async fn read_range<W: FsWork>(
        &self,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_range_with_accounting(range, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn read_range_with_accounting(
        &self,
        range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        crate::kernel::read_file_range_with_accounting(
            &self.volume.fs.inner.objects,
            FileRangeRequest {
                record: self.record,
                range,
                maximum_spans: self.volume.config.limits.maximum_mutations_per_batch,
                limits: DecodeLimits::for_volume(self.volume.config),
                budget: accounting.remaining()?,
            },
            accounting,
            cancellation,
        )
        .await
    }

    /// Reads at most `maximum_bytes`, clipping to this detached record's EOF.
    pub async fn read_up_to(
        &self,
        offset: u64,
        maximum_bytes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRangeRead> {
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let accounting = &mut accounting;
            let maximum_bytes = u64::from(maximum_bytes);
            if maximum_bytes > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            accounting.check()?;
            if self.record.kind != FileKind::Regular {
                return Err(FsError::FileRead(FileRangeReadError::NotRegular));
            }
            let logical_bytes = match self.record.payload {
                FilePayload::InlineRegular(data) => {
                    u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
                }
                FilePayload::Regular { logical_bytes, .. } => logical_bytes,
                _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
            };
            let length = maximum_bytes.min(logical_bytes.saturating_sub(offset));
            if length == 0 {
                return Ok(FileRangeRead {
                    bytes: crate::kernel::owned_read_bytes(
                        (Vec::new(), accounting.claim_owned(0, 0)?).into(),
                    )?,
                    logical_owned_bytes: 0,
                    work: accounting.spent(),
                });
            }
            self.read_range_with_accounting(ByteRange { offset, length }, accounting, cancellation)
                .await
        }
        .await;
        measured(result, &accounting)
    }
}
