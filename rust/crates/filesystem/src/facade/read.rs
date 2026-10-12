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
    /// Looks up file records by stable identity, reading each distinct
    pub async fn file_records_by_id(
        &self,
        file_ids: &[FileId],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<FileRecord>>> {
        let lookup = lookup_file_records(
            &self.volume.fs.inner.objects,
            self.root.file_table,
            file_ids,
            self.volume.config.limits.maximum_paths_per_batch,
            DecodeLimits::for_volume(self.volume.config),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            OperationFailure::new(FsError::FileRecord(failure.error), *failure.work)
        })?;
        Ok(OperationReceipt {
            value: lookup.value,
            work: lookup.work,
        })
    }

    /// Looks up one authenticated stable identity on the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn file_record_by_id_with_native_work(
        &self,
        file_id: FileId,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (record, allocation) = lookup_file_record_async_with_accounting(
            &self.volume.fs.inner.objects,
            self.root.file_table,
            file_id,
            DecodeLimits::for_volume(self.volume.config),
            &mut accounting,
            cancellation,
            None,
        )
        .await?;
        drop(allocation);
        Ok(record)
    }
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

    /// Reads the exact symbolic-link packet, including producer-measured capacity.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_record_symbolic_link_owned_with_native_work(
        &self,
        record: FileRecord,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        read_symbolic_link_record_with_accounting(
            &self.volume,
            record,
            &mut accounting,
            cancellation,
        )
        .await
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
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.pinned.read_file_range",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty
        );
        let mut accounting = budget.accounting(cancellation);
        let result = crate::obs::in_span(&span, async {
            if range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            let record = self
                .resolve_file(path, &mut accounting, cancellation)
                .await?;
            self.read_range_record(record, range, &mut accounting, cancellation)
                .await
        })
        .await;
        let observed = result
            .map(|value| (value, accounting.spent()))
            .map_err(|error| (error, accounting.spent()));
        let result = crate::obs::measured_inline_on(&span, observed, |(_, work)| work)
            .map(|(value, _)| value)
            .map_err(|(error, _)| error);
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
        let (lookup, allocation) = self
            .lookup_no_follow_with_accounting(path, accounting, cancellation, true)
            .await?;
        drop(allocation);
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
            let (lookup, allocation) = self
                .lookup_no_follow_with_accounting(path, &mut accounting, cancellation, true)
                .await?;
            drop(allocation);
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
            let (record, allocation) = crate::kernel::lookup_file_record_async_with_accounting(
                &self.volume.fs.inner.objects,
                self.root.file_table,
                file_id,
                DecodeLimits::for_volume(self.volume.config),
                &mut accounting,
                cancellation,
                Some(&self.dependencies.data_custody),
            )
            .await?;
            drop(allocation);
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

    /// Reads one exact logical range by stable file identity.
    pub async fn read_file_range_by_id<W: FsWork>(
        &mut self,
        file_id: FileId,
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
                .lookup_file_record_by_id_with_accounting(file_id, &mut accounting, cancellation)
                .await?
                .ok_or(FsError::NotFound)?;
            self.read_record_range_with_accounting(
                record,
                range,
                range,
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

    /// Reads at most `maximum_bytes`, clipping to the current logical EOF.
    pub async fn read_file_up_to_by_id<W: FsWork>(
        &mut self,
        file_id: FileId,
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
                .lookup_file_record_by_id_with_accounting(file_id, &mut accounting, cancellation)
                .await?
                .ok_or(FsError::NotFound)?;
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
                        file_id,
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

    pub(super) async fn read_record_range_with_accounting(
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

    /// Reads one exact logical regular-file range without materializing holes
    pub async fn read_file_range<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .read_file_range_with_accounting(path, range, &mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_file_range_with_accounting(
        &mut self,
        path: &NamespacePath,
        range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let (lookup, allocation) = self
            .lookup_no_follow_with_accounting(path, accounting, cancellation, false)
            .await?;
        drop(allocation);
        let record = lookup.record.ok_or(FsError::NotFound)?;
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
