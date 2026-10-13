use super::*;

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostSnapshot,
    maximum_extent_spans: u32,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
    staged_file_bytes: &mut u64,
) -> Result<Vec<(HostDataRange, StagedContent)>, CaptureError> {
    let opened = (|| {
        cancellation.check()?;
        #[cfg(unix)]
        let file = source_root
            .open_file_positional(host_path, &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        #[cfg(windows)]
        let file = source_root
            .open_file_wide(
                host_path,
                windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
                &mut meter.native.clone(),
            )
            .map_err(capture_demand_error)?;
        let file = file.into_std();
        let metadata = source_root
            .stat_file(&file, &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        if !metadata.file_type().is_file() {
            return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale));
        }
        ensure_same_host_node(snapshot, &metadata)?;
        let logical_bytes = metadata.len();
        let ranges = allocated_data_ranges(
            &file,
            0,
            logical_bytes,
            maximum_extent_spans,
            &mut meter.native.clone(),
        )
        .map_err(capture_demand_error)?;
        let positional = native_positional_file(file, &meter.native)?;
        capture_allocate(
            meter,
            std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64,
        )?;
        let native = std::sync::Arc::new(positional);
        Ok((metadata, ranges, native))
    })();

    let (metadata, ranges, native) = opened?;
    #[cfg(windows)]
    let ranges = {
        let ranges = settled_sparse_ranges(
            &native,
            &metadata,
            ranges,
            maximum_extent_spans,
            cancellation,
            meter,
        )
        .await?;

        ranges
    };
    #[cfg(unix)]
    let _ = metadata;
    let staged = stage_host_ranges(stager, &native, &ranges, meter, staged_file_bytes).await;
    let content = staged?;
    let verified = (|| {
        cancellation.check()?;
        let after = source_root
            .stat_file(native.as_file(), &mut meter.native.clone())
            .map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &after)?;
        let current = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &current)?;
        cancellation.check()?;
        meter.native.clone().check().map_err(capture_demand_error)
    })();
    let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
    let native_bytes = std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64;
    drop(ranges);
    drop(native);
    {
        meter
            .native
            .clone()
            .release(ranges_bytes)
            .map_err(capture_demand_error)?;
        meter
            .native
            .clone()
            .release(native_bytes)
            .map_err(capture_demand_error)
    }?;
    if let Err(error) = verified {
        let bytes = content.capacity() as u64
            * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        drop(content);
        meter
            .native
            .clone()
            .release(bytes)
            .map_err(capture_demand_error)?;
        return Err(error);
    }

    Ok(content)
}

pub(super) fn append_staged_regular_state(
    path: NamespacePath,
    logical_bytes: u64,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    ranges: Vec<(HostDataRange, StagedContent)>,
    mutations: &mut Vec<AuthoredMutation>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    {
        let ranges_bytes =
            ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        let count = ranges
            .len()
            .checked_add(2 + usize::from(logical_bytes != 0))
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        capture_reserve(mutations, count, meter)?;
        if exists_with_kind {
            mutations.push(AuthoredMutation::Resize {
                path: clone_capture_path(&path, meter)?,
                logical_bytes: 0,
            });
        } else {
            mutations.push(AuthoredMutation::CreateFile {
                path: clone_capture_path(&path, meter)?,
                bytes: acyclic_stream::OwnedBytes::new(),
                metadata: canonical_metadata,
            });
        }
        if logical_bytes != 0 {
            mutations.push(AuthoredMutation::Resize {
                path: clone_capture_path(&path, meter)?,
                logical_bytes,
            });
        }
        for (range, content) in ranges {
            mutations.push(AuthoredMutation::WriteFromContent {
                path: clone_capture_path(&path, meter)?,
                offset: range.offset,
                content,
            });
        }
        meter
            .native
            .clone()
            .release(ranges_bytes)
            .map_err(capture_demand_error)?;
        mutations.push(AuthoredMutation::SetMetadata {
            path,
            metadata: canonical_metadata,
        });
        Ok(())
    }
}

pub(super) async fn stage_host_ranges<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    ranges: &[HostDataRange],
    meter: &CaptureWorkScope,
    staged_file_bytes: &mut u64,
) -> Result<Vec<(HostDataRange, StagedContent)>, CaptureError> {
    let mut staged = capture_vec(ranges.len(), meter)?;
    for range in ranges {
        let mut bounded = NativeRangeSource {
            reader: acyclic_native_runtime::AsyncRangeReader::new(
                std::sync::Arc::clone(native),
                range.offset,
                range.length,
            ),
        };
        let stage_cancellation = meter.native.cancellation().clone();
        let mut accounting =
            crate::kernel::KernelAccounting::native(&meter.native, &stage_cancellation);
        let content = stager
            .stage_content_with_accounting(
                &mut bounded,
                range.length,
                &mut accounting,
                &stage_cancellation,
            )
            .await;
        drop(bounded);

        let content = content?;
        if content.logical_bytes() != range.length {
            return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale));
        }
        *staged_file_bytes = staged_file_bytes
            .checked_add(range.length)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        staged.push((*range, content));
    }
    Ok(staged)
}

pub(super) struct NativeRangeSource {
    pub(super) reader: acyclic_native_runtime::AsyncRangeReader,
}

#[allow(
    unsafe_code,
    reason = "the measured host opener transfers its fresh positional handle"
)]
pub(super) fn native_positional_file(
    file: std::fs::File,
    original: &crate::NativeMaterializationWork,
) -> Result<acyclic_native_runtime::NativeFile, CaptureError> {
    let meter = original.runtime_meter();
    #[cfg(windows)]
    let result = unsafe {
        // SAFETY: the fresh positional handle is overlapped and has no prior completion port.
        acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(
            file,
            Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)),
        )
    };
    #[cfg(not(windows))]
    let result = acyclic_native_runtime::NativeFile::from_file(
        file,
        Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)),
    );
    result.map_err(|(error, file)| {
        // This is an ordinary std file, not an explicit SDK close operation. Retire its physical handle before projecting the original failure.
        drop(file);
        capture_demand_error(
            original
                .state
                .take_native_io_failure()
                .unwrap_or_else(|| native_demand_error(error)),
        )
    })
}

impl crate::kernel::AsyncBlobSource for NativeRangeSource {
    async fn read<'a>(
        &'a mut self,
        _destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        cancellation
            .check()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Interrupted))?;
        // This internal producer requires the original native invocation, rather than constructing another counter/budget bank for a caller.
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }

    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, crate::FsError> {
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        let reader = &mut self.reader;
        let maximum = destination
            .len()
            .min(usize::try_from(reader.remaining()).unwrap_or(usize::MAX));
        if maximum == 0 {
            return Ok(0);
        }
        original.admit(WorkCounters {
            source_bytes_read: maximum as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let copy = original.reserve_work(WorkCounters {
            bytes_copied: maximum as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let runtime = original.runtime_meter();
        let result = reader.read(maximum, runtime, true).await.map_err(|error| {
            original
                .state
                .take_native_io_failure()
                .map(crate::materializer::native_materialization_work_error)
                .map(crate::FsError::NativeKernel)
                .unwrap_or_else(|| {
                    if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        crate::FsError::SourceSnapshotStale
                    } else {
                        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Io(
                            error,
                        ))
                    }
                })
        })?;
        if result.0.len() != maximum {
            return Err(crate::FsError::SourceSnapshotStale);
        }
        destination[..maximum].copy_from_slice(&result.0);
        copy.record_completed(WorkCounters {
            bytes_copied: maximum as u64,
            ..WorkCounters::UNCHARGED
        })?;
        // The packet retains its buffer until the copy and late checks finish.
        drop(result);
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(maximum)
    }
}
