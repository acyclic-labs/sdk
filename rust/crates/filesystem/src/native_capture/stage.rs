use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostSnapshot,
    maximum_extent_spans: u32,
    meter: &NativeMaterializationWork,
    staged_file_bytes: &mut u64,
) -> Result<Vec<(HostDataRange, StagedContent)>, CaptureError> {
    meter.cancellation().check()?;
    #[cfg(unix)]
    let file = source_root
        .open_file(host_path, &mut meter.clone())
        .map_err(capture_demand_error)?;
    #[cfg(windows)]
    let file = source_root
        .open_file(
            host_path,
            windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
            &mut meter.clone(),
        )
        .map_err(capture_demand_error)?;
    let file = file.into_std();
    let metadata = source_root
        .stat_file(&file, &mut meter.clone())
        .map_err(capture_demand_error)?;
    if !metadata.file_type().is_file() {
        return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale));
    }
    ensure_same_host_node(snapshot, &metadata)?;
    let ranges = allocated_data_ranges(
        &file,
        0,
        metadata.len(),
        maximum_extent_spans,
        &mut meter.clone(),
    )
    .map_err(capture_demand_error)?;
    let positional = native_positional_file(file, meter)?;
    let native_bytes = std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64;
    capture_allocate(meter, native_bytes)?;
    let native = std::sync::Arc::new(positional);
    #[cfg(windows)]
    let ranges =
        settled_sparse_ranges(&native, &metadata, ranges, maximum_extent_spans, meter).await?;
    let mut staged = capture_vec(ranges.len(), meter)?;
    for range in &ranges {
        let mut bounded = acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(&native),
            range.offset,
            range.length,
        );
        let mut accounting = crate::kernel::KernelAccounting::native(meter, meter.cancellation());
        let content = stager
            .stage_content_with_accounting(
                &mut bounded,
                range.length,
                &mut accounting,
                meter.cancellation(),
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
    meter.cancellation().check()?;
    let after = source_root
        .stat_file(native.as_file(), &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(snapshot, &after)?;
    let current = source_root
        .stat(host_path, &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(snapshot, &current)?;
    meter.cancellation().check()?;
    meter.clone().check().map_err(capture_demand_error)?;
    let bytes = vector_bytes(&ranges)
        .checked_add(native_bytes)
        .ok_or(WorkError::Overflow)?;
    drop(ranges);
    drop(native);
    meter.clone().release(bytes).map_err(capture_demand_error)?;
    Ok(staged)
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

impl crate::kernel::AsyncBlobSource for acyclic_native_runtime::AsyncRangeReader {
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
        let reader = self;
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
