// Frozen regular-file staging and reconciliation from 2eb8a99a3aa05d8ad131c3fa6f0cb6aa1f298d87.
use super::*;

pub(super) async fn regular<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    batch: &mut CaptureBatch<'_>,
    checkout: &mut Checkout<A, O>,
    path: NamespacePath,
    host_path: CaptureHostPath,
    metadata: &HostStat,
    canonical_metadata: FileMetadata,
    existing: (Option<FileRecord>, bool),
) -> Result<bool, CaptureError> {
    // Staging opens the file once and proves it is still the observed node before reading a byte.
    let staged = stage_regular_body(
        checkout,
        batch.source_root,
        &host_path,
        metadata,
        batch.maximum_extent_spans,
        batch.meter,
        &mut batch.receipt.staged_file_bytes,
    )
    .await?;
    capture_drop_host_path(host_path, batch.meter)?;
    let logical_bytes = metadata.len();
    let ranges = staged;
    let ranges_bytes =
        ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
    let unchanged = if logical_bytes == 0
        && ranges.is_empty()
        && existing.1
        && existing.0.is_some_and(|record| record.link_count == 1)
        && existing.0.is_some_and(|record| match record.payload {
            crate::kernel::FilePayload::InlineRegular(data) => data.as_bytes().is_empty(),
            _ => false,
        }) {
        true
    } else if let (Some(record), true, [(range, content)]) =
        (existing.0, existing.1, ranges.as_slice())
        && matches!(record.payload, crate::kernel::FilePayload::Regular { logical_bytes: prior, .. } if prior == logical_bytes)
        && range.offset == 0
        && range.length == logical_bytes
        && content.logical_bytes() == logical_bytes
    {
        let cancellation = batch.meter.cancellation();
        let mut accounting = crate::kernel::KernelAccounting::native(batch.meter, cancellation);
        let planned = checkout
            .plan_record_extents(
                record,
                crate::ByteRange {
                    offset: 0,
                    length: logical_bytes,
                },
                batch.maximum_extent_spans,
                &mut accounting,
                cancellation,
            )
            .await;
        let planned = match planned {
            Ok(value) => Some(value),
            Err(crate::FsError::FileRead(crate::kernel::FileRangeReadError::Extent(
                crate::kernel::ExtentReadError::TooManySpans,
            ))) => None,
            Err(error) => return Err(error.into()),
        };
        let expected = crate::kernel::ExtentSlice {
            offset: 0,
            length: logical_bytes,
            source_end: logical_bytes,
            kind: crate::kernel::ExtentKind::Content {
                object: content.root(),
                object_offset: 0,
            },
        };
        let unchanged = planned.as_ref().is_some_and(|(plan, _)| {
            plan.as_ref()
                .is_some_and(|plan| plan.spans.as_slice() == std::slice::from_ref(&expected))
        });
        drop(planned);
        unchanged
    } else {
        false
    };
    #[cfg(test)]
    let unchanged = batch
        .baseline
        .and_then(|baseline| baseline.reuse_override)
        .unwrap_or(unchanged);
    if unchanged {
        drop(ranges);
        batch
            .meter
            .clone()
            .release(ranges_bytes)
            .map_err(capture_demand_error)?;
        capture_drop_path(path, batch.meter)?;
        return Ok(false);
    }
    let count = ranges
        .len()
        .checked_add(2 + usize::from(logical_bytes != 0))
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    capture_reserve(&mut batch.mutations, count, batch.meter)?;
    if existing.0.is_some() {
        batch.mutations.push(AuthoredMutation::Resize {
            path: clone_capture_path(&path, batch.meter)?,
            logical_bytes: 0,
        });
    } else {
        batch.mutations.push(AuthoredMutation::CreateFile {
            path: clone_capture_path(&path, batch.meter)?,
            bytes: acyclic_stream::OwnedBytes::new(),
            metadata: canonical_metadata,
        });
    }
    if logical_bytes != 0 {
        batch.mutations.push(AuthoredMutation::Resize {
            path: clone_capture_path(&path, batch.meter)?,
            logical_bytes,
        });
    }
    for (range, content) in ranges {
        batch.mutations.push(AuthoredMutation::WriteFromContent {
            path: clone_capture_path(&path, batch.meter)?,
            offset: range.offset,
            content,
        });
    }
    batch
        .meter
        .clone()
        .release(ranges_bytes)
        .map_err(capture_demand_error)?;
    batch.mutations.push(AuthoredMutation::SetMetadata {
        path,
        metadata: canonical_metadata,
    });
    Ok(true)
}

async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostStat,
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
        .stat_file_metadata(&file, &mut meter.clone())
        .map_err(capture_demand_error)?;
    if metadata.file_type() != crate::demand::SourceNodeKind::RegularFile {
        return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
    }
    ensure_same_host_node(snapshot, &metadata)?;
    #[cfg(unix)]
    let ranges = allocated_data_ranges(
        &file,
        0,
        metadata.len(),
        maximum_extent_spans,
        &mut meter.clone(),
    )
    .map(Some)
    .map_err(capture_demand_error)?;
    #[cfg(windows)]
    let ranges = regular_data_ranges(&file, &metadata, maximum_extent_spans, meter)?;
    let skip_import = ranges.as_ref().is_some_and(Vec::is_empty);
    #[cfg(all(test, feature = "memory"))]
    let skip_import = super::empty_stage_tests::after_ranges(skip_import);
    if skip_import {
        let staged = capture_vec(0, meter)?;
        finish_regular_capture(source_root, host_path, snapshot, &file, meter)?;
        let bytes = ranges.as_ref().map_or(0, vector_bytes);
        drop(ranges);
        drop(file);
        meter.clone().release(bytes).map_err(capture_demand_error)?;
        return Ok(staged);
    }
    let native = native_positional_file(file, meter)?;
    #[cfg(all(test, feature = "memory", any(windows, target_os = "linux")))]
    let native = super::owner_allocation_tests::owner(native, meter);
    #[cfg(windows)]
    let ranges = match ranges {
        Some(ranges) => Some(
            settled_sparse_ranges(&native, &metadata, ranges, maximum_extent_spans, meter).await?,
        ),
        None => None,
    };
    let full = [HostDataRange {
        offset: 0,
        length: metadata.len(),
    }];
    let data_ranges = ranges.as_deref().unwrap_or(&full);
    let mut staged = capture_vec(data_ranges.len(), meter)?;
    for range in data_ranges {
        let mut bounded = acyclic_native_runtime::AsyncRangeReader::new(
            std::borrow::Borrow::<acyclic_native_runtime::NativeFile>::borrow(&native),
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
            return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
        }
        *staged_file_bytes = staged_file_bytes
            .checked_add(range.length)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        staged.push((*range, content));
    }
    finish_regular_capture(source_root, host_path, snapshot, native.as_file(), meter)?;
    let bytes = ranges.as_ref().map_or(0, vector_bytes);
    drop(ranges);
    drop(native);
    meter.clone().release(bytes).map_err(capture_demand_error)?;
    Ok(staged)
}

fn finish_regular_capture(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostStat,
    file: &std::fs::File,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    #[cfg(all(test, feature = "memory"))]
    if super::empty_stage_tests::omit_finish() {
        return Ok(());
    }
    meter.cancellation().check()?;
    let after = source_root
        .stat_file_metadata(file, &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(snapshot, &after)?;
    let current = source_root
        .stat_source_metadata(host_path, &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(snapshot, &current)?;
    meter.clone().check().map_err(capture_demand_error)
}
