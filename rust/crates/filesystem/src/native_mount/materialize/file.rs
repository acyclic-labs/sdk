use super::*;

pub(super) async fn write_generation_bytes<E, F, Fut>(
    file: &acyclic_native_runtime::NativeFile,
    offset: u64,
    bytes: acyclic_stream::OwnedBytes,
    owned_bytes: usize,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<u64, E>
where
    E: From<MaterializeError>, F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
    let length = bytes.len() as u64;
    let custody = receipt.custody.split_off(owned_bytes as u64).map_err(materialize_native_error)?;
    let meter = receipt.custody.work().runtime_meter();
    let write = file.write_async(offset, bytes, meter, custody).map_err(materialize_runtime_error)?;
    write.await.map_err(materialize_runtime_error)?;
    generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
    Ok(length)
}

pub(super) async fn sync_generation_file<E, F, Fut>(
    file: &acyclic_native_runtime::NativeFile,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(), E>
where
    E: From<MaterializeError>, F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
    let meter = receipt.custody.work().runtime_meter();
    let sync = file.sync_async(acyclic_native_runtime::Durability::Full, Some(meter)).map_err(materialize_runtime_error)?;
    sync.await.map_err(materialize_runtime_error)?;
    generation_action(before_effect, NativeMaterializationActionPhase::Completed).await
}

#[cfg(windows)]
pub(super) fn mark_sparse_measured(file: &File, meter: &dyn acyclic_native_runtime::NativeReadMeter) -> std::io::Result<()> {
    let mut output = [];
    acyclic_native_runtime::device_control_in_place(file, windows::Win32::System::Ioctl::FSCTL_SET_SPARSE, &[], &mut output, Some((meter, acyclic_native_runtime::NativeOperationKind::Write)))?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "writes one sparse file from the reader under the options, budget, cancellation, and shared receipt"
)]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered pass reads, writes, and punches every extent of the file"
)]
pub(super) async fn materialize_sparse_file<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    path: &NamespacePath,
    file: &mut acyclic_native_runtime::NativeFile,
    logical_bytes: u64,
    options: &MaterializeOptions,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<FileMetadata, E>
where
    A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut, Fut: Future<Output = Result<(), E>>,
{
    let resolved = read_source!(reader, receipt, remaining, value,
        reader.resolve_file_ranges_with_native_work(std::slice::from_ref(path), receipt.custody.work(), cancellation)
            => receipt.accept_source(value.0, value.1)?,
        reader.resolve_files(std::slice::from_ref(path), remaining, cancellation) => {
            receipt.adopt(vec_bytes(&value))?;
            value
        },
    );
    let resolved_bytes = vec_bytes(&resolved);
    let resolved = resolved.into_iter().next().flatten().ok_or(MaterializeError::MissingPath)?;
    receipt.custody.release(resolved_bytes as u64).map_err(materialize_native_error)?;
    let metadata = resolved.description().metadata;
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let length = options.transfer_bytes.min(logical_bytes - offset);
        let plan = read_source!(reader, receipt, remaining, value,
            resolved.plan_extents_with_native_work(
                ByteRange { offset, length }, options.maximum_extent_spans, receipt.custody.work(), cancellation,
            ) => {
                let (plan, custody) = value.ok_or(MaterializeError::InvalidPath)?;
                receipt.accept_source(plan, custody)?
            },
            resolved.plan_extents(ByteRange { offset, length }, options.maximum_extent_spans, remaining, cancellation) => {
                let plan = value.ok_or(MaterializeError::InvalidPath)?;
                receipt.adopt(vec_bytes(&plan.spans))?;
                plan
            },
        );
        let plan_bytes = vec_bytes(&plan.spans);
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        for span in plan.spans {
            match span.kind {
                ExtentKind::Hole => {
                    #[cfg(target_os = "macos")]
                    {
                        generation_action(before_effect, NativeMaterializationActionPhase::Before).await?;
                        let meter = receipt.custody.work().runtime_meter();
                        let control = file.control_async(move |handle, meter| { let meter = meter.expect("metered control");
                            crate::native_host::punch_hole(handle, span.offset, span.length, Some(meter))
                        }, Some(meter)).map_err(materialize_runtime_error)?;
                        control.await.map_err(materialize_runtime_error)?;
                        generation_action(before_effect, NativeMaterializationActionPhase::Completed).await?;
                    }
                }
                ExtentKind::AllocatedZero => queue_zero_writes(
                    &mut writes, span.offset, span.length, options.transfer_bytes, receipt,
                )?,
                ExtentKind::Content { .. } => receipt.push(&mut reads, ResolvedFileRangeReadRequest {
                    file: &resolved, range: ByteRange { offset: span.offset, length: span.length },
                })?,
            }
        }
        receipt.custody.release(plan_bytes as u64).map_err(materialize_native_error)?;
        if !reads.is_empty() {
            read_content_spans(reader, reads, options.maximum_extent_spans, cancellation, receipt, &mut writes).await?;
        }
        let write_bytes = vec_bytes(&writes);
        for write in writes {
            let written = write_generation_bytes(file, write.offset, write.bytes, write.owned_bytes, receipt, before_effect).await?;
            account_written(receipt, written)?;
        }
        receipt.custody.release(write_bytes as u64).map_err(materialize_native_error)?;
        offset += length;
    }
    Ok(metadata)
}

pub(super) async fn read_content_spans<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    reads: Vec<ResolvedFileRangeReadRequest<'_, A, O>>,
    maximum_extent_spans: u32,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    writes: &mut Vec<GenerationWrite>,
) -> Result<(), MaterializeError> {
    let request_bytes = vec_bytes(&reads);
    let read = read_source!(reader, receipt, remaining, value,
        reader.read_resolved_ranges_with_native_work(
            &reads, usize::try_from(maximum_extent_spans).unwrap_or(usize::MAX), receipt.custody.work(), cancellation,
        ) => receipt.accept_source(value.0, value.1)?,
        reader.read_resolved_ranges(
            &reads, usize::try_from(maximum_extent_spans).unwrap_or(usize::MAX), remaining, cancellation,
        ) => {
            receipt.adopt(vec_bytes(&value))?;
            for result in &value { receipt.adopt(result.bytes.len())?; }
            value
        },
    );
    let result_bytes = vec_bytes(&read);
    if reads.len() != read.len() {
        return Err(MaterializeError::Engine("range batch returned a different number of results"));
    }
    for (request, result) in reads.into_iter().zip(read) {
        let owned_bytes = if reader.validate_native_work(receipt.custody.work()).is_ok() { 0 } else { result.bytes.len() };
        receipt.push(writes, GenerationWrite { offset: request.range.offset, bytes: result.bytes, owned_bytes })?;
    }
    receipt.custody.release(request_bytes as u64).map_err(materialize_native_error)?;
    receipt.custody.release(result_bytes as u64).map_err(materialize_native_error)?;
    Ok(())
}

pub(super) fn queue_zero_writes(
    writes: &mut Vec<GenerationWrite>,
    offset: u64,
    length: u64,
    transfer_bytes: u64,
    receipt: &mut GenerationReceipt,
) -> Result<(), MaterializeError> {
    static ZERO_BYTES: [u8; 65_536] = [0; 65_536];
    let chunk_bytes = transfer_bytes.min(ZERO_BYTES.len() as u64);
    let mut remaining = length;
    let mut position = offset;
    while remaining != 0 {
        let count = remaining.min(chunk_bytes) as usize;
        receipt.push(writes, GenerationWrite {
            offset: position, bytes: acyclic_stream::OwnedBytes::from_static(&ZERO_BYTES[..count]), owned_bytes: 0,
        })?;
        position = position.checked_add(count as u64).ok_or(MaterializeError::Work(WorkError::Overflow))?;
        remaining -= count as u64;
    }
    Ok(())
}

pub(super) struct GenerationWrite {
    pub(super) offset: u64,
    pub(super) bytes: acyclic_stream::OwnedBytes,
    pub(super) owned_bytes: usize,
}

pub(super) fn vec_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity() * std::mem::size_of::<T>()
}
