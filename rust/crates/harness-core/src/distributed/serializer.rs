use super::*;

fn stream_path(
    value: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    if let Some(work) = work.as_deref_mut() {
        work.admit_allocation(value.len() as u64)?;
        work.copy_bytes(value.len() as u64)?;
    }
    let path = acyclic_stream::StreamPath::new(value)
        .map_err(|error| operations::stream_error_with_work(error, work))?;
    if let Some(work) = work.as_deref_mut() {
        work.retain_owned(value.len() as u64)?;
    }
    Ok(path)
}

pub(crate) fn digest_key(
    digest: &[u8; 32],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamIdempotencyKey, u64)> {
    let mut custody = CodecQuota::new(work, 0)?;
    let bytes = crate::core::copy_copy_vector(digest, &mut custody)?;
    custody.transfer();
    let (bytes, retained) = operations::owned_bytes_from_retained_vec(bytes, custody.work())?;
    Ok((
        StreamIdempotencyKey::new(bytes)
            .map_err(|error| operations::stream_error_with_work(error, custody.work()))?,
        retained,
    ))
}

pub(crate) fn execution_path(
    operation: OperationId,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    const PREFIX: &[u8] = b"harness/v2/execution/";
    let mut text = [0; PREFIX.len() + 36];
    if let Some(work) = work.as_deref_mut() {
        work.copy_bytes(text.len() as u64)?;
    }
    let (prefix, id) = text.split_at_mut(PREFIX.len());
    prefix.copy_from_slice(PREFIX);
    uuid::Uuid::from_bytes(operation.into_bytes())
        .hyphenated()
        .encode_lower(id);
    stream_path(
        std::str::from_utf8(&text).map_err(|_| {
            guarded_boundary(
                work,
                wire::ErrorCode::Invalid,
                "execution path is not UTF-8",
            )
        })?,
        work,
    )
}
