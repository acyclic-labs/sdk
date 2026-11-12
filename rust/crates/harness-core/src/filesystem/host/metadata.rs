use super::*;

pub(in crate::filesystem) async fn read_file_annotation<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    generation: &Generation<A, O>,
    path: &str,
    key: &AttributeName,
    maximum_bytes: Option<u64>,
    io: Option<&PreparationIo>,
) -> Result<Option<acyclic_stream::OwnedBytes>> {
    let owned;
    let io = if let Some(io) = io {
        io
    } else {
        owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        &owned
    };
    let absolute = crate::executor::preparation_format(
        format_args!("/{}", path.trim_start_matches('/')),
        &mut Some(&mut io.history_io()),
    )?;
    let _hold = io.guard_owned(absolute.capacity() as u64)?;
    let value = io
        .sdk_if_present(
            generation
                .read_named_attribute(&absolute, key, maximum_bytes, io, io.cancellation())
                .await,
        )?
        .flatten();
    let Some(value) = value else {
        return Ok(None);
    };
    if io.original_native_work().is_none() {
        io.retain_owned(value.bytes.retained_capacity().unwrap_or(0) as u64)?;
    }
    Ok(Some(value.bytes))
}
