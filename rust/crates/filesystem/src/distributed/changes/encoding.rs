//! Both producer paths admit storage and copies before constructing the record.
use super::*;

pub(super) fn entries(
    context: &[u8],
    selected: &[ChangeEntryRef<'_>],
    work: &mut AuthorityWork<'_>,
) -> Result<AppendRecord, AuthorityStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_native() {
        return AppendRecord::changes_native(context, selected, &work.native_meter()?)
            .map_err(|error| work.native_io_failure(error))?
            .map_err(Into::into);
    }
    let encoded_len = acyclic_stream::changes_encoded_len(context, selected)?;
    let mut entries = work.vec(selected.len())?;
    for entry in selected {
        entries.push(acyclic_stream::wire::ChangeEntry {
            key: work.bytes(entry.key)?,
            operation: text(entry.operation, work)?,
            metadata: work.bytes(entry.metadata)?,
        });
    }
    let writes = codec_work(encoded_len as u64, encoded_len as u64, 0);
    work.admit(writes)?;
    work.accounting()
        .claim_owned(RecordHeaders::control_bytes() as u64, 1)
        .map_err(authority_error)?;
    let record = AppendRecord::changes(&work.bytes(context)?, &entries, Vec::new())?;
    work.charge(writes)?;
    Ok(record)
}

fn text(value: &str, work: &mut AuthorityWork<'_>) -> Result<String, AuthorityStoreError> {
    let writes = codec_work(0, value.len() as u64, 0);
    work.admit(writes)?;
    let mut bytes = work.vec(value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    work.charge(writes)?;
    String::from_utf8(bytes).map_err(|_| crate::WorkError::Overflow.into())
}
