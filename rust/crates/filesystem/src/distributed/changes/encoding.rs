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
    let mut keys = work.vec(selected.len())?;
    let mut entries = work.vec(selected.len())?;
    for entry in selected {
        let key = work.bytes(entry.key)?;
        keys.push(key.clone().into());
        entries.push(acyclic_stream::wire::ChangeEntry {
            key,
            operation: text(entry.operation, work)?,
            metadata: work.bytes(entry.metadata)?,
        });
    }
    let change = acyclic_stream::wire::ChangeRecord {
        context: work.bytes(context)?, entries,
    };
    let encoded_len = change.encoded_len();
    let header_len = selected.iter().map(|entry| entry.key.len()).sum::<usize>()
        + b"acyclic.recordchanges/v1".len();
    if encoded_len + header_len > acyclic_stream::MAX_RECORD_BYTES {
        return Err(acyclic_stream::StreamError::LimitExceeded.into());
    }
    let mut value = work.vec(encoded_len)?;
    let writes = codec_work(encoded_len as u64, encoded_len as u64, 0);
    work.admit(writes)?;
    change.encode(&mut value).map_err(|_| crate::WorkError::Overflow)?;
    work.charge(writes)?;
    let mut attributes = work.vec(1)?;
    attributes.push(Attribute {
        name: text("acyclic.record", work)?,
        value: b"changes/v1".as_slice().into(),
    });
    work.accounting().claim_owned(RecordHeaders::control_bytes() as u64, 1)
        .map_err(authority_error)?;
    let headers = RecordHeaders::new(keys, attributes)?;
    Ok(AppendRecord { value: work.plain_bytes(value)?.into(), headers })
}

fn text(value: &str, work: &mut AuthorityWork<'_>) -> Result<String, AuthorityStoreError> {
    let writes = codec_work(0, value.len() as u64, 0);
    work.admit(writes)?;
    let mut bytes = work.vec(value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    work.charge(writes)?;
    String::from_utf8(bytes).map_err(|_| crate::WorkError::Overflow.into())
}
