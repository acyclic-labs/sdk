use super::*;

pub(super) fn record(bytes: Vec<u8>) -> acyclic_stream::Record {
    acyclic_stream::Record {
        sequence: 0,
        value: bytes.into(),
        commit_id: acyclic_stream::CommitId::from_bytes([0; 32]),
        committed_at_micros: 1_000,
    }
}

fn check_record(record: &acyclic_stream::Record) -> Result<()> {
    let Ok((revision, key, digest, time, event)) = decode(record, &mut None) else {
        return Ok(());
    };
    let mut work = None;
    let mut quota = CodecQuota::new(&mut work, 0)?;
    if replayed_intent_digest(record, &mut quota)? != digest {
        return Err(Error::Invalid("replayed intent digest differs".into()));
    }
    let canonical = crate::contract::canonical_json_bytes(&event)?;
    let encoded = encode(
        revision,
        event.operation_id(),
        &key,
        *blake3::hash(&canonical).as_bytes(),
        canonical,
        time,
        &mut None,
    )?;
    let mut roundtrip = record.clone();
    roundtrip.value = encoded.into();
    if decode(&roundtrip, &mut None)?.4 != event {
        return Err(Error::Invalid("scheduler event roundtrip differs".into()));
    }
    Ok(())
}

pub(crate) fn check(data: &[u8]) -> Result<()> {
    // The real record reader rejects larger frames before either projection.
    if data.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Ok(());
    }
    check_record(&record(data.to_vec()))?;
    // Valid checksums also send arbitrary JSON through the typed decoder;
    // fuzzers cannot ordinarily guess a BLAKE3 digest around a new payload.
    let canonical = data.to_vec();
    let encoded = encode(
        1,
        OperationId::from_bytes([0; 16]),
        "fuzz-intent",
        *blake3::hash(&canonical).as_bytes(),
        canonical,
        1,
        &mut None,
    )?;
    if encoded.len() <= acyclic_stream::MAX_RECORD_BYTES {
        check_record(&record(encoded))?;
    }
    Ok(())
}
