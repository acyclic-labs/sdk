use super::*;

pub(crate) fn object_key(object_id: ObjectId) -> String {
    format!(
        "fs/v1/{}/{}",
        object_id.kind.canonical_tag(),
        hex::encode(object_id.digest.as_bytes())
    )
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) fn object_id_from_key(key: &str) -> Option<ObjectId> {
    let (tag, digest) = key.strip_prefix("fs/v1/")?.split_once('/')?;
    let kind = crate::storage::ObjectKind::from_canonical_tag(tag.parse().ok()?).ok()?;
    let digest = <[u8; 32]>::try_from(hex::decode(digest).ok()?).ok()?;
    let object = ObjectId {
        kind,
        digest: crate::foundation::Digest::from_bytes(digest),
    };
    (object_key(object) == key).then_some(object)
}

pub(super) fn authority_child(
    child: &acyclic_stream::StreamPath,
) -> Result<AuthorityId, AuthorityStoreError> {
    let encoded = child
        .as_str()
        .strip_prefix("fs/authorities/")
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream authority child escaped its parent".to_owned())
        })?;
    if encoded.len() != 32 || encoded.contains('/') {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority child has a noncanonical identity".to_owned(),
        ));
    }
    let mut bytes = [0_u8; 16];
    hex::decode_to_slice(encoded, &mut bytes).map_err(|_| {
        AuthorityStoreError::Corrupt(
            "Stream authority child has a noncanonical identity".to_owned(),
        )
    })?;
    Ok(AuthorityId::from_bytes(bytes))
}

pub(super) fn decode_publication_gate(
    value: &[u8],
) -> Result<Option<OperationId>, AuthorityStoreError> {
    if value == PUBLICATION_GATE_FREE {
        return Ok(None);
    }
    let operation = value
        .strip_prefix(PUBLICATION_GATE_ACTIVE)
        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("invalid publication gate record".to_owned())
        })?;
    Ok(Some(OperationId::from_bytes(operation)))
}

pub(super) fn stream_key(
    domain: &[u8],
    identity: &[u8],
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let mut value = Vec::with_capacity(
        domain
            .len()
            .saturating_add(identity.len())
            .saturating_add(1),
    );
    value.extend_from_slice(domain);
    value.push(0);
    value.extend_from_slice(identity);
    acyclic_stream::IdempotencyKey::new(Bytes::from(value)).map_err(map_stream_error)
}

pub(super) fn map_stream_error(error: acyclic_stream::StreamError) -> AuthorityStoreError {
    match error {
        acyclic_stream::StreamError::NotFound => AuthorityStoreError::Missing,
        acyclic_stream::StreamError::Capacity => {
            AuthorityStoreError::Rejected("Stream capacity exhausted".to_owned())
        }
        other => AuthorityStoreError::Rejected(other.to_string()),
    }
}

pub(super) fn authority_read_work(records: u64) -> WorkCounters {
    WorkCounters {
        authority_records_read: records,
        backend_read_operations: records,
        ..WorkCounters::default()
    }
}

pub(super) fn record_bytes<'a, const N: usize>(
    bytes: &'a [u8],
    domain: &[u8],
) -> Option<&'a [u8; N]> {
    bytes.strip_prefix(domain)?.try_into().ok()
}
pub(super) fn stream_path(
    authority: AuthorityId,
    suffix: &str,
) -> Result<StreamPath, AuthorityStoreError> {
    StreamPath::new(format!(
        "fs/authorities/{}{suffix}",
        hex::encode(authority.into_bytes())
    ))
    .map_err(map_stream_error)
}
pub(super) fn decode_lineage_generation(bytes: &[u8]) -> Result<GenerationId, AuthorityStoreError> {
    record_bytes::<32>(bytes, LINEAGE_DOMAIN)
        .map(|bytes| GenerationId::new(Digest::from_bytes(*bytes)))
        .ok_or(AuthorityStoreError::CorruptStatic(
            "invalid Stream generation-lineage record",
        ))
}
pub(super) fn decode_lineage_tail(bytes: &[u8]) -> Result<u64, AuthorityStoreError> {
    record_bytes::<8>(bytes, LINEAGE_TAIL_DOMAIN)
        .map(|bytes| u64::from_le_bytes(*bytes))
        .ok_or(AuthorityStoreError::CorruptStatic(
            "invalid Stream generation locator",
        ))
}
pub(super) fn generation_from_payload(
    authority_id: AuthorityId,
    sequence: Sequence,
    payload: &[u8],
    meter: &mut AuthorityWork<'_>,
) -> Result<Option<GenerationId>, AuthorityStoreError> {
    meter.check()?;
    let examined = WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    meter.admit(examined)?;
    const CREATED: &[u8] = b"acyclic-fs-volume-created-v1\0";
    const PUBLISHED: &[u8] = b"acyclic-fs-publish-generation-v1\0";
    let created = payload.len()
        == CREATED.len() + 2 + 16 + crate::kernel::CONFIG_ENCODED_BYTES + 1 + 32
        && payload.starts_with(CREATED);
    meter.record_completed(examined)?;
    let published = payload.starts_with(PUBLISHED);
    if !created && !published {
        return Ok(None);
    }
    let mut observe = |step| {
        meter
            .observe_record(
                step,
                "fixed authority facts emitted an allocating decoder step",
            )
            .map_err(crate::FsError::from)
    };
    let fact = if created {
        crate::kernel::decode_volume_created(payload, STREAM_RECORD_LIMIT, &mut observe)
            .map(|created| (created.volume_id, created.initial_generation_root.digest))
    } else {
        crate::kernel::decode_published_generation(payload, STREAM_RECORD_LIMIT, &mut observe)
            .map(|published| (published.volume_id, published.generation_root.digest))
    };
    let (volume, digest) = match fact {
        Ok(fact) => fact,
        Err(failure)
            if matches!(
                failure,
                crate::FsError::Decode(_) | crate::FsError::VolumeCreated(_)
            ) =>
        {
            return Ok(None);
        }
        Err(failure) => return Err(authority_error(failure)),
    };
    if volume_authority_id(volume) != authority_id || created != (sequence == Sequence::new(1)) {
        return Err(authority_corrupt(
            "generation fact identity or sequence does not match its authority",
            meter,
        ));
    }
    Ok(Some(GenerationId::new(digest)))
}
pub(super) async fn read_one<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    from: u64,
) -> Result<acyclic_stream::Record, AuthorityStoreError> {
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from,
            limit: 1,
        })
        .await
        .map_err(map_stream_error)?;
    let record = records
        .next()
        .await
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned())
        })?
        .map_err(map_stream_error)?;
    if record.sequence != from {
        return Err(AuthorityStoreError::Corrupt(
            "Stream returned a non-contiguous record".to_owned(),
        ));
    }
    Ok(record)
}
pub(super) fn decode_record<'a>(
    record: &'a acyclic_stream::Record,
) -> Result<crate::streams_record::BorrowedStreamsDurableRecord<'a>, AuthorityStoreError> {
    StreamsDurableRecord::decode_borrowed(&record.value, STREAM_RECORD_LIMIT)
        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))
}
pub(super) fn validate_record(
    authority: AuthorityId,
    record: &acyclic_stream::Record,
    durable: &crate::streams_record::BorrowedStreamsDurableRecord<'_>,
) -> Result<(), AuthorityStoreError> {
    if durable.sequence.get() != record.sequence {
        return Err(AuthorityStoreError::Corrupt(
            "Stream sequence does not match filesystem authority sequence".to_owned(),
        ));
    }
    let digest = authority_commit_digest(
        authority,
        durable.epoch,
        durable.sequence,
        durable.operation_id,
        durable.fingerprint,
        durable.previous_digest,
        durable.payload,
    );
    if digest != durable.digest {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority record digest mismatch".to_owned(),
        ));
    }
    Ok(())
}
