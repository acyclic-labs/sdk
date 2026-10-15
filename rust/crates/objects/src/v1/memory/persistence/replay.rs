use super::*;

pub(super) fn restore_body(
    records: Vec<BodyRecord>,
    root: &Arc<crate::physical::LocalRoot>,
    expected: u64,
) -> Result<StoredBody, LocalOpenError> {
    if records.is_empty() || records.len() > 10_000 {
        return Err(LocalOpenError::Corrupt);
    }
    let mut length = 0usize;
    let mut leaves = Vec::with_capacity(records.len());
    for value in records {
        let count = usize::try_from(value.length).map_err(corrupt)?;
        length = length.checked_add(count).ok_or(LocalOpenError::Corrupt)?;
        if value.offset < 68
            || value.journal && (count > inline::LIMIT || !value.segment.is_empty())
        {
            return Err(LocalOpenError::Corrupt);
        }
        leaves.push(StoredBody::Local {
            root: Arc::clone(root),
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            length: count,
            location: if value.journal {
                LocalBodyLocation::Journal {
                    offset: value.offset,
                }
            } else {
                LocalBodyLocation::Segment {
                    id: value.segment.as_slice().try_into().map_err(corrupt)?,
                    offset: value.offset,
                }
            },
        });
    }
    if length as u64 != expected {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(StoredBody::Composite {
        parts: Arc::new(crate::v1::Guarded {
            value: leaves,
            _allocation: None,
        }),
        length,
    })
}

pub(super) fn apply(
    state: &mut State,
    delta: Delta,
    root: &Arc<crate::physical::LocalRoot>,
    options: MemoryOptions,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    if delta.sequence < state.sequence {
        return Err(LocalOpenError::Corrupt);
    }
    let mut next = state.clone();
    next.sequence = delta.sequence;
    let mut seen = BTreeSet::new();
    for value in delta.buckets {
        if !seen.insert(value.name.clone()) {
            return Err(LocalOpenError::Corrupt);
        }
        if let Some(info) = value.info {
            response::bucket(
                &info,
                &wire::BucketRef {
                    name: value.name.clone(),
                },
            )
            .map_err(corrupt)?;
            if next.buckets.contains_key(value.name.as_str()) {
                return Err(LocalOpenError::Corrupt);
            }
            next.buckets.insert(
                value.name.into(),
                Bucket {
                    info: info.into(),
                    objects: OrdMap::new(),
                },
            );
        } else if next
            .buckets
            .remove(value.name.as_str())
            .is_none_or(|bucket| !bucket.objects.is_empty())
        {
            return Err(LocalOpenError::Corrupt);
        }
    }
    let mut seen = BTreeSet::new();
    for value in delta.objects {
        request::key(&value.key).map_err(corrupt)?;
        if !seen.insert((value.bucket.clone(), value.key.clone())) {
            return Err(LocalOpenError::Corrupt);
        }
        let bucket = next
            .buckets
            .get_mut(value.bucket.as_str())
            .ok_or(LocalOpenError::Corrupt)?;
        if let Some(info) = value.info {
            response::object_info(&info).map_err(corrupt)?;
            if info.size > limits.maximum_object_bytes {
                return Err(LocalOpenError::Corrupt);
            }
            let body = restore_body(value.bodies, root, info.size)?;
            bucket.objects.insert(
                value.key.into(),
                Stored {
                    info: info.into(),
                    body,
                },
            );
        } else if !value.bodies.is_empty() || bucket.objects.remove(value.key.as_str()).is_none() {
            return Err(LocalOpenError::Corrupt);
        }
    }
    apply_uploads(&mut next, delta.uploads)?;
    apply_parts(&mut next, delta.parts, root, limits)?;
    apply_receipts(&mut next, delta.receipts)?;
    validate_state(state, &mut next, options)?;
    *state = next;
    Ok(())
}

pub(super) fn apply_uploads(
    next: &mut State,
    changes: Vec<UploadChange>,
) -> Result<(), LocalOpenError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        request::upload_id(&value.id).map_err(corrupt)?;
        if !seen.insert(value.id.clone()) {
            return Err(LocalOpenError::Corrupt);
        }
        if value.deleted {
            if !value.bucket.is_empty()
                || !value.key.is_empty()
                || value.metadata.is_some()
                || next.uploads.remove(value.id.as_str()).is_none()
            {
                return Err(LocalOpenError::Corrupt);
            }
        } else {
            request::key(&value.key).map_err(corrupt)?;
            request::metadata(&value.metadata).map_err(corrupt)?;
            if !next.buckets.contains_key(value.bucket.as_str())
                || next.uploads.contains_key(value.id.as_str())
            {
                return Err(LocalOpenError::Corrupt);
            }
            next.uploads.insert(
                value.id.into(),
                Upload {
                    bucket: value.bucket.into(),
                    key: value.key.into(),
                    metadata: value.metadata.into(),
                    parts: OrdMap::new(),
                },
            );
        }
    }
    Ok(())
}

pub(super) fn apply_parts(
    next: &mut State,
    changes: Vec<PartChange>,
    root: &Arc<crate::physical::LocalRoot>,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        if !seen.insert((value.upload.clone(), value.number)) {
            return Err(LocalOpenError::Corrupt);
        }
        let upload = next
            .uploads
            .get_mut(value.upload.as_str())
            .ok_or(LocalOpenError::Corrupt)?;
        let receipt = value.receipt.ok_or(LocalOpenError::Corrupt)?;
        let query = wire::UploadPartHeader {
            bucket: Some(wire::BucketRef {
                name: upload.bucket.to_string(),
            }),
            object_key: upload.key.to_string(),
            upload_id: value.upload,
            part_number: value.number,
            mutation: None,
        };
        response::validate_binary(
            "multipart/upload-part",
            &query.encode_to_vec(),
            &receipt.encode_to_vec(),
            receipt.size,
        )
        .map_err(corrupt)?;
        if receipt.size > limits.maximum_object_bytes {
            return Err(LocalOpenError::Corrupt);
        }
        let body = restore_body(value.bodies, root, receipt.size)?;
        upload.parts.insert(value.number, (receipt.into(), body));
    }
    Ok(())
}

pub(super) fn apply_receipts(
    next: &mut State,
    changes: Vec<ReceiptChange>,
) -> Result<(), LocalOpenError> {
    for value in changes {
        request::identity(&Some(wire::MutationIdentity {
            idempotency_key: value.key.clone(),
        }))
        .map_err(corrupt)?;
        validate_receipt(value.kind, &value.response)?;
        let put_info = if value.kind == 2 {
            Some(
                wire::ObjectInfo::decode(value.response.as_slice())
                    .map_err(corrupt)?
                    .into(),
            )
        } else {
            None
        };
        let receipt = Receipt {
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            response: value.response.into(),
            put_info,
            kind: value.kind,
        };
        if next.receipts.insert(value.key.into(), receipt).is_some() {
            return Err(LocalOpenError::Corrupt);
        }
    }
    Ok(())
}

pub(super) fn validate_state(
    before: &State,
    next: &mut State,
    options: MemoryOptions,
) -> Result<(), LocalOpenError> {
    let size = super::stored_bytes(before, next).ok_or(LocalOpenError::Corrupt)?;
    if size > options.maximum_bytes
        || next
            .uploads
            .values()
            .any(|upload| !next.buckets.contains_key(upload.bucket.as_str()))
    {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(())
}

pub(super) fn validate_receipt(kind: u32, bytes: &[u8]) -> Result<(), LocalOpenError> {
    macro_rules! checked {
        ($ty:ident) => {{
            let value = wire::$ty::decode(bytes).map_err(corrupt)?;
            if value.encode_to_vec() != bytes {
                return Err(LocalOpenError::Corrupt);
            }
            value
        }};
    }
    match kind {
        1 => {
            let value = checked!(Bucket);
            response::bucket(
                &value,
                value.bucket.as_ref().ok_or(LocalOpenError::Corrupt)?,
            )
            .map_err(corrupt)?;
        }
        2 => {
            response::object_info(&checked!(ObjectInfo)).map_err(corrupt)?;
        }
        3 => {
            checked!(DeleteBucketResponse);
        }
        4 => {
            checked!(DeleteObjectResponse);
        }
        5 => {
            request::upload_id(&checked!(MultipartUpload).upload_id).map_err(corrupt)?;
        }
        6 => {
            let part = checked!(UploadedPart);
            let reply = wire::ListPartsResponse {
                parts: vec![part],
                ..Default::default()
            };
            response::parts(
                &wire::ListPartsRequest {
                    page_size: 1,
                    ..Default::default()
                },
                &reply,
            )
            .map_err(corrupt)?;
        }
        7 => {
            checked!(AbortMultipartResponse);
        }
        _ => return Err(LocalOpenError::Corrupt),
    }
    Ok(())
}
