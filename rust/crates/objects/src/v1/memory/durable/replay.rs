use super::*;

pub(in crate::v1::memory) fn apply(
    state: &mut State,
    delta: Delta,
    restore_body: &impl Fn(Vec<BodyRecord>, u64) -> Result<StoredBody, RestoreError>,
    options: MemoryOptions,
    maximum_object_bytes: u64,
) -> Result<(), RestoreError> {
    if delta.sequence < state.sequence {
        return Err(RestoreError);
    }
    let mut next = state.clone();
    next.sequence = delta.sequence;
    let mut seen = BTreeSet::new();
    for value in delta.buckets {
        if !seen.insert(value.name.clone()) {
            return Err(RestoreError);
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
                return Err(RestoreError);
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
            return Err(RestoreError);
        }
    }
    let mut seen = BTreeSet::new();
    for value in delta.objects {
        request::key(&value.key).map_err(corrupt)?;
        if !seen.insert((value.bucket.clone(), value.key.clone())) {
            return Err(RestoreError);
        }
        let bucket = next
            .buckets
            .get_mut(value.bucket.as_str())
            .ok_or(RestoreError)?;
        if let Some(info) = value.info {
            response::object_info(&info).map_err(corrupt)?;
            if info.size > maximum_object_bytes {
                return Err(RestoreError);
            }
            let body = restore_body(value.bodies, info.size)?;
            bucket.objects.insert(
                value.key.into(),
                Stored {
                    info: info.into(),
                    body,
                },
            );
        } else if !value.bodies.is_empty() || bucket.objects.remove(value.key.as_str()).is_none() {
            return Err(RestoreError);
        }
    }
    apply_uploads(&mut next, delta.uploads)?;
    apply_parts(&mut next, delta.parts, restore_body, maximum_object_bytes)?;
    apply_receipts(&mut next, delta.receipts)?;
    validate_state(state, &mut next, options)?;
    *state = next;
    Ok(())
}

pub(in crate::v1::memory) fn apply_uploads(
    next: &mut State,
    changes: Vec<UploadChange>,
) -> Result<(), RestoreError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        request::upload_id(&value.id).map_err(corrupt)?;
        if !seen.insert(value.id.clone()) {
            return Err(RestoreError);
        }
        if value.deleted {
            if !value.bucket.is_empty()
                || !value.key.is_empty()
                || value.metadata.is_some()
                || next.uploads.remove(value.id.as_str()).is_none()
            {
                return Err(RestoreError);
            }
        } else {
            request::key(&value.key).map_err(corrupt)?;
            request::metadata(&value.metadata).map_err(corrupt)?;
            if !next.buckets.contains_key(value.bucket.as_str())
                || next.uploads.contains_key(value.id.as_str())
            {
                return Err(RestoreError);
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

pub(in crate::v1::memory) fn apply_parts(
    next: &mut State,
    changes: Vec<PartChange>,
    restore_body: &impl Fn(Vec<BodyRecord>, u64) -> Result<StoredBody, RestoreError>,
    maximum_object_bytes: u64,
) -> Result<(), RestoreError> {
    let mut seen = BTreeSet::new();
    for value in changes {
        if !seen.insert((value.upload.clone(), value.number)) {
            return Err(RestoreError);
        }
        let upload = next
            .uploads
            .get_mut(value.upload.as_str())
            .ok_or(RestoreError)?;
        let receipt = value.receipt.ok_or(RestoreError)?;
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
        if receipt.size > maximum_object_bytes {
            return Err(RestoreError);
        }
        let body = restore_body(value.bodies, receipt.size)?;
        upload.parts.insert(value.number, (receipt.into(), body));
    }
    Ok(())
}

pub(in crate::v1::memory) fn apply_receipts(
    next: &mut State,
    changes: Vec<ReceiptChange>,
) -> Result<(), RestoreError> {
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
            return Err(RestoreError);
        }
    }
    Ok(())
}

pub(in crate::v1::memory) fn validate_state(
    before: &State,
    next: &mut State,
    options: MemoryOptions,
) -> Result<(), RestoreError> {
    let size = super::super::stored_bytes(before, next).ok_or(RestoreError)?;
    if size > options.maximum_bytes
        || next.entry_count() > options.maximum_entries
        || next
            .uploads
            .values()
            .any(|upload| !next.buckets.contains_key(upload.bucket.as_str()))
    {
        return Err(RestoreError);
    }
    Ok(())
}

pub(in crate::v1::memory) fn validate_receipt(kind: u32, bytes: &[u8]) -> Result<(), RestoreError> {
    macro_rules! checked {
        ($ty:ident) => {{
            let value = wire::$ty::decode(bytes).map_err(corrupt)?;
            if value.encode_to_vec() != bytes {
                return Err(RestoreError);
            }
            value
        }};
    }
    match kind {
        1 => {
            let value = checked!(Bucket);
            response::bucket(&value, value.bucket.as_ref().ok_or(RestoreError)?)
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
        _ => return Err(RestoreError),
    }
    Ok(())
}

fn corrupt<T>(_: T) -> RestoreError {
    RestoreError
}
