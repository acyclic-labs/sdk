use super::*;

pub(super) fn bodies(
    body: &StoredBody,
    publication: &mut Publication,
) -> Result<Vec<BodyRecord>, NativeObjectsError> {
    fn ordered(
        body: &StoredBody,
        out: &mut Vec<BodyRecord>,
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        match body {
            StoredBody::Local {
                digest,
                length,
                location,
                ..
            } => {
                let (segment, offset, journal) = match location {
                    LocalBodyLocation::Segment { id, offset } => {
                        (publication.bytes(id)?, *offset, false)
                    }
                    LocalBodyLocation::Journal { offset } => (Vec::new(), *offset, true),
                };
                let digest = publication.bytes(digest)?;
                publication.push(
                    out,
                    BodyRecord {
                        segment,
                        digest,
                        length: *length as u64,
                        offset,
                        journal,
                    },
                )?;
                Ok(())
            }
            StoredBody::Composite { parts, .. } => {
                for part in parts.value.iter() {
                    ordered(part, out, publication)?;
                }
                Ok(())
            }
            _ => Err(Error::from(Unavailable).into()),
        }
    }
    let mut output = Vec::new();
    ordered(body, &mut output, publication)?;
    Ok(output)
}

pub(super) fn changed_keys(
    before: Option<&Bucket>,
    next: &Bucket,
    publication: &mut Publication,
) -> Result<Vec<SharedInfo<String>>, NativeObjectsError> {
    let mut output = Vec::new();
    if let Some(before) = before {
        for item in before.objects.diff(&next.objects) {
            if let imbl::ordmap::DiffItem::Add(key, _)
            | imbl::ordmap::DiffItem::Update { new: (key, _), .. } = item
            {
                publication.push(&mut output, key.clone())?;
            }
        }
    } else {
        for key in next.objects.keys() {
            publication.push(&mut output, key.clone())?;
        }
    }
    Ok(output)
}

pub(super) fn bucket_changes(
    before: &State,
    next: &State,
    delta: &mut Delta,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    fn changed(
        name: &str,
        key: &str,
        value: &Stored,
        publication: &mut Publication,
    ) -> Result<ObjectChange, NativeObjectsError> {
        Ok(ObjectChange {
            bucket: publication.text(name)?,
            key: publication.text(key)?,
            info: Some(publication.info(&value.info)?),
            bodies: bodies(&value.body, publication)?,
        })
    }
    let mut removed = Vec::new();
    for (name, bucket) in &next.buckets {
        let old = before.buckets.get(name);
        if old.is_none_or(|old| old.info != bucket.info) {
            let info = wire::Bucket {
                bucket: bucket
                    .info
                    .bucket
                    .as_ref()
                    .map(|bucket| {
                        publication
                            .text(&bucket.name)
                            .map(|name| wire::BucketRef { name })
                    })
                    .transpose()?,
                created_at: bucket.info.created_at,
            };
            let change = BucketChange {
                name: publication.text(name)?,
                info: Some(info),
            };
            publication.push(&mut delta.buckets, change)?;
        }
        let Some(old) = old else {
            for (key, value) in &bucket.objects {
                let change = changed(name, key.as_str(), value, publication)?;
                publication.push(&mut delta.objects, change)?;
            }
            continue;
        };
        for item in old.objects.diff(&bucket.objects) {
            match item {
                imbl::ordmap::DiffItem::Add(key, value) => {
                    let change = changed(name, key.as_str(), value, publication)?;
                    publication.push(&mut delta.objects, change)?;
                }
                imbl::ordmap::DiffItem::Update {
                    old: (_, old),
                    new: (key, value),
                } => {
                    let refs = bodies(&value.body, publication)?;
                    if old.info != value.info || bodies(&old.body, publication)? != refs {
                        let change = ObjectChange {
                            bucket: publication.text(name)?,
                            key: publication.text(key.as_str())?,
                            info: Some(publication.info(&value.info)?),
                            bodies: refs,
                        };
                        publication.push(&mut delta.objects, change)?;
                    }
                }
                imbl::ordmap::DiffItem::Remove(key, _) => {
                    let change = ObjectChange {
                        bucket: publication.text(name)?,
                        key: publication.text(key.as_str())?,
                        ..Default::default()
                    };
                    publication.push(&mut removed, change)?;
                }
            }
        }
    }
    publication.reserve(&mut delta.objects, removed.len())?;
    delta.objects.extend(removed);
    for name in before
        .buckets
        .keys()
        .filter(|name| !next.buckets.contains_key(*name))
    {
        let change = BucketChange {
            name: publication.text(name)?,
            info: None,
        };
        publication.push(&mut delta.buckets, change)?;
    }
    Ok(())
}

pub(super) fn difference(
    before: &State,
    next: &State,
    ordinal: u64,
    publication: &mut Publication,
) -> Result<Delta, NativeObjectsError> {
    let mut delta = Delta {
        sequence: next.sequence,
        ordinal,
        ..Default::default()
    };
    bucket_changes(before, next, &mut delta, publication)?;
    for (id, upload) in &next.uploads {
        let old = before.uploads.get(id);
        if old.is_none() {
            let metadata = upload
                .metadata
                .as_ref()
                .map(|value| publication.metadata(value))
                .transpose()?;
            let change = UploadChange {
                id: publication.text(id)?,
                bucket: publication.text(&upload.bucket)?,
                key: publication.text(&upload.key)?,
                metadata,
                deleted: false,
            };
            publication.push(&mut delta.uploads, change)?;
        }
        for (number, (receipt, body)) in &upload.parts {
            let refs = bodies(body, publication)?;
            if old
                .and_then(|old| old.parts.get(number))
                .is_none_or(|(old, _)| old != receipt)
            {
                let change = PartChange {
                    upload: publication.text(id)?,
                    number: *number,
                    receipt: Some(wire::UploadedPart {
                        etag: publication.text(&receipt.etag)?,
                        part_number: receipt.part_number,
                        size: receipt.size,
                    }),
                    bodies: refs,
                };
                publication.push(&mut delta.parts, change)?;
            }
        }
    }
    for id in before
        .uploads
        .keys()
        .filter(|id| !next.uploads.contains_key(*id))
    {
        let change = UploadChange {
            id: publication.text(id)?,
            deleted: true,
            ..Default::default()
        };
        publication.push(&mut delta.uploads, change)?;
    }
    for item in before.receipts.diff(&next.receipts) {
        if let imbl::ordmap::DiffItem::Add(key, receipt) = item {
            let change = ReceiptChange {
                key: publication.text(key.as_str())?,
                digest: publication.bytes(&receipt.digest)?,
                response: publication.bytes(receipt.response.as_ref())?,
                kind: receipt.kind,
            };
            publication.push(&mut delta.receipts, change)?;
        }
    }
    Ok(delta)
}
