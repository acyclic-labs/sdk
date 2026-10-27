use super::*;

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
    let mut removed = Vec::new();
    for (name, bucket) in &next.buckets {
        let old = before.buckets.get(name);
        if old.is_none_or(|old| old.info != bucket.info) {
            let change = bucket_record(name, bucket, publication)?;
            publication.push(&mut delta.buckets, change)?;
        }
        let Some(old) = old else {
            for (key, value) in &bucket.objects {
                let change = object_record(name, key.as_str(), value, publication)?;
                publication.push(&mut delta.objects, change)?;
            }
            continue;
        };
        for item in old.objects.diff(&bucket.objects) {
            match item {
                imbl::ordmap::DiffItem::Add(key, value) => {
                    let change = object_record(name, key.as_str(), value, publication)?;
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
    #[cfg(test)]
    work::restore_encoding(next, publication)?;
    for change in before.uploads.diff(&next.uploads) {
        let (id, upload, old) = match change {
            imbl::ordmap::DiffItem::Add(id, upload) => (id, upload, None),
            imbl::ordmap::DiffItem::Update {
                old: (_, old),
                new: (id, upload),
            } => (id, upload, Some(old)),
            imbl::ordmap::DiffItem::Remove(id, _) => {
                let change = UploadChange {
                    id: publication.text(id)?,
                    deleted: true,
                    ..Default::default()
                };
                publication.push(&mut delta.uploads, change)?;
                continue;
            }
        };
        if old.is_none() {
            let change = upload_record(id, upload, publication)?;
            publication.push(&mut delta.uploads, change)?;
        }
        let empty = OrdMap::new();
        for change in old
            .map_or(&empty, |upload| &upload.parts)
            .diff(&upload.parts)
        {
            let (number, receipt, body) = match change {
                imbl::ordmap::DiffItem::Add(number, (receipt, body)) => (number, receipt, body),
                imbl::ordmap::DiffItem::Update {
                    old: (_, (old, _)),
                    new: (number, (receipt, body)),
                } if old != receipt => (number, receipt, body),
                _ => continue,
            };
            let change = part_record(id, *number, receipt, body, publication)?;
            publication.push(&mut delta.parts, change)?;
        }
    }
    for item in before.receipts.diff(&next.receipts) {
        if let imbl::ordmap::DiffItem::Add(key, receipt) = item {
            let change = receipt_record(key, receipt, publication)?;
            publication.push(&mut delta.receipts, change)?;
        }
    }
    Ok(delta)
}
