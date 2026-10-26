use super::changes::*;
use super::*;

/// Borrow the persistent trees in recovery order without materializing a snapshot.
pub(in crate::v1::memory) enum Entry<'a> {
    Bucket(&'a str, &'a Bucket),
    Object(&'a str, &'a str, &'a Stored),
    Upload(&'a str, &'a Upload),
    Part(&'a str, u32, &'a wire::UploadedPart, &'a StoredBody),
    Receipt(&'a str, &'a Receipt),
}

pub(in crate::v1::memory) fn walk<E>(
    state: &State,
    mut visit: impl FnMut(Entry<'_>) -> Result<(), E>,
) -> Result<(), E> {
    for (name, bucket) in &state.buckets {
        visit(Entry::Bucket(name, bucket))?;
    }
    for (name, bucket) in &state.buckets {
        for (key, value) in &bucket.objects {
            visit(Entry::Object(name, key, value))?;
        }
    }
    for (id, upload) in &state.uploads {
        visit(Entry::Upload(id, upload))?;
    }
    for (id, upload) in &state.uploads {
        for (number, (receipt, body)) in &upload.parts {
            visit(Entry::Part(id, *number, receipt, body))?;
        }
    }
    for (key, receipt) in &state.receipts {
        visit(Entry::Receipt(key, receipt))?;
    }
    Ok(())
}

pub(in crate::v1::memory) enum Change {
    Bucket(BucketChange),
    Object(ObjectChange),
    Upload(UploadChange),
    Part(PartChange),
    Receipt(ReceiptChange),
}

impl Entry<'_> {
    pub(in crate::v1::memory) fn record(
        self,
        publication: &mut Publication,
    ) -> Result<Change, NativeObjectsError> {
        match self {
            Self::Bucket(name, bucket) => {
                bucket_record(name, bucket, publication).map(Change::Bucket)
            }
            Self::Object(name, key, object) => {
                object_record(name, key, object, publication).map(Change::Object)
            }
            Self::Upload(id, upload) => upload_record(id, upload, publication).map(Change::Upload),
            Self::Part(id, number, receipt, body) => {
                part_record(id, number, receipt, body, publication).map(Change::Part)
            }
            Self::Receipt(key, receipt) => {
                receipt_record(key, receipt, publication).map(Change::Receipt)
            }
        }
    }
}

impl Change {
    pub(in crate::v1::memory) fn encoded_len(&self) -> usize {
        match self {
            Self::Bucket(value) => value.encoded_len(),
            Self::Object(value) => value.encoded_len(),
            Self::Upload(value) => value.encoded_len(),
            Self::Part(value) => value.encoded_len(),
            Self::Receipt(value) => value.encoded_len(),
        }
    }
    pub(in crate::v1::memory) fn insert(
        self,
        delta: &mut Delta,
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        match self {
            Self::Bucket(value) => publication.push(&mut delta.buckets, value),
            Self::Object(value) => publication.push(&mut delta.objects, value),
            Self::Upload(value) => publication.push(&mut delta.uploads, value),
            Self::Part(value) => publication.push(&mut delta.parts, value),
            Self::Receipt(value) => publication.push(&mut delta.receipts, value),
        }
    }
}
