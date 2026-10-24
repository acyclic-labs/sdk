use super::*;

pub(super) enum Change {
    Bucket(BucketChange),
    Object(ObjectChange),
    Upload(UploadChange),
    Part(PartChange),
    Receipt(ReceiptChange),
}

impl Change {
    pub(super) fn encoded_len(&self) -> usize {
        match self {
            Self::Bucket(value) => value.encoded_len(),
            Self::Object(value) => value.encoded_len(),
            Self::Upload(value) => value.encoded_len(),
            Self::Part(value) => value.encoded_len(),
            Self::Receipt(value) => value.encoded_len(),
        }
    }

    pub(super) fn insert(
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

impl Checkpoint {
    pub(super) fn snapshot(&mut self, state: &State) -> Result<(), NativeObjectsError> {
        // Preserve replay order without ever constructing a whole-state Delta.
        for (name, bucket) in &state.buckets {
            self.append(|publication| {
                bucket_record(name, bucket, publication).map(Change::Bucket)
            })?;
        }
        for (name, bucket) in &state.buckets {
            for (key, value) in &bucket.objects {
                self.append(|publication| {
                    object_record(name, key, value, publication).map(Change::Object)
                })?;
            }
        }
        for (id, upload) in &state.uploads {
            self.append(|publication| upload_record(id, upload, publication).map(Change::Upload))?;
        }
        for (id, upload) in &state.uploads {
            for (number, (receipt, body)) in &upload.parts {
                self.append(|publication| {
                    part_record(id, *number, receipt, body, publication).map(Change::Part)
                })?;
            }
        }
        for (key, receipt) in &state.receipts {
            self.append(|publication| {
                receipt_record(key, receipt, publication).map(Change::Receipt)
            })?;
        }
        if self.pending.has_changes() || self.operations == 0 && self.pending.delta.sequence != 0 {
            self.flush()?;
        }
        Ok(())
    }
}
