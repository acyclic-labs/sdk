//! One canonical wire conversion for ordinary deltas and streamed checkpoints.
use super::bodies::bodies;
use super::*;

pub(in crate::v1::memory) fn bucket_record(
    name: &str,
    bucket: &Bucket,
    publication: &mut Publication,
) -> Result<BucketChange, NativeObjectsError> {
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
    Ok(BucketChange {
        name: publication.text(name)?,
        info: Some(info),
    })
}

pub(in crate::v1::memory) fn object_record(
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

pub(in crate::v1::memory) fn upload_record(
    id: &str,
    upload: &Upload,
    publication: &mut Publication,
) -> Result<UploadChange, NativeObjectsError> {
    let metadata = upload
        .metadata
        .as_ref()
        .map(|value| publication.metadata(value))
        .transpose()?;
    Ok(UploadChange {
        id: publication.text(id)?,
        bucket: publication.text(&upload.bucket)?,
        key: publication.text(&upload.key)?,
        metadata,
        deleted: false,
    })
}

pub(in crate::v1::memory) fn part_record(
    id: &str,
    number: u32,
    receipt: &wire::UploadedPart,
    body: &StoredBody,
    publication: &mut Publication,
) -> Result<PartChange, NativeObjectsError> {
    let refs = bodies(body, publication)?;
    Ok(PartChange {
        upload: publication.text(id)?,
        number,
        receipt: Some(wire::UploadedPart {
            etag: publication.text(&receipt.etag)?,
            part_number: receipt.part_number,
            size: receipt.size,
        }),
        bodies: refs,
    })
}

pub(in crate::v1::memory) fn receipt_record(
    key: &str,
    receipt: &Receipt,
    publication: &mut Publication,
) -> Result<ReceiptChange, NativeObjectsError> {
    Ok(ReceiptChange {
        key: publication.text(key)?,
        digest: publication.bytes(&receipt.digest)?,
        response: publication.bytes(receipt.response.as_ref())?,
        kind: receipt.kind,
    })
}
