//! Typed provider operations over the wire-compatible Objects v2 provider.
//!
//! The transport trait remains generated-message shaped for compatibility with
//! local, HTTP, and gRPC implementations. This extension converts validated
//! domain requests at the boundary and converts responses only after the
//! canonical response validators have run.

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;

use super::{domain, response, wire, Error, ObjectsProvider};

/// Typed Objects operations layered over any existing wire provider.
#[async_trait]
pub trait TypedObjectsProvider: ObjectsProvider {
    /// Creates a bucket and validates its response identity and timestamp.
    async fn typed_create_bucket(
        &self,
        request: domain::CreateBucketRequest,
    ) -> Result<domain::Bucket, Error> {
        let expected = request.name().clone();
        let result = <Self as ObjectsProvider>::create_bucket(self, request.into()).await?;
        domain::Bucket::try_from_wire(result, &expected)
    }

    /// Reads a bucket and validates its response identity and timestamp.
    async fn typed_head_bucket(
        &self,
        request: domain::HeadBucketRequest,
    ) -> Result<domain::Bucket, Error> {
        let expected = request.bucket().clone();
        let result = <Self as ObjectsProvider>::head_bucket(self, request.into()).await?;
        domain::Bucket::try_from_wire(result, &expected)
    }

    /// Deletes a bucket after converting its validated request.
    async fn typed_delete_bucket(
        &self,
        request: domain::DeleteBucketRequest,
    ) -> Result<bool, Error> {
        Ok(<Self as ObjectsProvider>::delete_bucket(self, request.into())
            .await?
            .existed)
    }

    /// Publishes one complete object with a validated typed header.
    async fn typed_put(
        &self,
        request: domain::PutObjectHeader,
        body: Bytes,
    ) -> Result<domain::ObjectInfo, Error> {
        let result = <Self as ObjectsProvider>::put(self, request.into(), body).await?;
        domain::ObjectInfo::try_from(result)
    }

    /// Reads one bounded object and validates its selected range and body size.
    async fn typed_get(
        &self,
        request: domain::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<domain::DownloadedObject, Error> {
        let wire_request = wire::GetObjectRequest::from(request.clone());
        let result = <Self as ObjectsProvider>::get(self, wire_request, maximum_bytes).await?;
        domain::DownloadedObject::try_from_wire(result, &request, maximum_bytes)
    }

    /// Reads object metadata and validates the returned object information.
    async fn typed_head(
        &self,
        request: domain::HeadObjectRequest,
    ) -> Result<domain::ObjectInfo, Error> {
        let result = <Self as ObjectsProvider>::head(self, request.into()).await?;
        domain::ObjectInfo::try_from(result.object.ok_or_else(response::invalid)?)
    }

    /// Deletes an object after converting its validated request.
    async fn typed_delete(
        &self,
        request: domain::DeleteObjectRequest,
    ) -> Result<bool, Error> {
        Ok(<Self as ObjectsProvider>::delete(self, request.into())
            .await?
            .existed)
    }

    /// Traverses a live listing and validates its page against the typed query.
    async fn typed_list(
        &self,
        request: domain::ListObjectsRequest,
    ) -> Result<domain::ListObjectsPage, Error> {
        let result = <Self as ObjectsProvider>::list(self, request.clone().into()).await?;
        domain::ListObjectsPage::try_from_wire(result, &request)
    }

    /// Starts staged multipart work and validates the returned upload identity.
    async fn typed_create_multipart(
        &self,
        request: domain::CreateMultipartRequest,
    ) -> Result<domain::MultipartUpload, Error> {
        let result = <Self as ObjectsProvider>::create_multipart(self, request.into()).await?;
        domain::MultipartUpload::try_from(result)
    }

    /// Publishes one staged part and validates its receipt against the request/body.
    async fn typed_upload_part(
        &self,
        request: domain::UploadPartHeader,
        body: Bytes,
    ) -> Result<domain::CompletedPart, Error> {
        let wire_request = wire::UploadPartHeader::from(request);
        let body_length = body.len() as u64;
        let result =
            <Self as ObjectsProvider>::upload_part(self, wire_request.clone(), body).await?;
        response::validate_binary(
            "multipart/upload-part",
            &wire_request.encode_to_vec(),
            &result.encode_to_vec(),
            body_length,
        )?;
        domain::CompletedPart::try_from(result)
    }

    /// Lists staged parts and validates the page against its typed query.
    async fn typed_list_parts(
        &self,
        request: domain::ListPartsRequest,
    ) -> Result<domain::ListPartsPage, Error> {
        let result = <Self as ObjectsProvider>::list_parts(self, request.clone().into()).await?;
        domain::ListPartsPage::try_from_wire(result, &request)
    }

    /// Publishes selected staged receipts and validates the resulting object info.
    async fn typed_complete_multipart(
        &self,
        request: domain::CompleteMultipartRequest,
    ) -> Result<domain::ObjectInfo, Error> {
        let result =
            <Self as ObjectsProvider>::complete_multipart(self, request.into()).await?;
        domain::ObjectInfo::try_from(result)
    }

    /// Aborts staged multipart work after converting its validated request.
    async fn typed_abort_multipart(
        &self,
        request: domain::AbortMultipartRequest,
    ) -> Result<bool, Error> {
        Ok(<Self as ObjectsProvider>::abort_multipart(self, request.into())
            .await?
            .existed)
    }
}

impl<P> TypedObjectsProvider for P where P: ObjectsProvider + ?Sized {}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{domain, MemoryObjects, MemoryOptions};

    fn bucket() -> domain::BucketName {
        domain::BucketName::try_from("typed.bucket").expect("test bucket is valid")
    }

    #[tokio::test]
    async fn memory_provider_round_trips_all_typed_routes() -> Result<(), Error> {
        let provider = MemoryObjects::new(MemoryOptions::default())?;
        let bucket = bucket();
        let created = provider
            .typed_create_bucket(domain::CreateBucketRequest::new(bucket.clone()))
            .await?;
        assert_eq!(created.name(), &bucket);
        assert_eq!(
            provider
                .typed_head_bucket(domain::HeadBucketRequest::new(bucket.clone()))
                .await?
                .name(),
            &bucket
        );

        let object_key = domain::ObjectKey::try_from("typed-object")?;
        let info = provider
            .typed_put(
                domain::PutObjectHeader::new(bucket.clone(), object_key.clone()),
                Bytes::from_static(b"value"),
            )
            .await?;
        assert_eq!(info.size(), 5);
        let request = domain::GetObjectRequest::new(bucket.clone(), object_key.clone());
        let object = provider.typed_get(request, 5).await?;
        assert_eq!(object.info().size(), 5);
        assert_eq!(object.body(), &Bytes::from_static(b"value"));
        assert_eq!(
            provider
                .typed_head(domain::HeadObjectRequest::new(
                    bucket.clone(),
                    object_key.clone(),
                ))
                .await?
                .size(),
            5
        );
        let page = provider
            .typed_list(domain::ListObjectsRequest::new(bucket.clone()))
            .await?;
        assert_eq!(page.entries().len(), 1);
        assert_eq!(page.entries()[0].0.as_str(), "typed-object");

        let upload_key = domain::ObjectKey::try_from("typed-multipart")?;
        let upload = provider
            .typed_create_multipart(domain::CreateMultipartRequest::new(
                bucket.clone(),
                upload_key.clone(),
            ))
            .await?;
        let part_request = domain::UploadPartHeader::new(
            bucket.clone(),
            upload_key.clone(),
            upload.upload_id().clone(),
            domain::PartNumber::try_from(1)?,
        );
        let part = provider
            .typed_upload_part(part_request, Bytes::from_static(b"part"))
            .await?;
        let parts = provider
            .typed_list_parts(domain::ListPartsRequest::new(
                bucket.clone(),
                upload_key.clone(),
                upload.upload_id().clone(),
            ))
            .await?;
        assert_eq!(parts.parts(), &[part.clone()]);
        let completed = provider
            .typed_complete_multipart(
                domain::CompleteMultipartRequest::new(
                    bucket.clone(),
                    upload_key.clone(),
                    upload.upload_id().clone(),
                    vec![part],
                )?,
            )
            .await?;
        assert_eq!(completed.size(), 4);

        let aborted_key = domain::ObjectKey::try_from("typed-aborted")?;
        let aborted = provider
            .typed_create_multipart(domain::CreateMultipartRequest::new(
                bucket.clone(),
                aborted_key.clone(),
            ))
            .await?;
        assert!(provider
            .typed_abort_multipart(domain::AbortMultipartRequest::new(
                bucket.clone(),
                aborted_key,
                aborted.upload_id().clone(),
            ))
            .await?);

        assert!(provider
            .typed_delete(domain::DeleteObjectRequest::new(bucket.clone(), object_key))
            .await?);
        assert!(provider
            .typed_delete(domain::DeleteObjectRequest::new(bucket.clone(), upload_key))
            .await?);
        assert!(provider
            .typed_delete_bucket(domain::DeleteBucketRequest::new(bucket))
            .await?);
        Ok(())
    }

    #[test]
    fn typed_response_wrappers_reject_malformed_wire_values() -> Result<(), Error> {
        let bucket = bucket();
        let object_key = domain::ObjectKey::try_from("typed-object")?;
        let request = domain::GetObjectRequest::new(bucket.clone(), object_key);
        let header = wire::GetObjectHeader {
            object: Some(wire::ObjectInfo {
                etag: "etag".into(),
                size: 5,
                last_modified: Some(prost_types::Timestamp::default()),
                ..Default::default()
            }),
            content_range: None,
        };
        assert!(domain::DownloadedObject::try_from_wire(
            super::super::Object {
                header,
                body: Bytes::from_static(b"bad"),
            },
            &request,
            5,
        )
        .is_err());

        let list_request = domain::ListObjectsRequest::new(bucket.clone());
        assert!(domain::ListObjectsPage::try_from_wire(
            wire::ListObjectsResponse {
                continuation_token: "unexpected".into(),
                ..Default::default()
            },
            &list_request,
        )
        .is_err());

        let parts_request = domain::ListPartsRequest::new(
            bucket,
            domain::ObjectKey::try_from("typed-multipart")?,
            domain::UploadId::try_from("upload-1")?,
        );
        assert!(domain::ListPartsPage::try_from_wire(
            wire::ListPartsResponse {
                parts: vec![wire::UploadedPart {
                    part_number: 1,
                    etag: String::new(),
                    size: 1,
                }],
                ..Default::default()
            },
            &parts_request,
        )
        .is_err());
        Ok(())
    }
}
