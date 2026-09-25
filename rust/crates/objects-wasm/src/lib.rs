//! Browser binding for the canonical Rust Objects memory provider.

#[cfg(target_arch = "wasm32")]
mod browser {
    use acyclic_objects::{
        Condition, GetRequest, MemoryObjects, ObjectsError, ObjectsProvider, PutRequest,
        ReadTarget, wire,
    };
    use bytes::Bytes;
    use wasm_bindgen::prelude::*;

    fn decode<T: prost::Message + Default>(bytes: &[u8]) -> Result<T, JsValue> {
        T::decode(bytes).map_err(|_| error(ObjectsError::Invalid("malformed protobuf request")))
    }
    fn encode<T: prost::Message>(value: T) -> Vec<u8> {
        value.encode_to_vec()
    }
    fn required<T>(value: Option<T>) -> Result<T, JsValue> {
        value.ok_or_else(|| error(ObjectsError::Invalid("required field is missing")))
    }
    fn optional(value: String) -> Option<String> {
        (!value.is_empty()).then_some(value)
    }
    fn mutation(value: Option<wire::MutationIdentity>) -> Option<String> {
        value.map(|value| value.idempotency_key)
    }
    fn condition(value: Option<wire::Preconditions>) -> Result<Option<Condition>, JsValue> {
        match value.and_then(|value| value.condition) {
            None => Ok(None),
            Some(wire::preconditions::Condition::IfAbsent(true)) => Ok(Some(Condition::IfAbsent)),
            Some(wire::preconditions::Condition::IfMatch(value)) => {
                Ok(Some(Condition::IfMatch(value)))
            }
            Some(wire::preconditions::Condition::IfVersion(value)) => {
                Ok(Some(Condition::IfVersion(value)))
            }
            _ => Err(error(ObjectsError::Invalid("invalid precondition"))),
        }
    }
    fn target(value: wire::ReadTarget) -> Result<ReadTarget, JsValue> {
        match required(value.target)? {
            wire::read_target::Target::Bucket(value) => Ok(ReadTarget::Bucket(value)),
            wire::read_target::Target::Snapshot(value) => Ok(ReadTarget::Snapshot(value)),
        }
    }
    fn error(value: ObjectsError) -> JsValue {
        let code = match value {
            ObjectsError::Invalid(message) if message.contains("not empty") => "bucket_not_empty",
            ObjectsError::Invalid(message) if message.contains("continuation") => {
                "invalid_continuation"
            }
            ObjectsError::Invalid(message) if message.contains("range") => "invalid_range",
            ObjectsError::Invalid(message)
                if message.contains("page") || message == "invalid listing" =>
            {
                "invalid_page_size"
            }
            ObjectsError::Invalid(message)
                if message.contains("part") || message.contains("multipart") =>
            {
                "invalid_part"
            }
            ObjectsError::Invalid(message) if message.contains("bucket") => "invalid_name",
            ObjectsError::Invalid(_) => "invalid_key",
            ObjectsError::NotFound => "not_found",
            ObjectsError::AlreadyExists => "bucket_exists",
            ObjectsError::PreconditionFailed => "precondition_failed",
            ObjectsError::IdempotencyMismatch => "idempotency_mismatch",
            ObjectsError::Capacity => "capacity_exhausted",
            ObjectsError::Unsupported => "invalid_part",
            ObjectsError::Unauthorized => "not_found",
            ObjectsError::Unavailable => "not_found",
        };
        error_with_code(value, code)
    }
    fn error_with_code(value: ObjectsError, code: &str) -> JsValue {
        let message = js_sys::Error::new(&value.to_string());
        let _ = js_sys::Reflect::set(
            &message,
            &JsValue::from_str("code"),
            &JsValue::from_str(code),
        );
        message.into()
    }

    #[wasm_bindgen]
    pub struct MemoryObjectsBinding {
        inner: MemoryObjects,
    }

    #[wasm_bindgen]
    impl MemoryObjectsBinding {
        #[wasm_bindgen(constructor)]
        pub fn new() -> Self {
            Self {
                inner: MemoryObjects::default(),
            }
        }

        pub async fn create_bucket(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::CreateBucketRequest = decode(&request)?;
            self.inner
                .create_bucket(request.name, mutation(request.mutation))
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn head_bucket(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::HeadBucketRequest = decode(&request)?;
            self.inner
                .head_bucket(&required(request.bucket)?)
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn delete_bucket(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::DeleteBucketRequest = decode(&request)?;
            let existed = self
                .inner
                .delete_bucket(&required(request.bucket)?, mutation(request.mutation))
                .await
                .map_err(|value| {
                    if value == ObjectsError::PreconditionFailed {
                        error_with_code(value, "bucket_not_empty")
                    } else {
                        error(value)
                    }
                })?;
            Ok(encode(wire::DeleteBucketResponse { existed }))
        }
        pub async fn put(&self, header: Vec<u8>, body: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let header: wire::PutObjectHeader = decode(&header)?;
            let request = PutRequest {
                bucket: required(header.bucket)?,
                object_key: header.object_key,
                body: Bytes::from(body),
                metadata: required(header.metadata)?,
                condition: condition(header.preconditions)?,
                idempotency_key: mutation(header.mutation),
            };
            self.inner.put(request).await.map(encode).map_err(error)
        }
        pub async fn get(&self, request: Vec<u8>) -> Result<JsValue, JsValue> {
            let request: wire::GetObjectRequest = decode(&request)?;
            let range = request
                .range_requested
                .then_some((request.range_start, request.range_end_inclusive));
            let object = self
                .inner
                .get(GetRequest {
                    target: target(required(request.target)?)?,
                    object_key: request.object_key,
                    version_id: optional(request.version_id),
                    range,
                    if_match: optional(request.if_match),
                    if_none_match: optional(request.if_none_match),
                    maximum_bytes: 64 * 1024 * 1024,
                })
                .await
                .map_err(error)?;
            let frames = js_sys::Array::new();
            frames.push(&js_sys::Uint8Array::from(encode(object.version).as_slice()));
            frames.push(&js_sys::Uint8Array::from(object.body.as_ref()));
            Ok(frames.into())
        }
        pub async fn head(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::HeadObjectRequest = decode(&request)?;
            let version = self
                .inner
                .head(GetRequest {
                    target: target(required(request.target)?)?,
                    object_key: request.object_key,
                    version_id: optional(request.version_id),
                    range: None,
                    if_match: optional(request.if_match),
                    if_none_match: optional(request.if_none_match),
                    maximum_bytes: 0,
                })
                .await
                .map_err(error)?;
            Ok(encode(wire::HeadObjectResponse {
                version: Some(version),
            }))
        }
        pub async fn delete(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::DeleteObjectRequest = decode(&request)?;
            let result = self
                .inner
                .delete(
                    required(request.bucket)?,
                    request.object_key,
                    optional(request.version_id),
                    condition(request.preconditions)?,
                    mutation(request.mutation),
                )
                .await
                .map_err(error)?;
            Ok(encode(wire::DeleteObjectResponse {
                existed: result.existed,
                version: result.marker,
            }))
        }
        pub async fn list(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ListObjectsRequest = decode(&request)?;
            let result = self
                .inner
                .list(
                    target(required(request.target)?)?,
                    request.prefix,
                    optional(request.delimiter),
                    request.mode == wire::ListingMode::Versions as i32,
                    request.page_size,
                    optional(request.continuation_token),
                )
                .await
                .map_err(error)?;
            Ok(encode(wire::ListObjectsResponse {
                entries: result.entries,
                common_prefixes: result.common_prefixes,
                continuation_token: result.continuation.unwrap_or_default(),
            }))
        }
        pub async fn snapshot(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::CreateSnapshotRequest = decode(&request)?;
            self.inner
                .snapshot(required(request.bucket)?, mutation(request.mutation))
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn destroy_snapshot(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::DestroySnapshotRequest = decode(&request)?;
            let existed = self
                .inner
                .destroy_snapshot(required(request.snapshot)?, mutation(request.mutation))
                .await
                .map_err(error)?;
            Ok(encode(wire::DestroySnapshotResponse { existed }))
        }
        pub async fn fork_bucket(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ForkBucketRequest = decode(&request)?;
            self.inner
                .fork(
                    ReadTarget::Bucket(required(request.source)?),
                    request.destination_name,
                    mutation(request.mutation),
                )
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn fork_snapshot(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ForkSnapshotRequest = decode(&request)?;
            self.inner
                .fork(
                    ReadTarget::Snapshot(required(request.snapshot)?),
                    request.destination_name,
                    mutation(request.mutation),
                )
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn create_multipart(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::CreateMultipartRequest = decode(&request)?;
            self.inner
                .create_multipart(
                    required(request.bucket)?,
                    request.object_key,
                    required(request.metadata)?,
                    condition(request.preconditions)?,
                    mutation(request.mutation),
                )
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn upload_part(
            &self,
            header: Vec<u8>,
            body: Vec<u8>,
        ) -> Result<Vec<u8>, JsValue> {
            let header: wire::UploadPartHeader = decode(&header)?;
            self.inner
                .upload_part(
                    required(header.bucket)?,
                    header.object_key,
                    header.upload_id,
                    header.part_number,
                    Bytes::from(body),
                    mutation(header.mutation),
                )
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn list_parts(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ListPartsRequest = decode(&request)?;
            let parts = self
                .inner
                .list_parts(
                    required(request.bucket)?,
                    request.object_key,
                    request.upload_id,
                )
                .await
                .map_err(error)?;
            Ok(encode(wire::ListPartsResponse { parts }))
        }
        pub async fn complete_multipart(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::CompleteMultipartRequest = decode(&request)?;
            self.inner
                .complete_multipart(
                    required(request.bucket)?,
                    request.object_key,
                    request.upload_id,
                    request.parts,
                    mutation(request.mutation),
                )
                .await
                .map(encode)
                .map_err(error)
        }
        pub async fn abort_multipart(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::AbortMultipartRequest = decode(&request)?;
            let existed = self
                .inner
                .abort_multipart(
                    required(request.bucket)?,
                    request.object_key,
                    request.upload_id,
                    mutation(request.mutation),
                )
                .await
                .map_err(error)?;
            Ok(encode(wire::AbortMultipartResponse { existed }))
        }
    }
}
