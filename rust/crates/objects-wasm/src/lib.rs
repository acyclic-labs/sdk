//! WebAssembly adapter for the canonical Objects reference provider.

#![cfg(target_arch = "wasm32")]

use acyclic_objects::{
    Condition, GetRequest, MemoryObjects, ObjectsError, ObjectsProvider, PutRequest, ReadTarget,
    wire,
};
use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;

mod http;
mod memory_projection;

// Keep the memory response relationship in Rust beside the projector. The WASM declaration
// receives this section from wasm-bindgen, while the build script only specializes the exported
// function's operation generic.
macro_rules! define_memory_response_contract {
    ($( $variant:ident => $operation:literal => $response:literal ),+ $(,)?) => {
        #[derive(Clone, Copy)]
        pub(crate) enum MemoryResponseOperation {
            $( $variant ),+
        }

        impl MemoryResponseOperation {
            pub(crate) fn parse(value: &str) -> Result<Self, String> {
                match value {
                    $( $operation => Ok(Self::$variant), )+
                    _ => Err(format!("unsupported Objects response operation: {value}")),
                }
            }
        }

        #[wasm_bindgen(typescript_custom_section)]
        const MEMORY_RESPONSE_CONTRACT: &'static str = concat!(
            "import type { BucketRef, ListPage, ObjectVersion, SnapshotRef, UploadedPart } from \"@acyclic-labs/objects\";\n\n",
            "export type MemoryResponseOperation =\n",
            $("  | \"", $operation, "\"\n",)+
            ";\n",
            "export interface MemoryResponseMap {\n",
            $("  ", $operation, ": ", $response, ";\n",)+
            "}\n\n",
            "export type MemoryResponseFor<Operation extends MemoryResponseOperation> = MemoryResponseMap[Operation];\n",
        );
    };
}

define_memory_response_contract!(
    CreateBucket => "create_bucket" => "BucketRef",
    HeadBucket => "head_bucket" => "BucketRef",
    ForkBucket => "fork_bucket" => "BucketRef",
    ForkSnapshot => "fork_snapshot" => "BucketRef",
    DeleteBucket => "delete_bucket" => "{ readonly existed: boolean }",
    AbortMultipart => "abort_multipart" => "{ readonly existed: boolean }",
    DestroySnapshot => "destroy_snapshot" => "{ readonly existed: boolean }",
    Put => "put" => "ObjectVersion",
    Head => "head" => "ObjectVersion",
    GetVersion => "get_version" => "ObjectVersion",
    CompleteMultipart => "complete_multipart" => "ObjectVersion",
    Delete => "delete" => "{ readonly existed: boolean; readonly marker?: ObjectVersion }",
    List => "list" => "ListPage",
    Snapshot => "snapshot" => "SnapshotRef",
    CreateMultipart => "create_multipart" => "{ readonly uploadId: string }",
    UploadPart => "upload_part" => "UploadedPart",
    ListParts => "list_parts" => "readonly UploadedPart[]",
);

fn invalid(message: &'static str) -> ObjectsError {
    ObjectsError::Invalid(message)
}

fn required<T>(value: Option<T>, message: &'static str) -> Result<T, ObjectsError> {
    value.ok_or_else(|| invalid(message))
}

fn mutation(value: Option<wire::MutationIdentity>) -> Option<String> {
    value.and_then(|identity| {
        (!identity.idempotency_key.is_empty()).then_some(identity.idempotency_key)
    })
}

fn condition(value: Option<wire::Preconditions>) -> Result<Option<Condition>, ObjectsError> {
    let Some(preconditions) = value else {
        return Ok(None);
    };
    let Some(condition) = preconditions.condition else {
        return Err(invalid("preconditions must contain a condition"));
    };
    Ok(Some(match condition {
        wire::preconditions::Condition::IfAbsent(value) if value => Condition::IfAbsent,
        wire::preconditions::Condition::IfAbsent(_) => {
            return Err(invalid("if_absent must be true"));
        }
        wire::preconditions::Condition::IfMatch(value) => Condition::IfMatch(value),
        wire::preconditions::Condition::IfVersion(value) => Condition::IfVersion(value),
    }))
}

fn target(value: Option<wire::ReadTarget>) -> Result<ReadTarget, ObjectsError> {
    match required(value, "read target is required")?.target {
        Some(wire::read_target::Target::Bucket(bucket)) => Ok(ReadTarget::Bucket(bucket)),
        Some(wire::read_target::Target::Snapshot(snapshot)) => Ok(ReadTarget::Snapshot(snapshot)),
        None => Err(invalid("read target is required")),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Tsify)]
#[serde(rename_all = "snake_case")]
#[tsify(from_wasm_abi, into_wasm_abi)]
#[allow(missing_docs)]
pub enum ObjectsErrorCode {
    InvalidArgument,
    InvalidRange,
    InvalidPageSize,
    InvalidPart,
    InvalidContinuation,
    InvalidName,
    InvalidKey,
    NotFound,
    BucketExists,
    PreconditionFailed,
    IdempotencyMismatch,
    QuotaExceeded,
    Unsupported,
    Unauthorized,
    Unavailable,
    BucketNotEmpty,
}

impl ObjectsErrorCode {
    #[cfg(test)]
    const ALL: [Self; 16] = [
        Self::InvalidArgument,
        Self::InvalidRange,
        Self::InvalidPageSize,
        Self::InvalidPart,
        Self::InvalidContinuation,
        Self::InvalidName,
        Self::InvalidKey,
        Self::NotFound,
        Self::BucketExists,
        Self::PreconditionFailed,
        Self::IdempotencyMismatch,
        Self::QuotaExceeded,
        Self::Unsupported,
        Self::Unauthorized,
        Self::Unavailable,
        Self::BucketNotEmpty,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::InvalidRange => "invalid_range",
            Self::InvalidPageSize => "invalid_page_size",
            Self::InvalidPart => "invalid_part",
            Self::InvalidContinuation => "invalid_continuation",
            Self::InvalidName => "invalid_name",
            Self::InvalidKey => "invalid_key",
            Self::NotFound => "not_found",
            Self::BucketExists => "bucket_exists",
            Self::PreconditionFailed => "precondition_failed",
            Self::IdempotencyMismatch => "idempotency_mismatch",
            Self::QuotaExceeded => "quota_exceeded",
            Self::Unsupported => "unsupported",
            Self::Unauthorized => "unauthorized",
            Self::Unavailable => "unavailable",
            Self::BucketNotEmpty => "bucket_not_empty",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "invalid_argument" => Self::InvalidArgument,
            "invalid_range" => Self::InvalidRange,
            "invalid_page_size" => Self::InvalidPageSize,
            "invalid_part" => Self::InvalidPart,
            "invalid_continuation" => Self::InvalidContinuation,
            "invalid_name" => Self::InvalidName,
            "invalid_key" => Self::InvalidKey,
            "not_found" => Self::NotFound,
            "bucket_exists" => Self::BucketExists,
            "precondition_failed" => Self::PreconditionFailed,
            "idempotency_mismatch" => Self::IdempotencyMismatch,
            "quota_exceeded" => Self::QuotaExceeded,
            "unsupported" => Self::Unsupported,
            "unauthorized" => Self::Unauthorized,
            "unavailable" => Self::Unavailable,
            "bucket_not_empty" => Self::BucketNotEmpty,
            _ => return None,
        })
    }
}

fn code(error: &ObjectsError) -> ObjectsErrorCode {
    match error {
        ObjectsError::Invalid(message) => match *message {
            "invalid range" => ObjectsErrorCode::InvalidRange,
            "invalid listing" => ObjectsErrorCode::InvalidPageSize,
            "invalid multipart part"
            | "invalid multipart completion"
            | "multipart receipts do not match" => ObjectsErrorCode::InvalidPart,
            "invalid continuation" => ObjectsErrorCode::InvalidContinuation,
            "invalid bucket name" => ObjectsErrorCode::InvalidName,
            "invalid object key" => ObjectsErrorCode::InvalidKey,
            _ => ObjectsErrorCode::InvalidArgument,
        },
        ObjectsError::NotFound => ObjectsErrorCode::NotFound,
        ObjectsError::AlreadyExists => ObjectsErrorCode::BucketExists,
        ObjectsError::PreconditionFailed => ObjectsErrorCode::PreconditionFailed,
        ObjectsError::IdempotencyMismatch => ObjectsErrorCode::IdempotencyMismatch,
        ObjectsError::Capacity => ObjectsErrorCode::QuotaExceeded,
        ObjectsError::Unsupported => ObjectsErrorCode::Unsupported,
        ObjectsError::Unauthorized => ObjectsErrorCode::Unauthorized,
        ObjectsError::Unavailable => ObjectsErrorCode::Unavailable,
    }
}

fn js_error_with_code(error: &ObjectsError, public_code: ObjectsErrorCode) -> JsValue {
    js_error_message_with_code(&error.to_string(), public_code)
}

fn js_error_message_with_code(message: &str, public_code: ObjectsErrorCode) -> JsValue {
    let value = js_sys::Error::new(message);
    let _property_result = js_sys::Reflect::set(
        value.as_ref(),
        &JsValue::from_str("code"),
        &JsValue::from_str(public_code.as_str()),
    );
    value.into()
}

fn js_error(error: &ObjectsError) -> JsValue {
    js_error_with_code(error, code(error))
}

/// Return whether a code can be emitted by this WASM adapter.
///
/// Keeping this validator beside the Rust error mapping prevents the TypeScript adapter from
/// maintaining a second, potentially stale list of public error codes.
#[wasm_bindgen]
pub fn is_objects_error_code(value: &str) -> bool {
    ObjectsErrorCode::from_str(value).is_some()
}

/// Project a hosted error code through the Rust-owned public Objects contract.
/// The delete-bucket route uses a more specific public precondition error.
#[wasm_bindgen(js_name = publicHttpErrorCode)]
pub fn public_http_error_code(raw: &str, route: &str) -> Option<ObjectsErrorCode> {
    let code = ObjectsErrorCode::from_str(raw)?;
    let code = if route == "buckets/delete" && code == ObjectsErrorCode::PreconditionFailed {
        ObjectsErrorCode::BucketNotEmpty
    } else {
        code
    };
    Some(code)
}

/// Type-only bridge for the complete Rust-owned Objects error-code contract.
#[wasm_bindgen(js_name = __objectsErrorCodeContract)]
pub fn objects_error_code_contract(value: ObjectsErrorCode) -> ObjectsErrorCode {
    value
}

/// Maximum number of entries accepted in one listing page.
#[wasm_bindgen]
pub fn objects_list_page_entries() -> u32 {
    acyclic_objects::limits::LIST_PAGE_ENTRIES
}

/// Maximum number of parts accepted in one multipart upload.
#[wasm_bindgen]
pub fn objects_multipart_parts() -> u32 {
    acyclic_objects::limits::MULTIPART_PARTS
}

/// Decode one unary memory-provider response into the public JavaScript result shape.
///
/// The protobuf response remains the canonical wire contract. Rust owns the response projection
/// so uint64 values, metadata maps, timestamps, and optional fields have the same behavior for
/// the memory and hosted providers.
#[wasm_bindgen(js_name = projectMemoryResponse, unchecked_return_type = "unknown")]
pub fn project_memory_response(operation: &str, input: &[u8]) -> Result<JsValue, JsValue> {
    memory_projection::project(operation, input).map_err(|error| match error {
        memory_projection::ProjectionError::Invalid(message) => {
            js_error_message_with_code(&message, ObjectsErrorCode::InvalidArgument)
        }
        memory_projection::ProjectionError::Unavailable(message) => {
            js_error_message_with_code(&message, ObjectsErrorCode::Unavailable)
        }
    })
}

fn operation_error(operation: &str, error: &ObjectsError) -> JsValue {
    if operation == "delete_bucket" && matches!(error, ObjectsError::PreconditionFailed) {
        js_error_with_code(error, ObjectsErrorCode::BucketNotEmpty)
    } else {
        js_error(error)
    }
}

fn encode<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

fn decode<M: Message + Default>(bytes: &[u8]) -> Result<M, JsValue> {
    M::decode(bytes).map_err(|_| JsValue::from_str("invalid protobuf request"))
}

fn bucket(value: Option<wire::BucketRef>) -> Result<wire::BucketRef, ObjectsError> {
    required(value, "bucket reference is required")
}

fn metadata(value: Option<wire::ObjectMetadata>) -> wire::ObjectMetadata {
    value.unwrap_or_default()
}

/// Result of a canonical buffered object read.
#[wasm_bindgen]
pub struct WasmGetResult {
    version: Vec<u8>,
    body: Vec<u8>,
}

#[wasm_bindgen]
impl WasmGetResult {
    /// Encoded `ObjectVersion` descriptor.
    #[wasm_bindgen(getter)]
    pub fn version(&self) -> Vec<u8> {
        self.version.clone()
    }

    /// Selected object body bytes.
    #[wasm_bindgen(getter)]
    pub fn body(&self) -> Vec<u8> {
        self.body.clone()
    }
}

/// Stateful per-instance adapter around `acyclic_objects::MemoryObjects`.
#[wasm_bindgen]
pub struct WasmMemoryObjects {
    inner: MemoryObjects,
    maximum_bytes: u64,
}

#[wasm_bindgen]
impl WasmMemoryObjects {
    /// Construct an empty provider with an aggregate byte ceiling.
    #[wasm_bindgen(constructor)]
    pub fn new(maximum_bytes: u64) -> Result<WasmMemoryObjects, JsValue> {
        let inner = MemoryObjects::new(maximum_bytes).map_err(|error| js_error(&error))?;
        Ok(Self {
            inner,
            maximum_bytes,
        })
    }

    /// Execute one protobuf operation. Request and result are encoded public wire messages.
    /// `put` and `upload_part` take their body in the optional third argument.
    pub async fn dispatch(
        &self,
        operation: String,
        request_bytes: &[u8],
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, JsValue> {
        let result = match operation.as_str() {
            "create_bucket" => self
                .create_bucket(request_bytes)
                .await
                .map(|value| encode(&value)),
            "head_bucket" => self
                .head_bucket(request_bytes)
                .await
                .map(|value| encode(&value)),
            "delete_bucket" => self
                .delete_bucket(request_bytes)
                .await
                .map(|value| encode(&value)),
            "put" => self
                .put(request_bytes, body.unwrap_or_default())
                .await
                .map(|value| encode(&value)),
            "head" => self.head(request_bytes).await.map(|value| encode(&value)),
            "delete" => self.delete(request_bytes).await.map(|value| encode(&value)),
            "list" => self.list(request_bytes).await.map(|value| encode(&value)),
            "snapshot" => self
                .snapshot(request_bytes)
                .await
                .map(|value| encode(&value)),
            "destroy_snapshot" => self
                .destroy_snapshot(request_bytes)
                .await
                .map(|value| encode(&value)),
            "fork_bucket" => self
                .fork_bucket(request_bytes)
                .await
                .map(|value| encode(&value)),
            "fork_snapshot" => self
                .fork_snapshot(request_bytes)
                .await
                .map(|value| encode(&value)),
            "create_multipart" => self
                .create_multipart(request_bytes)
                .await
                .map(|value| encode(&value)),
            "upload_part" => self
                .upload_part(request_bytes, body.unwrap_or_default())
                .await
                .map(|value| encode(&value)),
            "list_parts" => self
                .list_parts(request_bytes)
                .await
                .map(|value| encode(&value)),
            "complete_multipart" => self
                .complete_multipart(request_bytes)
                .await
                .map(|value| encode(&value)),
            "abort_multipart" => self
                .abort_multipart(request_bytes)
                .await
                .map(|value| encode(&value)),
            "get" => {
                return Err(JsValue::from_str(
                    "get returns a structured result; use get",
                ));
            }
            _ => return Err(JsValue::from_str("unsupported Objects operation")),
        };
        result.map_err(|error| operation_error(&operation, &error))
    }

    /// Read one object and return its descriptor and selected body separately.
    pub async fn get(&self, request_bytes: &[u8]) -> Result<WasmGetResult, JsValue> {
        let request: wire::GetObjectRequest = decode(request_bytes)?;
        let range = if request.range_requested {
            Some((request.range_start, request.range_end_inclusive))
        } else {
            if request.range_end_inclusive.is_some() || request.range_start != 0 {
                return Err(js_error(&invalid("range fields require range_requested")));
            }
            None
        };
        let result = self
            .inner
            .get(GetRequest {
                target: target(request.target).map_err(|error| js_error(&error))?,
                object_key: request.object_key,
                version_id: (!request.version_id.is_empty()).then_some(request.version_id),
                range,
                if_match: (!request.if_match.is_empty()).then_some(request.if_match),
                if_none_match: (!request.if_none_match.is_empty()).then_some(request.if_none_match),
                maximum_bytes: self.maximum_bytes,
            })
            .await
            .map_err(|error| js_error(&error))?;
        Ok(WasmGetResult {
            version: result.version.encode_to_vec(),
            body: result.body.to_vec(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{ObjectsError, ObjectsErrorCode, code, is_objects_error_code};

    #[test]
    fn invalid_messages_have_stable_public_codes() {
        for (message, expected) in [
            ("invalid range", "invalid_range"),
            ("invalid listing", "invalid_page_size"),
            ("invalid multipart part", "invalid_part"),
            ("invalid multipart completion", "invalid_part"),
            ("multipart receipts do not match", "invalid_part"),
            ("invalid continuation", "invalid_continuation"),
            ("invalid bucket name", "invalid_name"),
            ("invalid object key", "invalid_key"),
            ("invalid metadata", "invalid_argument"),
        ] {
            assert_eq!(code(&ObjectsError::Invalid(message)).as_str(), expected);
        }
        assert_eq!(
            code(&ObjectsError::AlreadyExists),
            ObjectsErrorCode::BucketExists
        );
    }

    #[test]
    fn validator_covers_the_complete_public_error_domain() {
        for code in ObjectsErrorCode::ALL {
            assert!(is_objects_error_code(code.as_str()));
        }
        assert!(!is_objects_error_code("token_expired"));
        assert!(!is_objects_error_code("unknown"));
    }
}

impl WasmMemoryObjects {
    async fn create_bucket(&self, bytes: &[u8]) -> Result<wire::Bucket, ObjectsError> {
        let request: wire::CreateBucketRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner
            .create_bucket(request.name, mutation(request.mutation))
            .await
    }

    async fn head_bucket(&self, bytes: &[u8]) -> Result<wire::Bucket, ObjectsError> {
        let request: wire::HeadBucketRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner.head_bucket(&bucket(request.bucket)?).await
    }

    async fn delete_bucket(
        &self,
        bytes: &[u8],
    ) -> Result<wire::DeleteBucketResponse, ObjectsError> {
        let request: wire::DeleteBucketRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let existed = self
            .inner
            .delete_bucket(&bucket(request.bucket)?, mutation(request.mutation))
            .await?;
        Ok(wire::DeleteBucketResponse { existed })
    }

    async fn put(&self, bytes: &[u8], body: Vec<u8>) -> Result<wire::ObjectVersion, ObjectsError> {
        let request: wire::PutObjectHeader =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let bucket = bucket(request.bucket)?;
        self.inner
            .put(PutRequest {
                bucket,
                object_key: request.object_key,
                body: Bytes::from(body),
                metadata: metadata(request.metadata),
                condition: condition(request.preconditions)?,
                idempotency_key: mutation(request.mutation),
            })
            .await
    }

    async fn head(&self, bytes: &[u8]) -> Result<wire::HeadObjectResponse, ObjectsError> {
        let request: wire::HeadObjectRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let object = self
            .inner
            .get(GetRequest {
                target: target(request.target)?,
                object_key: request.object_key,
                version_id: (!request.version_id.is_empty()).then_some(request.version_id),
                range: None,
                if_match: (!request.if_match.is_empty()).then_some(request.if_match),
                if_none_match: (!request.if_none_match.is_empty()).then_some(request.if_none_match),
                maximum_bytes: self.maximum_bytes,
            })
            .await?;
        Ok(wire::HeadObjectResponse {
            version: Some(object.version),
        })
    }

    async fn delete(&self, bytes: &[u8]) -> Result<wire::DeleteObjectResponse, ObjectsError> {
        let request: wire::DeleteObjectRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let result = self
            .inner
            .delete(
                bucket(request.bucket)?,
                request.object_key,
                (!request.version_id.is_empty()).then_some(request.version_id),
                condition(request.preconditions)?,
                mutation(request.mutation),
            )
            .await?;
        Ok(wire::DeleteObjectResponse {
            existed: result.existed,
            version: result.marker,
        })
    }

    async fn list(&self, bytes: &[u8]) -> Result<wire::ListObjectsResponse, ObjectsError> {
        let request: wire::ListObjectsRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let mode =
            wire::ListingMode::try_from(request.mode).unwrap_or(wire::ListingMode::Unspecified);
        let page = self
            .inner
            .list(
                target(request.target)?,
                request.prefix,
                (!request.delimiter.is_empty()).then_some(request.delimiter),
                matches!(mode, wire::ListingMode::Versions),
                request.page_size,
                (!request.continuation_token.is_empty()).then_some(request.continuation_token),
            )
            .await?;
        Ok(wire::ListObjectsResponse {
            entries: page.entries,
            common_prefixes: page.common_prefixes,
            continuation_token: page.continuation.unwrap_or_default(),
        })
    }

    async fn snapshot(&self, bytes: &[u8]) -> Result<wire::Snapshot, ObjectsError> {
        let request: wire::CreateSnapshotRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner
            .snapshot(bucket(request.bucket)?, mutation(request.mutation))
            .await
    }

    async fn destroy_snapshot(
        &self,
        bytes: &[u8],
    ) -> Result<wire::DestroySnapshotResponse, ObjectsError> {
        let request: wire::DestroySnapshotRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let snapshot = required(request.snapshot, "snapshot reference is required")?;
        let existed = self
            .inner
            .destroy_snapshot(snapshot, mutation(request.mutation))
            .await?;
        Ok(wire::DestroySnapshotResponse { existed })
    }

    async fn fork_bucket(&self, bytes: &[u8]) -> Result<wire::Bucket, ObjectsError> {
        let request: wire::ForkBucketRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner
            .fork(
                ReadTarget::Bucket(bucket(request.source)?),
                request.destination_name,
                mutation(request.mutation),
            )
            .await
    }

    async fn fork_snapshot(&self, bytes: &[u8]) -> Result<wire::Bucket, ObjectsError> {
        let request: wire::ForkSnapshotRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let snapshot = required(request.snapshot, "snapshot reference is required")?;
        self.inner
            .fork(
                ReadTarget::Snapshot(snapshot),
                request.destination_name,
                mutation(request.mutation),
            )
            .await
    }

    async fn create_multipart(&self, bytes: &[u8]) -> Result<wire::MultipartUpload, ObjectsError> {
        let request: wire::CreateMultipartRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner
            .create_multipart(
                bucket(request.bucket)?,
                request.object_key,
                metadata(request.metadata),
                condition(request.preconditions)?,
                mutation(request.mutation),
            )
            .await
    }

    async fn upload_part(
        &self,
        bytes: &[u8],
        body: Vec<u8>,
    ) -> Result<wire::UploadedPart, ObjectsError> {
        let request: wire::UploadPartHeader =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner
            .upload_part(
                bucket(request.bucket)?,
                request.object_key,
                request.upload_id,
                request.part_number,
                Bytes::from(body),
                mutation(request.mutation),
            )
            .await
    }

    async fn list_parts(&self, bytes: &[u8]) -> Result<wire::ListPartsResponse, ObjectsError> {
        let request: wire::ListPartsRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let parts = self
            .inner
            .list_parts(
                bucket(request.bucket)?,
                request.object_key,
                request.upload_id,
            )
            .await?;
        Ok(wire::ListPartsResponse { parts })
    }

    async fn complete_multipart(&self, bytes: &[u8]) -> Result<wire::ObjectVersion, ObjectsError> {
        let request: wire::CompleteMultipartRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        self.inner
            .complete_multipart(
                bucket(request.bucket)?,
                request.object_key,
                request.upload_id,
                request.parts,
                mutation(request.mutation),
            )
            .await
    }

    async fn abort_multipart(
        &self,
        bytes: &[u8],
    ) -> Result<wire::AbortMultipartResponse, ObjectsError> {
        let request: wire::AbortMultipartRequest =
            decode(bytes).map_err(|_| invalid("invalid protobuf request"))?;
        let existed = self
            .inner
            .abort_multipart(
                bucket(request.bucket)?,
                request.object_key,
                request.upload_id,
                mutation(request.mutation),
            )
            .await?;
        Ok(wire::AbortMultipartResponse { existed })
    }
}
