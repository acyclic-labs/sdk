//! Natural JavaScript projection for unary memory-provider responses.
//!
//! The memory provider still uses protobuf bytes as its transport boundary.  This module owns
//! the second, public boundary: uint64 values become `bigint`, metadata maps become `Map`, and
//! protobuf timestamps become `Date` values through the shared Objects HTTP projector.

use acyclic_objects::wire;
use prost::Message;
use prost_types::Timestamp;
use serde_json::{Map, Value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use wasm_bindgen::JsValue;

use super::MemoryResponseOperation;

pub(crate) enum ProjectionError {
    Invalid(String),
    Unavailable(String),
}

type Result<T> = std::result::Result<T, ProjectionError>;

#[allow(clippy::too_many_lines)]
pub(crate) fn project(operation: &str, input: &[u8]) -> Result<JsValue> {
    let operation = MemoryResponseOperation::parse(operation).map_err(ProjectionError::Invalid)?;
    let value = match operation {
        MemoryResponseOperation::CreateBucket
        | MemoryResponseOperation::HeadBucket
        | MemoryResponseOperation::ForkBucket
        | MemoryResponseOperation::ForkSnapshot => {
            let response = decode::<wire::Bucket>(input)?;
            bucket_ref(
                response
                    .bucket
                    .as_ref()
                    .ok_or_else(|| unavailable("response omitted bucket"))?,
            )
        }
        MemoryResponseOperation::DeleteBucket
        | MemoryResponseOperation::AbortMultipart
        | MemoryResponseOperation::DestroySnapshot => {
            let response = match operation {
                MemoryResponseOperation::DeleteBucket => {
                    decode::<wire::DeleteBucketResponse>(input).map(|value| value.existed)
                }
                MemoryResponseOperation::AbortMultipart => {
                    decode::<wire::AbortMultipartResponse>(input).map(|value| value.existed)
                }
                _ => decode::<wire::DestroySnapshotResponse>(input).map(|value| value.existed),
            }?;
            object([("existed", Value::Bool(response))])
        }
        MemoryResponseOperation::Put
        | MemoryResponseOperation::CompleteMultipart
        | MemoryResponseOperation::GetVersion => {
            let response = decode::<wire::ObjectVersion>(input)?;
            version(&response)?
        }
        MemoryResponseOperation::Head => {
            let response = decode::<wire::HeadObjectResponse>(input)?;
            version(
                response
                    .version
                    .as_ref()
                    .ok_or_else(|| unavailable("response omitted version"))?,
            )?
        }
        MemoryResponseOperation::Delete => {
            let response = decode::<wire::DeleteObjectResponse>(input)?;
            let mut values = vec![("existed", Value::Bool(response.existed))];
            if let Some(marker) = response.version.as_ref() {
                values.push(("marker", version(marker)?));
            }
            object(values)
        }
        MemoryResponseOperation::List => {
            let response = decode::<wire::ListObjectsResponse>(input)?;
            let entries = response
                .entries
                .iter()
                .map(|entry| {
                    let entry_version = entry
                        .version
                        .as_ref()
                        .ok_or_else(|| unavailable("list entry omitted version"))?;
                    Ok(object([
                        ("objectKey", Value::String(entry.object_key.clone())),
                        ("version", version(entry_version)?),
                    ]))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut values = vec![
                ("entries", Value::Array(entries)),
                (
                    "commonPrefixes",
                    Value::Array(
                        response
                            .common_prefixes
                            .into_iter()
                            .map(Value::String)
                            .collect(),
                    ),
                ),
            ];
            if !response.continuation_token.is_empty() {
                values.push(("continuation", Value::String(response.continuation_token)));
            }
            object(values)
        }
        MemoryResponseOperation::Snapshot => {
            let response = decode::<wire::Snapshot>(input)?;
            snapshot_ref(
                response
                    .snapshot
                    .as_ref()
                    .ok_or_else(|| unavailable("response omitted snapshot"))?,
            )
        }
        MemoryResponseOperation::CreateMultipart => {
            let response = decode::<wire::MultipartUpload>(input)?;
            object([("uploadId", Value::String(response.upload_id))])
        }
        MemoryResponseOperation::UploadPart => {
            let response = decode::<wire::UploadedPart>(input)?;
            part(&response)?
        }
        MemoryResponseOperation::ListParts => {
            let response = decode::<wire::ListPartsResponse>(input)?;
            Value::Array(
                response
                    .parts
                    .iter()
                    .map(part)
                    .collect::<Result<Vec<_>>>()?,
            )
        }
    };
    super::http::project(&value).map_err(ProjectionError::Invalid)
}

fn decode<T: Message + Default>(input: &[u8]) -> Result<T> {
    T::decode(input).map_err(|_| invalid("invalid protobuf response"))
}

fn invalid(message: impl Into<String>) -> ProjectionError {
    ProjectionError::Invalid(message.into())
}

fn unavailable(message: impl Into<String>) -> ProjectionError {
    ProjectionError::Unavailable(message.into())
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<Map<_, _>>(),
    )
}

fn bigint(value: &impl ToString) -> Value {
    object([("$bigint", Value::String(value.to_string()))])
}

fn map(entries: impl IntoIterator<Item = (String, String)>) -> Value {
    let values = entries
        .into_iter()
        .map(|(key, value)| Value::Array(vec![Value::String(key), Value::String(value)]))
        .collect();
    object([("$map", Value::Array(values))])
}

fn bucket_ref(value: &wire::BucketRef) -> Value {
    object([
        ("bucketId", Value::String(value.bucket_id.clone())),
        ("name", Value::String(value.name.clone())),
    ])
}

fn snapshot_ref(value: &wire::SnapshotRef) -> Value {
    object([
        ("snapshotId", Value::String(value.snapshot_id.clone())),
        (
            "sourceBucketId",
            Value::String(value.source_bucket_id.clone()),
        ),
    ])
}

fn metadata(value: Option<&wire::ObjectMetadata>) -> Value {
    let value = value.cloned().unwrap_or_default();
    let mut entries = vec![
        ("contentType", Value::String(value.content_type)),
        ("contentEncoding", Value::String(value.content_encoding)),
        ("cacheControl", Value::String(value.cache_control)),
        (
            "contentDisposition",
            Value::String(value.content_disposition),
        ),
        ("contentLanguage", Value::String(value.content_language)),
        ("user", map(value.user.into_iter())),
    ];
    if let Some(expires) = value.expires_unix_seconds {
        entries.push(("expiresUnixSeconds", bigint(&expires)));
    }
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn version(value: &wire::ObjectVersion) -> Result<Value> {
    let mut entries = vec![
        ("versionId", Value::String(value.version_id.clone())),
        ("etag", Value::String(value.etag.clone())),
        ("size", bigint(&value.size)),
        ("deleteMarker", Value::Bool(value.delete_marker)),
        ("metadata", metadata(value.metadata.as_ref())),
    ];
    if let Some(created_at) = value.created_at.as_ref() {
        entries.push(("createdAt", timestamp(created_at)?));
    }
    Ok(Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    ))
}

fn timestamp(value: &Timestamp) -> Result<Value> {
    if !(0..1_000_000_000).contains(&value.nanos) {
        return Err(invalid("createdAt contains invalid nanoseconds"));
    }
    let date = OffsetDateTime::from_unix_timestamp(value.seconds)
        .and_then(|date| date.replace_nanosecond(value.nanos.cast_unsigned()))
        .map_err(|_| invalid("createdAt is outside the JavaScript Date range"))?;
    let text = date
        .format(&Rfc3339)
        .map_err(|_| invalid("createdAt is outside the JavaScript Date range"))?;
    Ok(Value::String(text))
}

fn part(value: &wire::UploadedPart) -> Result<Value> {
    Ok(object([
        ("partNumber", Value::Number(value.part_number.into())),
        ("etag", Value::String(value.etag.clone())),
        ("size", bigint(&value.size)),
    ]))
}
