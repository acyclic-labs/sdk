//! Rust-owned Objects v2 contract metadata.
//!
//! Objects v1 remains archived and immutable. This model is the active v2
//! schema authority candidate; its descriptor is checked against a pinned
//! protoc oracle before any generated artifact can replace a fixture.

use prost::Message;

use super::{
    Cardinality, ContractSpec, EnumSpec, EnumValueSpec, FieldSpec, FieldType, MapSpec, MessageSpec,
    MethodSpec, OneofSpec, RouteSpec, ServiceSpec,
};

const fn field(
    name: &'static str,
    number: u32,
    cardinality: Cardinality,
    field_type: FieldType,
    json_name: &'static str,
) -> FieldSpec {
    super::field(name, number, cardinality, field_type, json_name)
}
const fn optional(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    super::optional_field(name, number, field_type, json_name, oneof)
}
const fn oneof_field(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    super::oneof_field(name, number, field_type, json_name, oneof)
}
const fn map_field(
    name: &'static str,
    number: u32,
    key: FieldType,
    value: FieldType,
    entry_name: &'static str,
    json_name: &'static str,
) -> FieldSpec {
    FieldSpec {
        name,
        number,
        cardinality: Cardinality::Repeated,
        field_type: FieldType::Message(entry_name),
        json_name,
        oneof: None,
        proto3_optional: false,
        map: Some(MapSpec {
            key,
            value,
            entry_name,
        }),
    }
}
const fn oneof(name: &'static str, synthetic: bool) -> OneofSpec {
    super::oneof(name, synthetic)
}

const BUCKETREF: MessageSpec = MessageSpec {
    name: "BucketRef",
    fields: &[field(
        "name",
        1,
        Cardinality::Singular,
        FieldType::String,
        "name",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const BUCKET: MessageSpec = MessageSpec {
    name: "Bucket",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "created_at",
            2,
            Cardinality::Singular,
            FieldType::ExternalMessage("google.protobuf.Timestamp"),
            "createdAt",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const USERENTRY: MessageSpec = MessageSpec {
    name: "UserEntry",
    fields: &[
        field("key", 1, Cardinality::Singular, FieldType::String, "key"),
        field(
            "value",
            2,
            Cardinality::Singular,
            FieldType::String,
            "value",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: true,
    reserved_ranges: &[],
    reserved_names: &[],
};
const OBJECTMETADATA: MessageSpec = MessageSpec {
    name: "ObjectMetadata",
    fields: &[
        field(
            "content_type",
            1,
            Cardinality::Singular,
            FieldType::String,
            "contentType",
        ),
        map_field(
            "user",
            2,
            FieldType::String,
            FieldType::String,
            "UserEntry",
            "user",
        ),
        field(
            "content_encoding",
            3,
            Cardinality::Singular,
            FieldType::String,
            "contentEncoding",
        ),
        field(
            "cache_control",
            4,
            Cardinality::Singular,
            FieldType::String,
            "cacheControl",
        ),
        field(
            "content_disposition",
            5,
            Cardinality::Singular,
            FieldType::String,
            "contentDisposition",
        ),
        field(
            "content_language",
            6,
            Cardinality::Singular,
            FieldType::String,
            "contentLanguage",
        ),
        optional(
            "expires_unix_seconds",
            7,
            FieldType::Int64,
            "expiresUnixSeconds",
            "_expires_unix_seconds",
        ),
    ],
    oneofs: &[oneof("_expires_unix_seconds", true)],
    nested_messages: &[USERENTRY],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const PRECONDITIONS: MessageSpec = MessageSpec {
    name: "Preconditions",
    fields: &[
        oneof_field("if_absent", 1, FieldType::Bool, "ifAbsent", "condition"),
        oneof_field("if_match", 2, FieldType::String, "ifMatch", "condition"),
    ],
    oneofs: &[oneof("condition", false)],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const MUTATIONIDENTITY: MessageSpec = MessageSpec {
    name: "MutationIdentity",
    fields: &[field(
        "idempotency_key",
        1,
        Cardinality::Singular,
        FieldType::String,
        "idempotencyKey",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const OBJECTINFO: MessageSpec = MessageSpec {
    name: "ObjectInfo",
    fields: &[
        field("etag", 1, Cardinality::Singular, FieldType::String, "etag"),
        field("size", 2, Cardinality::Singular, FieldType::Uint64, "size"),
        field(
            "metadata",
            3,
            Cardinality::Singular,
            FieldType::Message("ObjectMetadata"),
            "metadata",
        ),
        field(
            "last_modified",
            4,
            Cardinality::Singular,
            FieldType::ExternalMessage("google.protobuf.Timestamp"),
            "lastModified",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const CREATEBUCKETREQUEST: MessageSpec = MessageSpec {
    name: "CreateBucketRequest",
    fields: &[
        field("name", 1, Cardinality::Singular, FieldType::String, "name"),
        field(
            "mutation",
            2,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const HEADBUCKETREQUEST: MessageSpec = MessageSpec {
    name: "HeadBucketRequest",
    fields: &[field(
        "bucket",
        1,
        Cardinality::Singular,
        FieldType::Message("BucketRef"),
        "bucket",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const DELETEBUCKETREQUEST: MessageSpec = MessageSpec {
    name: "DeleteBucketRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "mutation",
            2,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const DELETEBUCKETRESPONSE: MessageSpec = MessageSpec {
    name: "DeleteBucketResponse",
    fields: &[field(
        "existed",
        1,
        Cardinality::Singular,
        FieldType::Bool,
        "existed",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const PUTOBJECTHEADER: MessageSpec = MessageSpec {
    name: "PutObjectHeader",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "metadata",
            3,
            Cardinality::Singular,
            FieldType::Message("ObjectMetadata"),
            "metadata",
        ),
        field(
            "preconditions",
            4,
            Cardinality::Singular,
            FieldType::Message("Preconditions"),
            "preconditions",
        ),
        field(
            "mutation",
            5,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const PUTOBJECTREQUEST: MessageSpec = MessageSpec {
    name: "PutObjectRequest",
    fields: &[
        oneof_field(
            "header",
            1,
            FieldType::Message("PutObjectHeader"),
            "header",
            "frame",
        ),
        oneof_field("body", 2, FieldType::Bytes, "body", "frame"),
        oneof_field("complete", 3, FieldType::Bool, "complete", "frame"),
    ],
    oneofs: &[oneof("frame", false)],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const INCLUSIVERANGE: MessageSpec = MessageSpec {
    name: "InclusiveRange",
    fields: &[
        field(
            "start",
            1,
            Cardinality::Singular,
            FieldType::Uint64,
            "start",
        ),
        optional("end", 2, FieldType::Uint64, "end", "_end"),
    ],
    oneofs: &[oneof("_end", true)],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const BYTERANGE: MessageSpec = MessageSpec {
    name: "ByteRange",
    fields: &[
        oneof_field(
            "bytes",
            1,
            FieldType::Message("InclusiveRange"),
            "bytes",
            "selection",
        ),
        oneof_field(
            "suffix_length",
            2,
            FieldType::Uint64,
            "suffixLength",
            "selection",
        ),
    ],
    oneofs: &[oneof("selection", false)],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const GETOBJECTREQUEST: MessageSpec = MessageSpec {
    name: "GetObjectRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "range",
            3,
            Cardinality::Singular,
            FieldType::Message("ByteRange"),
            "range",
        ),
        field(
            "if_match",
            4,
            Cardinality::Singular,
            FieldType::String,
            "ifMatch",
        ),
        field(
            "if_none_match",
            5,
            Cardinality::Singular,
            FieldType::String,
            "ifNoneMatch",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const CONTENTRANGE: MessageSpec = MessageSpec {
    name: "ContentRange",
    fields: &[
        field(
            "start",
            1,
            Cardinality::Singular,
            FieldType::Uint64,
            "start",
        ),
        field("end", 2, Cardinality::Singular, FieldType::Uint64, "end"),
        field(
            "total",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "total",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const GETOBJECTHEADER: MessageSpec = MessageSpec {
    name: "GetObjectHeader",
    fields: &[
        field(
            "object",
            1,
            Cardinality::Singular,
            FieldType::Message("ObjectInfo"),
            "object",
        ),
        field(
            "content_range",
            2,
            Cardinality::Singular,
            FieldType::Message("ContentRange"),
            "contentRange",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const GETOBJECTRESPONSE: MessageSpec = MessageSpec {
    name: "GetObjectResponse",
    fields: &[
        oneof_field(
            "header",
            1,
            FieldType::Message("GetObjectHeader"),
            "header",
            "frame",
        ),
        oneof_field("body", 2, FieldType::Bytes, "body", "frame"),
        oneof_field(
            "error",
            3,
            FieldType::Message("ErrorDetail"),
            "error",
            "frame",
        ),
    ],
    oneofs: &[oneof("frame", false)],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const HEADOBJECTREQUEST: MessageSpec = MessageSpec {
    name: "HeadObjectRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "if_match",
            3,
            Cardinality::Singular,
            FieldType::String,
            "ifMatch",
        ),
        field(
            "if_none_match",
            4,
            Cardinality::Singular,
            FieldType::String,
            "ifNoneMatch",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const HEADOBJECTRESPONSE: MessageSpec = MessageSpec {
    name: "HeadObjectResponse",
    fields: &[field(
        "object",
        1,
        Cardinality::Singular,
        FieldType::Message("ObjectInfo"),
        "object",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const DELETEOBJECTREQUEST: MessageSpec = MessageSpec {
    name: "DeleteObjectRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "preconditions",
            3,
            Cardinality::Singular,
            FieldType::Message("Preconditions"),
            "preconditions",
        ),
        field(
            "mutation",
            4,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const DELETEOBJECTRESPONSE: MessageSpec = MessageSpec {
    name: "DeleteObjectResponse",
    fields: &[field(
        "existed",
        1,
        Cardinality::Singular,
        FieldType::Bool,
        "existed",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const LISTOBJECTSREQUEST: MessageSpec = MessageSpec {
    name: "ListObjectsRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "prefix",
            2,
            Cardinality::Singular,
            FieldType::String,
            "prefix",
        ),
        field(
            "delimiter",
            3,
            Cardinality::Singular,
            FieldType::String,
            "delimiter",
        ),
        field(
            "page_size",
            4,
            Cardinality::Singular,
            FieldType::Uint32,
            "pageSize",
        ),
        field(
            "continuation_token",
            5,
            Cardinality::Singular,
            FieldType::String,
            "continuationToken",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const LISTENTRY: MessageSpec = MessageSpec {
    name: "ListEntry",
    fields: &[
        field(
            "object_key",
            1,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "object",
            2,
            Cardinality::Singular,
            FieldType::Message("ObjectInfo"),
            "object",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const LISTOBJECTSRESPONSE: MessageSpec = MessageSpec {
    name: "ListObjectsResponse",
    fields: &[
        field(
            "entries",
            1,
            Cardinality::Repeated,
            FieldType::Message("ListEntry"),
            "entries",
        ),
        field(
            "common_prefixes",
            2,
            Cardinality::Repeated,
            FieldType::String,
            "commonPrefixes",
        ),
        field(
            "continuation_token",
            3,
            Cardinality::Singular,
            FieldType::String,
            "continuationToken",
        ),
        field(
            "is_truncated",
            4,
            Cardinality::Singular,
            FieldType::Bool,
            "isTruncated",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const CREATEMULTIPARTREQUEST: MessageSpec = MessageSpec {
    name: "CreateMultipartRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "metadata",
            3,
            Cardinality::Singular,
            FieldType::Message("ObjectMetadata"),
            "metadata",
        ),
        field(
            "mutation",
            4,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const MULTIPARTUPLOAD: MessageSpec = MessageSpec {
    name: "MultipartUpload",
    fields: &[field(
        "upload_id",
        1,
        Cardinality::Singular,
        FieldType::String,
        "uploadId",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const UPLOADPARTHEADER: MessageSpec = MessageSpec {
    name: "UploadPartHeader",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "upload_id",
            3,
            Cardinality::Singular,
            FieldType::String,
            "uploadId",
        ),
        field(
            "part_number",
            4,
            Cardinality::Singular,
            FieldType::Uint32,
            "partNumber",
        ),
        field(
            "mutation",
            5,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const UPLOADPARTREQUEST: MessageSpec = MessageSpec {
    name: "UploadPartRequest",
    fields: &[
        oneof_field(
            "header",
            1,
            FieldType::Message("UploadPartHeader"),
            "header",
            "frame",
        ),
        oneof_field("body", 2, FieldType::Bytes, "body", "frame"),
        oneof_field("complete", 3, FieldType::Bool, "complete", "frame"),
    ],
    oneofs: &[oneof("frame", false)],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const UPLOADEDPART: MessageSpec = MessageSpec {
    name: "UploadedPart",
    fields: &[
        field(
            "part_number",
            1,
            Cardinality::Singular,
            FieldType::Uint32,
            "partNumber",
        ),
        field("etag", 2, Cardinality::Singular, FieldType::String, "etag"),
        field("size", 3, Cardinality::Singular, FieldType::Uint64, "size"),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const LISTPARTSREQUEST: MessageSpec = MessageSpec {
    name: "ListPartsRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "upload_id",
            3,
            Cardinality::Singular,
            FieldType::String,
            "uploadId",
        ),
        field(
            "after_part_number",
            4,
            Cardinality::Singular,
            FieldType::Uint32,
            "afterPartNumber",
        ),
        field(
            "page_size",
            5,
            Cardinality::Singular,
            FieldType::Uint32,
            "pageSize",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const LISTPARTSRESPONSE: MessageSpec = MessageSpec {
    name: "ListPartsResponse",
    fields: &[
        field(
            "parts",
            1,
            Cardinality::Repeated,
            FieldType::Message("UploadedPart"),
            "parts",
        ),
        field(
            "next_part_number",
            2,
            Cardinality::Singular,
            FieldType::Uint32,
            "nextPartNumber",
        ),
        field(
            "is_truncated",
            3,
            Cardinality::Singular,
            FieldType::Bool,
            "isTruncated",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const COMPLETEMULTIPARTREQUEST: MessageSpec = MessageSpec {
    name: "CompleteMultipartRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "upload_id",
            3,
            Cardinality::Singular,
            FieldType::String,
            "uploadId",
        ),
        field(
            "parts",
            4,
            Cardinality::Repeated,
            FieldType::Message("UploadedPart"),
            "parts",
        ),
        field(
            "preconditions",
            5,
            Cardinality::Singular,
            FieldType::Message("Preconditions"),
            "preconditions",
        ),
        field(
            "mutation",
            6,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const ABORTMULTIPARTREQUEST: MessageSpec = MessageSpec {
    name: "AbortMultipartRequest",
    fields: &[
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::Message("BucketRef"),
            "bucket",
        ),
        field(
            "object_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "objectKey",
        ),
        field(
            "upload_id",
            3,
            Cardinality::Singular,
            FieldType::String,
            "uploadId",
        ),
        field(
            "mutation",
            4,
            Cardinality::Singular,
            FieldType::Message("MutationIdentity"),
            "mutation",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const ABORTMULTIPARTRESPONSE: MessageSpec = MessageSpec {
    name: "AbortMultipartResponse",
    fields: &[field(
        "existed",
        1,
        Cardinality::Singular,
        FieldType::Bool,
        "existed",
    )],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};
const ERRORDETAIL: MessageSpec = MessageSpec {
    name: "ErrorDetail",
    fields: &[
        field(
            "code",
            1,
            Cardinality::Singular,
            FieldType::Enum("ErrorCode"),
            "code",
        ),
        field(
            "request_id",
            2,
            Cardinality::Singular,
            FieldType::String,
            "requestId",
        ),
    ],
    oneofs: &[],
    nested_messages: &[],
    is_map_entry: false,
    reserved_ranges: &[],
    reserved_names: &[],
};

/// Maximum encoded JSON/NDJSON record size accepted by the Objects HTTP
/// framing layer, including its line terminator.
///
/// This transport ceiling is separate from the decoded body-frame limit in
/// `ObjectsLimit`: a protobuf-JSON record contains field names and base64
/// expansion in addition to the decoded body bytes.
pub const OBJECTS_HTTP_JSON_FRAME_BYTES: usize = 128 * 1024;

pub const OBJECTSLIMIT: EnumSpec = EnumSpec {
    name: "ObjectsLimit",
    values: &[
        EnumValueSpec {
            name: "OBJECTS_LIMIT_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES",
            number: 256,
        },
        EnumValueSpec {
            name: "OBJECTS_LIMIT_MAX_KEY_BYTES",
            number: 1024,
        },
        EnumValueSpec {
            name: "OBJECTS_LIMIT_MAX_USER_METADATA_BYTES",
            number: 2048,
        },
        EnumValueSpec {
            name: "OBJECTS_LIMIT_MAX_PAGE_ENTRIES",
            number: 1000,
        },
        EnumValueSpec {
            name: "OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES",
            number: 65536,
        },
        EnumValueSpec {
            name: "OBJECTS_LIMIT_MAX_MULTIPART_PARTS",
            number: 10000,
        },
    ],
};
pub const ERRORCODE: EnumSpec = EnumSpec {
    name: "ErrorCode",
    values: &[
        EnumValueSpec {
            name: "ERROR_CODE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "ERROR_CODE_INVALID_ARGUMENT",
            number: 1,
        },
        EnumValueSpec {
            name: "ERROR_CODE_NOT_FOUND",
            number: 2,
        },
        EnumValueSpec {
            name: "ERROR_CODE_ALREADY_EXISTS",
            number: 3,
        },
        EnumValueSpec {
            name: "ERROR_CODE_PRECONDITION_FAILED",
            number: 4,
        },
        EnumValueSpec {
            name: "ERROR_CODE_IDEMPOTENCY_MISMATCH",
            number: 5,
        },
        EnumValueSpec {
            name: "ERROR_CODE_QUOTA_EXCEEDED",
            number: 6,
        },
        EnumValueSpec {
            name: "ERROR_CODE_UNSUPPORTED",
            number: 7,
        },
        EnumValueSpec {
            name: "ERROR_CODE_UNAVAILABLE",
            number: 8,
        },
        EnumValueSpec {
            name: "ERROR_CODE_ACCESS_DENIED",
            number: 9,
        },
        EnumValueSpec {
            name: "ERROR_CODE_RANGE_NOT_SATISFIABLE",
            number: 10,
        },
        EnumValueSpec {
            name: "ERROR_CODE_NOT_MODIFIED",
            number: 11,
        },
    ],
};

pub const BUCKETSSERVICE: ServiceSpec = ServiceSpec {
    name: "BucketsService",
    methods: &[
        MethodSpec {
            name: "CreateBucket",
            input: "CreateBucketRequest",
            output: "Bucket",
            docs: "Creates a logical bucket.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "HeadBucket",
            input: "HeadBucketRequest",
            output: "Bucket",
            docs: "Returns logical bucket metadata.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "DeleteBucket",
            input: "DeleteBucketRequest",
            output: "DeleteBucketResponse",
            docs: "Deletes a logical bucket when it is empty.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};
pub const OBJECTSSERVICE: ServiceSpec = ServiceSpec {
    name: "ObjectsService",
    methods: &[
        MethodSpec {
            name: "PutObject",
            input: "PutObjectRequest",
            output: "ObjectInfo",
            docs: "Publishes one complete object after an explicit upload completion frame.",
            client_streaming: true,
            server_streaming: false,
        },
        MethodSpec {
            name: "GetObject",
            input: "GetObjectRequest",
            output: "GetObjectResponse",
            docs: "Reads one complete object representation as bounded response frames.",
            client_streaming: false,
            server_streaming: true,
        },
        MethodSpec {
            name: "HeadObject",
            input: "HeadObjectRequest",
            output: "HeadObjectResponse",
            docs: "Returns object metadata without its body.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "DeleteObject",
            input: "DeleteObjectRequest",
            output: "DeleteObjectResponse",
            docs: "Deletes the current object representation atomically.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "ListObjects",
            input: "ListObjectsRequest",
            output: "ListObjectsResponse",
            docs: "Lists objects through an eventual lexical traversal.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};
pub const MULTIPARTSERVICE: ServiceSpec = ServiceSpec {
    name: "MultipartService",
    methods: &[
        MethodSpec {
            name: "CreateMultipart",
            input: "CreateMultipartRequest",
            output: "MultipartUpload",
            docs: "Starts a multipart upload.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "UploadPart",
            input: "UploadPartRequest",
            output: "UploadedPart",
            docs: "Uploads one multipart part after an explicit completion frame.",
            client_streaming: true,
            server_streaming: false,
        },
        MethodSpec {
            name: "ListParts",
            input: "ListPartsRequest",
            output: "ListPartsResponse",
            docs: "Lists the uploaded parts for a multipart upload.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "CompleteMultipart",
            input: "CompleteMultipartRequest",
            output: "ObjectInfo",
            docs: "Publishes a multipart object from exact ordered part receipts.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "AbortMultipart",
            input: "AbortMultipartRequest",
            output: "AbortMultipartResponse",
            docs: "Aborts a multipart upload.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

/// Rust-owned HTTP projection for the hosted Objects v2 transport.
///
/// The paths are relative to the Objects gateway root and deliberately retain
/// the complete protobuf RPC identity. HTTP/OpenAPI exporters must consume
/// this table instead of rebuilding a second route inventory.
pub const OBJECTS_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        method: "POST",
        path: "/v2/objects/buckets/create",
        operation_id: "createBucket",
        rpc: "acyclic.objects.v2.BucketsService/CreateBucket",
        request: "CreateBucketRequest",
        response: "Bucket",
        docs: "Creates a logical bucket.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/buckets/head",
        operation_id: "headBucket",
        rpc: "acyclic.objects.v2.BucketsService/HeadBucket",
        request: "HeadBucketRequest",
        response: "Bucket",
        docs: "Returns logical bucket metadata.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/buckets/delete",
        operation_id: "deleteBucket",
        rpc: "acyclic.objects.v2.BucketsService/DeleteBucket",
        request: "DeleteBucketRequest",
        response: "DeleteBucketResponse",
        docs: "Deletes a logical bucket when it is empty.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/objects/put",
        operation_id: "putObject",
        rpc: "acyclic.objects.v2.ObjectsService/PutObject",
        request: "PutObjectRequest",
        response: "ObjectInfo",
        docs: "Publishes one complete object after an explicit upload completion frame.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/objects/get",
        operation_id: "getObject",
        rpc: "acyclic.objects.v2.ObjectsService/GetObject",
        request: "GetObjectRequest",
        response: "GetObjectResponse",
        docs: "Reads one complete object representation as bounded response frames.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/objects/head",
        operation_id: "headObject",
        rpc: "acyclic.objects.v2.ObjectsService/HeadObject",
        request: "HeadObjectRequest",
        response: "HeadObjectResponse",
        docs: "Returns object metadata without its body.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/objects/delete",
        operation_id: "deleteObject",
        rpc: "acyclic.objects.v2.ObjectsService/DeleteObject",
        request: "DeleteObjectRequest",
        response: "DeleteObjectResponse",
        docs: "Deletes the current object representation atomically.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/objects/list",
        operation_id: "listObjects",
        rpc: "acyclic.objects.v2.ObjectsService/ListObjects",
        request: "ListObjectsRequest",
        response: "ListObjectsResponse",
        docs: "Lists objects through an eventual lexical traversal.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/multipart/create",
        operation_id: "createMultipart",
        rpc: "acyclic.objects.v2.MultipartService/CreateMultipart",
        request: "CreateMultipartRequest",
        response: "MultipartUpload",
        docs: "Starts a multipart upload.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/multipart/upload-part",
        operation_id: "uploadPart",
        rpc: "acyclic.objects.v2.MultipartService/UploadPart",
        request: "UploadPartRequest",
        response: "UploadedPart",
        docs: "Uploads one multipart part after an explicit completion frame.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/multipart/list-parts",
        operation_id: "listParts",
        rpc: "acyclic.objects.v2.MultipartService/ListParts",
        request: "ListPartsRequest",
        response: "ListPartsResponse",
        docs: "Lists the uploaded parts for a multipart upload.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/multipart/complete",
        operation_id: "completeMultipart",
        rpc: "acyclic.objects.v2.MultipartService/CompleteMultipart",
        request: "CompleteMultipartRequest",
        response: "ObjectInfo",
        docs: "Publishes a multipart object from exact ordered part receipts.",
    },
    RouteSpec {
        method: "POST",
        path: "/v2/objects/multipart/abort",
        operation_id: "abortMultipart",
        rpc: "acyclic.objects.v2.MultipartService/AbortMultipart",
        request: "AbortMultipartRequest",
        response: "AbortMultipartResponse",
        docs: "Aborts a multipart upload.",
    },
];

pub const OBJECTS_V2: ContractSpec = ContractSpec {
    file_name: "objects/v2/objects.proto",
    syntax: "proto3",
    package: "acyclic.objects.v2",
    dependencies: &["google/protobuf/timestamp.proto"],
    options: super::FileOptionsSpec {
        go_package: "github.com/acyclic-labs/sdk/go/gen/objects/v2;objectsv2",
    },
    messages: &[
        BUCKETREF,
        BUCKET,
        OBJECTMETADATA,
        PRECONDITIONS,
        MUTATIONIDENTITY,
        OBJECTINFO,
        CREATEBUCKETREQUEST,
        HEADBUCKETREQUEST,
        DELETEBUCKETREQUEST,
        DELETEBUCKETRESPONSE,
        PUTOBJECTHEADER,
        PUTOBJECTREQUEST,
        INCLUSIVERANGE,
        BYTERANGE,
        GETOBJECTREQUEST,
        CONTENTRANGE,
        GETOBJECTHEADER,
        GETOBJECTRESPONSE,
        HEADOBJECTREQUEST,
        HEADOBJECTRESPONSE,
        DELETEOBJECTREQUEST,
        DELETEOBJECTRESPONSE,
        LISTOBJECTSREQUEST,
        LISTENTRY,
        LISTOBJECTSRESPONSE,
        CREATEMULTIPARTREQUEST,
        MULTIPARTUPLOAD,
        UPLOADPARTHEADER,
        UPLOADPARTREQUEST,
        UPLOADEDPART,
        LISTPARTSREQUEST,
        LISTPARTSRESPONSE,
        COMPLETEMULTIPARTREQUEST,
        ABORTMULTIPARTREQUEST,
        ABORTMULTIPARTRESPONSE,
        ERRORDETAIL,
    ],
    enums: &[OBJECTSLIMIT, ERRORCODE],
    services: &[BUCKETSSERVICE, OBJECTSSERVICE, MULTIPARTSERVICE],
    routes: OBJECTS_ROUTES,
};

/// A documentation item owned by the Objects contract model.
///
/// Keeping these descriptions beside the corresponding wire names lets docs,
/// OpenAPI, and SDK metadata exporters share one source without making prose
/// a second contract authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectsDoc {
    pub name: &'static str,
    pub text: &'static str,
}

pub const OBJECTS_SERVICE_DOCS: &[ObjectsDoc] = &[
    ObjectsDoc {
        name: "BucketsService",
        text: "Creates, inspects, and deletes logical object buckets.",
    },
    ObjectsDoc {
        name: "ObjectsService",
        text: "Publishes, reads, lists, and deletes logical objects.",
    },
    ObjectsDoc {
        name: "MultipartService",
        text: "Stages and publishes multipart object uploads.",
    },
];

#[rustfmt::skip]
pub const OBJECTS_METHOD_DOCS: &[ObjectsDoc] = &[
    ObjectsDoc { name: "CreateBucket", text: "Creates a logical bucket." },
    ObjectsDoc { name: "HeadBucket", text: "Returns logical bucket metadata." },
    ObjectsDoc { name: "DeleteBucket", text: "Deletes a logical bucket when it is empty." },
    ObjectsDoc { name: "PutObject", text: "Publishes one complete object after an explicit upload completion frame." },
    ObjectsDoc { name: "GetObject", text: "Reads one complete object representation as bounded response frames." },
    ObjectsDoc { name: "HeadObject", text: "Returns object metadata without its body." },
    ObjectsDoc { name: "DeleteObject", text: "Deletes the current object representation atomically." },
    ObjectsDoc { name: "ListObjects", text: "Lists objects through an eventual lexical traversal." },
    ObjectsDoc { name: "CreateMultipart", text: "Starts a multipart upload." },
    ObjectsDoc { name: "UploadPart", text: "Uploads one multipart part after an explicit completion frame." },
    ObjectsDoc { name: "ListParts", text: "Lists the uploaded parts for a multipart upload." },
    ObjectsDoc { name: "CompleteMultipart", text: "Publishes a multipart object from exact ordered part receipts." },
    ObjectsDoc { name: "AbortMultipart", text: "Aborts a multipart upload." },
];

#[rustfmt::skip]
pub const OBJECTS_ENUM_DOCS: &[ObjectsDoc] = &[
    ObjectsDoc { name: "ObjectsLimit", text: "Fixed protocol limits for Objects operations." },
    ObjectsDoc { name: "ErrorCode", text: "Stable customer-visible error categories returned by Objects services." },
];

#[rustfmt::skip]
pub const OBJECTS_MESSAGE_DOCS: &[ObjectsDoc] = &[
    ObjectsDoc { name: "BucketRef", text: "Identifies an object bucket by its logical tenant name." },
    ObjectsDoc { name: "Bucket", text: "A logical object bucket and its creation metadata." },
    ObjectsDoc { name: "UserEntry", text: "One entry in the user metadata map." },
    ObjectsDoc { name: "ObjectMetadata", text: "Metadata associated with an object representation." },
    ObjectsDoc { name: "Preconditions", text: "Conditional requirements evaluated at publication." },
    ObjectsDoc { name: "MutationIdentity", text: "The idempotency identity for a mutation." },
    ObjectsDoc { name: "ObjectInfo", text: "The current object representation metadata." },
    ObjectsDoc { name: "CreateBucketRequest", text: "Creates a logical bucket." },
    ObjectsDoc { name: "HeadBucketRequest", text: "Reads logical bucket metadata." },
    ObjectsDoc { name: "DeleteBucketRequest", text: "Deletes a logical bucket." },
    ObjectsDoc { name: "DeleteBucketResponse", text: "Reports whether the bucket existed." },
    ObjectsDoc { name: "PutObjectHeader", text: "Headers for an object upload." },
    ObjectsDoc { name: "PutObjectRequest", text: "Frames for an object upload." },
    ObjectsDoc { name: "InclusiveRange", text: "An inclusive byte range selection." },
    ObjectsDoc { name: "ByteRange", text: "A byte range or suffix selection for an object read." },
    ObjectsDoc { name: "GetObjectRequest", text: "Reads an object representation." },
    ObjectsDoc { name: "ContentRange", text: "The byte range returned by an object read." },
    ObjectsDoc { name: "GetObjectHeader", text: "Metadata headers for an object read." },
    ObjectsDoc { name: "GetObjectResponse", text: "Frames returned by an object read." },
    ObjectsDoc { name: "HeadObjectRequest", text: "Reads object metadata without its body." },
    ObjectsDoc { name: "HeadObjectResponse", text: "The object metadata response." },
    ObjectsDoc { name: "DeleteObjectRequest", text: "Deletes an object representation." },
    ObjectsDoc { name: "DeleteObjectResponse", text: "Reports whether the object existed." },
    ObjectsDoc { name: "ListObjectsRequest", text: "Lists objects in a bucket." },
    ObjectsDoc { name: "ListEntry", text: "One object in a listing page." },
    ObjectsDoc { name: "ListObjectsResponse", text: "One page of an eventual object listing." },
    ObjectsDoc { name: "CreateMultipartRequest", text: "Starts a multipart upload." },
    ObjectsDoc { name: "MultipartUpload", text: "An in-progress multipart upload." },
    ObjectsDoc { name: "UploadPartHeader", text: "Headers for a multipart part upload." },
    ObjectsDoc { name: "UploadPartRequest", text: "Frames for a multipart part upload." },
    ObjectsDoc { name: "UploadedPart", text: "A successfully uploaded multipart part receipt." },
    ObjectsDoc { name: "ListPartsRequest", text: "Lists parts for a multipart upload." },
    ObjectsDoc { name: "ListPartsResponse", text: "One page of uploaded multipart parts." },
    ObjectsDoc { name: "CompleteMultipartRequest", text: "Publishes a multipart object from exact part receipts." },
    ObjectsDoc { name: "AbortMultipartRequest", text: "Aborts a multipart upload." },
    ObjectsDoc { name: "AbortMultipartResponse", text: "Reports whether the multipart upload existed." },
    ObjectsDoc { name: "ErrorDetail", text: "A customer-visible typed Objects service error." },
];

/// Field descriptions keyed by protobuf field name. The shared renderer may
/// use this table while preserving message-qualified lookup for diagnostics.
#[rustfmt::skip]
pub const OBJECTS_FIELD_DOCS: &[ObjectsDoc] = &[
    ObjectsDoc { name: "name", text: "A logical tenant bucket name." },
    ObjectsDoc { name: "bucket", text: "The logical object bucket." },
    ObjectsDoc { name: "created_at", text: "The bucket creation timestamp." },
    ObjectsDoc { name: "content_type", text: "The object media type." },
    ObjectsDoc { name: "user", text: "User-defined metadata entries." },
    ObjectsDoc { name: "content_encoding", text: "The object content encoding." },
    ObjectsDoc { name: "cache_control", text: "The object cache-control directive." },
    ObjectsDoc { name: "content_disposition", text: "The object content-disposition directive." },
    ObjectsDoc { name: "content_language", text: "The object content language." },
    ObjectsDoc { name: "expires_unix_seconds", text: "Optional object expiry time in Unix seconds." },
    ObjectsDoc { name: "if_absent", text: "Requires the current object to be absent." },
    ObjectsDoc { name: "if_match", text: "Requires the current opaque ETag to match." },
    ObjectsDoc { name: "idempotency_key", text: "A client key that makes a mutation idempotent." },
    ObjectsDoc { name: "etag", text: "The opaque current representation ETag." },
    ObjectsDoc { name: "size", text: "The complete object size in bytes." },
    ObjectsDoc { name: "metadata", text: "Metadata for the object representation." },
    ObjectsDoc { name: "last_modified", text: "The last publication timestamp." },
    ObjectsDoc { name: "object_key", text: "The logical object key." },
    ObjectsDoc { name: "preconditions", text: "Conditions evaluated atomically at publication." },
    ObjectsDoc { name: "header", text: "The required first upload or download header frame." },
    ObjectsDoc { name: "body", text: "A bounded object body frame." },
    ObjectsDoc { name: "complete", text: "Explicitly completes an upload stream." },
    ObjectsDoc { name: "start", text: "The inclusive range start offset." },
    ObjectsDoc { name: "end", text: "The inclusive range end offset." },
    ObjectsDoc { name: "bytes", text: "An inclusive byte range selection." },
    ObjectsDoc { name: "suffix_length", text: "The number of bytes selected from the end." },
    ObjectsDoc { name: "range", text: "The requested object byte range." },
    ObjectsDoc { name: "if_none_match", text: "Requires the current ETag not to match." },
    ObjectsDoc { name: "object", text: "The complete current object metadata." },
    ObjectsDoc { name: "content_range", text: "The selected range and complete size." },
    ObjectsDoc { name: "error", text: "A terminal semantic error frame." },
    ObjectsDoc { name: "existed", text: "Whether the target existed before deletion." },
    ObjectsDoc { name: "prefix", text: "The lexical object-key prefix." },
    ObjectsDoc { name: "delimiter", text: "The listing grouping delimiter." },
    ObjectsDoc { name: "page_size", text: "The requested bounded page size." },
    ObjectsDoc { name: "continuation_token", text: "An opaque query-bound listing cursor." },
    ObjectsDoc { name: "entries", text: "Objects in this listing page." },
    ObjectsDoc { name: "common_prefixes", text: "Grouped prefixes in this listing page." },
    ObjectsDoc { name: "is_truncated", text: "Whether more entries remain after this page." },
    ObjectsDoc { name: "upload_id", text: "The multipart upload identifier." },
    ObjectsDoc { name: "part_number", text: "The multipart part number." },
    ObjectsDoc { name: "after_part_number", text: "Only parts after this number are returned." },
    ObjectsDoc { name: "parts", text: "Exact ordered multipart part receipts." },
    ObjectsDoc { name: "next_part_number", text: "The next part number after this page." },
    ObjectsDoc { name: "code", text: "The typed Objects service error code." },
    ObjectsDoc { name: "request_id", text: "A customer-visible request diagnostic identifier." },
];

pub fn objects_descriptor() -> Vec<u8> {
    OBJECTS_V2.descriptor_set().encode_to_vec()
}
pub fn objects_proto() -> String {
    OBJECTS_V2.render_proto()
}
