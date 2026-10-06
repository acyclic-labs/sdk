use acyclic_sdk_contract_wire::objects::{
    OBJECTS_ENUM_DOCS, OBJECTS_FIELD_DOCS, OBJECTS_MESSAGE_DOCS, OBJECTS_METHOD_DOCS,
    OBJECTS_ROUTES, OBJECTS_SERVICE_DOCS, OBJECTS_V2, objects_descriptor,
};
use prost::Message;
use prost_types::FileDescriptorSet;
use sha2::{Digest, Sha256};

const OBJECTS_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/objects-v2.descriptor.bin"
));

const ARCHIVED_OBJECTS_V1_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../compatibility/objects/v1/objects_descriptor.bin"
));

const ARCHIVED_OBJECTS_V1_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../compatibility/objects/v1/manifest.json"
));

const GENERATED_OBJECTS_MESSAGES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../generated/rust/acyclic/objects/v2/acyclic.objects.v2.rs"
));

const GENERATED_OBJECTS_TONIC: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../generated/rust/acyclic/objects/v2/acyclic.objects.v2.tonic.rs"
));

#[test]
fn objects_model_matches_archived_wire_descriptor() {
    assert_eq!(objects_descriptor().as_slice(), OBJECTS_GOLDEN_DESCRIPTOR);

    let descriptor = FileDescriptorSet::decode(OBJECTS_GOLDEN_DESCRIPTOR)
        .expect("decode Objects v2 golden descriptor");
    let file = descriptor.file.first().expect("Objects v2 file descriptor");
    assert_eq!(file.name.as_deref(), Some(OBJECTS_V2.file_name));
    assert_eq!(file.package.as_deref(), Some(OBJECTS_V2.package));
    assert_eq!(file.syntax.as_deref(), Some(OBJECTS_V2.syntax));
    assert_eq!(
        file.dependency,
        vec!["google/protobuf/timestamp.proto".to_owned()]
    );
    assert_eq!(file.source_code_info, None, "golden omits source info");
    assert_eq!(file.message_type.len(), 36);
    assert_eq!(file.enum_type.len(), 2);
    assert_eq!(file.service.len(), 3);
    assert_eq!(
        file.service
            .iter()
            .map(|service| service.method.len())
            .sum::<usize>(),
        13
    );

    let metadata = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("ObjectMetadata"))
        .expect("ObjectMetadata descriptor");
    let expires = metadata
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("expires_unix_seconds"))
        .expect("optional expires field");
    assert_eq!(expires.number, Some(7));
    assert_eq!(expires.proto3_optional, Some(true));
    assert_eq!(metadata.nested_type.len(), 1);
    assert_eq!(metadata.nested_type[0].name.as_deref(), Some("UserEntry"));
    assert_eq!(
        metadata.nested_type[0]
            .options
            .as_ref()
            .and_then(|options| options.map_entry),
        Some(true)
    );

    let put = file
        .service
        .iter()
        .find(|service| service.name.as_deref() == Some("ObjectsService"))
        .expect("ObjectsService")
        .method
        .iter()
        .find(|method| method.name.as_deref() == Some("PutObject"))
        .expect("PutObject");
    assert_eq!(
        put.input_type.as_deref(),
        Some(".acyclic.objects.v2.PutObjectRequest")
    );
    assert_eq!(
        put.output_type.as_deref(),
        Some(".acyclic.objects.v2.ObjectInfo")
    );
    assert_eq!(put.client_streaming, Some(true));
    assert_eq!(put.server_streaming, None);

    let get = file
        .service
        .iter()
        .find(|service| service.name.as_deref() == Some("ObjectsService"))
        .expect("ObjectsService")
        .method
        .iter()
        .find(|method| method.name.as_deref() == Some("GetObject"))
        .expect("GetObject");
    assert_eq!(get.client_streaming, None);
    assert_eq!(get.server_streaming, Some(true));
}

#[test]
fn objects_descriptor_keeps_enum_values_presence_and_oneof_roles() {
    let descriptor = FileDescriptorSet::decode(OBJECTS_GOLDEN_DESCRIPTOR)
        .expect("decode Objects v2 golden descriptor");
    let file = descriptor.file.first().expect("Objects v2 file descriptor");
    let enum_values = |name: &str| {
        file.enum_type
            .iter()
            .find(|enum_type| enum_type.name.as_deref() == Some(name))
            .expect("Objects enum")
            .value
            .iter()
            .map(|value| {
                (
                    value.name.clone().unwrap_or_default(),
                    value.number.unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        enum_values("ObjectsLimit"),
        vec![
            ("OBJECTS_LIMIT_UNSPECIFIED".to_owned(), 0),
            ("OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES".to_owned(), 256),
            ("OBJECTS_LIMIT_MAX_KEY_BYTES".to_owned(), 1024),
            ("OBJECTS_LIMIT_MAX_USER_METADATA_BYTES".to_owned(), 2048),
            ("OBJECTS_LIMIT_MAX_PAGE_ENTRIES".to_owned(), 1000),
            ("OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES".to_owned(), 65536),
            ("OBJECTS_LIMIT_MAX_MULTIPART_PARTS".to_owned(), 10000),
        ]
    );
    assert_eq!(
        enum_values("ErrorCode"),
        [
            ("ERROR_CODE_UNSPECIFIED", 0),
            ("ERROR_CODE_INVALID_ARGUMENT", 1),
            ("ERROR_CODE_NOT_FOUND", 2),
            ("ERROR_CODE_ALREADY_EXISTS", 3),
            ("ERROR_CODE_PRECONDITION_FAILED", 4),
            ("ERROR_CODE_IDEMPOTENCY_MISMATCH", 5),
            ("ERROR_CODE_QUOTA_EXCEEDED", 6),
            ("ERROR_CODE_UNSUPPORTED", 7),
            ("ERROR_CODE_UNAVAILABLE", 8),
            ("ERROR_CODE_ACCESS_DENIED", 9),
            ("ERROR_CODE_RANGE_NOT_SATISFIABLE", 10),
            ("ERROR_CODE_NOT_MODIFIED", 11),
        ]
        .into_iter()
        .map(|(name, number)| (name.to_owned(), number))
        .collect::<Vec<_>>()
    );

    let inclusive_range = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("InclusiveRange"))
        .expect("InclusiveRange descriptor");
    let end = inclusive_range
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("end"))
        .expect("InclusiveRange.end");
    assert_eq!(end.number, Some(2));
    assert_eq!(end.proto3_optional, Some(true));
    assert_eq!(inclusive_range.oneof_decl[0].name.as_deref(), Some("_end"));

    for (message_name, oneof_name) in [
        ("Preconditions", "condition"),
        ("PutObjectRequest", "frame"),
        ("ByteRange", "selection"),
        ("GetObjectResponse", "frame"),
        ("UploadPartRequest", "frame"),
    ] {
        let message = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some(message_name))
            .expect("Objects oneof message");
        assert!(
            message
                .oneof_decl
                .iter()
                .any(|oneof| oneof.name.as_deref() == Some(oneof_name)),
            "missing {message_name}.{oneof_name} oneof"
        );
    }
}

#[test]
fn objects_routes_cover_every_rpc_with_canonical_http_projection() {
    assert_eq!(OBJECTS_ROUTES.len(), 13);
    assert_eq!(
        OBJECTS_ROUTES.len(),
        OBJECTS_V2
            .services
            .iter()
            .map(|service| service.methods.len())
            .sum::<usize>()
    );

    let expected = [
        ("createBucket", "/v2/objects/buckets/create"),
        ("headBucket", "/v2/objects/buckets/head"),
        ("deleteBucket", "/v2/objects/buckets/delete"),
        ("putObject", "/v2/objects/objects/put"),
        ("getObject", "/v2/objects/objects/get"),
        ("headObject", "/v2/objects/objects/head"),
        ("deleteObject", "/v2/objects/objects/delete"),
        ("listObjects", "/v2/objects/objects/list"),
        ("createMultipart", "/v2/objects/multipart/create"),
        ("uploadPart", "/v2/objects/multipart/upload-part"),
        ("listParts", "/v2/objects/multipart/list-parts"),
        ("completeMultipart", "/v2/objects/multipart/complete"),
        ("abortMultipart", "/v2/objects/multipart/abort"),
    ];
    for (route, (operation_id, path)) in OBJECTS_ROUTES.iter().zip(expected) {
        assert_eq!(route.method, "POST");
        assert_eq!(route.operation_id, operation_id);
        assert_eq!(route.path, path);
        assert!(route.rpc.starts_with("acyclic.objects.v2."));
        assert!(!route.docs.is_empty());

        let method = OBJECTS_V2
            .services
            .iter()
            .flat_map(|service| service.methods.iter().map(move |method| (service, method)))
            .find(|(service, method)| {
                route
                    .rpc
                    .ends_with(&format!("{}/{}", service.name, method.name))
            })
            .map(|(_, method)| method)
            .expect("route RPC exists in Objects services");
        assert_eq!(route.request, method.input);
        assert_eq!(route.response, method.output);
        assert_eq!(route.docs, method.docs);
    }
}

#[test]
fn objects_documentation_tables_cover_the_contract_kinds() {
    assert_eq!(OBJECTS_SERVICE_DOCS.len(), OBJECTS_V2.services.len());
    assert_eq!(OBJECTS_METHOD_DOCS.len(), 13);
    assert_eq!(OBJECTS_ENUM_DOCS.len(), OBJECTS_V2.enums.len());
    assert_eq!(OBJECTS_MESSAGE_DOCS.len(), 37);
    assert!(OBJECTS_FIELD_DOCS.len() >= 40);
    for service in OBJECTS_V2.services {
        assert!(
            OBJECTS_SERVICE_DOCS
                .iter()
                .any(|doc| doc.name == service.name)
        );
        for method in service.methods {
            assert!(
                OBJECTS_METHOD_DOCS
                    .iter()
                    .any(|doc| doc.name == method.name)
            );
        }
    }
    for enum_ in OBJECTS_V2.enums {
        assert!(OBJECTS_ENUM_DOCS.iter().any(|doc| doc.name == enum_.name));
    }
    for message in OBJECTS_V2.messages {
        assert!(
            OBJECTS_MESSAGE_DOCS
                .iter()
                .any(|doc| doc.name == message.name)
        );
    }
}

#[test]
fn objects_proto_emits_rust_owned_documentation_without_placeholders() {
    let proto = acyclic_sdk_contract_wire::objects::objects_proto();
    for doc in OBJECTS_SERVICE_DOCS
        .iter()
        .chain(OBJECTS_METHOD_DOCS)
        .chain(OBJECTS_ENUM_DOCS)
        .chain(OBJECTS_MESSAGE_DOCS)
        .chain(OBJECTS_FIELD_DOCS)
    {
        assert!(
            proto.contains(&format!("// {}", doc.text)),
            "Objects proto is missing documentation for {}",
            doc.name
        );
    }
    assert!(!proto.contains("TODO"));
    assert!(!proto.contains("placeholder"));
}

#[test]
fn objects_descriptor_preserves_published_options_and_declares_no_extensions() {
    let descriptor = FileDescriptorSet::decode(OBJECTS_GOLDEN_DESCRIPTOR)
        .expect("decode Objects v2 golden descriptor");
    let file = descriptor.file.first().expect("Objects v2 file descriptor");
    let options = file.options.as_ref().expect("file options");
    assert_eq!(
        options.go_package.as_deref(),
        Some("github.com/acyclic-labs/sdk/go/gen/objects/v2;objectsv2")
    );
    assert!(
        options.uninterpreted_option.is_empty(),
        "custom/uninterpreted options would change the deployed contract"
    );

    fn assert_message_is_unreserved(message: &prost_types::DescriptorProto) {
        assert!(
            message.reserved_range.is_empty(),
            "{} has reserved ranges",
            message.name.as_deref().unwrap_or("<anonymous>")
        );
        assert!(
            message.reserved_name.is_empty(),
            "{} has reserved names",
            message.name.as_deref().unwrap_or("<anonymous>")
        );
        for nested in &message.nested_type {
            assert_message_is_unreserved(nested);
        }
    }
    for message in &file.message_type {
        assert_message_is_unreserved(message);
    }
    for enum_type in &file.enum_type {
        assert!(
            enum_type.reserved_range.is_empty(),
            "{} has reserved enum ranges",
            enum_type.name.as_deref().unwrap_or("<anonymous>")
        );
        assert!(
            enum_type.reserved_name.is_empty(),
            "{} has reserved enum names",
            enum_type.name.as_deref().unwrap_or("<anonymous>")
        );
    }
}

#[test]
fn archived_objects_v1_descriptor_remains_immutable_and_separate() {
    let digest = Sha256::digest(ARCHIVED_OBJECTS_V1_DESCRIPTOR);
    let digest_hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert!(ARCHIVED_OBJECTS_V1_MANIFEST.contains(&format!("sha256:{digest_hex}")));
    assert_ne!(ARCHIVED_OBJECTS_V1_DESCRIPTOR, OBJECTS_GOLDEN_DESCRIPTOR);

    let archived = FileDescriptorSet::decode(ARCHIVED_OBJECTS_V1_DESCRIPTOR)
        .expect("decode archived Objects v1 descriptor");
    let active = FileDescriptorSet::decode(OBJECTS_GOLDEN_DESCRIPTOR)
        .expect("decode active Objects v2 descriptor");
    assert_eq!(
        archived
            .file
            .first()
            .and_then(|file| file.package.as_deref()),
        Some("google.protobuf")
    );
    assert_eq!(
        archived
            .file
            .iter()
            .find(|file| file.package.as_deref() == Some("acyclic.objects.v1"))
            .and_then(|file| file.package.as_deref()),
        Some("acyclic.objects.v1")
    );
    assert_eq!(
        active.file.first().and_then(|file| file.package.as_deref()),
        Some("acyclic.objects.v2")
    );
}

#[test]
fn generated_objects_rust_and_tonic_surfaces_cover_the_model() {
    let messages = [
        "BucketRef",
        "Bucket",
        "ObjectMetadata",
        "Preconditions",
        "MutationIdentity",
        "ObjectInfo",
        "CreateBucketRequest",
        "HeadBucketRequest",
        "DeleteBucketRequest",
        "DeleteBucketResponse",
        "PutObjectHeader",
        "PutObjectRequest",
        "InclusiveRange",
        "ByteRange",
        "GetObjectRequest",
        "ContentRange",
        "GetObjectHeader",
        "GetObjectResponse",
        "HeadObjectRequest",
        "HeadObjectResponse",
        "DeleteObjectRequest",
        "DeleteObjectResponse",
        "ListObjectsRequest",
        "ListEntry",
        "ListObjectsResponse",
        "CreateMultipartRequest",
        "MultipartUpload",
        "UploadPartHeader",
        "UploadPartRequest",
        "UploadedPart",
        "ListPartsRequest",
        "ListPartsResponse",
        "CompleteMultipartRequest",
        "AbortMultipartRequest",
        "AbortMultipartResponse",
        "ErrorDetail",
    ];
    for name in messages {
        assert!(
            GENERATED_OBJECTS_MESSAGES.contains(&format!("pub struct {name} {{")),
            "generated Rust message missing: {name}"
        );
    }
    assert!(
        GENERATED_OBJECTS_MESSAGES
            .contains("#[prost(btree_map = \"string, string\", tag = \"2\")]")
    );
    assert!(GENERATED_OBJECTS_MESSAGES.contains("#[prost(int64, optional, tag = \"7\")]"));
    assert!(GENERATED_OBJECTS_MESSAGES.contains("pub mod preconditions"));
    assert!(GENERATED_OBJECTS_MESSAGES.contains("pub mod byte_range"));
    assert!(GENERATED_OBJECTS_MESSAGES.contains("pub mod put_object_request"));
    assert!(GENERATED_OBJECTS_MESSAGES.contains("pub mod get_object_response"));
    assert!(GENERATED_OBJECTS_MESSAGES.contains("pub mod upload_part_request"));

    for (module, service, methods) in [
        (
            "buckets_service_client",
            "BucketsServiceClient",
            ["create_bucket", "head_bucket", "delete_bucket"].as_slice(),
        ),
        (
            "objects_service_client",
            "ObjectsServiceClient",
            [
                "put_object",
                "get_object",
                "head_object",
                "delete_object",
                "list_objects",
            ]
            .as_slice(),
        ),
        (
            "multipart_service_client",
            "MultipartServiceClient",
            [
                "create_multipart",
                "upload_part",
                "list_parts",
                "complete_multipart",
                "abort_multipart",
            ]
            .as_slice(),
        ),
    ] {
        assert!(
            GENERATED_OBJECTS_TONIC.contains(&format!("pub mod {module}")),
            "generated tonic module missing: {module}"
        );
        assert!(
            GENERATED_OBJECTS_TONIC.contains(&format!("pub struct {service}<T>")),
            "generated tonic client missing: {service}"
        );
        for method in methods {
            assert!(
                GENERATED_OBJECTS_TONIC.contains(&format!("pub async fn {method}")),
                "generated tonic method missing: {service}.{method}"
            );
        }
    }
}
