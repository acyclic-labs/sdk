//! External consumers can validate durable generated metadata without transport features.
use acyclic_objects::v2::{response, wire};

#[test]
fn core_metadata_validation_rejects_malformed_persisted_values() {
    let time = prost_types::Timestamp {
        seconds: 1,
        nanos: 0,
    };
    assert!(response::timestamp(&time).is_ok());
    assert!(
        response::timestamp(&prost_types::Timestamp {
            seconds: 1,
            nanos: -1
        })
        .is_err()
    );
    let expected = wire::BucketRef {
        name: "persisted-bucket".into(),
    };
    let bucket = wire::Bucket {
        bucket: Some(expected.clone()),
        created_at: Some(time),
    };
    assert!(response::bucket(&bucket, &expected).is_ok());
    let invalid = wire::BucketRef {
        name: "../invalid".into(),
    };
    assert!(
        response::bucket(
            &wire::Bucket {
                bucket: Some(invalid.clone()),
                created_at: bucket.created_at,
            },
            &invalid
        )
        .is_err()
    );
    let mut object = wire::ObjectInfo {
        etag: "opaque-etag".into(),
        size: 1,
        last_modified: bucket.created_at,
        metadata: Some(wire::ObjectMetadata::default()),
    };
    assert!(response::object_info(&object).is_ok());
    // Object metadata may describe a completed multipart object larger than a
    // single PUT/part limit. Selection bounds are checked by get_header.
    object.size = 6 * 1024 * 1024 * 1024;
    assert!(response::object_info(&object).is_ok());
    assert_eq!(
        response::get_header(
            &wire::GetObjectHeader {
                object: Some(object.clone()),
                content_range: None,
            },
            &None,
            1024,
        )
        .err()
        .map(|error| error.code),
        Some(wire::ErrorCode::QuotaExceeded),
    );
    object.size = 1;
    object.etag = "bad\nheader".into();
    assert!(response::object_info(&object).is_err());
}
