use super::*;
use crate::foundation::Digest;

#[test]
fn request_validation_rejects_every_shape_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let extent = ObjectId {
        storage: None,
        kind: ObjectKind::ExtentPage,
        digest: Digest::ZERO,
    };
    let valid = ExtentCloneRequest {
        source_root: extent,
        source_logical_bytes: 1,
        source_range: ByteRange {
            offset: 0,
            length: 1,
        },
        destination_root: extent,
        destination_logical_bytes: 1,
        destination_offset: 0,
        maximum_spans: Some(1),
    };
    let cases = [
        (
            ExtentCloneRequest {
                source_root: ObjectId {
                    storage: None,
                    kind: ObjectKind::Blob,
                    digest: Digest::ZERO,
                },
                ..valid
            },
            ExtentCloneError::WrongRootKind,
        ),
        (
            ExtentCloneRequest {
                destination_root: ObjectId {
                    storage: None,
                    kind: ObjectKind::Blob,
                    digest: Digest::ZERO,
                },
                ..valid
            },
            ExtentCloneError::WrongRootKind,
        ),
        (
            ExtentCloneRequest {
                source_range: ByteRange {
                    offset: 0,
                    length: 0,
                },
                ..valid
            },
            ExtentCloneError::EmptyRange,
        ),
        (
            ExtentCloneRequest {
                source_range: ByteRange {
                    offset: u64::MAX,
                    length: 1,
                },
                ..valid
            },
            ExtentCloneError::RangeOverflow,
        ),
        (
            ExtentCloneRequest {
                destination_offset: u64::MAX,
                ..valid
            },
            ExtentCloneError::RangeOverflow,
        ),
        (
            ExtentCloneRequest {
                maximum_spans: Some(0),

                ..valid
            },
            ExtentCloneError::InvalidLimits,
        ),
        (valid, ExtentCloneError::InvalidLimits),
    ];
    let case_count = cases.len();
    for (index, (request, expected)) in cases.into_iter().enumerate() {
        let limits = if index + 1 == case_count {
            DecodeLimits {
                maximum_page_height: 0,
                ..DecodeLimits::default()
            }
        } else {
            DecodeLimits::default()
        };
        let Err(failure) = validate_request(request, limits) else {
            return Err("invalid clone request unexpectedly succeeded".into());
        };
        assert_eq!(
            std::mem::discriminant(&failure),
            std::mem::discriminant(&expected)
        );
    }
    Ok(())
}
