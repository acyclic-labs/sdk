#![no_main]

use acyclic_auth::refusal::REASON_HEADER;
use acyclic_objects::{Error, wire::ErrorCode};
use acyclic_stream::{RefusalReason, StreamError};
use libfuzzer_sys::fuzz_target;
use tonic::{Code, Status, metadata::MetadataValue};

// Generated codec inputs exercise the shipping projections. They are not a
// substitute for a captured service response and its deployment provenance.
fuzz_target!(|input: (i32, u16, Vec<u8>, Vec<u8>, bool)| {
    let (grpc, http, reason_bytes, details, present) = input;
    let grpc_failure = Error::from_grpc_status(grpc);
    let http_failure = Error::from_http_status(http);
    // Unknown protocol statuses cannot invent a definitive service refusal.
    if !(0..=16).contains(&grpc) {
        assert_eq!(grpc_failure.code, ErrorCode::Unavailable);
    }
    if !(400..600).contains(&http) {
        assert_eq!(http_failure.code, ErrorCode::Unavailable);
    }
    // Terminal capability/authentication refusals must never turn into an
    // outage because that would authorize a transport retry.
    if matches!(grpc, 7 | 16) {
        assert_eq!(grpc_failure.code, ErrorCode::AccessDenied);
    }
    if grpc == 12 {
        assert_eq!(grpc_failure.code, ErrorCode::Unsupported);
    }
    if matches!(http, 401 | 403) {
        assert_eq!(http_failure.code, ErrorCode::AccessDenied);
    }
    if http == 501 {
        assert_eq!(http_failure.code, ErrorCode::Unsupported);
    }

    let mut original =
        Status::with_details(Code::from_i32(grpc), "original", details.clone().into());
    let reason = if present {
        match MetadataValue::try_from(reason_bytes.as_slice()) {
            Ok(value) => {
                original.metadata_mut().insert(REASON_HEADER, value);
                RefusalReason::from_header(Some(&reason_bytes))
            }
            Err(_) => RefusalReason::Unspecified,
        }
    } else {
        RefusalReason::Unspecified
    };
    let mut without_reason = original.clone();
    without_reason.metadata_mut().remove(REASON_HEADER);
    let stream = acyclic_stream::wire_codec::error_from_status(&original);
    let stream_without = acyclic_stream::wire_codec::error_from_status(&without_reason);
    assert_eq!(stream.code(), stream_without.code());
    assert_eq!(stream.uncertain(), stream_without.uncertain());
    assert_eq!(stream, stream_without.with_refusal_reason(reason.clone()));
    let objects = acyclic_objects::grpc::error_from_status(&original);
    let objects_without = acyclic_objects::grpc::error_from_status(&without_reason);
    assert_eq!(objects, objects_without.with_refusal_reason(reason.clone()));
    match acyclic_actors::client::error_from_status(&original) {
        acyclic_actors::client::Error::Service {
            grpc_code,
            reason: actual,
            message,
            raw_details,
            ..
        } => {
            assert_eq!(grpc_code, Code::from_i32(grpc) as i32);
            assert_eq!(message, "original");
            assert_eq!(raw_details, details);
            assert_eq!(
                actual,
                if grpc_code == Code::Unavailable as i32 {
                    reason
                } else {
                    RefusalReason::Unspecified
                }
            );
        }
        other => panic!("service status lost: {other}"),
    }

    // Diagnostic bytes may not turn an unavailable outcome into a definite one,
    // including bytes the gRPC header cannot represent.
    let reason = RefusalReason::from_header(Some(&reason_bytes));
    let stream = StreamError::unavailable().with_refusal_reason(reason.clone());
    let stream_status = acyclic_stream::wire_codec::error_status(&stream);
    assert_eq!(stream_status.code(), Code::Unavailable);
    let objects = Error::from(ErrorCode::Unavailable).with_refusal_reason(reason);
    let objects_status = acyclic_objects::grpc::error_status(&objects, String::new());
    assert_eq!(objects_status.code(), Code::Unavailable);
    if MetadataValue::<tonic::metadata::Ascii>::try_from(reason_bytes.as_slice()).is_ok() {
        assert_eq!(
            acyclic_stream::wire_codec::error_from_status(&stream_status),
            stream
        );
        assert_eq!(
            acyclic_objects::grpc::error_from_status(&objects_status),
            objects
        );
    } else {
        assert_eq!(
            std::error::Error::source(&stream_status)
                .and_then(|source| source.downcast_ref::<StreamError>()),
            Some(&stream)
        );
        assert_eq!(
            std::error::Error::source(&objects_status)
                .and_then(|source| source.downcast_ref::<Error>()),
            Some(&objects)
        );
    }
});
