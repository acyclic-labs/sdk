//! Fuzz admission validation and protobuf round-trip stability.
#![no_main]

use acyclic_harness::{wire, wire_api::validate_admission_identity};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fuzz_target!(|data: &[u8]| {
    let mut data = data;
    // The first Admission carries only the optional expected operation identity.
    let (Ok(request), Ok(admission)) = (
        wire::Admission::decode_length_delimited(&mut data),
        wire::Admission::decode_length_delimited(&mut data),
    ) else {
        return;
    };
    let expected = match request.operation.as_ref() {
        None => Err(wire::ErrorCode::Invalid),
        Some(identity)
            if !identity.operation_id.is_empty()
                && !identity.idempotency_key.is_empty()
                && admission.operation.as_ref() == Some(identity) =>
        {
            Ok(())
        }
        Some(_) => Err(wire::ErrorCode::Conflict),
    };
    assert_eq!(
        validate_admission_identity(request.operation.as_ref(), &admission)
            .map_err(|error| error.code()),
        expected,
    );
    let request_round_trip = wire::Admission::decode(request.encode_to_vec().as_slice());
    let admission_round_trip = wire::Admission::decode(admission.encode_to_vec().as_slice());
    assert!(matches!(&request_round_trip, Ok(value) if value == &request));
    assert!(matches!(&admission_round_trip, Ok(value) if value == &admission));
    if let (Ok(request), Ok(admission)) = (request_round_trip, admission_round_trip) {
        assert_eq!(
            validate_admission_identity(request.operation.as_ref(), &admission)
                .map_err(|error| error.code()),
            expected,
        );
    }
});
