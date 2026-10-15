//! Fuzz admission validation and protobuf round-trip stability.
#![no_main]

use acyclic_harness::{wire, wire_api::validate_admission};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fuzz_target!(|data: &[u8]| {
    let mut data = data;
    let (Ok(command), Ok(admission)) = (
        wire::CommandEnvelope::decode_length_delimited(&mut data),
        wire::Admission::decode_length_delimited(&mut data),
    ) else {
        return;
    };
    let expected = validate_admission(&command, &admission).map_err(|error| error.code());
    let command_round_trip = wire::CommandEnvelope::decode(command.encode_to_vec().as_slice());
    let admission_round_trip = wire::Admission::decode(admission.encode_to_vec().as_slice());
    assert!(matches!(&command_round_trip, Ok(value) if value == &command));
    assert!(matches!(&admission_round_trip, Ok(value) if value == &admission));
    if let (Ok(command), Ok(admission)) = (command_round_trip, admission_round_trip) {
        assert_eq!(
            validate_admission(&command, &admission).map_err(|error| error.code()),
            expected,
        );
    }
});
