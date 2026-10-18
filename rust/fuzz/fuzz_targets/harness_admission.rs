//! Fuzz admission validation and protobuf round-trip stability.
#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{wire, wire_api::validate_admission};
use prost::Message as _;

fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    let Some((command, admission)) =
        fuzz_support::protobuf_inputs!(fuzz_input, wire::CommandEnvelope, wire::Admission,)
    else {
        return;
    };
    let expected = validate_admission(&command, &admission).map_err(|error| error.code());
    let command = fuzz_support::round_trip(
        &command,
        wire::CommandEnvelope::decode(command.encode_to_vec().as_slice()),
    );
    let admission = fuzz_support::round_trip(
        &admission,
        wire::Admission::decode(admission.encode_to_vec().as_slice()),
    );
    assert_eq!(
        validate_admission(&command, &admission).map_err(|error| error.code()),
        expected
    );
});
