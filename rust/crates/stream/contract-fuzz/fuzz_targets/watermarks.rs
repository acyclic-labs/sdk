#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_stream::{StreamError, StreamWatermarks, wire, wire_codec::watermarks_from_wire};
use prost::Message;

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if input.len() > 4096 {
        return;
    }
    if let Some(request) = fuzz_support::FuzzInput::parse(fuzz_input, wire::TailRequest::decode) {
        let _ = fuzz_support::round_trip(
            &(request),
            wire::TailRequest::decode(request.encode_to_vec().as_slice()),
        );
    }
    if let Some(response) = fuzz_support::FuzzInput::parse(fuzz_input, wire::TailResponse::decode) {
        let _ = fuzz_support::round_trip(
            &(response),
            wire::TailResponse::decode(response.encode_to_vec().as_slice()),
        );
        let expected = match response.durable {
            None => Err(StreamError::Unsupported),
            Some(durable) if durable > response.tail => Err(StreamError::Unavailable),
            Some(durable) => Ok(StreamWatermarks {
                accepted: response.tail,
                durable,
            }),
        };
        assert_eq!(watermarks_from_wire(response), expected);
    }
});
