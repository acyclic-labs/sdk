#![no_main]
//! Preserve protobuf round trips and explicit acceptance/durability boundaries.

use acyclic_stream::{StreamError, StreamWatermarks, wire, wire_codec::watermarks_from_wire};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|input: &[u8]| {
    if let Ok(request) = wire::TailRequest::decode(input) {
        assert_eq!(
            wire::TailRequest::decode(request.encode_to_vec().as_slice())
                .ok()
                .as_ref(),
            Some(&request)
        );
    }
    if let Ok(response) = wire::TailResponse::decode(input) {
        assert_eq!(
            wire::TailResponse::decode(response.encode_to_vec().as_slice())
                .ok()
                .as_ref(),
            Some(&response)
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
