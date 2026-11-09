//! Check recorded and arbitrary invocation headers against production admission.
#![no_main]

use acyclic_actors::{domain, wire};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

macro_rules! check {
    ($input:expr, $request:ident, $header:ident) => {
        if let Ok(wire) = wire::$request::decode($input)
            && let Ok(request) = domain::$request::try_from(wire.clone())
        {
            assert_eq!(
                domain::$header::try_from(wire.clone()).is_ok(),
                wire.body.is_empty(),
                "a canonical header refuses any buffered body"
            );
            let (header, body) = request.into_parts();
            let encoded: wire::$request = header.into();
            assert_eq!(body, wire.body);
            let mut expected = wire;
            expected.body.clear();
            assert_eq!(encoded, expected, "metadata must remain unchanged");
            let decoded = wire::$request::decode(encoded.encode_to_vec().as_slice())
                .map_err(|error| format!("canonical request decode: {error}"))
                .and_then(|decoded| {
                    domain::$header::try_from(decoded)
                        .map_err(|error| format!("body-free header admission: {error}"))
                })
                .map(wire::$request::from);
            assert_eq!(decoded, Ok(expected));
            true
        } else {
            false
        }
    };
}

fuzz_target!(|input: &[u8]| {
    // Original public-constructor/prost captures; the first eight bytes select
    // the shared protobuf corpus type and are not part of the request packet.
    assert!(
        match include_bytes!(
            "../../rust/codec-fuzz/corpus/protobuf/acyclic.actors.v1.InvokeActorRequest-semantic"
        )
        .as_slice()
        {
            [_, _, _, _, _, _, _, _, payload @ ..] =>
                check!(payload, InvokeActorRequest, InvokeActorHeader),
            _ => false,
        },
        "captured actor invocation was refused"
    );
    assert!(
        match include_bytes!(
            "../../rust/codec-fuzz/corpus/protobuf/acyclic.actors.v1.InvokeWorkerRequest-semantic"
        )
        .as_slice()
        {
            [_, _, _, _, _, _, _, _, payload @ ..] =>
                check!(payload, InvokeWorkerRequest, InvokeWorkerHeader),
            _ => false,
        },
        "captured worker invocation was refused"
    );
    check!(input, InvokeActorRequest, InvokeActorHeader);
    check!(input, InvokeWorkerRequest, InvokeWorkerHeader);
});
