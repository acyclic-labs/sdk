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
                .expect("canonical request encoding");
            assert_eq!(
                wire::$request::from(
                    domain::$header::try_from(decoded).expect("body-free canonical header")
                ),
                expected
            );
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
        check!(
            &include_bytes!(
                "../../rust/codec-fuzz/corpus/protobuf/acyclic.actors.v1.InvokeActorRequest-semantic"
            )[8..],
            InvokeActorRequest,
            InvokeActorHeader
        ),
        "captured actor invocation was refused"
    );
    assert!(
        check!(
            &include_bytes!(
                "../../rust/codec-fuzz/corpus/protobuf/acyclic.actors.v1.InvokeWorkerRequest-semantic"
            )[8..],
            InvokeWorkerRequest,
            InvokeWorkerHeader
        ),
        "captured worker invocation was refused"
    );
    check!(input, InvokeActorRequest, InvokeActorHeader);
    check!(input, InvokeWorkerRequest, InvokeWorkerHeader);
});
