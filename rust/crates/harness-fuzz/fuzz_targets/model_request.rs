#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{
    conversation::Limits,
    model::{ModelRequest, PreparedModelRequest},
};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(manifest) = fuzz_support::FuzzInput::parse(fuzz_input, |bytes| {
        acyclic_harness::context::ContextManifest::decode(bytes, Limits::default())
    }) {
        let encoded = manifest.encode(Limits::default()).unwrap();
        let _ = fuzz_support::round_trip(
            &manifest,
            acyclic_harness::context::ContextManifest::decode(&encoded, Limits::default()),
        );
    }
    let Some(request) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<ModelRequest>)
    else {
        return;
    };
    let encoded = serde_json::to_vec(&request).unwrap();
    let _ = fuzz_support::round_trip(&(request), serde_json::from_slice::<ModelRequest>(&encoded));
    if let Ok(prepared) = acyclic_harness::model::PreparedModelRequest::prepare(
        request.clone(),
        Limits::default(),
        None,
    ) {
        let _ = fuzz_support::round_trip_by(
            &(request.clone(), prepared.request_digest().to_owned()),
            PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None),
            |decoded| {
                Ok::<_, std::convert::Infallible>((
                    decoded.request().clone(),
                    decoded.request_digest().to_owned(),
                ))
            },
        );
    }
});
