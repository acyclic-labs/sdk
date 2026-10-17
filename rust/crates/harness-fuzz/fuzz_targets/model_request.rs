#![no_main]

use acyclic_harness::{
    conversation::Limits,
    model::{ModelRequest, PreparedModelRequest},
};

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(manifest) =
        acyclic_harness::context::ContextManifest::decode(bytes, Limits::default())
    {
        let encoded = manifest.encode(Limits::default()).unwrap();
        assert_eq!(
            acyclic_harness::context::ContextManifest::decode(&encoded, Limits::default()).unwrap(),
            manifest
        );
    }
    let Ok(request) = serde_json::from_slice::<ModelRequest>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&request).unwrap();
    assert_eq!(
        serde_json::from_slice::<ModelRequest>(&encoded).unwrap(),
        request
    );
    if let Ok(prepared) = PreparedModelRequest::prepare(request.clone(), Limits::default(), None) {
        let decoded =
            PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None).unwrap();
        assert_eq!(decoded.request(), &request);
        assert_eq!(decoded.request_digest(), prepared.request_digest());
    }
});
