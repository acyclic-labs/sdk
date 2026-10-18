#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::client::InterestDocument;
use acyclic_stream::StreamPath;
#[path = "../../harness/tests/support/client_resume.rs"]
mod resume;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    resume::exercise(bytes);
    // Arbitrary untrusted bytes exercise both rejection and successful parsing.
    if let Some(document) = fuzz_support::FuzzInput::parse(fuzz_input, |raw_input| {
        InterestDocument::decode(raw_input, 4096)
    }) {
        let encoded = serde_json::to_vec(&document).unwrap();
        let _ = fuzz_support::round_trip(
            &(document),
            InterestDocument::decode(&encoded, encoded.len()),
        );
    }
    // Every input also produces a valid document. Unconditional success and exact
    // equality prevent a reject-all or dropped-selection decoder from passing.
    let document = InterestDocument {
        in_view: bytes
            .iter()
            .take(32)
            .map(|id| StreamPath::new(format!("swarm/{id}")).unwrap())
            .collect(),
        prefetch: bytes
            .iter()
            .skip(32)
            .take(32)
            .map(|id| StreamPath::new(format!("swarm/{id}")).unwrap())
            .collect(),
    };
    let encoded = serde_json::to_vec(&document).unwrap();
    let _ = fuzz_support::round_trip(
        &(document),
        InterestDocument::decode(&encoded, encoded.len()),
    );
    fuzz_support::must_reject(InterestDocument::decode(&encoded, encoded.len() - 1));

    // Invalid strings cannot bypass the platform validator through JSON.
    let path = String::from_utf8_lossy(&bytes[..bytes.len().min(128)]);
    let encoded = serde_json::to_vec(&serde_json::json!({"in_view": [&path]})).unwrap();
    let decoded = fuzz_support::accepts(
        StreamPath::new(path.as_ref()).is_ok(),
        InterestDocument::decode(&encoded, encoded.len()),
    );
    if let Some(document) = decoded {
        assert_eq!(document.in_view[0].as_str(), path);
    }
});
