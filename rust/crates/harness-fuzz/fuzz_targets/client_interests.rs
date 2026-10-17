#![no_main]
use acyclic_harness::client::InterestDocument;
use acyclic_stream::StreamPath;
use libfuzzer_sys::fuzz_target;
#[path = "../../harness/tests/support/client_resume.rs"]
mod resume;

fuzz_target!(|bytes: &[u8]| {
    resume::exercise(bytes);
    // Arbitrary untrusted bytes exercise both rejection and successful parsing.
    if let Ok(document) = InterestDocument::decode(bytes, 4096) {
        let encoded = serde_json::to_vec(&document).unwrap();
        assert_eq!(
            InterestDocument::decode(&encoded, encoded.len()).unwrap(),
            document
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
    assert_eq!(
        InterestDocument::decode(&encoded, encoded.len()).unwrap(),
        document
    );
    assert!(InterestDocument::decode(&encoded, encoded.len() - 1).is_err());

    // Invalid strings cannot bypass the platform validator through JSON.
    let path = String::from_utf8_lossy(&bytes[..bytes.len().min(128)]);
    let encoded = serde_json::to_vec(&serde_json::json!({"in_view": [&path]})).unwrap();
    let decoded = InterestDocument::decode(&encoded, encoded.len());
    assert_eq!(decoded.is_ok(), StreamPath::new(path.as_ref()).is_ok());
    if let Ok(document) = decoded {
        assert_eq!(document.in_view[0].as_str(), path);
    }
});
