#![no_main]

use acyclic_stream::StreamError;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let expected = serde_json::from_slice::<String>(input)
        .ok()
        .and_then(|code| StreamError::from_code(&code));
    let decoded = serde_json::from_slice::<StreamError>(input);
    assert_eq!(decoded.ok(), expected);
    if let Some(error) = expected {
        let encoded = serde_json::to_vec(&error).expect("canonical failure encoding");
        assert_eq!(
            serde_json::from_slice::<String>(&encoded).expect("canonical code string"),
            error.code()
        );
        assert_eq!(
            serde_json::from_slice::<StreamError>(&encoded).ok(),
            expected
        );
    }
});
