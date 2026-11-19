//! Compare failure admission and recorded public codes through production codecs.
#![no_main]

use acyclic_stream::{StreamCategory, StreamError};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let expected = serde_json::from_slice::<String>(input)
        .ok()
        .and_then(|code| StreamError::from_code(&code));
    let decoded = serde_json::from_slice::<StreamError>(input);
    assert_eq!(
        decoded.as_ref().ok().map(StreamError::category),
        expected.as_ref().map(StreamError::category),
        "failure admission changed: decoded={decoded:?}"
    );
    // Admission must hold even when the arbitrary input is not a valid code.
    // Select from the public values independently of the decoder under test.
    let all = StreamCategory::ALL.map(StreamError::from);
    let index = usize::from(input.first().copied().unwrap_or(0)) % all.len();
    let selected = all.get(index);
    assert!(selected.is_some(), "public failure selection was absent");
    if let Some(error) = selected {
        let encoded = serde_json::to_vec(error);
        assert!(encoded.is_ok(), "canonical failure encoding: {encoded:?}");
        if let Ok(encoded) = encoded {
            let code = serde_json::from_slice::<String>(&encoded);
            assert!(
                code.as_ref().is_ok_and(|code| code == error.code()),
                "canonical failure code: original={error:?}; decoded={code:?}"
            );
            let decoded = serde_json::from_slice::<StreamError>(&encoded);
            assert!(
                decoded
                    .as_ref()
                    .is_ok_and(|decoded| decoded.category() == error.category()),
                "canonical failure writer output was refused: original={error:?}; decoded={decoded:?}"
            );
        }
    }
});
