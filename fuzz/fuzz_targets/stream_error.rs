#![no_main]

use acyclic_stream::StreamError;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let expected = serde_json::from_slice::<String>(input)
        .ok()
        .and_then(|code| StreamError::from_code(&code));
    let decoded = serde_json::from_slice::<StreamError>(input);
    assert_eq!(decoded.ok(), expected);
    // Admission must hold even when the arbitrary input is not a valid code.
    // Select from the public values independently of the decoder under test.
    let error = &StreamError::ALL
        [usize::from(input.first().copied().unwrap_or(0)) % StreamError::ALL.len()];
    let encoded = serde_json::to_vec(error).expect("canonical failure encoding");
    assert_eq!(
        serde_json::from_slice::<String>(&encoded).expect("canonical code string"),
        error.code()
    );
    assert_eq!(
        serde_json::from_slice::<StreamError>(&encoded)
            .as_ref()
            .ok(),
        Some(error),
        "canonical failure writer output was refused"
    );
});
