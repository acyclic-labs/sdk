#![no_main]
use acyclic_auth::credential::{MAX_CREDENTIAL_BYTES, valid_credential};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|value: &str| {
    let admitted = valid_credential(value);
    assert_eq!(
        admitted,
        value.len() <= MAX_CREDENTIAL_BYTES
            && value.chars().any(|c| !c.is_whitespace())
            && !value.bytes().any(|c| [0, 10, 13].contains(&c))
    );
    if admitted {
        // UTF-8 transport preserves the opaque credential without normalization.
        assert_eq!(std::str::from_utf8(value.as_bytes()).unwrap(), value);
    }
});
