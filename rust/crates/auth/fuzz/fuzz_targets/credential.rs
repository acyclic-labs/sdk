#![no_main]

use acyclic_auth::credential::{MAX_CREDENTIAL_BYTES, valid_credential};
pub use acyclic_fuzz_support as fuzz_support;

libfuzzer_sys::fuzz_target!(|value: &str| {
    let admitted = valid_credential(value);
    assert_eq!(
        admitted,
        value.len() <= MAX_CREDENTIAL_BYTES
            && value.chars().any(|c| !c.is_whitespace())
            && !value.bytes().any(|c| [0, 10, 13].contains(&c))
    );
    if admitted {
        // UTF-8 transport preserves the opaque credential without normalization.
        assert_eq!(
            fuzz_support::must_decode(std::str::from_utf8(value.as_bytes())),
            value
        );
    }
});
