#![no_main]
use acyclic_auth::refusal::RefusalReason;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|value: &[u8]| {
    let reason = RefusalReason::from_header(Some(value));
    assert_eq!(reason.as_bytes(), Some(value));
    assert_eq!(RefusalReason::from_header(reason.as_bytes()), reason);
    assert_eq!(RefusalReason::from_header(None).as_bytes(), None);
});
