#![no_main]
use acyclic_auth::refusal::RefusalReason;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|value: &[u8]| {
    let reason = RefusalReason::from_header(Some(value));
    assert_eq!(reason.as_bytes(), Some(value));
    assert_eq!(RefusalReason::from_header(reason.as_bytes()), reason);
    assert_eq!(RefusalReason::from_header(None).as_bytes(), None);
    // The generated vocabulary and shipping decoder must agree on classification,
    // not just round-trip bytes (which an always-Unknown decoder could satisfy).
    for known in RefusalReason::ALL {
        assert_eq!(RefusalReason::from_header(known.as_bytes()), *known);
        assert!(known.class().is_some());
    }
    if !RefusalReason::ALL
        .iter()
        .any(|known| known.as_bytes() == Some(value))
    {
        assert!(matches!(reason, RefusalReason::Unknown(_)));
        assert_eq!(reason.class(), None);
    }
});
