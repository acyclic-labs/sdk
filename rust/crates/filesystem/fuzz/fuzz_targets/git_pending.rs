//! Pending join identities and conflict paths retain a canonical wire form.
#![no_main]
use acyclic_fs::GitPendingTransition;

fn check(bytes: &[u8]) -> serde_json::Result<()> {
    if let Ok(pending) = serde_json::from_slice::<GitPendingTransition>(bytes) {
        let encoded = serde_json::to_vec(&pending)?;
        let restored: GitPendingTransition = serde_json::from_slice(&encoded)?;
        assert_eq!(pending, restored);
        assert_eq!(serde_json::to_vec(&restored)?, encoded);
    }
    Ok(())
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let result = check(bytes);
    assert!(result.is_ok(), "pending join round trip: {result:?}");
});
