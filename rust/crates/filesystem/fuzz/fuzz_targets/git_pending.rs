//! Pending join identities and conflict paths retain a canonical wire form.
#![no_main]
use acyclic_fs::GitPendingTransition;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(pending) = serde_json::from_slice::<GitPendingTransition>(bytes) {
        let encoded = serde_json::to_vec(&pending).unwrap();
        let restored: GitPendingTransition = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(pending, restored);
        assert_eq!(serde_json::to_vec(&restored).unwrap(), encoded);
    }
});
