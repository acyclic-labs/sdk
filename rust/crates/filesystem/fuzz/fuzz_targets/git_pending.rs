//! Pending join identities and conflict paths retain a canonical wire form.
#![no_main]

use acyclic_fs::GitPendingTransition;
pub use acyclic_fuzz_support as fuzz_support;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(pending) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<GitPendingTransition>)
    {
        let encoded = fuzz_support::must_decode(serde_json::to_vec(&pending));
        let restored: GitPendingTransition =
            fuzz_support::round_trip(&pending, serde_json::from_slice(&encoded));

        assert_eq!(
            fuzz_support::must_decode(serde_json::to_vec(&restored)),
            encoded
        );
    }
});
