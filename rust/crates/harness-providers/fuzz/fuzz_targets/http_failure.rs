//! Exercises production HTTP failure classification with untrusted response bytes.
#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    assert_eq!(
        acyclic_harness_providers::fuzzing::assert_http_failure_invariants(bytes)
            .map(|_| ())
            .map_err(|error| format!("{error:?}")),
        Ok(())
    );
});
