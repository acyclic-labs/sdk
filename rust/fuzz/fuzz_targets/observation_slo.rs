//! Exercise the persisted SLO decoder and public evidence evaluator together.
#![no_main]

#[path = "../support/observation_slo.rs"]
mod oracle;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    assert!(oracle::check(bytes).is_ok());
});
