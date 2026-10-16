#![no_main]
#[path = "../support/swarm_limits.rs"]
mod limits;
#[path = "../support/swarm_views.rs"]
mod oracle;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    limits::exercise(bytes, false);
    oracle::exercise(bytes, false);
});
