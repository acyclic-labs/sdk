#![no_main]
#[path = "../support/swarm_limits.rs"]
mod limits;
#[path = "../support/swarm_membership.rs"]
mod membership;
#[path = "../support/swarm_views.rs"]
mod oracle;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    membership::exercise(bytes);
    limits::exercise(bytes, false);
    oracle::exercise(bytes, false);
});
