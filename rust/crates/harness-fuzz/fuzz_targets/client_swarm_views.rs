#![no_main]
use acyclic_harness::test_support::fuzz_cases::notification_files as notifications;
use acyclic_harness::test_support::fuzz_cases::swarm_limits as limits;
use acyclic_harness::test_support::fuzz_cases::swarm_membership as membership;
use acyclic_harness::test_support::fuzz_cases::swarm_views as oracle;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    notifications::exercise(bytes);
    membership::exercise(bytes);
    membership::query(bytes, false);
    limits::exercise(bytes, false);
    oracle::exercise(bytes, false);
});
