#![no_main]
mod case {
    use acyclic_harness_core as acyclic_harness;
    include!("../../harness/tests/support/bounded_json.rs");
}
libfuzzer_sys::fuzz_target!(|input: &[u8]| case::check(input));
