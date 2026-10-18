#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/support/follow_contract.rs"
    ));
}

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    checks::check(fuzz_input, checks::Fault::None).expect("follow wire contract");
});
