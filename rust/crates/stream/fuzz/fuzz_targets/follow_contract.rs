#![no_main]

mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/support/follow_contract.rs"
    ));
}

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    checks::check(input, checks::Fault::None).expect("follow wire contract");
});
