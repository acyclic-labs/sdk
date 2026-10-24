#![no_main]
#[cfg(test)]
acyclic_test_support::counting_allocator!();

libfuzzer_sys::fuzz_target!(|input: &str| {
    acyclic_harness::tools::fuzz_command(input);
});
