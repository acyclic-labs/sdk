#![no_main]
#[cfg(test)]
acyclic_test_support::counting_allocator!();

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    acyclic_harness::hooks::fuzz_output(bytes);
});
