//! Exercise production Git conflict marker parsing.
#![no_main]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| acyclic_fs::git_compat::fuzz_markers(bytes));
