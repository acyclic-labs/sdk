#![no_main]

use acyclic_auth_wasm::block_ids;
pub use acyclic_fuzz_support as fuzz_support;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Some(ids) = fuzz_support::accepts(bytes.len().is_multiple_of(32), block_ids(bytes)) {
        assert!(ids.iter().flatten().copied().eq(bytes.iter().copied()));
    }
});
