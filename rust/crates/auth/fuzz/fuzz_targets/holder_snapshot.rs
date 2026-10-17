#![no_main]
use acyclic_auth_wasm::block_ids;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    assert_eq!(block_ids(bytes).is_ok(), bytes.len() % 32 == 0);
    if let Ok(ids) = block_ids(bytes) {
        assert!(ids.iter().flatten().copied().eq(bytes.iter().copied()));
    }
});
