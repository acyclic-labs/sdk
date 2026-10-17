#![no_main]
#[path = "../../harness/tests/search_request/support.rs"]
mod support;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(support::check(data));
});
