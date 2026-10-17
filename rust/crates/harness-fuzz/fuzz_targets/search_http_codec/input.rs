#[path = "../../../harness/tests/search_http_codec/support.rs"]
mod support;

pub fn run(data: &[u8]) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let bytes = support::fuzz_response(data);
    runtime.block_on(support::check(bytes.clone(), true));
    runtime.block_on(support::check(bytes, false));
}
