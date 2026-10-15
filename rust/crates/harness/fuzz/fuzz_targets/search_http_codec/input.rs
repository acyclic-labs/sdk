#[path = "../../../tests/search_http_codec/support.rs"]
mod support;

pub fn run(data: &[u8], exa: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(support::check(support::fuzz_response(data), exa));
}
