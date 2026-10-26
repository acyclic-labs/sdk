#[path = "../../../harness/tests/search_http_codec/support.rs"]
mod support;

pub fn run(data: &[u8]) {
    let bytes = support::fuzz_response(data);
    support::check(bytes.clone(), true);
    support::check(bytes, false);
}
