#[path = "../../../harness-contracts/tests/search_http_codec/support.rs"]
mod support;

pub fn run(data: &[u8]) {
    let result = (|| {
        let bytes = support::fuzz_response(data)?;
        support::check(bytes.clone(), true)?;
        support::check(bytes, false)
    })();
    assert!(result.is_ok(), "search response oracle failed: {result:?}");
}
