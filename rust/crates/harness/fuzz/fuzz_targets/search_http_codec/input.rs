#[path = "../../../tests/search_http_codec/support.rs"]
mod support;

pub fn run(data: &[u8], exa: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Raw corpus covers protocol shapes; generated envelopes independently
    // vary depth and scalar size, including responses above the HTTP bound.
    let depth = u16::from_le_bytes([
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
    ]) as usize;
    let size = u32::from_le_bytes([
        data.get(2).copied().unwrap_or(0),
        data.get(3).copied().unwrap_or(0),
        data.get(4).copied().unwrap_or(0),
        data.get(5).copied().unwrap_or(0),
    ]) as usize
        % 1_048_578;
    runtime.block_on(support::check(data.to_vec(), exa));
    runtime.block_on(support::check(
        support::response(
            depth,
            size,
            data.get(6).copied().unwrap_or(0),
            &serde_json::Value::String(String::from_utf8_lossy(data).into_owned()),
        ),
        exa,
    ));
}
