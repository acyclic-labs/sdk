use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::{
    Error,
    search::{SearchResponse, decode_search_http_response},
};
#[path = "oracle.rs"]
mod oracle;

// Both codec paths agree with the independent oracle, and a funded native
// decode returns every byte it charged to its ledger.
pub fn check(bytes: Vec<u8>, exa: bool) {
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &CancellationToken::new(),
    )
    .unwrap();
    let baseline = work.live_allocation_bytes();
    let expected = oracle::expected(&bytes, exa);
    let ordinary = decode_search_http_response(&bytes, 5, exa, None);
    assert!(
        oracle::agrees(&expected, ordinary.as_ref().map(|(response, _)| response)),
        "ordinary codec disagrees with independent oracle: {ordinary:?}"
    );
    if let Ok((_, custody)) = &ordinary {
        assert!(custody.is_none());
    }
    drop(ordinary);
    let result = decode_search_http_response(&bytes, 5, exa, Some(&work));
    assert!(
        oracle::agrees(&expected, result.as_ref().map(|(response, _)| response)),
        "codec disagrees with independent oracle: {result:?}"
    );
    match result {
        Ok((response, custody)) => {
            assert!(custody.as_ref().unwrap().work().shares_ledger(&work));
            let encoded = serde_json::to_vec(&response).unwrap();
            let ordinary = serde_json::from_slice::<SearchResponse>(&encoded).unwrap();
            assert_eq!(ordinary, response);
            drop((response, custody));
        }
        Err(error) => assert!(matches!(error, Error::ToolRejected { .. }), "{error:?}"),
    }
    assert_eq!(work.live_allocation_bytes(), baseline);
}

pub fn fuzz_response(data: &[u8]) -> Vec<u8> {
    let Some(data) = data.strip_prefix(&[0xff]) else {
        return data.to_vec();
    };
    let depth = u16::from_le_bytes([
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
    ]) as usize;
    let size = u32::from_le_bytes([
        data.get(2).copied().unwrap_or(0),
        data.get(3).copied().unwrap_or(0),
        data.get(4).copied().unwrap_or(0),
        data.get(5).copied().unwrap_or(0),
    ]) as usize;
    let size = if size > 1_048_576 {
        1_048_577
    } else {
        size % 16_385
    };
    response(
        depth,
        size,
        data.get(6).copied().unwrap_or(0),
        &serde_json::Value::String(
            String::from_utf8_lossy(data.get(7..).unwrap_or_default()).into_owned(),
        ),
    )
}

// Iterative construction explores depth independently of allocation/stack
// limits in serde_json::Value. Scalar size includes the HTTP byte boundary.
pub fn response(depth: usize, size: usize, shape: u8, leaf: &serde_json::Value) -> Vec<u8> {
    let leaf = serde_json::to_string(leaf).unwrap();
    let text = serde_json::to_string(&leaf).unwrap();
    let mut body = match shape % 6 {
        0 => r#"{"results":[],"metadata":"#.to_owned(),
        1 | 4 | 5 => {
            let highlights = match shape % 6 {
                1 => format!("[{text}]"),
                4 => format!("[{text},{text}]"),
                _ => "[]".into(),
            };
            format!(
                "{{\"results\":[{{\"url\":\"https://example.com\",\"title\":\"source\",\"snippet\":{text},\"highlights\":{highlights}}}],\"metadata\":"
            )
        }
        2 => r#"{"results":false,"metadata":"#.to_owned(),
        _ => r#"{"results":[{"url":null}],"metadata":"#.to_owned(),
    };
    for level in 0..depth {
        body.push_str(if level % 2 == 0 { "[" } else { "{\"x\":" });
    }
    body.push_str("[\"");
    body.extend(std::iter::repeat_n('x', size));
    body.push_str("\",");
    body.push_str(&leaf);
    body.push(']');
    for level in (0..depth).rev() {
        body.push(if level % 2 == 0 { ']' } else { '}' });
    }
    body.push('}');
    body.into_bytes()
}
