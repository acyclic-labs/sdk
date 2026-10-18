use acyclic_harness::{
    Error,
    search::{SearchResponse, SearchResult},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Exa {
    results: Vec<ExaItem>,
}
#[derive(Deserialize)]
struct ExaItem {
    url: String,
    title: Option<String>,
    #[serde(default)]
    highlights: Vec<String>,
}
#[derive(Deserialize)]
struct TinyFish {
    results: Vec<TinyFishItem>,
}
#[derive(Deserialize)]
struct TinyFishItem {
    url: String,
    title: String,
    snippet: String,
}

// Independent wire DTOs and normalization, without the production admission or
// codec. A lexical depth guard keeps the reference parser's stack bounded.
pub fn expected(bytes: &[u8], exa: bool) -> Option<SearchResponse> {
    let mut depth = 0_u32;
    let mut string = false;
    let mut escaped = false;
    if bytes.len() > 1_048_576 {
        return None;
    }
    for &byte in bytes {
        if string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                string = false;
            }
        } else {
            match byte {
                b'"' => {
                    if depth > 256 {
                        return None;
                    }
                    string = true;
                }
                b'[' | b'{' => {
                    if depth > 256 {
                        return None;
                    }
                    depth += 1;
                }
                b']' | b'}' => depth = depth.checked_sub(1)?,
                b':' | b',' | b' ' | b'\n' | b'\r' | b'\t' => {}
                _ if depth > 256 => return None,
                _ => {}
            }
        }
    }
    decode(bytes, exa)
}

fn decode(bytes: &[u8], exa: bool) -> Option<SearchResponse> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let results = if exa {
        Exa::deserialize(&mut decoder)
            .ok()?
            .results
            .into_iter()
            .take(5)
            .map(|item| SearchResult {
                url: item.url,
                title: item.title.unwrap_or_default(),
                snippet: item.highlights.join("\n"),
            })
            .collect()
    } else {
        TinyFish::deserialize(&mut decoder)
            .ok()?
            .results
            .into_iter()
            .take(5)
            .map(|item| SearchResult {
                url: item.url,
                title: item.title,
                snippet: item.snippet,
            })
            .collect()
    };
    decoder.end().ok()?;
    Some(SearchResponse { results })
}

pub fn agrees(
    expected: &Option<SearchResponse>,
    actual: Result<&SearchResponse, &Error>,
    native: bool,
) -> bool {
    match (expected, actual) {
        (Some(expected), Ok(actual)) => expected == actual,
        (None, Err(Error::ToolRejected { .. })) => native,
        (None, Err(Error::Invalid(_))) => !native,
        _ => false,
    }
}

#[cfg(test)]
#[test]
fn oracle_detects_broken_decoding_and_bounds() {
    for exa in [true, false] {
        // A container's depth is its position before opening it. At depth 256,
        // an empty container is accepted, while another contained value is not.
        for arrays in [255, 256, 257] {
            for leaf in ["", "0", "\"x\""] {
                let body = format!(
                    "{{\"results\":[],\"metadata\":{}{leaf}{}}}",
                    "[".repeat(arrays),
                    "]".repeat(arrays)
                );
                assert_eq!(
                    expected(body.as_bytes(), exa).is_some(),
                    arrays < 256 || (arrays == 256 && leaf.is_empty())
                );
            }
        }
        let valid = br#"{"results":[{"url":"https://example.com","title":"source","snippet":"one","highlights":["one","two"]}]}"#;
        let wanted = expected(valid, exa);
        assert!(wanted.is_some());
        // An always-rejecting decoder must fail on an accepted document.
        assert!(!agrees(
            &wanted,
            Err(&Error::ToolRejected {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "broken decoder"
            }),
            true,
        ));
        assert!(!agrees(
            &wanted,
            Err(&Error::Invalid("broken decoder".into())),
            false,
        ));
        let mut broken = wanted.clone().unwrap();
        broken.results[0].snippet.push_str("wrong");
        for native in [false, true] {
            assert!(!agrees(&wanted, Ok(&broken), native));
        }
        let deep = format!(
            "{{\"results\":[],\"metadata\":{}0{}}}",
            "[".repeat(258),
            "]".repeat(258)
        );
        let oversized = format!(
            "{{\"results\":[],\"metadata\":\"{}\"}}",
            "x".repeat(1_048_576)
        );
        let ordinary_bypass = format!(
            "{{\"results\":[],\"metadata\":{}0{}}}",
            "[".repeat(4_096),
            "]".repeat(4_096)
        );
        for bytes in [
            deep.as_bytes(),
            ordinary_bypass.as_bytes(),
            oversized.as_bytes(),
        ] {
            assert!(expected(bytes, exa).is_none());
            // A decoder bypassing either fixed bound accepts these documents.
            let broken = decode(bytes, exa).unwrap();
            for native in [false, true] {
                assert!(!agrees(&expected(bytes, exa), Ok(&broken), native));
            }
        }
    }
}
