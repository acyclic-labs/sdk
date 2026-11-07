//! Check Rustdoc decoder parity, round trips and production source selection.
#![no_main]

#[path = "../../src/public_api/decode.rs"]
mod decode;
#[path = "../../src/relative_spans.rs"]
mod relative_spans;
#[path = "../relative_spans.rs"]
mod relative_spans_oracle;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(decoded) = decode::from_slice(bytes) {
        if let Some(prototype) = decoded.index.values().next() {
            relative_spans_oracle::check(
                prototype,
                prototype.name.as_deref().unwrap_or_default().as_bytes(),
            );
        }
        let paths = relative_spans::sources(decoded.index.values());
        for item in decoded.index.values().filter(|item| item.crate_id == 0) {
            if let Some(span) = item
                .span
                .as_ref()
                .filter(|span| !span.filename.is_absolute())
            {
                assert!(paths.contains(span.filename.as_path()));
            }
        }
        let replay = serde_json::to_vec(&decoded)
            .ok()
            .and_then(|encoded| decode::from_slice(&encoded).ok());
        assert!(
            replay.is_some_and(|replay| {
                relative_spans::sources(replay.index.values()) == paths
                    && same_fields(&decoded, &replay)
            }),
            "Rustdoc round trip lost fields or source paths",
        );
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let owned = serde_json::from_value::<rustdoc_types::Crate>(value.clone());
        let canonical = serde_json::to_vec(&value);
        assert!(
            canonical.is_ok_and(|canonical| match (owned, decode::from_slice(&canonical)) {
                (Ok(owned), Ok(decoded)) => same_fields(&owned, &decoded),
                (Err(_), Err(_)) => true,
                _ => false,
            }),
            "Rustdoc decoder changed admission or fields",
        );
    }
});

fn same_fields(left: &rustdoc_types::Crate, right: &rustdoc_types::Crate) -> bool {
    matches!(
        (serde_json::to_value(left), serde_json::to_value(right)),
        (Ok(left), Ok(right)) if left == right
    )
}
