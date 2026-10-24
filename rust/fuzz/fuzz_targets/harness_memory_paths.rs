//! Fuzz ordered memory-path conflicts and JSON input round-trip stability.
#![no_main]
use libfuzzer_sys::fuzz_target;
#[path = "../../crates/harness-core/src/memory_store/directory/conflicts.rs"]
mod conflicts;
fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    let Ok(input) = serde_json::from_slice::<(Vec<String>, String)>(data) else {
        return;
    };
    if input.0.len() > 256 {
        return;
    }
    let encoded = serde_json::to_vec(&input);
    assert!(encoded.is_ok());
    if let Ok(encoded) = encoded {
        assert_eq!(
            serde_json::from_slice::<(Vec<String>, String)>(&encoded)
                .ok()
                .as_ref(),
            Some(&input)
        );
    }
    let paths: std::collections::BTreeMap<_, _> =
        input.0.into_iter().map(|name| (name, ())).collect();
    let expected = paths
        .keys()
        .any(|name| conflicts::paths_conflict(name, &input.1));
    assert_eq!(conflicts::conflicts(&paths, &input.1), expected);
});
