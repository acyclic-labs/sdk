//! Fuzz ordered memory-path conflicts and JSON input round-trip stability.
#![no_main]

#[path = "../../crates/harness/src/memory_store/directory/conflicts.rs"]
mod conflicts;
pub use acyclic_fuzz_support as fuzz_support;
fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    if data.len() > 65_536 {
        return;
    }
    let Some(input) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<(Vec<String>, String)>)
    else {
        return;
    };
    if input.0.len() > 256 {
        return;
    }
    let encoded = serde_json::to_vec(&input)
        .unwrap_or_else(|error| unreachable!("input encoding failed: {error}"));
    let _ = fuzz_support::round_trip(
        &input,
        serde_json::from_slice::<(Vec<String>, String)>(&encoded),
    );
    let paths: std::collections::BTreeMap<_, _> =
        input.0.into_iter().map(|name| (name, ())).collect();
    let expected = paths
        .keys()
        .any(|name| conflicts::paths_conflict(name, &input.1));
    assert_eq!(conflicts::conflicts(&paths, &input.1), expected);
});
