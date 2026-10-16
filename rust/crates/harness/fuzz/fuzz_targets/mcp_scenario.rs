#![no_main]

#[path = "../../tests/e2e/mcp_script.rs"]
mod mcp_script;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(script) = serde_json::from_slice::<mcp_script::McpScript>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&script).unwrap();
    let decoded: mcp_script::McpScript = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        serde_json::to_value(script).unwrap(),
        serde_json::to_value(decoded).unwrap()
    );
});
