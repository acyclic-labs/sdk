#![no_main]

#[path = "../../harness/tests/e2e/task_query_case.rs"]
mod task_query_case;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(case) = serde_json::from_slice::<task_query_case::TaskQueryCase>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let decoded: task_query_case::TaskQueryCase = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(case, decoded);
    // Parse every input independently of fixture expectations and prior input failures.
    for input in &case.writes {
        if let Ok(file) = input.write() {
            use acyclic_harness::task_volume::FileKey;
            if let FileKey::Plan(name) = &file.key {
                assert!(!name.as_str().is_empty());
            }
            assert_eq!(FileKey::parse(&file.key.path()).unwrap(), file.key);
        }
    }
});
