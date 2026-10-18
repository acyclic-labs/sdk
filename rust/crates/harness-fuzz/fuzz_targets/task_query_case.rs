#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../harness/tests/e2e/task_query_case.rs"]
mod task_query_case;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(case) = fuzz_support::json_input!(fuzz_input, task_query_case::TaskQueryCase) else {
        return;
    };
    let encoded = serde_json::to_vec(&*case).unwrap();
    let _ = fuzz_support::round_trip(
        &*case,
        serde_json::from_slice::<task_query_case::TaskQueryCase>(&encoded),
    );

    // Parse every input independently of fixture expectations and prior input failures.
    for (index, input) in case.writes.iter().enumerate() {
        let fuzz_input = case.field_with(index, |case, index| {
            case.writes
                .get(index)
                .unwrap_or_else(|| unreachable!("enumerated input field"))
                .path
                .as_bytes()
        });
        if let Some(key) = fuzz_support::FuzzInput::parse_text(
            fuzz_input,
            acyclic_harness::task_volume::FileKey::parse,
        ) {
            let file = fuzz_support::must_decode(input.write());
            assert_eq!(file.key, key);
            use acyclic_harness::task_volume::FileKey;
            if let FileKey::Plan(name) = &file.key {
                assert!(!name.as_str().is_empty());
            }
            let _ = fuzz_support::round_trip(&(file.key), FileKey::parse(&file.key.path()));
        }
    }
});
