#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    acyclic_harness::distributed::fuzz_origin_locator(input);
    if input.len() > 4096 {
        return;
    }
    if let Ok(path) = std::str::from_utf8(input) {
        if let Some(operation) = acyclic_harness::executor::fuzz_summary_operation(path) {
            let retained = format!(".system/execution/{operation}/{}.json", "0".repeat(64));
            assert_eq!(
                acyclic_harness::executor::fuzz_summary_operation(&retained),
                Some(operation)
            );
        }
    }
    if let Ok(summary) = serde_json::from_slice::<acyclic_harness::context::ContextSummary>(input) {
        if summary.output.validate(&mut None).is_ok() {
            let encoded = serde_json::to_vec(&summary).unwrap();
            assert_eq!(
                serde_json::from_slice::<acyclic_harness::context::ContextSummary>(&encoded)
                    .unwrap(),
                summary
            );
        }
    }
});
