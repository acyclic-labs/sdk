#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::OperationId;
#[path = "../../src/distributed/origin_locator.rs"]
mod origin_locator;
#[path = "../../src/executor/summary_identity.rs"]
mod summary_identity;

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if let Some((sequence, key)) =
        fuzz_support::FuzzInput::optional(fuzz_input, origin_locator::decode)
    {
        let encoded = origin_locator::encode(sequence, key);
        assert_eq!(input, encoded);
    } else {
        assert_ne!(input.len(), 40);
    }
    if input.len() > 4096 {
        return;
    }
    if let Some(path) = fuzz_input.text() {
        if let Some(operation) = summary_identity::summary_operation(path) {
            let retained = format!(".system/execution/{operation}/{}.json", "0".repeat(64));
            assert_eq!(
                summary_identity::summary_operation(&retained),
                Some(operation)
            );
        }
    }
    if let Some(summary) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::context::ContextSummary>,
    ) {
        if summary.output.validate(&mut None).is_ok() {
            let encoded = serde_json::to_vec(&summary).unwrap();
            let _ = fuzz_support::round_trip(
                &(summary),
                serde_json::from_slice::<acyclic_harness::context::ContextSummary>(&encoded),
            );
        }
    }
});
