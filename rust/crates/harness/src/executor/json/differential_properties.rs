use super::*;
use crate::test_support::differential;
use acyclic_test_support::AllocationInfo;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

struct Source(Value);
impl Serialize for Source {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        differential::record("serialize input", 1);
        self.0.serialize(serializer)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Charges {
    encoded: u64,
    items: u64,
    copied: u64,
    read: u64,
    inspected: u64,
    allocations: u64,
    retained: u64,
    released: u64,
    admitted_copy: u64,
}
impl Charges {
    fn record(&mut self, event: JsonCodecCharge) -> Result<()> {
        match event {
            JsonCodecCharge::Work { bytes, items } => {
                self.encoded += bytes;
                self.items += items;
            }
            JsonCodecCharge::Copy { bytes } => self.copied += bytes,
            JsonCodecCharge::Read { bytes } => {
                differential::record("decoder read", bytes);
                self.read += bytes;
            }
            JsonCodecCharge::Inspect { items } => self.inspected += items,
            JsonCodecCharge::AllocationOperation { .. } => self.allocations += 1,
            JsonCodecCharge::AllocationOperations { operations } => self.allocations += operations,
            JsonCodecCharge::Retain { bytes } => self.retained += bytes,
            JsonCodecCharge::Release { bytes } => self.released += bytes,
            JsonCodecCharge::CopyAdmission { bytes } => self.admitted_copy += bytes,
            JsonCodecCharge::Admission {
                maximum_owned_bytes,
                value_shape,
            } => {
                differential::record("decoder admission", maximum_owned_bytes);
                differential::record("decoder source bytes", value_shape.input_bytes);
                differential::record("decoder items", value_shape.lexical_items);
                differential::record("decoder containers", value_shape.containers);
            }
        }
        Ok(())
    }
}

fn run(
    source: Source,
    old: bool,
    canonical: bool,
    bounds: JsonBounds,
    repeat: bool,
) -> (
    std::result::Result<Vec<u8>, String>,
    Charges,
    AllocationInfo,
) {
    let mut charges = Charges::default();
    let mut output = Ok(Vec::new());
    let allocations = acyclic_test_support::measure(|| {
        let mut encode = || {
            let charge = &mut |event| charges.record(event);
            let code = Some(crate::wire::ErrorCode::Invalid);
            match (old, canonical) {
                (true, true) => baseline::serialization::canonical_json_bytes_bounded(
                    &source, bounds, code, charge,
                ),
                (true, false) => {
                    baseline::serialization::json_bytes_bounded(&source, bounds, code, charge)
                }
                (false, true) => canonical_json_bytes_bounded(&source, bounds, code, charge),
                (false, false) => json_bytes_bounded(&source, bounds, code, charge),
            }
        };
        output = encode();
        if repeat {
            // Real redundant serialization, accounting and allocation, not an altered receipt.
            drop(encode());
        }
    });
    (
        output.map_err(|error| format!("{error:?}")),
        charges,
        allocations,
    )
}

fn compare(value: &Value, canonical: bool, bounds: JsonBounds) -> TestCaseResult {
    let [(old, mut old_trace), (new, mut new_trace)] = differential::pair(
        || Source(value.clone()),
        |state, old| run(state, old, canonical, bounds, false),
    );
    prop_assert_eq!(&old.0, &new.0, "complete bytes or exact error differs");
    if new.0.is_ok() {
        prop_assert_eq!(
            old_trace
                .iter()
                .filter(|(op, _)| *op == "serialize input")
                .count(),
            2
        );
        prop_assert_eq!(
            new_trace
                .iter()
                .filter(|(op, _)| *op == "serialize input")
                .count(),
            1
        );
        // This invocation is the removed work. Every decoder observation still
        // has to agree, in order and amount, across disjoint decoder instances.
        old_trace.retain(|(op, _)| *op != "serialize input");
        new_trace.retain(|(op, _)| *op != "serialize input");
        prop_assert_eq!(old_trace, new_trace, "decoder operation trace differs");
    } else {
        prop_assert_eq!(old_trace, new_trace, "failure operation trace differs");
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn old_and_new_encoders_keep_complete_bytes_errors_and_input_trace(
        text in "[a-z\\x00\\x7fé🦀]{0,96}", width in 0_usize..32, limit in 0_u64..256,
    ) {
        let value = serde_json::json!({"z": text, "a": (0..width).collect::<Vec<_>>()});
        for canonical in [false, true] {
            for bounds in [
                JsonBounds { maximum_bytes: 65536, maximum_items: 65536, maximum_depth: 128 },
                JsonBounds { maximum_bytes: limit, maximum_items: 65536, maximum_depth: 128 },
                JsonBounds { maximum_bytes: 65536, maximum_items: limit, maximum_depth: 128 },
                JsonBounds { maximum_bytes: 65536, maximum_items: 65536, maximum_depth: 0 },
            ] { compare(&value, canonical, bounds)?; }
        }
    }
}

#[path = "exact_encoding_properties.rs"]
mod exact;
