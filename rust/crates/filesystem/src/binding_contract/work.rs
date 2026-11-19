use crate::WorkCounters;
use serde::ser::{Serialize, SerializeStruct, Serializer};

struct Decimal(u64);

impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

struct DecimalWorkCounters<'a>(&'a WorkCounters);

impl Serialize for DecimalWorkCounters<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        macro_rules! fields {
            ($($field:ident => $name:literal),+ $(,)?) => {{
                // Exhaustive destructuring makes a new counter require a wire
                // projection instead of silently omitting it from the receipt.
                let WorkCounters { $($field),+ } = self.0;
                let mut state = serializer.serialize_struct(
                    "WorkCounters", [$(stringify!($field)),+].len(),
                )?;
                $(state.serialize_field($name, &Decimal(*$field))?;)+
                state.end()
            }};
        }
        // Retain the existing JSON object's lexicographic field order.
        fields! {
            allocation_operations => "allocationOperations",
            authority_bytes_read => "authorityBytesRead",
            authority_bytes_written => "authorityBytesWritten",
            authority_records_appended => "authorityRecordsAppended",
            authority_records_read => "authorityRecordsRead",
            backend_read_operations => "backendReadOperations",
            backend_write_operations => "backendWriteOperations",
            bytes_copied => "bytesCopied",
            bytes_encoded => "bytesEncoded",
            bytes_hashed => "bytesHashed",
            durability_operations => "durabilityOperations",
            items_examined => "itemsExamined",
            items_returned => "itemsReturned",
            materializations => "materializations",
            object_bytes_read => "objectBytesRead",
            object_bytes_written => "objectBytesWritten",
            object_probes => "objectProbes",
            output_bytes => "outputBytes",
            page_reads => "pageReads",
            page_writes => "pageWrites",
            peak_allocation_bytes => "peakAllocationBytes",
            source_bytes_read => "sourceBytesRead",
            source_entries_visited => "sourceEntriesVisited",
            source_path_components => "sourcePathComponents",
        }
    }
}

/// Encodes binding work receipts with exact decimal strings for every `u64`.
///
/// Counters stream directly into the returned JSON buffer, without an
/// intermediate object or one allocated string per counter.
pub fn work_counters_json(work: &WorkCounters) -> Result<String, serde_json::Error> {
    serde_json::to_string(&DecimalWorkCounters(work))
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(not(unit_test_split), unit_test_foundation))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // The original binding encoder is the independent precision/shape oracle.
    fn original_json(work: &WorkCounters) -> Result<String, Box<dyn std::error::Error>> {
        let mut value = serde_json::to_value(work)?;
        for value in value
            .as_object_mut()
            .ok_or("work receipt is not an object")?
            .values_mut()
        {
            *value = serde_json::Value::String(
                value
                    .as_u64()
                    .ok_or("work counter is not an integer")?
                    .to_string(),
            );
        }
        Ok(serde_json::to_string(&value)?)
    }

    fn check(work: &WorkCounters) -> Result<(), Box<dyn std::error::Error>> {
        let (json, [before, after]) = acyclic_test_support::differential(
            || original_json(work).expect("original receipt encoding"),
            || work_counters_json(work).expect("receipt encoding"),
        );
        assert!(
            after.count_total < before.count_total,
            "{before:?} -> {after:?}"
        );
        assert!(
            after.bytes_total < before.bytes_total,
            "{before:?} -> {after:?}"
        );
        assert_eq!(after.count_current, 1, "only the returned buffer remains");
        // Reinstating the canonical numeric encoding loses the binding's exact
        // string-valued contract, even when the numbers happen to be small.
        assert_ne!(json, serde_json::to_string(work)?);
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn decimal_receipts_preserve_original_precision_with_less_allocation(
            counters in any::<[u64; 24]>(),
        ) {
            let mut canonical = serde_json::to_value(WorkCounters::default())?;
            let object = canonical.as_object_mut().expect("work receipt is an object");
            prop_assert_eq!(object.len(), counters.len());
            for (value, number) in object.values_mut().zip(counters) {
                *value = number.into();
            }
            check(&serde_json::from_value(canonical.clone())?).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
            // Every field, including zero and values not representable by a
            // JavaScript number, traverses the actual codec in each control.
            for number in [0, 9_007_199_254_740_993, u64::MAX] {
                for value in canonical.as_object_mut().expect("work receipt is an object").values_mut() {
                    *value = number.into();
                }
                check(&serde_json::from_value(canonical.clone())?).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
            }
        }
    }
}
