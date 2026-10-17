#![cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]

#[path = "../../../harness-fuzz/fuzz_targets/recorded_invocation_input.rs"]
mod input;
use crate::{OperationId, conversation::Limits, tool::ToolInvocation};
use input::Record;
use proptest::prelude::*;
use serde_json::{Value, json};

fn record(arguments: Value, metadata: Value) -> Record {
    Record {
        invocation: ToolInvocation {
            operation_id: OperationId::from_bytes([7; 16]),
            call_id: "call".into(),
            name: "echo".into(),
            arguments,
        },
        metadata,
    }
}

#[test]
fn valid_record_is_admitted_and_projects_with_exact_metadata() {
    let record = record(
        json!({"path":"λ\0🦀"}),
        json!({"signature":"opaque", "blocks":[{"thinking":"plan"}]}),
    );
    assert!(
        record
            .invocation
            .validate_recorded(&record.metadata, Limits::default(), None)
            .is_ok()
    );
    input::assert_replay(record, Limits::default());
}

#[test]
fn complete_depth_and_numeric_bounds_apply_before_recording() {
    let mut deep = json!(0);
    for _ in 0..127 {
        deep = json!([deep]);
    }
    for record in [
        record(deep, Value::Null),
        record(json!({}), json!(u64::MAX)),
        record(json!({"value":u64::MAX}), Value::Null),
    ] {
        assert!(
            record
                .invocation
                .validate_recorded(&record.metadata, Limits::default(), None)
                .is_err()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Property: ordinary/native admission agrees, releases all temporary custody,
    // and every admitted complete record survives actual history projection/replay.
    #[test]
    fn admitted_records_always_project_and_replay(
        text in ".{0,96}", signature in ".{0,96}", depth in 0usize..140,
        file_bytes in 128u64..4096, render_bytes in 128u64..4096,
        reserved in prop::option::of((any::<bool>(), any::<bool>(), 0usize..8)),
    ) {
        let mut nested = json!(0);
        for _ in 0..depth { nested = json!([nested]); }
        let mut record = record(json!({"data":text, "nested":nested}), json!({"signature":signature}));
        if let Some((in_arguments, number_marker, nesting)) = reserved {
            let key = if number_marker { "$serde_json::private::Number" } else { "$serde_json::private::RawValue" };
            let mut marker = json!({"signature":"opaque", key:"null"});
            for _ in 0..nesting { marker = json!([marker]); }
            if in_arguments { record.invocation.arguments["marker"] = marker; }
            else { record.metadata["marker"] = marker; }
        }
        let limits = Limits {file_bytes, render_bytes, ..Limits::default()};
        let admitted = record.invocation.validate_recorded(&record.metadata, limits, None).is_ok();
        if reserved.is_some() { prop_assert!(!admitted); }
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(2_000_000, 4*1024*1024),
            &acyclic_fs::CancellationToken::new(),
        ).expect("fixture ledger");
        let before = work.live_allocation_bytes();
        let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
        let native = record.invocation.validate_recorded(&record.metadata, limits, Some(&io));
        prop_assert_eq!(admitted, native.is_ok());
        drop(native);
        drop(io);
        prop_assert_eq!(before, work.live_allocation_bytes());
        input::assert_replay(record, limits);
    }
}

#[test]
fn checked_in_saved_record_corpus() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../harness-fuzz/corpus/recorded_invocation");
    for entry in std::fs::read_dir(directory).unwrap() {
        input::check(&std::fs::read(entry.unwrap().path()).unwrap());
    }
}

#[test]
fn private_serde_marker_maps_are_rejected_before_execution() {
    for marker in [
        "$serde_json::private::RawValue",
        "$serde_json::private::Number",
    ] {
        for in_arguments in [true, false] {
            let payload = json!({"signature":"opaque", marker:"null"});
            let record = if in_arguments {
                record(payload, Value::Null)
            } else {
                record(json!({}), payload)
            };
            assert!(
                record
                    .invocation
                    .validate_recorded(&record.metadata, Limits::default(), None)
                    .is_err()
            );
        }
    }
}

#[test]
fn reserved_marker_negative_control_breaks_unchecked_persisted_replay() {
    let record = record(
        json!({}),
        json!({"signature":"opaque", "$serde_json::private::RawValue":"null"}),
    );
    let unchecked = serde_json::to_vec(&record).unwrap();
    assert!(serde_json::from_slice::<Record>(&unchecked).is_err());
    assert!(
        record
            .invocation
            .validate_recorded(&record.metadata, Limits::default(), None)
            .is_err()
    );
}
