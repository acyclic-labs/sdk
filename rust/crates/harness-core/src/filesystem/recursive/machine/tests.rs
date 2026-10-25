use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Task-owned forks have no conversation witness; other output contracts
    // retain the previously required witness field, including its nullable form.
    #[test]
    fn output_contract_distinguishes_task_forks_without_weakening_conversation_reports(
        task in any::<bool>(), witness in 0u8..3, digest in any::<[u8;32]>(),
    ) {
        let schema: Value = serde_json::from_slice(PREPARATION_OUTPUT_SCHEMA).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let mut report = serde_json::json!({
            "request":{"parent":{"kind":if task {"task"} else {"conversation"}}},
            "captures":[], "child_private_volume":{}, "child_private_generation":{},
            "inherited_context":[], "summary":null, "inherited_through_sequence":0,
            "shared_grants":[], "reference_grants":[], "attachment_manifests":[],
        });
        if witness != 0 {
            report["history_witness"] = if witness == 1 { Value::Null } else { serde_json::json!(digest) };
        }
        prop_assert_eq!(validator.is_valid(&report), if task { witness == 0 } else { witness != 0 });
        report["unexpected"] = Value::Null;
        prop_assert!(!validator.is_valid(&report));
    }
}

/// A measured settlement re-encodes the report's request under the admitted
/// request's finite allowance, which the machine state carries from admission.
/// Negative control: the former unbounded allowance is refused under measured
/// work, so a measured preparation could never settle.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn measured_settlement_uses_the_admitted_finite_allowance() -> Result<()> {
    let fixture = crate::durable_host::tests::created_fixture::CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let report = created.report;
    let operation_id = report.request.operation_id;
    let request_digest = crate::executor::json::canonical_json_digest(
        &report.request,
        created.request.maximum_bytes,
        &mut None,
    )?;
    let input = serde_json::json!({"commands": [{
        "operation_id": operation_id,
        "value": Outcome::Succeeded(report),
    }]});
    let machine = ForkPreparationMachine::new();
    let settle = |maximum_bytes: u64| {
        let state = serde_json::json!({
            "phase": "waiting",
            "operation_id": operation_id,
            "request_digest": request_digest,
            "maximum_bytes": maximum_bytes,
        });
        // Original native work, as a measured task step runs the machine.
        let cancellation = acyclic_fs::CancellationToken::new();
        let native = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                authority_records_read: 1 << 26,
                authority_records_appended: 1 << 26,
                authority_bytes_read: 1 << 26,
                authority_bytes_written: 1 << 26,
                object_probes: 1 << 26,
                backend_read_operations: 1 << 26,
                backend_write_operations: 1 << 26,
                durability_operations: 1 << 26,
                page_reads: 1 << 26,
                page_writes: 1 << 26,
                object_bytes_read: 1 << 26,
                object_bytes_written: 1 << 26,
                bytes_hashed: 1 << 26,
                bytes_copied: 1 << 26,
                bytes_encoded: 1 << 26,
                source_bytes_read: 1 << 26,
                source_path_components: 1 << 26,
                source_entries_visited: 1 << 26,
                output_bytes: 1 << 26,
                items_examined: 1 << 26,
                items_returned: 1 << 26,
                allocation_operations: 1 << 26,
                peak_allocation_bytes: 1 << 26,
                materializations: 1 << 26,
            },
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&native)?;
        machine.next_state(&state, &input, Some(&io))
    };
    let settled = settle(created.request.maximum_bytes)?;
    assert!(matches!(settled.status, MachineStatus::Completed { .. }));
    assert!(settled.commands.is_empty());
    let Err(refused) = settle(u64::MAX) else {
        panic!("an unbounded settlement allowance was accepted under measured work");
    };
    assert!(
        refused.to_string().contains("finite and positive"),
        "{refused}"
    );
    Ok(())
}
