use super::*;
use proptest::prelude::*;

fn values() -> impl Strategy<Value = Value> {
    let text = prop::collection::vec(any::<char>(), 0..24)
        .prop_map(|chars| chars.into_iter().collect::<String>());
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        any::<f64>().prop_filter_map("finite JSON number", |value| {
            serde_json::Number::from_f64(value).map(Value::Number)
        }),
        text.clone().prop_map(Value::String),
    ]
    .prop_recursive(4, 64, 8, move |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
            prop::collection::btree_map(text.clone(), inner, 0..8)
                .prop_map(|entries| Value::Object(entries.into_iter().collect())),
        ]
    })
}

pub(crate) fn verify_native_callbacks() -> Result<()> {
    let machine = Machine {
        identity: MachineIdentity {
            name: "example.native".into(),
            version: "1".into(),
            digest: [7; 32],
        },
        schema: json!({"type":"integer"}),
    };
    // Property: native callbacks preserve the ordinary state/transition for JSON
    // trees, retain every returned value on the original ledger, and release it.
    runner()
        .run(&values(), |state| {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                WorkBudget {
                    peak_allocation_bytes: 1_000_000,
                    allocation_operations: 10_000,
                    bytes_copied: 1_000_000,
                    bytes_encoded: 1_000_000,
                    items_examined: 10_000,
                    ..Default::default()
                },
                &cancellation,
            )
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            let baseline = work.live_allocation_bytes();
            let before = work.receipt().allocation_operations;
            let mut copied = None;
            let allocations = acyclic_test_support::measure(|| {
                copied = Some(machine.initialize_with_native_work(&state, &work));
            });
            let (initial, initial_custody) = copied.unwrap()?;
            prop_assert!(work.receipt().allocation_operations - before >= allocations.count_total);
            prop_assert!(initial_custody.bytes() >= allocations.bytes_current as u64);
            prop_assert_eq!(&initial, &machine.initialize(&state)?);
            prop_assert!(initial_custody.work().shares_ledger(&work));
            prop_assert_eq!(
                work.live_allocation_bytes(),
                baseline + initial_custody.bytes()
            );
            drop((initial, initial_custody));
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
            let mut copied = None;
            let before = work.receipt().allocation_operations;
            let allocations = acyclic_test_support::measure(|| {
                copied = Some(machine.transition_with_native_work(&state, Value::Null, &work));
            });
            let (transition, transition_custody) = copied.unwrap()?;
            prop_assert!(work.receipt().allocation_operations - before >= allocations.count_total);
            prop_assert!(transition_custody.bytes() >= allocations.bytes_current as u64);
            prop_assert_eq!(&transition, &machine.transition(&state, &Value::Null)?);
            prop_assert!(transition_custody.work().shares_ledger(&work));
            prop_assert_eq!(
                work.live_allocation_bytes(),
                baseline + transition_custody.bytes()
            );
            drop((transition, transition_custody));
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
            cancellation.cancel();
            prop_assert!(machine.identity_with_native_work(&work).is_err());
            prop_assert!(machine.initialize_with_native_work(&state, &work).is_err());
            prop_assert!(
                machine
                    .transition_with_native_work(&state, Value::Null, &work)
                    .is_err()
            );
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
            Ok(())
        })
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    verify_allocation_boundaries(&machine)?;
    verify_operation_exhaustion(&machine)
}

fn verify_operation_exhaustion(machine: &Machine) -> Result<()> {
    // Every allocation attempt, including cleanup after any exhausted operation
    // prefix, must be admitted. Vary trees and the exhausted prefix independently.
    runner()
        .run(&(values(), any::<u16>()), |(state, prefix)| {
            let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
            let work = acyclic_fs::NativeMaterializationWork::new(
                budget,
                &acyclic_fs::CancellationToken::new(),
            )
            .unwrap();
            let construction = work.receipt().allocation_operations;
            drop(machine.transition_with_native_work(&state, Value::Null, &work)?);
            let operations = work.receipt().allocation_operations - construction;
            let limit = construction + u64::from(prefix) % (operations + 1);
            let work = acyclic_fs::NativeMaterializationWork::new(
                WorkBudget {
                    allocation_operations: limit,
                    ..budget
                },
                &acyclic_fs::CancellationToken::new(),
            )
            .unwrap();
            let baseline = work.live_allocation_bytes();
            let before = work.receipt().allocation_operations;
            let measured = acyclic_test_support::measure(|| {
                drop(machine.transition_with_native_work(&state, Value::Null, &work));
            });
            prop_assert!(
                work.receipt().allocation_operations - before >= measured.count_total,
                "{} physical allocations, {} charged operations, limit {}",
                measured.count_total,
                work.receipt().allocation_operations - before,
                limit
            );
            prop_assert_eq!(measured.bytes_current, 0);
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
            Ok(())
        })
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
}

fn verify_allocation_boundaries(machine: &Machine) -> Result<()> {
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let new_work = |budget| {
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })
    };
    let state = json!({"a": 1});
    let work = new_work(budget)?;
    let mut copied = None;
    let measured = acyclic_test_support::measure(|| {
        copied = Some(machine.initialize_with_native_work(&state, &work));
    });
    let (value, custody) = copied.unwrap()?;
    assert!(custody.bytes() >= measured.bytes_current as u64);
    // Negative control: the old entry-only inventory omits the B-tree node.
    let entry_only = size_of::<Value>() + size_of::<(String, Value)>() + 1;
    assert!(i64::try_from(entry_only).unwrap() < measured.bytes_current);
    drop((value, custody));

    let baseline = work.live_allocation_bytes();
    for count in [11, 12, 64, 128] {
        let state = Value::Object(
            (0..count)
                .map(|index| (index.to_string(), Value::Null))
                .collect(),
        );
        let mut copied = None;
        let before = work.receipt().allocation_operations;
        let measured = acyclic_test_support::measure(|| {
            copied = Some(machine.initialize_with_native_work(&state, &work));
        });
        let (value, custody) = copied.unwrap()?;
        assert!(work.receipt().allocation_operations - before >= measured.count_total);
        assert!(custody.bytes() >= measured.bytes_current as u64);
        drop((value, custody));
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    // Measure this transition alone so its last physical allocation exactly
    // exhausts the new ledger; inline transition bytes require no operation.
    let reference = new_work(budget)?;
    drop(machine.transition_with_native_work(&state, Value::Null, &reference)?);
    let exact = reference.receipt().allocation_operations;
    let constrained = new_work(WorkBudget {
        allocation_operations: exact,
        ..budget
    })?;
    let baseline_exact = constrained.live_allocation_bytes();
    drop(machine.transition_with_native_work(&state, Value::Null, &constrained)?);
    assert_eq!(constrained.receipt().allocation_operations, exact);
    assert_eq!(constrained.live_allocation_bytes(), baseline_exact);
    let exhausted = new_work(WorkBudget {
        allocation_operations: exact - 1,
        ..budget
    })?;
    let baseline_exhausted = exhausted.live_allocation_bytes();
    let measured = acyclic_test_support::measure(|| {
        assert!(
            machine
                .transition_with_native_work(&state, Value::Null, &exhausted)
                .is_err()
        );
    });
    assert_eq!(measured.bytes_current, 0);
    assert_eq!(exhausted.live_allocation_bytes(), baseline_exhausted);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

fn runner() -> proptest::test_runner::TestRunner {
    proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        source_file: Some(file!()),
        failure_persistence: Some(Box::new(
            proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"),
        )),
        ..Default::default()
    })
}
