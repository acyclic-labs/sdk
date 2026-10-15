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
    proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        source_file: Some(file!()),
        failure_persistence: Some(Box::new(
            proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"),
        )),
        ..Default::default()
    })
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
        let (initial, initial_custody) = machine.initialize_with_native_work(&state, &work)?;
        prop_assert_eq!(&initial, &machine.initialize(&state)?);
        prop_assert!(initial_custody.work().shares_ledger(&work));
        prop_assert_eq!(
            work.live_allocation_bytes(),
            baseline + initial_custody.bytes()
        );
        drop((initial, initial_custody));
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
        let (transition, transition_custody) =
            machine.transition_with_native_work(&state, Value::Null, &work)?;
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
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))
}
