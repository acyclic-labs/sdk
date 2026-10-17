use super::*;
use acyclic_harness::client::AttentionEvent;
use proptest::prelude::*;
use serde_json::json;
use std::path::Path;

fn record() -> impl Strategy<Value = AttentionNotification> {
    (
        0_u8..4,
        any::<u64>(),
        0_u8..5,
        proptest::collection::vec(any::<char>(), 0..32),
    )
        .prop_map(|(task, sequence, event, detail)| AttentionNotification {
            task: format!("task-{task}"),
            sequence,
            event: [
                AttentionEvent::TaskSettled,
                AttentionEvent::TaskFailed,
                AttentionEvent::TaskBlocked,
                AttentionEvent::GoalDone,
                AttentionEvent::GoalBlocked,
            ][usize::from(event)],
            detail: detail.into_iter().collect(),
        })
}

proptest! {
    // Current file membership, including removal, is the entire terminal input.
    #[test]
    fn file_changes_and_removals_preserve_the_shared_view(
        operations in proptest::collection::vec((record(), any::<bool>()), 0..64),
    ) {
        crate::notification_history::verify(operations,
            (NotificationMode::On, TerminalNotificationProtocol::Osc9, false), false);
    }

    // The actual case codec preserves all data, defaults and typed enum values.
    #[test]
    fn notification_case_codec_round_trips(
        records in proptest::collection::vec(record(), 0..16),
        focused in any::<bool>(),
        mode in proptest::option::of(prop_oneof![Just("on"), Just("off"), Just("attention-only")]),
        protocol in prop_oneof![Just("osc9"), Just("osc777"), Just("bell")],
    ) {
        let mut value = json!({"records":records, "focused":focused, "protocol":protocol,
            "expect":{"presented":records.len(), "output":""}});
        if let Some(mode) = mode { value["mode"] = json!(mode); }
        let case = Notifications::parse(value).unwrap();
        let _ = case.verify_history();
        let bytes = serde_json::to_vec(&case).unwrap();
        let decoded = Notifications::parse(serde_json::from_slice(&bytes).unwrap()).unwrap();
        prop_assert_eq!(case, decoded);
    }

    // Only the protocol's fixed wrapper may contain control characters or OSC
    // separators; arbitrary Unicode payloads cannot inject a terminal command.
    #[test]
    fn terminal_payload_cannot_inject_controls(mut notification in record()) {
        notification.task = notification.detail.clone();
        for protocol in [TerminalNotificationProtocol::Osc9,
            TerminalNotificationProtocol::Osc777, TerminalNotificationProtocol::Bell] {
            let _ = crate::notification_history::encode(&notification, (NotificationMode::On, protocol, false));
        }
    }
}

#[test]
fn fuzz_history_continues_after_stale_fixture_expectations() -> TestResult {
    let case = Notifications::parse(serde_json::from_slice(include_bytes!(
        "../../../harness-fuzz/corpus/notification_scenario/early-oracle-mismatch.json"
    ))?)?;
    assert_eq!(
        case.verify_history(),
        b"\x1b]9;Goal done: task-1: Ready\x07"
    );
    assert!(
        case.run().is_err(),
        "fixture oracle must still reject the mutation"
    );
    Ok(())
}

#[test]
fn notification_oracles_reject_wrong_output_counts_removal_and_view() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = crate::scenario::load_value(
        &root,
        &root.join("notifications/osc777/on/task_failed.json"),
    )?;
    for mutation in [
        "output",
        "presented",
        "remove",
        "view",
        "focused",
        "mode",
        "protocol",
        "detail",
    ] {
        let mut value = baseline.clone();
        value.as_object_mut().unwrap().remove("driver");
        Notifications::parse(value.clone())?.run()?;
        match mutation {
            "output" => value["expect"]["output"] = json!("wrong"),
            "presented" => value["expect"]["presented"] = json!(0),
            "remove" => value["remove"] = json!([0]),
            "view" => value["expect"]["view"] = json!([{"event":"goal_done"}]),
            "focused" => value["focused"] = json!(true),
            "mode" => value["mode"] = json!("off"),
            "protocol" => value["protocol"] = json!("bell"),
            "detail" => value["records"][0]["detail"] = json!("changed"),
            _ => unreachable!(),
        }
        assert!(
            Notifications::parse(value)?.run().is_err(),
            "accepted wrong {mutation}"
        );
    }
    Ok(())
}

#[test]
fn notification_parser_rejects_ignored_fields_and_vacuous_expectations() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut baseline = crate::scenario::load_value(
        &root,
        &root.join("notifications/osc777/on/task_failed.json"),
    )?;
    baseline.as_object_mut().unwrap().remove("driver");
    for (field, replacement) in [
        ("typo", json!(true)),
        ("prompt", json!("wrong driver")),
        ("protocol", json!("unknown")),
        ("mode", json!("always")),
        ("remove", json!([1])),
        ("expect", json!({"output":""})),
        ("expect", json!({"presented":0})),
        ("expect", json!({"presented":1, "output":"", "view":{}})),
        (
            "expect",
            json!({"presented":1, "output":"", "applied":[true]}),
        ),
    ] {
        let mut value = baseline.clone();
        value[field] = replacement;
        assert!(
            Notifications::parse(value).is_err(),
            "accepted invalid {field}"
        );
    }
    Ok(())
}

fn bounded_history(count: u64, detail: &str) -> Vec<(AttentionNotification, bool)> {
    let records: Vec<_> = (1..=count)
        .map(|sequence| AttentionNotification {
            task: "task-1".into(),
            sequence,
            event: AttentionEvent::TaskFailed,
            detail: detail.into(),
        })
        .collect();
    let mut operations: Vec<_> = records
        .iter()
        .cloned()
        .map(|record| (record, false))
        .collect();
    // A rejected write must not poison subsequent removals or admission.
    operations.push((records[0].clone(), true));
    operations.push((records.last().unwrap().clone(), false));
    operations
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Independently bound file count and serialized bytes. Rejected changes
    // preserve the exact view and cursor, and a removal releases capacity.
    #[test]
    fn notification_limits_are_atomic_and_recoverable(count in 120_u64..134) {
        crate::notification_history::verify(bounded_history(count, "Ready"),
            (NotificationMode::Off, TerminalNotificationProtocol::Bell, false), false);
    }
    #[test]
    fn notification_byte_limit_is_atomic(detail_length in 1_048_400_usize..1_048_650) {
        crate::notification_history::verify(bounded_history(1, &"x".repeat(detail_length)),
            (NotificationMode::Off, TerminalNotificationProtocol::Bell, false), false);
    }
}

#[test]
fn notification_limit_seed_and_unwrap_negative_control() -> TestResult {
    let case = Notifications::parse(serde_json::from_slice(include_bytes!(
        "../../../harness-fuzz/corpus/notification_scenario/129-files.json"
    ))?)?;
    assert_eq!(case.records.len(), 129);
    assert!(case.verify_history().is_empty());
    for operations in [
        bounded_history(129, "Ready"),
        bounded_history(1, &"x".repeat(1_048_576)),
    ] {
        assert!(
            std::panic::catch_unwind(|| crate::notification_history::verify_limits(
                operations,
                (
                    NotificationMode::Off,
                    TerminalNotificationProtocol::Bell,
                    false
                ),
                false,
                true
            ))
            .is_err(),
            "former unwrap must fail the shared oracle"
        );
    }
    Ok(())
}
