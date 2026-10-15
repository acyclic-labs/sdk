use super::*;
use acyclic_harness::client::{AttentionEvent, terminal_notification};
use proptest::prelude::*;
use serde_json::json;
use std::{collections::VecDeque, path::Path};

fn record() -> impl Strategy<Value = AttentionNotification> {
    (
        0_u8..4,
        any::<u64>(),
        0_u8..4,
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
            ][usize::from(event)],
            detail: detail.into_iter().collect(),
        })
}

proptest! {
    // An arbitrary history preserves accepted records in FIFO order. Rejected
    // and replayed records cannot change either the queue or source cursors.
    #[test]
    fn replay_and_backpressure_preserve_the_view(
        operations in proptest::collection::vec((record(), any::<bool>()), 0..64),
        maximum_tasks in 1_usize..5,
        maximum_pending in 1_usize..9,
    ) {
        let mut view = NotificationView::new(maximum_tasks, maximum_pending, 256).unwrap();
        let mut pending = VecDeque::new();
        for (record, drain) in operations {
            let before = serde_json::to_value(&view).unwrap();
            let previous = before["latest"].get(&record.task);
            let eligible = record.sequence > 0
                && previous.is_none_or(|old| old["sequence"].as_u64().unwrap() < record.sequence)
                && (previous.is_some() || before["latest"].as_object().unwrap().len() < maximum_tasks)
                && pending.len() < maximum_pending;
            let applied = view.apply(record.clone()).unwrap_or(false);
            prop_assert_eq!(applied, eligible);
            if applied {
                let mut latest = before["latest"].clone();
                latest[&record.task] = serde_json::to_value(&record).unwrap();
                let actual = serde_json::to_value(&view).unwrap();
                prop_assert_eq!(&actual["latest"], &latest);
                pending.push_back(record.clone());
                prop_assert!(!view.apply(record.clone()).unwrap());
                let mut conflicting = record;
                conflicting.detail.push('!');
                let accepted = serde_json::to_value(&view).unwrap();
                prop_assert!(view.apply(conflicting).is_err());
                prop_assert_eq!(serde_json::to_value(&view).unwrap(), accepted);
            } else {
                prop_assert_eq!(serde_json::to_value(&view).unwrap(), before);
            }
            prop_assert_eq!(view.front(), pending.front());
            prop_assert_eq!(view.len(), pending.len());
            if drain {
                let actual = view.pop();
                let expected = pending.pop_front();
                prop_assert_eq!(actual.as_deref(), expected.as_ref());
            }
        }
        while let Some(expected) = pending.pop_front() {
            let actual = view.pop();
            prop_assert_eq!(actual.as_deref(), Some(&expected));
        }
        prop_assert!(view.is_empty());
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
            "maximum_tasks":4, "maximum_pending":16, "maximum_text_bytes":256,
            "expect":{"applied":vec![true; records.len()], "output":""}});
        if let Some(mode) = mode { value["mode"] = json!(mode); }
        let case = Notifications::parse(value).unwrap();
        let bytes = serde_json::to_vec(&case).unwrap();
        let decoded = Notifications::parse(serde_json::from_slice(&bytes).unwrap()).unwrap();
        prop_assert_eq!(case, decoded);
    }

    // Only the protocol's fixed wrapper may contain control characters or OSC
    // separators; arbitrary Unicode payloads cannot inject a terminal command.
    #[test]
    fn terminal_payload_cannot_inject_controls(mut notification in record()) {
        notification.task = notification.detail.clone();
        for (protocol, prefix, separators) in [
            (TerminalNotificationProtocol::Osc9, "\x1b]9;", 0),
            (TerminalNotificationProtocol::Osc777, "\x1b]777;notify;", 1),
        ] {
            let output = terminal_notification(&notification, NotificationMode::On, protocol, false);
            let body = output.strip_prefix(prefix).unwrap().strip_suffix('\x07').unwrap();
            prop_assert!(!body.chars().any(char::is_control));
            prop_assert_eq!(body.matches(';').count(), separators);
        }
    }
}

#[test]
fn notification_oracles_reject_wrong_output_counts_apply_and_view() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = crate::scenario::load_value(
        &root,
        &root.join("notifications/osc777/on/task_failed.json"),
    )?;
    for mutation in [
        "output",
        "presented",
        "applied",
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
            "applied" => value["expect"]["applied"] = json!([false]),
            "view" => value["expect"]["view"] = json!({"latest":{"task-1":{"event":"goal_done"}}}),
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
        ("maximum_tasks", json!(-1)),
        ("expect", json!({"applied":[true]})),
        (
            "expect",
            json!({"applied":[true], "output":"", "error":"x"}),
        ),
        ("expect", json!({"applied":[true], "error":""})),
        (
            "expect",
            json!({"applied":[true], "error":"x", "view":{"pending":[]}}),
        ),
        ("expect", json!({"applied":[true], "output":"", "view":{}})),
        ("expect", json!({"applied":[], "output":""})),
        ("expect", json!({"applied":[null], "output":""})),
        ("expect", json!({"applied":[""], "output":""})),
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
