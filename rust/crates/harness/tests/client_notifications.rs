//! Terminal delivery borrows the same file projection as every renderer.
use acyclic_harness::client::{
    AttentionEvent, AttentionNotification, NotificationMode, TerminalNotificationProtocol,
    terminal_notification, write_terminal_notifications,
};
#[path = "e2e/notification_history.rs"]
mod notification_history;
use notification_history::Projection;

fn on_runtime<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
fn notice(sequence: u64, event: AttentionEvent) -> AttentionNotification {
    AttentionNotification {
        task: "task-1".into(),
        sequence,
        event,
        detail: "Ready".into(),
    }
}
#[test]
fn lifecycle_mapping_uses_semantics_and_not_diagnostic_strings() {
    use acyclic_harness::core::LifecycleState::*;
    for (state, expected) in [
        (Pending, None),
        (Active, None),
        (Waiting, None),
        (Cancelled, None),
        (Completed, Some(AttentionEvent::TaskSettled)),
        (Failed, Some(AttentionEvent::TaskFailed)),
    ] {
        let event =
            AttentionNotification::lifecycle("task".into(), 1, state, "goal done; blocked".into());
        assert_eq!(event.map(|event| event.event), expected);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn terminal_encoding_allocates_only_its_output_buffer() {
    let notification = AttentionNotification {
        detail: "x".repeat(1_048_576),
        ..notice(1, AttentionEvent::TaskFailed)
    };
    let mut output = String::new();
    let bell = acyclic_test_support::measure(|| {
        output = terminal_notification(
            &notification,
            NotificationMode::On,
            TerminalNotificationProtocol::Bell,
            false,
        );
    });
    assert_eq!(output, "\x07");
    assert_eq!(bell.bytes_total, 1);
    assert_eq!(bell.count_total, 1);
    for protocol in [
        TerminalNotificationProtocol::Osc9,
        TerminalNotificationProtocol::Osc777,
    ] {
        let allocation = acyclic_test_support::measure(|| {
            output = terminal_notification(&notification, NotificationMode::On, protocol, false);
        });
        assert_eq!(allocation.count_total, 1);
        assert_eq!(allocation.bytes_total, output.len() as u64);
        assert!(output.ends_with('\x07'));
    }
}

#[derive(Default)]
struct Broken {
    writes: usize,
    flushes: usize,
}
impl std::io::Write for Broken {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        self.writes += 1;
        Err(std::io::ErrorKind::BrokenPipe.into())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.flushes += 1;
        Err(std::io::ErrorKind::BrokenPipe.into())
    }
}
async fn retry_after_removal(retain_removed: bool) {
    let options = (
        NotificationMode::On,
        TerminalNotificationProtocol::Osc9,
        false,
    );
    let record = notice(1, AttentionEvent::GoalBlocked);
    let mut projection = Projection::new().await;
    projection.apply(&record, false).unwrap();
    let mut terminal = Broken::default();
    assert!(
        write_terminal_notifications(
            projection.view.notifications(),
            &mut terminal,
            options.0,
            options.1,
            options.2
        )
        .is_err()
    );
    assert_eq!(projection.records(), [record.clone()]);
    if !retain_removed {
        projection.apply(&record, true).unwrap();
    }
    let mut output = Vec::new();
    assert_eq!(
        write_terminal_notifications(
            projection.view.notifications(),
            &mut output,
            options.0,
            options.1,
            options.2
        )
        .unwrap(),
        0
    );
    assert!(
        output.is_empty(),
        "removed file emitted a stale terminal alert"
    );
}
#[tokio::test]
async fn failed_terminal_write_then_file_removal_retires_the_alert() {
    retry_after_removal(false).await;
}
#[test]
fn retained_queue_negative_control_fails_the_same_removal_check() {
    assert!(std::panic::catch_unwind(|| on_runtime(retry_after_removal(true))).is_err());
}
#[tokio::test]
async fn write_failure_can_retry_current_rows_without_mutating_them() {
    let mut projection = Projection::new().await;
    let record = notice(1, AttentionEvent::GoalDone);
    projection.apply(&record, false).unwrap();
    assert!(
        write_terminal_notifications(
            projection.view.notifications(),
            &mut Broken::default(),
            NotificationMode::On,
            TerminalNotificationProtocol::Bell,
            false
        )
        .is_err()
    );
    let mut output = Vec::new();
    assert_eq!(
        write_terminal_notifications(
            projection.view.notifications(),
            &mut output,
            NotificationMode::On,
            TerminalNotificationProtocol::Bell,
            false
        )
        .unwrap(),
        1
    );
    assert_eq!(output, b"\x07");
    assert_eq!(projection.records(), [record]);
}
#[tokio::test]
async fn suppressed_rows_do_not_call_terminal_or_change_the_projection() {
    for (mode, focused, event) in [
        (NotificationMode::Off, false, AttentionEvent::TaskFailed),
        (NotificationMode::On, true, AttentionEvent::TaskFailed),
        (
            NotificationMode::AttentionOnly,
            false,
            AttentionEvent::TaskSettled,
        ),
    ] {
        for protocol in [
            TerminalNotificationProtocol::Osc9,
            TerminalNotificationProtocol::Osc777,
            TerminalNotificationProtocol::Bell,
        ] {
            let mut projection = Projection::new().await;
            let record = notice(1, event);
            projection.apply(&record, false).unwrap();
            let mut terminal = Broken::default();
            assert_eq!(
                write_terminal_notifications(
                    projection.view.notifications(),
                    &mut terminal,
                    mode,
                    protocol,
                    focused
                )
                .unwrap(),
                1
            );
            assert_eq!((terminal.writes, terminal.flushes), (0, 0));
            assert_eq!(projection.records(), [record]);
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn current_rows_are_borrowed_without_copying_payloads() {
    let mut projection = Projection::new().await;
    projection
        .apply(&notice(1, AttentionEvent::TaskFailed), false)
        .unwrap();
    let allocation = acyclic_test_support::measure(|| {
        for row in projection.view.notifications() {
            std::hint::black_box(row);
        }
    });
    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Replacements, replay and removals determine terminal output solely from
    // the current file set; retrying presentation never changes that set.
    #[test]
    fn file_churn_matches_the_independent_terminal_oracle(
        operations in proptest::collection::vec((0u8..4, 0u8..6, proptest::bool::ANY, ".{0,24}"), 0..24),
    ) {
        on_runtime(notification_history::verify(operations.into_iter().map(|(id,event,remove,detail)| {
            let mut record = notice(id as u64, [AttentionEvent::TaskSettled, AttentionEvent::TaskFailed,
                AttentionEvent::TaskBlocked, AttentionEvent::GoalDone, AttentionEvent::GoalBlocked, AttentionEvent::NeedsApproval][event as usize]);
            record.detail = detail;
            (record, remove)
        }), (NotificationMode::On, TerminalNotificationProtocol::Osc777, false), false));
    }
}
#[test]
fn retained_removal_mutant_fails_the_shrinking_property_oracle() {
    let record = notice(1, AttentionEvent::TaskFailed);
    assert!(
        std::panic::catch_unwind(|| on_runtime(notification_history::verify(
            [(record.clone(), false), (record, true)],
            (
                NotificationMode::On,
                TerminalNotificationProtocol::Osc9,
                false
            ),
            true
        )))
        .is_err()
    );
}

#[tokio::test]
async fn file_removal_keeps_other_rows_and_advances_the_shared_cursor() {
    let mut projection = Projection::new().await;
    let first = notice(1, AttentionEvent::TaskFailed);
    let second = notice(2, AttentionEvent::GoalBlocked);
    projection.apply(&first, false).unwrap();
    projection.apply(&second, false).unwrap();
    let before = projection.view.cursor().clone();
    projection.apply(&first, true).unwrap();
    assert_eq!(projection.records(), [second]);
    assert_eq!(projection.view.cursor().position, before.position + 1);
    for row in projection.view.notifications() {
        assert_eq!(row.cursor, projection.view.cursor());
    }
}

#[tokio::test]
async fn harness_approval_attention_survives_file_follow_and_terminal_policy() {
    let record = notice(1, AttentionEvent::NeedsApproval);
    let mut projection = Projection::new().await;
    projection.apply(&record, false).unwrap();
    let expected = b"\x1b]9;Approval needed: task-1: Ready\x07";
    for mode in [NotificationMode::On, NotificationMode::AttentionOnly] {
        let mut bytes = Vec::new();
        assert_eq!(
            write_terminal_notifications(
                projection.view.notifications(),
                &mut bytes,
                mode,
                TerminalNotificationProtocol::Osc9,
                false
            )
            .unwrap(),
            1
        );
        assert_eq!(bytes, expected);
        assert_ne!(
            bytes,
            Vec::<u8>::new(),
            "dropping approval attention must fail"
        );
    }
    projection.apply(&record, true).unwrap();
    assert_eq!(projection.view.notifications().count(), 0);
}
