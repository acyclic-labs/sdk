//! Table-driven attention mapping, cursor replay and terminal frames.

use acyclic_harness::{
    Result,
    client::{
        AttentionEvent, AttentionNotification, NotificationMode, NotificationView,
        TerminalNotificationProtocol,
    },
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::client::terminal_notification;

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

#[test]
fn terminal_write_failures_preserve_the_unpresented_notification() -> Result<()> {
    use acyclic_harness::client::write_terminal_notifications;
    let mut view = NotificationView::new(2, 2, 1024)?;
    let event = notice(1, AttentionEvent::GoalDone);
    view.apply(event.clone())?;
    struct Broken;
    impl std::io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    assert!(
        write_terminal_notifications(
            &mut view,
            &mut Broken,
            NotificationMode::On,
            TerminalNotificationProtocol::Osc9,
            false
        )
        .is_err()
    );
    assert_eq!(view.front(), Some(&event));
    let mut output = Vec::new();
    assert_eq!(
        write_terminal_notifications(
            &mut view,
            &mut output,
            NotificationMode::On,
            TerminalNotificationProtocol::Bell,
            false
        )
        .expect("terminal write"),
        1
    );
    assert_eq!(output, b"\x07");
    assert!(view.is_empty());
    Ok(())
}

#[test]
fn suppressed_notifications_drain_without_calling_a_broken_terminal() -> Result<()> {
    use acyclic_harness::client::write_terminal_notifications;
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
            let mut view = NotificationView::new(1, 1, 1024)?;
            view.apply(notice(1, event))?;
            let mut terminal = Broken::default();
            assert_eq!(
                write_terminal_notifications(&mut view, &mut terminal, mode, protocol, focused)
                    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?,
                1
            );
            assert!(view.is_empty());
            assert_eq!((terminal.writes, terminal.flushes), (0, 0));
        }
    }
    let mut view = NotificationView::new(1, 2, 1024)?;
    view.apply(notice(1, AttentionEvent::TaskSettled))?;
    let failed = notice(2, AttentionEvent::TaskFailed);
    view.apply(failed.clone())?;
    let mut terminal = Broken::default();
    assert!(
        write_terminal_notifications(
            &mut view,
            &mut terminal,
            NotificationMode::AttentionOnly,
            TerminalNotificationProtocol::Osc9,
            false
        )
        .is_err()
    );
    assert_eq!(view.front(), Some(&failed));
    assert_eq!(view.len(), 1);
    assert_eq!((terminal.writes, terminal.flushes), (1, 0));
    Ok(())
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

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn queue_insertion_cost_is_independent_of_payload_size() -> Result<()> {
    let mut previous = None;
    for bytes in [8, 1_048_576] {
        let notification = AttentionNotification {
            detail: "x".repeat(bytes),
            ..notice(1, AttentionEvent::GoalDone)
        };
        let payload = notification.detail.as_ptr();
        let mut view = NotificationView::new(1, 1, 1_048_640)?;
        let mut result = Ok(false);
        let allocation = acyclic_test_support::measure(|| {
            result = view.apply(notification);
        });
        assert!(result?);
        if let Some(previous) = previous {
            assert_eq!(allocation.bytes_total, previous);
        }
        previous = Some(allocation.bytes_total);
        assert_eq!(
            view.front().map(|record| record.detail.as_ptr()),
            Some(payload)
        );
        let cloned = view.clone();
        assert_eq!(
            cloned.front().map(|record| record.detail.as_ptr()),
            Some(payload)
        );
        assert_eq!(
            view.pop().map(|record| record.detail.as_ptr()),
            Some(payload)
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn existing_task_updates_allocate_only_shared_record_metadata() -> Result<()> {
    let mut previous_bytes = None;
    for task_bytes in [8, 1_048_576] {
        let mut view = NotificationView::new(1, 1, 1_048_640)?;
        view.apply(AttentionNotification {
            task: "t".repeat(task_bytes),
            ..notice(1, AttentionEvent::TaskSettled)
        })?;
        view.pop();
        let next = AttentionNotification {
            task: "t".repeat(task_bytes),
            ..notice(2, AttentionEvent::TaskFailed)
        };
        let mut result = Ok(false);
        let allocation = acyclic_test_support::measure(|| {
            result = view.apply(next);
        });
        assert!(result?);
        assert_eq!(allocation.count_total, 1);
        if let Some(previous) = previous_bytes {
            assert_eq!(allocation.bytes_total, previous);
        }
        previous_bytes = Some(allocation.bytes_total);
        assert_eq!(
            view.front()
                .map(|record| (record.task.len(), record.sequence)),
            Some((task_bytes, 2))
        );
    }
    Ok(())
}

#[test]
fn full_queue_preserves_attention_for_replay() -> Result<()> {
    let mut view = NotificationView::new(2, 1, 1024)?;
    let first = notice(3, AttentionEvent::TaskBlocked);
    assert!(view.apply(first.clone())?);
    assert!(!view.apply(first.clone())?);
    let mut conflict = first.clone();
    conflict.detail = "different".into();
    assert!(view.apply(conflict).is_err());
    let next = notice(9, AttentionEvent::GoalDone);
    assert!(view.apply(next.clone()).is_err());
    assert_eq!(view.front(), Some(&first));
    assert_eq!(view.pop().as_deref(), Some(&first));
    assert!(view.apply(next.clone())?);
    assert_eq!(view.pop().as_deref(), Some(&next));
    assert!(!view.apply(first)?);
    assert!(view.is_empty());
    Ok(())
}

#[test]
fn notification_bounds_and_other_tasks() -> Result<()> {
    let mut view = NotificationView::new(1, 4, 32)?;
    for notification in [
        notice(0, AttentionEvent::TaskSettled),
        AttentionNotification {
            detail: "x".repeat(40),
            ..notice(1, AttentionEvent::TaskFailed)
        },
    ] {
        assert!(view.apply(notification).is_err());
    }
    assert!(view.is_empty());
    view.apply(notice(1, AttentionEvent::TaskSettled))?;
    let mut other = notice(1, AttentionEvent::TaskFailed);
    other.task = "other".into();
    assert!(view.apply(other).is_err());
    assert_eq!(view.len(), 1);
    Ok(())
}

#[tokio::test]
async fn notification_stream_is_push_driven_and_replay_safe() -> Result<()> {
    use futures::{FutureExt as _, StreamExt as _};
    let (mut sender, receiver) = futures::channel::mpsc::channel(1);
    let stream = NotificationView::new(2, 1, 1024)?.follow(Box::pin(receiver));
    futures::pin_mut!(stream);
    assert!(stream.next().now_or_never().is_none());
    let first = notice(1, AttentionEvent::TaskSettled);
    sender.try_send(Ok(first.clone())).expect("push");
    assert_eq!(stream.next().await.transpose()?.as_deref(), Some(&first));
    sender.try_send(Ok(first)).expect("replay");
    assert!(stream.next().now_or_never().is_none());
    let next = notice(2, AttentionEvent::GoalDone);
    sender.try_send(Ok(next.clone())).expect("push");
    assert_eq!(stream.next().await.transpose()?.as_deref(), Some(&next));
    sender
        .try_send(Err(acyclic_harness::Error::Conflict("query reset".into())))
        .expect("push");
    assert!(stream.next().await.expect("source failure").is_err());
    assert!(stream.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn platform_follow_replays_the_same_attention_records_after_resume()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::{
        AppendRequest, MemoryStream, OwnedBytes, Record, StreamError, StreamPath, StreamProvider,
    };
    use futures::{FutureExt as _, StreamExt as _};
    fn decode(record: std::result::Result<Record, StreamError>) -> Result<AttentionNotification> {
        let record = record.map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
        serde_json::from_slice(&record.value)
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))
    }
    let provider = MemoryStream::default();
    let path = StreamPath::new("tasks/task-1/notifications")?;
    let first = notice(1, AttentionEvent::TaskSettled);
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from(serde_json::to_vec(&first)?)],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    {
        let records = provider.follow(path.clone(), 0).await?;
        let stream = NotificationView::new(1, 1, 1024)?.follow(Box::pin(records.map(decode)));
        futures::pin_mut!(stream);
        assert_eq!(stream.next().await.transpose()?.as_deref(), Some(&first));
        for (index, event) in [
            AttentionEvent::TaskFailed,
            AttentionEvent::TaskBlocked,
            AttentionEvent::GoalDone,
        ]
        .into_iter()
        .enumerate()
        {
            assert!(stream.next().now_or_never().is_none());
            let sequence = index as u64 + 2;
            let notification = notice(sequence, event);
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::from(serde_json::to_vec(&notification)?)],
                    if_tail: Some(sequence - 1),
                    idempotency_key: None,
                })
                .await?;
            assert_eq!(
                stream.next().await.transpose()?.as_deref(),
                Some(&notification)
            );
        }
    }
    let records = provider.follow(path, 2).await?;
    let resumed = NotificationView::new(1, 1, 1024)?.follow(Box::pin(records.map(decode)));
    futures::pin_mut!(resumed);
    assert_eq!(
        resumed.next().await.transpose()?.as_deref(),
        Some(&notice(3, AttentionEvent::TaskBlocked))
    );
    assert_eq!(
        resumed.next().await.transpose()?.as_deref(),
        Some(&notice(4, AttentionEvent::GoalDone))
    );
    assert!(resumed.next().now_or_never().is_none());
    Ok(())
}
