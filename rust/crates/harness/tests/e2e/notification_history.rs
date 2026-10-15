use acyclic_harness::client::{
    AttentionNotification, NotificationMode, NotificationView, TerminalNotificationProtocol,
    terminal_notification, write_terminal_notifications,
};
use std::collections::VecDeque;

pub(super) fn encode(
    record: &AttentionNotification,
    (mode, protocol, focused): (NotificationMode, TerminalNotificationProtocol, bool),
) -> String {
    let output = terminal_notification(record, mode, protocol, focused);
    if !output.is_empty() {
        let (prefix, separators) = match protocol {
            TerminalNotificationProtocol::Bell => {
                assert_eq!(output, "\x07");
                return output;
            }
            TerminalNotificationProtocol::Osc9 => ("\x1b]9;", 0),
            TerminalNotificationProtocol::Osc777 => ("\x1b]777;notify;", 1),
        };
        let body = output
            .strip_prefix(prefix)
            .unwrap()
            .strip_suffix('\x07')
            .unwrap();
        assert!(!body.chars().any(char::is_control));
        assert_eq!(body.matches(';').count(), separators);
    }
    output
}

// Shared by the shrinking property and byte fuzzer. Fixture expectations do not
// participate: every record is attempted, including those after a rejected one.
pub(super) fn verify(
    operations: impl IntoIterator<Item = (AttentionNotification, bool)>,
    (maximum_tasks, maximum_pending, maximum_text_bytes): (usize, usize, usize),
    (mode, protocol, focused): (NotificationMode, TerminalNotificationProtocol, bool),
) -> Vec<u8> {
    let Ok(mut view) = NotificationView::new(maximum_tasks, maximum_pending, maximum_text_bytes)
    else {
        assert!(maximum_tasks == 0 || maximum_pending == 0 || maximum_text_bytes == 0);
        for (record, _) in operations {
            let _ = encode(&record, (mode, protocol, focused));
        }
        return Vec::new();
    };
    assert!(maximum_tasks > 0 && maximum_pending > 0 && maximum_text_bytes > 0);
    let mut pending = VecDeque::new();
    for (record, drain) in operations {
        let _ = encode(&record, (mode, protocol, focused));
        let before = serde_json::to_value(&view).unwrap();
        let serialized = serde_json::to_value(&record).unwrap();
        let previous = before["latest"].get(&record.task);
        let valid = record.sequence > 0
            && !record.task.trim().is_empty()
            && record
                .task
                .len()
                .checked_add(record.detail.len())
                .is_some_and(|bytes| bytes <= maximum_text_bytes);
        let replay = valid
            && previous.is_some_and(|old| {
                old["sequence"].as_u64().unwrap() > record.sequence || old == &serialized
            });
        let eligible = valid
            && previous.is_none_or(|old| old["sequence"].as_u64().unwrap() < record.sequence)
            && (previous.is_some() || before["latest"].as_object().unwrap().len() < maximum_tasks)
            && pending.len() < maximum_pending;
        match view.apply(record.clone()) {
            Ok(true) => assert!(eligible),
            Ok(false) => assert!(replay),
            Err(_) => assert!(!eligible && !replay),
        }
        if eligible {
            let mut latest = before["latest"].clone();
            latest[&record.task] = serialized;
            assert_eq!(serde_json::to_value(&view).unwrap()["latest"], latest);
            pending.push_back(record.clone());
            assert!(!view.apply(record.clone()).unwrap());
            let mut conflicting = record;
            conflicting.detail.push('!');
            let accepted = serde_json::to_value(&view).unwrap();
            assert!(view.apply(conflicting).is_err());
            assert_eq!(serde_json::to_value(&view).unwrap(), accepted);
        } else {
            assert_eq!(serde_json::to_value(&view).unwrap(), before);
        }
        assert_eq!(view.front(), pending.front());
        assert_eq!(view.len(), pending.len());
        if drain {
            assert_eq!(view.pop().as_deref(), pending.pop_front().as_ref());
        }
    }
    let state = serde_json::to_value(&view).unwrap();
    assert_eq!(state["pending"], serde_json::to_value(&pending).unwrap());
    let latest = state["latest"].clone();
    let expected: String = pending
        .iter()
        .map(|record| encode(record, (mode, protocol, focused)))
        .collect();
    let mut output = Vec::new();
    let presented =
        write_terminal_notifications(&mut view, &mut output, mode, protocol, focused).unwrap();
    assert_eq!(presented, pending.len());
    assert_eq!(output, expected.as_bytes());
    assert!(view.is_empty());
    assert_eq!(serde_json::to_value(view).unwrap()["latest"], latest);
    output
}
