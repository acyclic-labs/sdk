//! The attention schema has one parser, shared by replay and live file folds.
use acyclic_harness::{
    client::{AttentionEvent, AttentionNotification, NotificationFileView},
    task_volume::{FileKey, FileValue},
};

pub fn encoded(event: u8, sequence: u64, detail: String) -> AttentionNotification {
    AttentionNotification {
        task: "t".into(),
        sequence,
        detail,
        event: match event % 5 {
            0 => AttentionEvent::TaskSettled,
            1 => AttentionEvent::TaskFailed,
            2 => AttentionEvent::TaskBlocked,
            3 => AttentionEvent::GoalDone,
            _ => AttentionEvent::GoalBlocked,
        },
    }
}
fn file(bytes: &[u8]) -> FileValue {
    FileValue {
        key: FileKey::parse(".tasks/t/notifications/n.json").unwrap(),
        bytes: bytes.to_vec().into(),
        channel: None,
        publication: None,
    }
}
pub fn check(record: &AttentionNotification, dropped: bool) {
    let value = file(&serde_json::to_vec(record).unwrap());
    let actual = if dropped {
        NotificationFileView::Invalid
    } else {
        NotificationFileView::from_file(&value)
    };
    if record.sequence == 0 || record.task != "t" {
        assert_eq!(actual, NotificationFileView::Invalid);
    } else {
        assert_eq!(
            actual,
            NotificationFileView::Present {
                notification: record.clone()
            }
        );
    }
}
pub fn exercise(bytes: &[u8]) {
    for event in 0..5 {
        check(
            &encoded(event, 1, String::from_utf8_lossy(bytes).into_owned()),
            false,
        );
    }
    let mut foreign = encoded(4, 1, "blocked".into());
    foreign.task = "foreign".into();
    check(&foreign, false);
    check(&encoded(5, 0, "blocked".into()), false);
    let unknown =
        br#"{"task":"t","sequence":1,"event":"goal_blocked","detail":"blocked","unknown":true}"#;
    assert_eq!(
        NotificationFileView::from_file(&file(unknown)),
        NotificationFileView::Invalid
    );
    if let NotificationFileView::Present { notification } =
        NotificationFileView::from_file(&file(bytes))
    {
        check(&notification, false);
        assert_eq!(notification.task, "t");
        assert_ne!(notification.sequence, 0);
    }
}
