use acyclic_harness::client::{
    AttentionNotification, FileQueryView, NotificationFileView, NotificationMode,
    TerminalNotificationProtocol, terminal_notification, write_terminal_notifications,
};
use acyclic_harness::task_volume::{Change, FileKey, Query, TaskVolume, Write};
use acyclic_stream::{MemoryStream, StreamPath};
use std::{collections::BTreeMap, sync::Arc};

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

// Exercise the same snapshot/change fold that receives filesystem publications.
// There is deliberately no independent terminal queue or replay cache.
pub(super) struct Projection {
    pub view: FileQueryView,
}
impl Projection {
    pub async fn new() -> Self {
        let snapshot = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("files").unwrap(),
        )
        .await
        .unwrap()
        .snapshot(&Query::Root, 128)
        .await
        .unwrap();
        Self {
            view: FileQueryView::from_snapshot(Query::Root, snapshot, 128, 1_048_576).unwrap(),
        }
    }
    pub fn key(record: &AttentionNotification) -> FileKey {
        FileKey::parse(&format!(
            ".tasks/{}/notifications/{}.json",
            record.task, record.sequence
        ))
        .unwrap_or_else(|_| FileKey::parse(".tasks/invalid/notifications/invalid.json").unwrap())
    }
    pub fn apply(
        &mut self,
        record: &AttentionNotification,
        remove: bool,
    ) -> acyclic_harness::Result<()> {
        let key = Self::key(record);
        // Deleting an absent file publishes no removal. The ordinary query
        // correctly rejects a fabricated removal of an unknown file.
        if remove && !self.view.files().contains_key(&key) {
            return Ok(());
        }
        let mut cursor = self.view.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        self.view
            .apply(Change {
                cursor,
                file: (!remove)
                    .then(|| Write::new(key.clone(), serde_json::to_vec(record).unwrap())),
                files: Vec::new(),
                removed: if remove { vec![key] } else { Vec::new() },
            })
            .map(|_| ())
    }
    pub fn records(&self) -> Vec<AttentionNotification> {
        self.view
            .notifications()
            .filter_map(|row| match row.notification {
                NotificationFileView::Present { notification } => Some(notification.clone()),
                NotificationFileView::Invalid => None,
            })
            .collect()
    }
}

// Independent file-state oracle shared by the shrinking property and byte fuzzer.
// Every operation is checked, independently of fixture expectations. The mutant
// models the former queue retaining a removed notification after failed output.
pub(super) async fn verify(
    operations: impl IntoIterator<Item = (AttentionNotification, bool)>,
    options: (NotificationMode, TerminalNotificationProtocol, bool),
    retain_removed: bool,
) -> Vec<u8> {
    verify_limits(operations, options, retain_removed, false).await
}

pub(super) async fn verify_limits(
    operations: impl IntoIterator<Item = (AttentionNotification, bool)>,
    options: (NotificationMode, TerminalNotificationProtocol, bool),
    retain_removed: bool,
    unwrap_limit: bool,
) -> Vec<u8> {
    let mut projection = Projection::new().await;
    let mut expected = BTreeMap::new();
    let mut output = Vec::new();
    for (record, remove) in operations {
        let key = Projection::key(&record);
        let mut candidate = expected.clone();
        if remove {
            candidate.remove(&key);
        } else {
            candidate.insert(key, record.clone());
        }
        let over_limit = candidate.len() > 128
            || candidate
                .values()
                .map(|record| serde_json::to_vec(record).unwrap().len())
                .sum::<usize>()
                > 1_048_576;
        if !remove || !retain_removed {
            let before = over_limit.then(|| projection.view.clone());
            let result = projection.apply(&record, remove);
            if over_limit && !unwrap_limit {
                assert!(matches!(result,
                    Err(acyclic_harness::Error::TaskFiles(error))
                        if matches!(*error, acyclic_harness::task_volume::Error::LimitExceeded)));
                assert_eq!(projection.view, before.unwrap());
            } else {
                // Negative control reproduces the former unconditional unwrap.
                result.unwrap();
            }
        }
        if !over_limit {
            expected = candidate;
        }
        let expected: Vec<_> = expected
            .iter()
            .filter_map(|(key, record)| match key {
                FileKey::Task { task, .. }
                    if task.as_str() == record.task && record.sequence != 0 =>
                {
                    Some(record.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(projection.records(), expected);
        let bytes: String = expected
            .iter()
            .map(|record| encode(record, options))
            .collect();
        output.clear();
        assert_eq!(
            write_terminal_notifications(
                projection.view.notifications(),
                &mut output,
                options.0,
                options.1,
                options.2
            )
            .unwrap(),
            expected.len()
        );
        assert_eq!(output, bytes.as_bytes());
        // A retry borrows the same rows and does not mutate the projection.
        assert_eq!(projection.records(), expected);
    }
    output
}
