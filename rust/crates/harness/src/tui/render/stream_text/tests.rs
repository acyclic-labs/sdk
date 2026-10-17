use super::*;
use crate as harness;
use crate::task_volume::{Change, Component, FileKey, FileValue, TaskFile};
use proptest::prelude::*;
#[path = "../../../../tests/support/empty_client_views.rs"]
mod initial;

fn replaced_head(bodies: &[String], forget_prefix: bool) {
    let mut views = initial::empty();
    let task = Component::new("task").unwrap();
    let key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::TextHead {
            turn: 0,
            channel: Channel::Text,
        },
    };
    let mut decoder = StreamText::default();
    let mut rows = Vec::new();
    for body in bodies {
        let mut cursor = views.selections().next().unwrap().1.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        views
            .query_change(Change {
                cursor,
                file: Some(FileValue {
                    key: key.clone(),
                    bytes: body.as_bytes().to_vec().into(),
                    channel: Some(Channel::Text),
                    publication: None,
                }),
                files: Vec::new(),
                removed: Vec::new(),
            })
            .unwrap();
        if forget_prefix {
            decoder.prefix = None;
        }
        let task = views.selections().next().unwrap().1.task(&task);
        let changed = decoder.update(&task, Channel::Text);
        rows.truncate(changed);
        rows.extend(decoder.rows(changed));
        let actual: String = rows
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(
            &actual, body,
            "a replaced logical head cannot retain old text"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn logical_head_replacement_matches_current_file(bodies in prop::collection::vec("[a-z日€🙂]{1,24}",1..12)) {
        replaced_head(&bodies,false);
    }
}
#[test]
fn lost_prefix_identity_negative_control() {
    let bodies = vec!["old".into(), "new".into()];
    replaced_head(&bodies, false);
    assert!(std::panic::catch_unwind(|| replaced_head(&bodies, true)).is_err());
}
