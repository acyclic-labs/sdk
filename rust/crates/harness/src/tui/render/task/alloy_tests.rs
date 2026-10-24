use super::*;
use crate::task_volume::{Change, FileValue};

#[test]
fn alloy_members_and_usage_follow_steps_and_rebuild_from_the_same_files() {
    let mut views = crate::test_support::empty_client_views();
    let task = Component::new("task").unwrap();
    let mut live = TaskRows::default();
    for (turn, member) in [(7, "a/tiny"), (12, "b/tiny")] {
        let mut cursor = views.selections().next().unwrap().1.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        views.query_change(Change {
            cursor,
            file: Some(FileValue::new(FileKey::Task {
                task: task.clone(), file: TaskFile::Completion(turn),
            }, serde_json::to_vec(&serde_json::json!({
                "member":member,"members":{member:{"usage":{"input_tokens":64,"output_tokens":2}}},
            })).unwrap())),
            files: Vec::new(), removed: Vec::new(),
        }).unwrap();
        live.update(
            Some(views.selections().next().unwrap().1.task(&task)),
            false,
            AnswerOwner::Task,
        );
        assert!(
            live.notices()
                .iter()
                .any(|line| line.to_string() == format!("Turn {turn}: {member}")),
            "member serving each step remains visible"
        );
    }
    assert_eq!(live.notices().len(), 2);
    let usage: serde_json::Value = serde_json::from_str(&live.usage()).unwrap();
    for member in ["a/tiny", "b/tiny"] {
        assert_eq!(usage["members"][member]["usage"]["input_tokens"], 64);
    }
    let mut replay = TaskRows::default();
    replay.update(
        Some(views.selections().next().unwrap().1.task(&task)),
        false,
        AnswerOwner::Task,
    );
    assert_eq!(live.notices(), replay.notices());
    assert_eq!(live.usage(), replay.usage());
}
