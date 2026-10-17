use super::*;
use crate::task_volume::{Cursor, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

fn check(
    operations: &[(u8, u8, String)],
    recent: usize,
    maximum_files: usize,
    maximum_bytes: usize,
    broken: bool,
) {
    let snapshot = futures::executor::block_on(async {
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("display").unwrap(),
        )
        .await
        .unwrap()
        .snapshot(&Query::Root, 16)
        .await
        .unwrap()
    });
    let mut cursor: Cursor = snapshot.cursor;
    let mut actual = Window {
        files: BTreeMap::new(),
        order: BTreeSet::new(),
        bytes: 0,
        per_file: PerFileByteLimit(2048),
        total: DisplayWindowBudget {
            files: maximum_files,
            bytes: maximum_bytes,
        },
        recent,
    };
    let mut expected: BTreeMap<FileKey, FileValue> = BTreeMap::new();
    let mut delivered = BTreeMap::new();
    for (index, (id, kind, text)) in operations.iter().enumerate() {
        let remove = kind % 5 >= 3;
        let path = match kind % 5 {
            0 | 4 => format!(".sessions/s/executions/{id}/result.json"),
            1 => format!(".sessions/s/requests/{id}.json"),
            _ => ".ui/session-s/layout.json".into(),
        };
        let file = FileValue::new(
            FileKey::parse(&path).unwrap(),
            serde_json::to_vec(&serde_json::json!({"sequence":index+1,"ok":{"text":text}}))
                .unwrap(),
        );
        if remove {
            expected.remove(&file.key);
        } else if kind % 5 == 0 {
            expected.insert(file.key.clone(), file.clone());
        }
        // Independent file-state selection. Inbox and UI files never enter this
        // display; retention applies only to outcomes, with a separate file bound.
        loop {
            let mut outcomes: Vec<_> = expected
                .iter()
                .filter_map(|(key, file)| {
                    if !matches!(key, FileKey::Session { .. }) {
                        return None;
                    }
                    let value: serde_json::Value = serde_json::from_slice(&file.bytes).unwrap();
                    Some((value["sequence"].as_u64().unwrap(), key.clone()))
                })
                .collect();
            outcomes.sort();
            let bytes: usize = expected.values().map(|file| file.bytes.len()).sum();
            if outcomes.len() <= recent && expected.len() <= maximum_files && bytes <= maximum_bytes
            {
                break;
            }
            expected.remove(&outcomes[0].1);
        }
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        let mut change = Change {
            cursor: cursor.clone(),
            removed: if remove {
                vec![file.key.clone()]
            } else {
                vec![]
            },
            file: (!remove).then_some(file),
            files: vec![],
        };
        if broken {
            actual.apply::<false>(&mut change).unwrap()
        } else {
            actual.apply::<true>(&mut change).unwrap()
        }
        for key in change.removed {
            assert!(
                delivered.remove(&key).is_some(),
                "retire only published display files"
            );
        }
        for file in change.file.into_iter().chain(change.files) {
            delivered.insert(file.key.clone(), file);
        }
        let same = |files: &BTreeMap<FileKey, FileValue>| {
            files
                .iter()
                .map(|(key, file)| (key.clone(), file.bytes.to_vec()))
                .collect::<BTreeMap<_, _>>()
        };
        assert_eq!(same(&delivered), same(&expected));
        assert_eq!(
            actual.bytes,
            expected
                .values()
                .map(|file| file.bytes.len())
                .sum::<usize>()
        );
        assert_eq!(actual.files.len(), expected.len());
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn command_display_retention_matches_immutable_files(operations in prop::collection::vec(
        (0u8..8,0u8..5,"[a-z]{0,1900}"),0..24)) {
        for (recent, files, bytes) in [(2,4,16_384),(8,2,16_384),(8,16,2048),(8,16,512)] {
            check(&operations, recent, files, bytes, false);
        }
    }
}
#[test]
fn retaining_finished_inbox_history_negative_control() {
    let rows = (0..3)
        .map(|id| (id, 0, "answer".repeat(200)))
        .collect::<Vec<_>>();
    for (recent, files, bytes) in [(2, 4, 16_384), (8, 2, 16_384), (8, 16, 2048)] {
        check(&rows, recent, files, bytes, false);
        assert!(std::panic::catch_unwind(|| check(&rows, recent, files, bytes, true)).is_err());
    }
}

fn prompt_budget_property(history: usize, draft: usize, shared_budget: bool) {
    let mut window = Window {
        files: BTreeMap::new(),
        order: BTreeSet::new(),
        bytes: 0,
        per_file: PerFileByteLimit(65_536),
        total: DisplayWindowBudget {
            files: 16,
            bytes: 512,
        },
        recent: 4,
    };
    let answer = FileValue::new(
        FileKey::parse(".sessions/s/executions/1/result.json").unwrap(),
        br#"{"sequence":1,"ok":{"text":"answer"}}"#.to_vec(),
    );
    assert!(window.include(&answer).unwrap());
    let prompt = [
        (
            ".ui/config/history.json",
            serde_json::json!(["h".repeat(history)]),
        ),
        (
            ".ui/session-s/draft.json",
            serde_json::json!("d".repeat(draft)),
        ),
        (
            ".ui/session-s/layout.json",
            serde_json::json!({"open_view":"history"}),
        ),
    ];
    for (path, value) in prompt {
        let file = FileValue::new(
            FileKey::parse(path).unwrap(),
            serde_json::to_vec(&value).unwrap(),
        );
        assert!(file.bytes.len() as u64 <= window.per_file.0);
        if shared_budget {
            // Restore the defect: untrimmable UI entries share the session's
            // per-file bound with answers. The same property must reject it.
            window.total.bytes = window.per_file.0 as usize;
            window.bytes += file.bytes.len();
            window.files.insert(file.key, (file.bytes.len(), 0));
        } else {
            assert!(!window.include(&file).unwrap());
        }
        window
            .trim::<true>(&mut Vec::new())
            .expect("prompt files must not exhaust answer capacity");
    }
    assert_eq!(window.files.len(), 1);
    assert!(window.files.contains_key(&answer.key));
    assert_eq!(window.bytes, answer.bytes.len());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn independently_bounded_prompt_files_leave_answer_capacity_unchanged(history in 60_000usize..65_501, draft in 6000usize..16_384) {
        prompt_budget_property(history, draft, false);
    }
}

#[test]
#[should_panic(expected = "prompt files must not exhaust answer capacity")]
fn shared_prompt_and_answer_budget_negative_control() {
    prompt_budget_property(65_500, 8192, true);
}

#[test]
fn oversized_answer_uses_the_per_file_limit_even_with_display_capacity() {
    let mut window = Window {
        files: BTreeMap::new(),
        order: BTreeSet::new(),
        bytes: 0,
        per_file: PerFileByteLimit(16),
        total: DisplayWindowBudget {
            files: 16,
            bytes: 65_536,
        },
        recent: 4,
    };
    let answer = FileValue::new(
        FileKey::parse(".sessions/s/executions/1/result.json").unwrap(),
        br#"{"sequence":1,"ok":{"text":"answer"}}"#.to_vec(),
    );
    assert!(matches!(window.include(&answer), Err(Error::LimitExceeded)));
    assert!(window.files.is_empty());
}
