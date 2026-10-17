use super::*;
use crate::task_volume::{Cursor, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

fn check(operations: &[(u8, u8, String)], broken: bool) {
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
        maximum_files: 4,
        maximum_bytes: 2048,
        recent: 2,
    };
    let mut expected: BTreeMap<FileKey, FileValue> = BTreeMap::new();
    let mut delivered = BTreeMap::new();
    for (index, (id, kind, text)) in operations.iter().enumerate() {
        let path = match kind % 3 {
            0 => format!(".sessions/s/executions/{id}/result.json"),
            1 => format!(".sessions/s/requests/{id}.json"),
            _ => ".ui/session-s/layout.json".into(),
        };
        let file = FileValue::new(
            FileKey::parse(&path).unwrap(),
            serde_json::to_vec(&serde_json::json!({"sequence":index+1,"ok":{"text":text}}))
                .unwrap(),
        );
        if kind % 3 != 1 {
            expected.insert(file.key.clone(), file.clone());
        }
        // Independent file-state selection. Completed inbox records never enter
        // this display; only the latest two outcomes and current layout remain.
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
            if outcomes.len() <= 2 && expected.len() <= 4 && bytes <= 2048 {
                break;
            }
            expected.remove(&outcomes[0].1);
        }
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        let mut change = Change {
            cursor: cursor.clone(),
            file: Some(file),
            files: vec![],
            removed: vec![],
        };
        if broken {
            actual.apply::<false>(&mut change).unwrap()
        } else {
            actual.apply::<true>(&mut change).unwrap()
        }
        for key in change.removed {
            delivered.remove(&key);
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
        (0u8..8,0u8..3,"[a-z]{0,1900}"),0..24)) {
        check(&operations, false);
    }
}
#[test]
fn retaining_finished_inbox_history_negative_control() {
    let rows = (0..3)
        .map(|id| (id, 0, "answer".into()))
        .collect::<Vec<_>>();
    check(&rows, false);
    assert!(std::panic::catch_unwind(|| check(&rows, true)).is_err());
}
