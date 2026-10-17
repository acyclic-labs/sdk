use super::*;
use proptest::prelude::*;
use serde_json::json;

fn file(id: u8, sequence: u64, owner: u8, text: &str) -> FileValue {
    let session = uuid::Uuid::from_u128(u128::from(owner % 3) + 1);
    let path = if owner < 3 {
        format!(".sessions/{session}/executions/{id}/result.json")
    } else {
        let operation = uuid::Uuid::from_u128(u128::from(id) + 1);
        if owner < 6 {
            format!(".tasks/t{owner}/executions/command-{session}.{operation}/result.json")
        } else {
            format!(".tasks/t{owner}/executions/{operation}/result.json")
        }
    };
    FileValue::new(
        FileKey::parse(&path).unwrap(),
        serde_json::to_vec(&json!({"sequence":sequence,"ok":{"text":text}})).unwrap(),
    )
}

fn check(operations: &[(u8, u64, u8, String, bool)], retain_removed: bool) {
    let mut actual = Outputs::default();
    let mut expected = BTreeMap::new();
    for (id, sequence, owner_id, text, remove) in operations {
        let file = file(*id, *sequence, *owner_id, text);
        if *remove {
            expected.remove(&file.key);
            if !retain_removed {
                actual.remove(&file.key);
            }
        } else {
            actual.insert(&file);
            expected.insert(file.key.clone(), file);
        }
        for owner_id in 0..3 {
            let owner = OutputOwner::Session(
                Component::new(uuid::Uuid::from_u128(owner_id + 1).to_string()).unwrap(),
            );
            let mut oracle: Vec<_> = expected
                .values()
                .filter_map(|file| {
                    let expected_owner = uuid::Uuid::from_u128(owner_id + 1).to_string();
                    // Independent identity extraction for the physical/session
                    // namespaces; do not use the candidate's owner index/parser.
                    let session = match &file.key {
                        FileKey::Session { session, .. } => Some(session.as_str()),
                        FileKey::Task {
                            file: TaskFile::ExecutionResult(id),
                            ..
                        } => id
                            .as_str()
                            .strip_prefix("command-")
                            .map(|id| id.split_once('.').unwrap().0),
                        _ => panic!("execution result"),
                    };
                    if session.is_some_and(|session| session != expected_owner) {
                        return None;
                    }
                    // Independent decode and validation, never using the candidate parser.
                    let envelope: serde_json::Value = serde_json::from_slice(&file.bytes).unwrap();
                    let sequence = envelope["sequence"].as_u64().unwrap();
                    (sequence != 0).then(|| {
                        (
                            sequence,
                            file.key.clone(),
                            envelope["ok"]["text"].as_str().unwrap().to_owned(),
                        )
                    })
                })
                .collect();
            oracle.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
            let rows: Vec<_> = actual
                .after(&owner, 0)
                .map(|(key, row)| {
                    (
                        row.sequence,
                        key.clone(),
                        CommandOutputView::from_file(&expected[key])
                            .unwrap()
                            .text
                            .unwrap()
                            .into_owned(),
                    )
                })
                .collect();
            assert_eq!(rows, oracle);
            for (index, (sequence, key, _)) in oracle.iter().enumerate() {
                let remaining: Vec<_> = actual
                    .from(&owner, Some((*sequence, key)))
                    .map(|(key, row)| {
                        (
                            row.sequence,
                            key.clone(),
                            CommandOutputView::from_file(&expected[key])
                                .unwrap()
                                .text
                                .unwrap()
                                .into_owned(),
                        )
                    })
                    .collect();
                assert_eq!(remaining, oracle[index + 1..]);
            }
        }
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Replacement, removal, owner isolation and tied ordering match independent
    // immutable-file state; exact resume never drops another tied outcome.
    #[test]
    fn outcome_indexes_match_files(operations in prop::collection::vec(
        (0u8..8, 0u64..6, 0u8..9, ".{0,24}", any::<bool>()),0..32)) {
        check(&operations,false);
    }
    #[test]
    fn outcome_envelope_preserves_exact_unicode(sequence in 1u64..=u64::MAX,text in ".{0,64}") {
        let file=file(1,sequence,0,&text);
        let row=CommandOutputView::from_file(&file).unwrap();
        prop_assert_eq!(row.sequence,sequence);
        prop_assert_eq!(row.text.as_deref(),Some(text.as_str()));
        prop_assert_eq!(row.completion,HeadlessOutcome::Completed);
    }
}
#[test]
fn retired_outcome_negative_control() {
    let operations = vec![
        (1, 1, 0, "answer".into(), false),
        (1, 1, 0, "answer".into(), true),
    ];
    check(&operations, false);
    assert!(std::panic::catch_unwind(|| check(&operations, true)).is_err());
}
#[test]
fn malformed_outcomes_never_become_ui_success() {
    for value in [
        json!({"sequence":0,"ok":{}}),
        json!({"sequence":1}),
        json!({"sequence":1,"ok":{},"error":"failed"}),
        json!({"sequence":1,"ok":{},"completion":"Failed"}),
        json!({"sequence":1,"error":5}),
    ] {
        let mut file = file(1, 1, 0, "");
        file.bytes = serde_json::to_vec(&value).unwrap().into();
        assert!(CommandOutputView::from_file(&file).is_none());
        assert!(
            serde_json::from_slice::<Outcome<'_>>(&file.bytes)
                .map_or(true, |outcome| settled(&outcome).is_err())
        );
    }
}

#[test]
fn plain_command_text_borrows_its_canonical_file_payload() {
    let file = file(1, 1, 0, &"answer".repeat(1024));
    let row = CommandOutputView::from_file(&file).unwrap();
    let Some(std::borrow::Cow::Borrowed(text)) = row.text else {
        panic!("plain text must remain borrowed");
    };
    let start = file.bytes.as_ptr() as usize;
    assert!((start..start + file.bytes.len()).contains(&(text.as_ptr() as usize)));
    let mut index = Outputs::default();
    index.insert(&file);
    assert_eq!(index.keys.len(), 1);
    // Metadata has no text buffer; all payload ownership remains in FileViews.
    let (_, metadata) = index
        .after(
            &OutputOwner::Session(Component::new(uuid::Uuid::from_u128(1).to_string()).unwrap()),
            0,
        )
        .next()
        .unwrap();
    assert!(metadata.has_text);
}

#[test]
fn task_scoped_commands_are_indexed_by_their_logical_session() {
    let operations = vec![
        (1, 1, 0, "first".into(), false),
        (2, 2, 3, "second".into(), false),
        (1, 3, 4, "other session".into(), false),
        (2, 2, 3, "second".into(), true),
    ];
    check(&operations, false);
    let file = file(2, 2, 3, "second");
    let mut index = Outputs::default();
    index.insert(&file);
    let owner = OutputOwner::Session(Component::new(uuid::Uuid::from_u128(1).to_string()).unwrap());
    assert_eq!(index.after(&owner, 0).count(), 1);
    // A physical-task-only index is the previous implementation.
    index.rows.remove(&Scope::Owner(owner.clone()));
    assert_ne!(index.after(&owner, 0).count(), 1);
}
