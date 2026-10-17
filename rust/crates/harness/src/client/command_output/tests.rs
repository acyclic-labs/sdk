use super::*;
use proptest::prelude::*;
use serde_json::json;

fn file(id: u8, sequence: u64, owner: u8, text: &str) -> FileValue {
    FileValue::new(
        FileKey::parse(&format!(
            ".ui/sessions/s{owner}/executions/{id}/result.json"
        ))
        .unwrap(),
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
            let owner = OutputOwner::Session(Component::new(format!("s{owner_id}")).unwrap());
            let mut oracle: Vec<_> = expected
                .values()
                .filter_map(|file| {
                    let FileKey::Session { session, .. } = &file.key else {
                        panic!("session");
                    };
                    if session.as_str() != format!("s{owner_id}") {
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
        (0u8..8, 0u64..6, 0u8..3, ".{0,24}", any::<bool>()),0..32)) {
        check(&operations,false);
    }
    #[test]
    fn outcome_envelope_preserves_exact_unicode(sequence in 1u64..=u64::MAX,text in ".{0,64}") {
        let file=file(1,sequence,0,&text);
        let row=CommandOutputView::from_file(&file).unwrap();
        prop_assert_eq!(row.sequence,sequence);
        prop_assert_eq!(row.text.as_deref(),Some(text.as_str()));
        prop_assert_eq!(completion(&file.bytes).unwrap(),HeadlessOutcome::Completed);
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
        assert!(completion(&file.bytes).is_err());
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
        .after(&OutputOwner::Session(Component::new("s0").unwrap()), 0)
        .next()
        .unwrap();
    assert!(metadata.has_text);
}
