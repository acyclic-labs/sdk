//! Model-choice rendering is a fold over canonical request files.
use acyclic_harness::{
    client::{ClientViews, ModelFileView},
    model::{Model, ModelContent, ModelMessage, ModelRequest, ModelRole},
    task_volume::{
        Change, Component, Cursor, FileKey, FileValue, Generation, Query, SCHEMA, Snapshot,
        TaskFile,
    },
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn key(task: &str, turn: u64) -> FileKey {
    FileKey::Task {
        task: Component::new(task).unwrap(),
        file: TaskFile::TurnRequest(turn),
    }
}
fn cut(volume: [u8; 32], position: u64) -> Cursor {
    Cursor {
        volume,
        epoch: uuid::Uuid::from_bytes([1; 16]),
        schema: SCHEMA,
        query: *blake3::hash(&serde_json::to_vec(&Query::Root).unwrap()).as_bytes(),
        position,
        accepted: position,
        durable: position,
        commit: *blake3::hash(&position.to_le_bytes()).as_bytes(),
    }
}
fn snapshot(cursor: Cursor, files: &BTreeMap<FileKey, Vec<u8>>) -> Snapshot {
    Snapshot {
        generation: Generation {
            volume: cursor.volume,
            epoch: cursor.epoch,
            position: cursor.position,
            commit: cursor.commit,
        },
        cursor,
        files: files
            .iter()
            .map(|(key, bytes)| FileValue::new(key.clone(), bytes.clone()))
            .collect(),
    }
}
fn request(provider: &str, name: &str, revision: &str, text: &str) -> Vec<u8> {
    serde_json::to_vec(&ModelRequest {
        model: Model::new(
            provider.into(),
            name.into(),
            revision.into(),
            json!({"private_option":text}),
        )
        .unwrap(),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(text.into()),
        }],
        tools: vec![],
        max_output_tokens: Some(32),
    })
    .unwrap()
}
fn export(views: &ClientViews) -> Value {
    serde_json::to_value(views).unwrap()["models"].clone()
}

#[test]
fn mixed_provider_choices_are_file_and_cursor_scoped() {
    let mut views = new_views();
    for (index, provider) in ["anthropic", "openai", "gemini", "openrouter", "anthropic"]
        .into_iter()
        .enumerate()
    {
        let cursor = cut([index as u8; 32], 1);
        let file = key("task", index as u64);
        views
            .query_snapshot(
                Query::Root,
                snapshot(
                    cursor.clone(),
                    &BTreeMap::from([(
                        file,
                        request(provider, "model", "1", "never render prompt or options"),
                    )]),
                ),
                8,
                4096,
            )
            .unwrap();
        let current = views
            .models()
            .find(|model| model.cursor.volume == cursor.volume)
            .unwrap();
        assert_eq!(current.turn, index as u64);
        assert_eq!(current.task.as_str(), "task");
        assert_eq!(
            current.model,
            &ModelFileView::Selected {
                provider: provider.into(),
                name: "model".into(),
                revision: "1".into()
            }
        );
    }
    assert_eq!(views.models().count(), 5);
    assert!(!export(&views).to_string().contains("never render"));
    assert!(!export(&views).to_string().contains("private_option"));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn model_identity_round_trips_escaped_unicode_and_ignores_prompt_options(
        provider in ".{0,32}", name in ".{0,32}", revision in ".{0,32}", text in ".{0,128}",
    ) {
        let provider = format!("p{provider}");
        let name = format!("m{name}");
        let revision = format!("r{revision}");
        let cursor = cut([1;32],1);
        let mut views = new_views();
        views.query_snapshot(Query::Root, snapshot(cursor, &BTreeMap::from([
            (key("task",0), request(&provider,&name,&revision,&text)),
        ])), 1, 8192)?;
        let expected = ModelFileView::Selected { provider, name, revision };
        prop_assert_eq!(views.models().next().unwrap().model, &expected);
        prop_assert!(export(&views)[0]["model"].get("options").is_none());
    }

    // Property: snapshot+follow equals a fresh snapshot after every arbitrary
    // edit/removal; rejected gaps preserve state, and replay is idempotent.
    #[test]
    fn model_fold_is_replay_stable_and_matches_current_files(
        actions in prop::collection::vec((0u8..5,0u64..4,prop::collection::vec(any::<u8>(),0..128)),1..24),
    ) {
        let volume = [2;32];
        let mut cursor = cut(volume,1);
        let mut files = BTreeMap::new();
        let mut views = new_views();
        views.query_snapshot(Query::Root,snapshot(cursor.clone(),&files),8,8192)?;
        for (kind,turn,bytes) in actions {
            let file_key = key("task",turn);
            cursor = cut(volume,cursor.position+1);
            let mut removed = Vec::new();
            let file = if kind==0 {
                if files.remove(&file_key).is_some() { removed.push(file_key.clone()); }
                None
            } else {
                let bytes = match kind {
                    1 => request("anthropic","model","1","prompt"),
                    2 => request("openai","model","1","prompt"),
                    3 => br#"{"model":{"provider":" ","name":"x","revision":"1"}}"#.to_vec(),
                    _ => bytes,
                };
                files.insert(file_key.clone(),bytes.clone());
                Some(FileValue::new(file_key,bytes))
            };
            let before = export(&views);
            let gap = Change { cursor:cut(volume,cursor.position+1),file:file.clone(),files:vec![],removed:removed.clone() };
            prop_assert!(views.query_change(gap).is_err());
            prop_assert_eq!(export(&views),before);
            views.query_change(Change {cursor:cursor.clone(),file:file.clone(),files:vec![],removed:removed.clone()})?;
            prop_assert!(!views.query_change(Change {cursor:cursor.clone(),file,files:vec![],removed})?, "replay was not idempotent");
            let mut fresh = new_views();
            fresh.query_snapshot(Query::Root,snapshot(cursor.clone(),&files),8,8192)?;
            prop_assert_eq!(export(&views),export(&fresh));
            assert_current_models(&views, &files)?;
        }
    }
}

fn assert_current_models(
    views: &ClientViews,
    files: &BTreeMap<FileKey, Vec<u8>>,
) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(
        views.models().count(),
        files.len(),
        "stale model survived removal"
    );
    for model in views.models() {
        let bytes = &files[&key(model.task.as_str(), model.turn)];
        let value = serde_json::from_slice::<Value>(bytes).unwrap_or(Value::Null);
        let identity = &value["model"];
        if ["provider", "name", "revision"].iter().all(|field| {
            identity[field]
                .as_str()
                .is_some_and(|text| !text.trim().is_empty())
        }) {
            let expected = json!({"kind":"selected", "provider":identity["provider"],
                "name":identity["name"],"revision":identity["revision"]});
            prop_assert_eq!(serde_json::to_value(model.model)?, expected);
        } else {
            prop_assert_eq!(model.model, &ModelFileView::Invalid);
        }
    }
    Ok(())
}

#[test]
#[should_panic(expected = "stale model survived removal")]
fn negative_control_skipping_removal_fails_the_same_file_oracle() {
    let files = BTreeMap::from([(key("task", 0), request("openai", "model", "1", ""))]);
    let mut views = new_views();
    views
        .query_snapshot(Query::Root, snapshot(cut([3; 32], 1), &files), 1, 4096)
        .unwrap();
    assert_current_models(&views, &files).unwrap();
    // Broken fold: retain the old model cache after its authoritative file is removed.
    assert_current_models(&views, &BTreeMap::new()).unwrap();
}

#[tokio::test]
async fn live_turn_files_publish_choices_and_reopen_at_the_same_cut() {
    use acyclic_harness::task_volume::{FileQuerySource, TaskVolume, Write};
    use acyclic_stream::{MemoryStream, StreamPath};
    use futures::StreamExt as _;
    use std::sync::Arc;

    let streams = Arc::new(MemoryStream::default());
    let path = StreamPath::new("models/live").unwrap();
    let volume = TaskVolume::open(streams.clone(), path.clone())
        .await
        .unwrap();
    let mut observation = volume
        .observe_files(Query::Root, None, 8, 8192)
        .await
        .unwrap();
    let mut views = new_views();
    views
        .query_snapshot(Query::Root, observation.snapshot.take().unwrap(), 8, 8192)
        .unwrap();
    for (ordinal, provider) in ["anthropic", "openai", "gemini", "openrouter", "anthropic"]
        .into_iter()
        .enumerate()
    {
        let write = Write::new(
            key("task", ordinal as u64),
            request(provider, "chosen", "1", "private"),
        );
        volume
            .append(volume.head().await.unwrap(), &write)
            .await
            .unwrap();
        views
            .query_change(observation.changes.next().await.unwrap().unwrap())
            .unwrap();
        let observed = views
            .models()
            .find(|view| view.turn == ordinal as u64)
            .unwrap();
        assert_eq!(
            observed.model,
            &ModelFileView::Selected {
                provider: provider.into(),
                name: "chosen".into(),
                revision: "1".into(),
            }
        );
    }
    let cursor = views.models().next().unwrap().cursor.clone();
    let reopened = TaskVolume::open_existing(streams, path).await.unwrap();
    let snapshot = reopened
        .observe_files(Query::Root, None, 8, 8192)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    assert_eq!(snapshot.cursor, cursor);
    let mut replay = new_views();
    replay
        .query_snapshot(Query::Root, snapshot, 8, 8192)
        .unwrap();
    assert_eq!(export(&replay), export(&views));
    let resumed = reopened
        .observe_files(Query::Root, Some(cursor.clone()), 8, 8192)
        .await
        .unwrap();
    assert!(resumed.snapshot.is_none());
    assert!(!resumed.reset);
}

#[test]
fn invalid_request_identity_never_inherits_an_earlier_selection() {
    for bytes in [
        br#"{"model":["openai","x","1"]}"#.as_slice(),
        br#"{"model":{"provider":"openai","name":"x","revision":"1"},"model":null}"#,
        br#"{"model":{"provider":"openai","provider":"gemini","name":"x","revision":"1"}}"#,
        br#"{"model":{"provider":"openai","name":"x","revision":"1"}} trailing"#,
        br#"{"model":{"provider":"openai","name":"x","revision":"\ud800"}}"#,
        br#"{"model":{"provider":"openai","name":"x","revision":" "}}"#,
    ] {
        let mut views = new_views();
        views
            .query_snapshot(
                Query::Root,
                snapshot(
                    cut([4; 32], 1),
                    &BTreeMap::from([(
                        key("task", 0),
                        request("openai", "chosen", "1", "private"),
                    )]),
                ),
                1,
                4096,
            )
            .unwrap();
        views
            .query_change(Change {
                cursor: cut([4; 32], 2),
                file: Some(FileValue::new(key("task", 0), bytes.to_vec())),
                files: vec![],
                removed: vec![],
            })
            .unwrap();
        assert_eq!(
            views.models().next().unwrap().model,
            &ModelFileView::Invalid
        );
    }
}

fn ignored_request(location: u8, depth: usize, payload: &[u8]) -> Vec<u8> {
    let value = [vec![b'['; depth], payload.to_vec(), vec![b']'; depth]].concat();
    let identity = br#""provider":"openai","name":"chosen","revision":"1""#;
    match location % 3 {
        0 => [
            b"{\"model\":{".as_slice(),
            identity,
            b",\"options\":",
            &value,
            b"}}",
        ]
        .concat(),
        1 => [
            b"{\"model\":{".as_slice(),
            identity,
            b"},\"messages\":",
            &value,
            b"}",
        ]
        .concat(),
        _ => [
            b"{\"model\":{".as_slice(),
            identity,
            b"},\"unknown\":",
            &value,
            b"}",
        ]
        .concat(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn malformed_ignored_fields_always_clear_selection(location in 0u8..3,depth in 0usize..8,bad in 0usize..3) {
        let payloads=[br#""\ud800""#.as_slice(),br#""\udfff""#,b"\"bad\xff\""];
        let bytes=ignored_request(location,depth,payloads[bad]);
        prop_assert!(serde_json::from_slice::<Value>(&bytes).is_err());
        let mut views=new_views();
        views.query_snapshot(Query::Root,snapshot(cut([5;32],1),&BTreeMap::from([(key("task",0),bytes)])),1,8192)?;
        prop_assert_eq!(views.models().next().unwrap().model,&ModelFileView::Invalid);
    }

    #[test]
    fn oversized_model_rows_do_not_block_other_updates(extra in 513usize..2048,edits in 1u64..8) {
        let volume=[6;32];
        let mut files=BTreeMap::from([
            (key("task",0),request("openai","chosen","1","")),
            (key("task",1),request("gemini","large","1",&"x".repeat(extra))),
        ]);
        let mut views=new_views();
        views.query_snapshot(Query::Root,snapshot(cut(volume,1),&files),2,512)?;
        prop_assert_eq!(views.models().count(),2);
        prop_assert_eq!(views.models().find(|row|row.turn==1).unwrap().model,&ModelFileView::Invalid);
        for step in 0..edits {
            let bytes=request("anthropic","chosen","1",&step.to_string());
            files.insert(key("task",0),bytes.clone());
            views.query_change(Change {cursor:cut(volume,step+2),file:Some(FileValue::new(key("task",0),bytes)),files:vec![],removed:vec![]})?;
            prop_assert_eq!(views.models().next().unwrap().cursor.position,step+2);
            prop_assert!(matches!(views.models().find(|row|row.turn==0).unwrap().model,ModelFileView::Selected{..}), "selected model was lost");
            let mut fresh=new_views();
            fresh.query_snapshot(Query::Root,snapshot(cut(volume,step+2),&files),2,512)?;
            prop_assert_eq!(export(&views),export(&fresh));
        }
        let bytes=request("gemini","recovered","1","");
        views.query_change(Change {cursor:cut(volume,edits+2),file:Some(FileValue::new(key("task",1),bytes)),files:vec![],removed:vec![]})?;
        prop_assert!(matches!(views.models().find(|row|row.turn==1).unwrap().model,ModelFileView::Selected{..}), "selected model was lost");
        views.query_change(Change {cursor:cut(volume,edits+3),file:None,files:vec![],removed:vec![key("task",1)]})?;
        prop_assert_eq!(views.models().count(),1);
    }
}

#[test]
fn oversized_follow_preserves_small_rows_and_discards_only_the_large_payload() {
    use acyclic_harness::client::FileQueryView;
    let volume = [7; 32];
    let files = BTreeMap::from([(key("task", 0), request("openai", "chosen", "1", ""))]);
    let mut view =
        FileQueryView::from_snapshot(Query::Root, snapshot(cut(volume, 1), &files), 3, 512)
            .unwrap();
    let change = || Change {
        cursor: cut(volume, 2),
        file: Some(FileValue::new(
            key("task", 1),
            request("gemini", "large", "1", &"x".repeat(2048)),
        )),
        files: vec![FileValue::new(FileKey::Goal, b"unrelated".to_vec())],
        removed: vec![],
    };
    assert!(view.apply(change()).unwrap());
    assert!(!view.apply(change()).unwrap());
    assert_eq!(view.cursor().position, 2);
    assert_eq!(view.files().len(), 2);
    assert!(!view.files().contains_key(&key("task", 1)));
    assert_eq!(
        view.models().find(|row| row.turn == 1).unwrap().model,
        &ModelFileView::Invalid
    );
    assert!(
        view.files()
            .values()
            .map(|file| file.length as usize)
            .sum::<usize>()
            <= 512
    );
    view.apply(Change {
        cursor: cut(volume, 3),
        file: Some(FileValue::new(FileKey::Goal, b"next".to_vec())),
        files: vec![],
        removed: vec![],
    })
    .unwrap();
    assert_eq!(view.cursor().position, 3);
    view.apply(Change {
        cursor: cut(volume, 4),
        file: None,
        files: vec![],
        removed: vec![key("task", 1)],
    })
    .unwrap();
    assert_eq!(view.models().count(), 1);
}

#[cfg(feature = "test-support")]
#[path = "../client_model_view_work.rs"]
mod work;

#[test]
fn skipped_fields_keep_the_json_decoders_recursion_bound() {
    let bytes = ignored_request(0, 256, b"null");
    assert!(serde_json::from_slice::<Value>(&bytes).is_err());
    let mut views = new_views();
    views
        .query_snapshot(
            Query::Root,
            snapshot(cut([9; 32], 1), &BTreeMap::from([(key("task", 0), bytes)])),
            1,
            8192,
        )
        .unwrap();
    assert_eq!(
        views.models().next().unwrap().model,
        &ModelFileView::Invalid
    );
}

fn new_views() -> ClientViews {
    ClientViews::new(
        acyclic_stream::StreamPath::new("swarm").unwrap(),
        acyclic_harness::client::ClientViewLimits {
            agents: 0,
            selected: 0,
            overview_values: 0,
            overview_bytes: 0,
            query: acyclic_harness::client::QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
    .unwrap()
}
