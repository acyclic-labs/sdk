//! Properties exercise the actual client fold, not a second reducer.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness::{
    Error, Result,
    client::{ClientQuery, QueryViewLimits, query_verification::FileViews},
    task_volume::{
        Change, Channel, Cursor, FileKey, HeadFile, Query, TaskFile, TaskVolume, ToolFile, Write,
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::{collections::BTreeMap, sync::Arc};

#[path = "../../../harness-fuzz/support/queries.rs"]
mod query_inputs;

const LIMITS: QueryViewLimits = QueryViewLimits {
    files: 128,
    segments: 128,
    bytes: 4096,
};

fn path(value: &str) -> StreamPath {
    StreamPath::new(value).unwrap()
}
fn key(value: &str) -> FileKey {
    FileKey::parse(value).unwrap()
}

async fn volume() -> TaskVolume<MemoryStream> {
    TaskVolume::open(Arc::new(MemoryStream::default()), path("agent/files"))
        .await
        .unwrap()
}

async fn put(volume: &TaskVolume<MemoryStream>, write: &Write) -> bool {
    let before = volume.snapshot(&Query::Root, 128).await.unwrap().generation;
    let after = volume.append(before, write).await.unwrap();
    after.position != before.position
}

fn chunk(turn: u64, sequence: u64, channel: Channel, bytes: &[u8]) -> Write {
    let mut write = Write::new(
        FileKey::Task {
            task: "t".to_owned().try_into().unwrap(),
            file: TaskFile::Chunk { turn, sequence },
        },
        bytes.to_vec(),
    );
    write.channel = Some(channel);
    write
}

fn files<'a>(
    values: impl Iterator<Item = &'a HeadFile>,
) -> BTreeMap<FileKey, (Vec<u8>, Option<u64>)> {
    values
        .map(|file| {
            (
                file.key.clone(),
                (
                    file.segments
                        .iter()
                        .flat_map(|segment| segment.iter().copied())
                        .collect(),
                    file.through_sequence,
                ),
            )
        })
        .collect()
}

fn unchanged(before: &Cursor, after: &Cursor, emissions: usize) -> bool {
    before == after && emissions == 0
}

type Model = BTreeMap<String, (Vec<u8>, Option<u64>)>;

fn model_apply(model: &mut Model, write: &Write) {
    let (path, sequence) = match (&write.key, write.channel) {
        (
            FileKey::Task {
                task,
                file: TaskFile::Chunk { turn, sequence },
            },
            Some(channel),
        ) => (
            format!(
                ".tasks/{}/turns/{turn}/{}.md",
                task.as_str(),
                match channel {
                    Channel::Text => "text",
                    Channel::Reasoning => "reasoning",
                }
            ),
            Some(*sequence),
        ),
        _ => (write.key.path(), None),
    };
    if sequence.is_some() {
        let entry = model.entry(path).or_default();
        entry.0.extend_from_slice(&write.bytes);
        entry.1 = sequence;
    } else {
        model.insert(path, (write.bytes.to_vec(), None));
    }
}

fn model_matches<'a>(actual: impl Iterator<Item = &'a HeadFile>, expected: &Model) -> bool {
    files(actual)
        .into_iter()
        .map(|(key, value)| (key.path(), value))
        .collect::<Model>()
        == *expected
}

#[tokio::test]
async fn negative_control_wrong_snapshot_channel_is_detected_by_the_property_oracle() {
    let volume = volume().await;
    let write = chunk(1, 1, Channel::Text, b"text");
    put(&volume, &write).await;
    let mut expected = Model::new();
    model_apply(&mut expected, &write);
    let snapshot = volume.heads(&Query::Root, None, 128).await.unwrap();
    let correct = FileViews::snapshot(snapshot, LIMITS).unwrap();
    assert!(model_matches(correct.files(), &expected));
    let mut broken = volume.heads(&Query::Root, None, 128).await.unwrap();
    broken.files[0].key = key(".tasks/t/turns/1/reasoning.md");
    let broken = FileViews::snapshot(broken, LIMITS).unwrap();
    assert!(!model_matches(broken.files(), &expected));
}

#[tokio::test]
async fn failed_publication_retains_the_frame_and_cannot_advance_the_cursor() -> Result<()> {
    let volume = volume().await;
    let mut query = ClientQuery::open(&volume, Query::Root, None, LIMITS).await?;
    let before = query.cursor().clone();
    put(&volume, &chunk(2, 1, Channel::Text, b"current")).await;
    let mut attempted = Vec::new();
    let result = query
        .publish_next(async |change| {
            attempted.push(change.cursor.position);
            Err(Error::Storage("stdout failed".into()))
        })
        .await;
    assert!(result.is_err());
    assert!(unchanged(&before, query.cursor(), 0));
    assert_eq!(query.files().count(), 0);
    put(&volume, &chunk(1, 1, Channel::Reasoning, b"older turn")).await;
    assert!(matches!(
        query.reconnect(&volume).await,
        Err(Error::Conflict(_))
    ));
    let mut pointer = std::ptr::null();
    query
        .publish_next(async |change| {
            attempted.push(change.cursor.position);
            pointer = change.file.as_ref().unwrap().bytes.as_ptr();
            Ok(())
        })
        .await?;
    let current = query.file(&key(".tasks/t/turns/2/text.md")).unwrap();
    assert_eq!(current.segments.len(), 1);
    assert_eq!(current.segments[0].as_ref(), b"current");
    assert_eq!(
        current.segments[0].as_ptr(),
        pointer,
        "retain the provider segment rather than copying text"
    );
    query
        .publish_next(async |change| {
            attempted.push(change.cursor.position);
            Ok(())
        })
        .await?;
    assert_eq!(
        attempted,
        [
            before.position + 1,
            before.position + 1,
            before.position + 2
        ]
    );
    assert_eq!(
        query
            .file(&key(".tasks/t/turns/2/text.md"))
            .unwrap()
            .segments[0]
            .as_ref(),
        b"current"
    );
    assert_eq!(
        query
            .file(&key(".tasks/t/turns/1/reasoning.md"))
            .unwrap()
            .segments[0]
            .as_ref(),
        b"older turn"
    );
    Ok(())
}

#[tokio::test]
async fn negative_control_committing_before_failed_output_is_rejected() -> Result<()> {
    let volume = volume().await;
    let mut query = ClientQuery::open(&volume, Query::Root, None, LIMITS).await?;
    let before = query.cursor().clone();
    put(&volume, &chunk(1, 1, Channel::Text, b"lost")).await;
    // Broken ordering: report publication success, then discover the output failed.
    query.publish_next(async |_| Ok(())).await?;
    assert!(
        !unchanged(&before, query.cursor(), 0),
        "the same failure checker must detect the broken ordering"
    );
    Ok(())
}

#[tokio::test]
async fn reopen_at_c_does_not_include_the_later_tail_until_it_is_published() -> Result<()> {
    let volume = volume().await;
    put(&volume, &chunk(2, 1, Channel::Text, b"first")).await;
    let query = ClientQuery::open(&volume, Query::Root, None, LIMITS).await?;
    let cut = query.cursor().clone();
    put(&volume, &chunk(2, 2, Channel::Text, b"second")).await;
    let mut restored = ClientQuery::open(&volume, Query::Root, Some(cut.clone()), LIMITS).await?;
    assert!(!restored.reset());
    assert_eq!(restored.cursor(), &cut);
    assert_eq!(
        restored
            .file(&key(".tasks/t/turns/2/text.md"))
            .unwrap()
            .length,
        5
    );
    assert!(!restored.reconnect(&volume).await?);
    restored.publish_next(async |_| Ok(())).await?;
    assert_eq!(
        restored
            .file(&key(".tasks/t/turns/2/text.md"))
            .unwrap()
            .length,
        11
    );
    Ok(())
}

#[tokio::test]
async fn reconnect_replaces_reset_views_and_rejects_another_target() -> Result<()> {
    let old = volume().await;
    put(&old, &chunk(2, 1, Channel::Text, b"old")).await;
    let mut query = ClientQuery::open(&old, Query::Root, None, LIMITS).await?;
    let replacement = volume().await;
    put(&replacement, &Write::new(FileKey::Goal, b"new".to_vec())).await;
    assert!(query.reconnect(&replacement).await?);
    assert!(query.reset());
    assert_eq!(query.files().count(), 1);
    assert_eq!(
        query.file(&FileKey::Goal).unwrap().segments[0].as_ref(),
        b"new"
    );
    let other = TaskVolume::open(Arc::new(MemoryStream::default()), path("other/files"))
        .await
        .unwrap();
    let before = query.cursor().clone();
    assert!(matches!(
        query.reconnect(&other).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(query.cursor(), &before);
    Ok(())
}

#[tokio::test]
async fn validation_and_quota_failures_precede_publication() -> Result<()> {
    for limits in [
        QueryViewLimits { files: 0, ..LIMITS },
        QueryViewLimits {
            segments: 0,
            ..LIMITS
        },
        QueryViewLimits { bytes: 0, ..LIMITS },
    ] {
        let volume = volume().await;
        let mut query = ClientQuery::open(&volume, Query::Root, None, limits).await?;
        let before = query.cursor().clone();
        put(&volume, &Write::new(FileKey::Goal, b"x".to_vec())).await;
        let mut emissions = 0;
        assert!(
            query
                .publish_next(async |_| {
                    emissions += 1;
                    Ok(())
                })
                .await
                .is_err()
        );
        assert!(unchanged(&before, query.cursor(), emissions));
        assert_eq!(query.files().count(), 0);
    }
    Ok(())
}

async fn rejected_frame_recovers(payload: &[u8], misclassify_as_output_failure: bool) -> bool {
    let old = volume().await;
    let limits = QueryViewLimits {
        bytes: payload.len() as u64 - u64::from(!misclassify_as_output_failure),
        ..LIMITS
    };
    let mut query = ClientQuery::open(&old, Query::Root, None, limits)
        .await
        .unwrap();
    let before = query.cursor().clone();
    put(&old, &Write::new(FileKey::Goal, payload.to_vec())).await;
    let mut attempts = 0;
    assert!(
        query
            .publish_next(async |_| {
                attempts += 1;
                // Broken classification: treat validation failure as a publication error.
                Err(Error::Storage(
                    "rejected frame misclassified as output failure".into(),
                ))
            })
            .await
            .is_err()
    );
    assert_eq!(attempts, usize::from(misclassify_as_output_failure));
    assert_eq!(query.cursor(), &before);
    assert_eq!(query.files().count(), 0);

    let replacement = volume().await;
    put(&replacement, &Write::new(FileKey::Goal, b"x".to_vec())).await;
    if !query.reconnect(&replacement).await.is_ok_and(|reset| reset) {
        return false;
    }
    assert!(query.reset());
    assert_eq!(
        query.file(&FileKey::Goal).unwrap().segments[0].as_ref(),
        b"x"
    );
    put(&replacement, &Write::new(FileKey::Goal, b"y".to_vec())).await;
    query.publish_next(async |_| Ok(())).await.unwrap();
    query.file(&FileKey::Goal).unwrap().segments[0].as_ref() == b"y"
}

#[tokio::test]
async fn rejected_frame_allows_reconnect_to_a_valid_replacement_history() {
    assert!(rejected_frame_recovers(b"too large", false).await);
}

#[tokio::test]
async fn negative_control_confusing_validation_with_output_failure_blocks_recovery() {
    assert!(!rejected_frame_recovers(b"too large", true).await);
}

proptest::proptest! {
    // Every rejected payload leaves the published cut unchanged and permits a
    // replacement snapshot followed by valid live delivery.
    #[test]
    fn rejected_frames_can_reset(payload in proptest::collection::vec(proptest::num::u8::ANY, 2..64)) {
        assert!(futures::executor::block_on(rejected_frame_recovers(&payload, false)));
    }

    #[test]
    fn query_inputs_round_trip(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..256)) {
        query_inputs::exercise(&bytes);
    }

    // Property: a snapshot at any cut plus all later changes equals the provider's
    // canonical heads, with mutable files replaced and chunks ordered numerically.
    #[test]
    fn snapshot_and_follow_equal_complete_heads(
        events in proptest::collection::vec((0u8..5, 0u8..4, proptest::bool::ANY, proptest::collection::vec(proptest::num::u8::ANY, 0..16)), 0..24),
        prefix in 0usize..25,
    ) {
        futures::executor::block_on(async {
            let volume = volume().await;
            let writes: Vec<_> = events.into_iter().enumerate().map(|(index,(kind,turn,reasoning,bytes))| {
                let sequence = index as u64 + 1;
                match kind {
                    0 => Write::new(FileKey::Goal, bytes),
                    1 => Write::new(key("workspace/draft.md"), bytes),
                    2 => chunk(turn.into(), sequence, if reasoning { Channel::Reasoning } else { Channel::Text }, &bytes),
                    _ => Write::new(FileKey::Task { task: "t".to_owned().try_into().unwrap(), file: TaskFile::Tool { turn: turn.into(), call: "c".to_owned().try_into().unwrap(), file: if kind == 3 { ToolFile::Stdout(sequence) } else { ToolFile::Arguments(sequence) } } }, bytes),
                }
            }).collect();
            let split = prefix.min(writes.len());
            let mut model = Model::new();
            for write in &writes[..split] { put(&volume, write).await; model_apply(&mut model, write); }
            let mut query = ClientQuery::open(&volume, Query::Root, None, LIMITS).await.unwrap();
            assert!(model_matches(query.files(), &model));
            let expected = volume.heads(&Query::Root, None, LIMITS.segments).await.unwrap();
            assert_eq!(files(query.files()), files(expected.files.iter()));
            for write in &writes[split..] {
                if put(&volume, write).await { query.publish_next(async |_| Ok(())).await.unwrap(); }
                model_apply(&mut model, write);
                assert!(model_matches(query.files(), &model));
                let expected = volume.heads(&Query::Root, None, LIMITS.segments).await.unwrap();
                assert_eq!(files(query.files()), files(expected.files.iter()));
                assert_eq!(query.cursor().position, expected.cursor.position);
            }
        });
    }

    // Property: every cursor identity/gap/watermark rejection preserves all view
    // state, including the previously applied cursor.
    #[test]
    fn invalid_cursor_or_publication_never_changes_the_fold(case in 0u8..11, bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..32)) {
        futures::executor::block_on(async {
            let volume = volume().await;
            let mut snapshot = volume.heads(&Query::Root, None, 128).await.unwrap();
            snapshot.cursor.durable = snapshot.cursor.position;
            let before = snapshot.cursor.clone();
            let view = FileViews::snapshot(snapshot, LIMITS).unwrap();
            let mut next = before.clone();
            next.position += 1;
            next.accepted = next.position;
            next.commit = [9;32];
            match case {
                0 => next.volume[0] ^= 1,
                1 => next.epoch = uuid::Uuid::nil(),
                2 => next.schema += 1,
                3 => next.query[0] ^= 1,
                4 => next.position -= 1,
                5 => { next.position += 1; next.accepted = next.position; },
                6 => next.accepted = 0,
                7 => next.durable = next.accepted + 1,
                8 => next.durable = 0,
                _ => {},
            }
            let mut change = Change { cursor: next, file: Some(Write::new(FileKey::Goal, bytes)), files: Vec::new(), removed: Vec::new() };
            match case {
                9 => change.files.push(Write::new(key("workspace/extra.md"), b"extra".to_vec())),
                10 => change.removed.push(key("workspace/removed.md")),
                _ => {},
            }
            assert!(view.prepare(&change).is_err());
            assert_eq!(view.cursor(), &before);
            assert_eq!(view.files().count(), 0);
        });
    }
}

#[test]
fn negative_control_query_counter_truncation_is_rejected() {
    let query = Query::Turn {
        task: "task".to_owned().try_into().unwrap(),
        turn: u64::MAX,
    };
    let encoded = serde_json::to_vec(&query).unwrap();
    query_inputs::check_query(&query, &encoded);
    let broken = Query::Turn {
        task: "task".to_owned().try_into().unwrap(),
        turn: u64::from(u32::MAX),
    };
    assert!(
        std::panic::catch_unwind(|| query_inputs::check_query(
            &query,
            &serde_json::to_vec(&broken).unwrap()
        ))
        .is_err()
    );
}

#[test]
fn empty_plan_stems_are_rejected_and_valid_spaces_and_unicode_are_preserved() {
    query_inputs::exercise(b"");
    query_inputs::exercise("plan α".as_bytes());
    assert!(std::panic::catch_unwind(|| query_inputs::check_plan_stem("", true)).is_err());
}

#[tokio::test]
async fn prepared_change_keeps_views_readable_and_rejects_an_advanced_cut() {
    use futures::StreamExt as _;
    let volume = volume().await;
    let mut observation = volume.observe(Query::Root, None, 128).await.unwrap();
    let snapshot = observation.snapshot.take().unwrap().into_heads().unwrap();
    let mut views = FileViews::snapshot(snapshot, LIMITS).unwrap();
    put(&volume, &chunk(1, 1, Channel::Text, b"one")).await;
    let change = observation.changes.next().await.unwrap().unwrap();
    let apply = views.prepare(&change).unwrap();
    assert_eq!(views.files().count(), 0);
    assert_eq!(views.cursor().position + 1, change.cursor.position);
    let stale = views.prepare(&change).unwrap();
    apply(&mut views).unwrap();
    let committed = files(views.files());
    assert!(stale(&mut views).is_err());
    assert_eq!(files(views.files()), committed);
    assert_eq!(views.cursor(), &change.cursor);
}

#[tokio::test]
async fn opening_published_files_does_not_initialize_a_missing_volume() {
    use acyclic_stream::{StreamError, StreamProvider as _};
    let provider = Arc::new(MemoryStream::default());
    assert!(
        TaskVolume::open_existing(provider.clone(), path("missing/files"))
            .await
            .is_err()
    );
    assert!(matches!(
        provider.tail(path("missing/files")).await,
        Err(StreamError::NotFound)
    ));
    let created = TaskVolume::open(provider.clone(), path("agent/files"))
        .await
        .unwrap();
    put(&created, &Write::new(FileKey::Goal, b"goal".to_vec())).await;
    let opened = TaskVolume::open_existing(provider, path("agent/files"))
        .await
        .unwrap();
    assert_eq!(
        opened.snapshot(&Query::Root, 128).await.unwrap().generation,
        created
            .snapshot(&Query::Root, 128)
            .await
            .unwrap()
            .generation,
    );
}
