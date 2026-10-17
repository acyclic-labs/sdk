use super::goals::{change, cut, new_views, snapshot};
use acyclic_harness::{
    client::ClientViews,
    task_volume::{Change, FileKey, FileQuerySource, FileValue, Query},
};
use proptest::prelude::*;

#[tokio::test]
async fn request_head_follows_creation_replacement_removal_and_recreation()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = super::Fixture::new().await?;
    let query = Query::File(FileKey::RequestHead);
    let observation = fixture
        .source
        .observe_files(query.clone(), None, 1, 4096)
        .await?;
    let mut changes = observation.changes;
    let mut view =
        super::FileQueryView::from_snapshot(query, observation.snapshot.unwrap(), 1, 4096)?;
    for value in [Some(head(0)), Some(head(2)), None, Some(head(7))] {
        let file = match &value {
            Some(bytes) => super::WorkspaceMutation::PutFile {
                path: "/inputs/current.json".into(),
                bytes: bytes.clone().into(),
            },
            None => super::WorkspaceMutation::Remove {
                path: "/inputs/current.json".into(),
            },
        };
        fixture
            .host
            .apply(
                &fixture.workspace,
                None,
                &[
                    super::WorkspaceMutation::CreateDirectory {
                        path: "/inputs".into(),
                    },
                    file,
                ],
                &super::IdempotencyKey::new(uuid::Uuid::new_v4().to_string())?,
            )
            .await?;
        view.apply(super::next(&mut changes).await)?;
        assert_eq!(
            view.files()
                .get(&FileKey::RequestHead)
                .map(|file| file.segments[0].as_ref()),
            value.as_deref()
        );
    }
    Ok(())
}

fn copy(change: &Change) -> Change {
    Change {
        cursor: change.cursor.clone(),
        file: change.file.clone(),
        files: change.files.clone(),
        removed: change.removed.clone(),
    }
}

fn head(count: u32) -> Vec<u8> {
    let mut value: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../../harness-fuzz/corpus/records/turn"))
            .unwrap();
    value["automatic_turns"] = count.into();
    serde_json::to_vec(&value).unwrap()
}

fn initial() -> ClientViews {
    let mut views = new_views();
    let goal = Query::File(FileKey::Goal);
    let request = Query::File(FileKey::RequestHead);
    let goal_cut = cut(&goal, [1; 32], 1);
    let request_cut = cut(&request, [2; 32], 1);
    views
        .query_snapshot(goal, snapshot(goal_cut.clone(), &None), 1, 4096)
        .unwrap();
    views
        .query_snapshot(request, snapshot(request_cut.clone(), &None), 1, 4096)
        .unwrap();
    views
        .bind_goal_sources(
            (goal_cut.volume, goal_cut.query),
            (request_cut.volume, request_cut.query),
        )
        .unwrap();
    views
}

fn updates(key: FileKey, volume: [u8; 32], values: Vec<Option<Vec<u8>>>) -> Vec<Change> {
    let query = Query::File(key.clone());
    let mut present = false;
    values
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| {
            let removed = if bytes.is_none() && present {
                vec![key.clone()]
            } else {
                Vec::new()
            };
            present = bytes.is_some();
            change(
                cut(&query, volume, index as u64 + 2),
                bytes.map(|bytes| FileValue::new(key.clone(), bytes)),
                removed,
            )
        })
        .collect()
}

// Enumerate every merge that preserves each source's publication order.
fn interleave(views: ClientViews, goal: &[Change], head: &[Change], expected: &serde_json::Value) {
    if goal.is_empty() && head.is_empty() {
        assert_eq!(&serde_json::to_value(views).unwrap(), expected);
        return;
    }
    for (updates, other, is_goal) in [(goal, head, true), (head, goal, false)] {
        if let Some((first, rest)) = updates.split_first() {
            let mut next = views.clone();
            next.query_change(copy(first)).unwrap();
            assert!(!next.query_change(copy(first)).unwrap());
            let view = next
                .goal_view([1; 32], cut(&Query::File(FileKey::Goal), [1; 32], 1).query)
                .unwrap();
            if is_goal {
                assert_eq!(view.cursor, &first.cursor);
                interleave(next, rest, other, expected);
            } else {
                assert_eq!(view.request_cursor, Some(&first.cursor));
                interleave(next, other, rest, expected);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn all_source_interleavings_equal_publication_order(
        goals in prop::collection::vec((0u8..4, prop::option::of(prop::collection::vec(any::<char>(), 0..24))), 1..4),
        heads in prop::collection::vec((0u8..3, any::<u32>()), 1..4),
    ) {
        let goal = updates(FileKey::Goal, [1; 32], goals.into_iter().map(|(kind, chars)| chars.map(|chars| {
            let body: String = chars.into_iter().collect();
            match kind {
                0 => body,
                1 => format!("---\nstatus: done\n---\n{body}"),
                2 => format!("---\nstatus: blocked\nreason: waiting\n---\n{body}"),
                _ => format!("---\nstatus: unknown\n---\n{body}"),
            }.into_bytes()
        })).collect());
        let head = updates(FileKey::RequestHead, [2; 32], heads.into_iter().map(|(kind, count)| match kind {
            0 => None, 1 => Some(b"invalid".to_vec()), _ => Some(head(count)),
        }).collect());
        let mut ordered = initial();
        for change in goal.iter().chain(&head) { ordered.query_change(copy(change)).unwrap(); }
        interleave(initial(), &goal, &head, &serde_json::to_value(ordered).unwrap());
    }
}

#[test]
fn count_requires_the_request_source_and_survives_goal_edits() {
    let mut views = initial();
    let goal = updates(
        FileKey::Goal,
        [1; 32],
        vec![Some(b"First".to_vec()), Some(b"Edited".to_vec())],
    );
    let query = goal[0].cursor.query;
    views.query_change(copy(&goal[0])).unwrap();
    assert!(views.goal_view([1; 32], query).unwrap().progress.is_none());
    views
        .query_change(updates(FileKey::RequestHead, [2; 32], vec![Some(head(u32::MAX))]).remove(0))
        .unwrap();
    views.query_change(copy(&goal[1])).unwrap();
    assert_eq!(
        views
            .goal_view([1; 32], query)
            .unwrap()
            .progress
            .unwrap()
            .continuations,
        u32::MAX
    );
    assert!(
        views
            .bind_goal_sources(([1; 32], query), ([1; 32], query))
            .is_err()
    );
    assert_eq!(
        FileKey::parse("inputs/current.json").unwrap(),
        FileKey::RequestHead
    );
    assert!(FileKey::parse("inputs/../current.json").is_err());
}

#[test]
fn request_head_reset_replaces_count_and_replay_cannot_increment_it() {
    let mut views = initial();
    let request = Query::File(FileKey::RequestHead);
    let head_cut = cut(&request, [2; 32], 10);
    let goal_cut = cut(&Query::File(FileKey::Goal), [1; 32], 1);
    let mut reset = snapshot(head_cut.clone(), &None);
    reset
        .files
        .push(FileValue::new(FileKey::RequestHead, head(7)));
    views
        .query_snapshot(request.clone(), reset, 1, 4096)
        .unwrap();
    let replay = change(
        head_cut,
        Some(FileValue::new(FileKey::RequestHead, head(7))),
        vec![],
    );
    assert!(!views.query_change(replay).unwrap());
    let view = views.goal_view(goal_cut.volume, goal_cut.query).unwrap();
    assert_eq!(view.progress.unwrap().continuations, 7);
    assert_eq!(view.cursor, &goal_cut);
    assert_eq!(view.request_cursor.unwrap().position, 10);
    assert!(
        views
            .query_change(change(
                cut(&request, [2; 32], 2),
                Some(FileValue::new(FileKey::RequestHead, head(99))),
                vec![]
            ))
            .is_err()
    );
    assert_eq!(
        views
            .goal_view(goal_cut.volume, goal_cut.query)
            .unwrap()
            .progress
            .unwrap()
            .continuations,
        7
    );
    views
        .query_change(change(
            cut(&request, [2; 32], 11),
            Some(FileValue::new(FileKey::RequestHead, b"invalid".to_vec())),
            vec![],
        ))
        .unwrap();
    assert!(
        views
            .goal_view(goal_cut.volume, goal_cut.query)
            .unwrap()
            .progress
            .is_none()
    );
    views
        .query_snapshot(
            request,
            snapshot(cut(&Query::File(FileKey::RequestHead), [2; 32], 12), &None),
            1,
            4096,
        )
        .unwrap();
    assert!(
        views
            .goal_view(goal_cut.volume, goal_cut.query)
            .unwrap()
            .progress
            .is_none()
    );
}
