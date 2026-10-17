use super::*;
use acyclic_harness::{
    client::{ClientViews, GoalFileView},
    goal::{GoalState, GoalStatus},
    task_volume::{Cursor, FileKey, FileValue, Generation, SCHEMA, Snapshot},
};
use proptest::prelude::*;

#[tokio::test]
async fn goal_state_follows_ordinary_file_publications_and_exports()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await?;
    let query = Query::File(FileKey::Goal);
    let observation = fixture
        .source
        .observe_files(query.clone(), None, 1, 4096)
        .await?;
    let snapshot = observation.snapshot.ok_or("snapshot missing")?;
    let identity = (snapshot.cursor.volume, snapshot.cursor.query);
    let mut changes = observation.changes;
    let mut views = new_views();
    assert!(views.goal(identity.0, identity.1).is_none());
    views.query_snapshot(query.clone(), snapshot, 1, 4096)?;
    assert_eq!(
        views.goal(identity.0, identity.1),
        Some(&GoalFileView::Missing)
    );

    for (text, status, reason, body) in [
        ("Ship it", GoalStatus::Active, None, "Ship it"),
        (
            "---\nstatus: blocked\nreason: waiting\n---\nShip it",
            GoalStatus::Blocked,
            Some("waiting"),
            "Ship it",
        ),
        (
            "---\r\nstatus: done\r\nreason: shipped\r\n---\r\n済み",
            GoalStatus::Done,
            Some("shipped"),
            "済み",
        ),
    ] {
        fixture.write(&[("GOAL.md", Some(text))]).await?;
        assert!(views.query_change(next(&mut changes).await)?);
        let expected = GoalFileView::Present {
            goal: GoalState {
                status,
                reason: reason.map(str::to_owned),
                markdown: body.into(),
            },
        };
        assert_eq!(views.goal(identity.0, identity.1), Some(&expected));
        let exported = serde_json::to_value(&views)?;
        assert_eq!(
            exported["goals"][0]["goal"],
            serde_json::to_value(&expected)?
        );
        assert_eq!(
            exported["goals"][0]["cursor"]["volume"],
            serde_json::json!(identity.0)
        );
        assert_eq!(
            exported["goals"][0]["cursor"]["query"],
            serde_json::json!(identity.1)
        );
    }
    // A malformed publication replaces the old status and keeps the file observable.
    fixture
        .write(&[("GOAL.md", Some("---\nstatus: unknown\n---\nBad"))])
        .await?;
    assert!(views.query_change(next(&mut changes).await)?);
    assert!(matches!(
        views.goal(identity.0, identity.1),
        Some(GoalFileView::Invalid { .. })
    ));
    fixture.write(&[("GOAL.md", None)]).await?;
    assert!(views.query_change(next(&mut changes).await)?);
    assert_eq!(
        views.goal(identity.0, identity.1),
        Some(&GoalFileView::Missing)
    );

    fixture.write(&[("GOAL.md", Some("Restart"))]).await?;
    let reset = fixture
        .source
        .observe_files(query.clone(), None, 1, 4096)
        .await?;
    views.query_snapshot(query, reset.snapshot.ok_or("reset missing")?, 1, 4096)?;
    assert!(
        matches!(views.goal(identity.0, identity.1), Some(GoalFileView::Present { goal }) if goal.markdown == "Restart")
    );
    Ok(())
}

pub(super) fn cut(query: &Query, volume: [u8; 32], position: u64) -> Cursor {
    Cursor {
        volume,
        epoch: uuid::Uuid::from_bytes([1; 16]),
        schema: SCHEMA,
        query: *blake3::hash(&serde_json::to_vec(query).unwrap()).as_bytes(),
        position,
        accepted: position,
        durable: position,
        commit: *blake3::hash(&position.to_le_bytes()).as_bytes(),
    }
}

pub(super) fn snapshot(cursor: Cursor, bytes: &Option<Vec<u8>>) -> Snapshot {
    Snapshot {
        files: bytes
            .as_ref()
            .map(|bytes| FileValue::new(FileKey::Goal, bytes.clone()))
            .into_iter()
            .collect(),
        generation: Generation {
            volume: cursor.volume,
            epoch: cursor.epoch,
            position: cursor.position,
            commit: cursor.commit,
        },
        cursor,
    }
}

pub(super) fn change(cursor: Cursor, file: Option<FileValue>, removed: Vec<FileKey>) -> Change {
    Change {
        cursor,
        file,
        removed,
        files: Vec::new(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: every incremental state equals a fresh fold of the latest bytes.
    // Replay and unrelated records preserve it; rejected gaps cannot mutate it.
    #[test]
    fn goal_fold_matches_snapshot_across_edits_removals_and_replay(
        volume in any::<[u8; 32]>(),
        root in any::<bool>(),
        actions in prop::collection::vec((0u8..7, prop::collection::vec(any::<char>(), 0..32), prop::collection::vec(any::<u8>(), 0..64)), 1..32),
    ) {
        let query = if root { Query::Root } else { Query::File(FileKey::Goal) };
        let mut bytes = None;
        let mut cursor = cut(&query, volume, 1);
        let mut view = FileQueryView::from_snapshot(query.clone(), snapshot(cursor.clone(), &bytes), 1, 4096)?;
        for (kind, chars, raw) in actions {
            cursor = cut(&query, volume, cursor.position + 1);
            let body: String = chars.into_iter().collect();
            let text = match kind {
                0 => Some(format!("G{body}")),
                1 => Some(format!("---\nstatus: done\n---\n{body}")),
                2 => Some(format!("---\nstatus: blocked\nreason: waiting\n---\n{body}")),
                3 => Some(format!("---\nstatus: unknown\n---\n{body}")),
                _ => None,
            };
            let file = if kind <= 4 {
                bytes = Some(text.map(String::into_bytes).unwrap_or(raw));
                bytes.as_ref().map(|bytes| FileValue::new(FileKey::Goal, bytes.clone()))
            } else { None };
            let removed = if kind == 5 && bytes.take().is_some() { vec![FileKey::Goal] } else { Vec::new() };
            let before = view.clone();
            let mut gap = cursor.clone();
            gap.position += 1;
            gap.accepted += 1;
            gap.durable += 1;
            prop_assert!(view.apply(change(gap, file.clone(), removed.clone())).is_err());
            prop_assert_eq!(&view, &before);
            view.apply(change(cursor.clone(), file.clone(), removed.clone()))?;
            prop_assert!(!view.apply(change(cursor.clone(), file, removed))?);
            let fresh = FileQueryView::from_snapshot(query.clone(), snapshot(cursor.clone(), &bytes), 1, 4096)?;
            prop_assert_eq!(&view, &fresh);
            let mut views = new_views();
            views.query_snapshot(query.clone(), snapshot(cursor.clone(), &bytes), 1, 4096)?;
            prop_assert_eq!(views.goal(volume, cursor.query), Some(view.goal()));
            let exported = serde_json::to_value(&views)?;
            prop_assert_eq!(&exported["goals"][0]["goal"], &serde_json::to_value(view.goal())?);
        }
    }
}

#[test]
fn unrelated_queries_do_not_export_a_missing_goal() -> Result<()> {
    let query = Query::Plans;
    let cursor = cut(&query, [2; 32], 1);
    let mut views = new_views();
    views.query_snapshot(query, snapshot(cursor.clone(), &None), 1, 4096)?;
    assert!(views.goal(cursor.volume, cursor.query).is_none());
    assert!(
        serde_json::to_value(&views).unwrap()["goals"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}

#[test]
fn goals_remain_scoped_to_the_observed_volume_and_query() -> Result<()> {
    let mut views = new_views();
    for (volume, query, text) in [
        ([1; 32], Query::Root, "first"),
        ([1; 32], Query::File(FileKey::Goal), "second"),
        ([2; 32], Query::File(FileKey::Goal), "third"),
    ] {
        let cursor = cut(&query, volume, 1);
        views.query_snapshot(
            query,
            snapshot(cursor.clone(), &Some(text.as_bytes().to_vec())),
            1,
            4096,
        )?;
        assert!(
            matches!(views.goal(volume, cursor.query), Some(GoalFileView::Present { goal }) if goal.markdown == text)
        );
    }
    let exported = serde_json::to_value(&views).unwrap();
    let mut bodies: Vec<_> = exported["goals"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["goal"]["goal"]["markdown"].as_str().unwrap())
        .collect();
    bodies.sort_unstable();
    assert_eq!(bodies, ["first", "second", "third"]);
    Ok(())
}

#[test]
fn goal_read_bounds_do_not_reserve_a_future_write() -> Result<()> {
    let query = Query::File(FileKey::Goal);
    let cursor = cut(&query, [3; 32], 1);
    let body = "Read only";
    let mut views = new_views();
    views.query_snapshot(
        query,
        snapshot(cursor.clone(), &Some(body.as_bytes().to_vec())),
        1,
        body.len(),
    )?;
    assert!(
        matches!(views.goal(cursor.volume, cursor.query), Some(GoalFileView::Present { goal }) if goal.markdown == body)
    );
    assert!(GoalState::from_bytes(body.as_bytes(), (body.len() - 1) as u64).is_err());
    Ok(())
}

pub(super) fn new_views() -> ClientViews {
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
