#[path = "../../harness/tests/support/client_views.rs"]
mod client_views_fixture;
use acyclic_harness::{
    AgentId, Result,
    client::GoalFileView,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    goal::{GOAL_METADATA_HEADROOM, GoalState, GoalStatus, GoalView},
    resources::ProviderRef,
    task_volume::{Cursor, FileKey, FileValue, Generation, Query, SCHEMA, Snapshot},
};

const LIMIT: u64 = 8192;

fn file(bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fuzz".into(), "filesystem".into(), "1".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        "GOAL.md".into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, "text/markdown", &mut None)?,
        "GOAL.md".into(),
        &mut None,
    )
}

fn round_trip(view: &GoalView, reason: Option<&str>) -> Result<()> {
    for status in [GoalStatus::Active, GoalStatus::Blocked, GoalStatus::Done] {
        let bytes = view.status_contents(status, reason)?;
        assert!(bytes.len() as u64 <= LIMIT);
        let rewritten = GoalView::from_file(file(bytes.as_bytes())?, bytes.as_bytes(), LIMIT)?;
        assert_eq!(rewritten.status, status);
        assert_eq!(rewritten.markdown, view.markdown);
        assert_eq!(rewritten.reason.as_deref(), reason);
    }
    Ok(())
}

pub fn check(data: &[u8]) -> Result<()> {
    if data.len() as u64 > LIMIT {
        return Ok(());
    }
    let record = acyclic_stream::Record {
        sequence: 1,
        value: data.to_vec().into(),
        commit_id: acyclic_stream::CommitId::from_bytes([1; 32]),
        committed_at_micros: 0,
    };
    if let Ok(Some(file)) = FileValue::from_record(&record) {
        let json = serde_json::to_vec(&file).unwrap();
        assert_eq!(serde_json::from_slice::<FileValue>(&json).unwrap(), file);
    }
    let state = GoalState::from_bytes(data, LIMIT);
    let view = GoalView::from_file(file(data)?, data, LIMIT);
    if let Ok(state) = &state {
        assert_eq!(
            view.is_ok(),
            state.markdown.len() + GOAL_METADATA_HEADROOM <= LIMIT as usize
        );
    } else {
        assert!(view.is_err());
    }
    let query = Query::File(FileKey::Goal);
    let cursor = Cursor {
        volume: [1; 32],
        epoch: Default::default(),
        schema: SCHEMA,
        query: *blake3::hash(&serde_json::to_vec(&query).unwrap()).as_bytes(),
        position: 1,
        accepted: 1,
        durable: 1,
        commit: [2; 32],
    };
    let mut views = client_views_fixture::file_views();
    views.query_snapshot(
        query,
        Snapshot {
            generation: Generation {
                volume: cursor.volume,
                epoch: cursor.epoch,
                position: cursor.position,
                commit: cursor.commit,
            },
            files: vec![FileValue::new(FileKey::Goal, data.to_vec())],
            cursor: cursor.clone(),
        },
        1,
        LIMIT as usize,
    )?;
    match (views.goal(cursor.volume, cursor.query), &state) {
        (Some(GoalFileView::Present { goal }), Ok(state)) => {
            assert_eq!(goal, state);
            let encoded = serde_json::to_value(state).unwrap();
            assert_eq!(
                serde_json::from_value::<GoalState>(encoded).unwrap(),
                *state
            );
        }
        (Some(GoalFileView::Invalid { .. }), Err(_)) => {}
        _ => panic!("client goal differs from the shared parser"),
    }
    let request = Query::File(FileKey::RequestHead);
    let request_cursor = Cursor {
        volume: [3; 32],
        query: *blake3::hash(&serde_json::to_vec(&request).unwrap()).as_bytes(),
        ..cursor.clone()
    };
    views.query_snapshot(
        request,
        Snapshot {
            generation: Generation {
                volume: request_cursor.volume,
                epoch: request_cursor.epoch,
                position: request_cursor.position,
                commit: request_cursor.commit,
            },
            cursor: request_cursor.clone(),
            files: vec![FileValue::new(FileKey::RequestHead, data.to_vec())],
        },
        1,
        LIMIT as usize,
    )?;
    views.bind_goal_sources(
        (cursor.volume, cursor.query),
        (request_cursor.volume, request_cursor.query),
    )?;
    let combined = views.goal_view(cursor.volume, cursor.query).unwrap();
    assert_eq!(combined.request_cursor, Some(&request_cursor));
    if let Some(progress) = combined.progress {
        let raw: serde_json::Value = serde_json::from_slice(data).unwrap();
        assert_eq!(raw["automatic_turns"], progress.continuations);
        assert_eq!(
            raw["operation"],
            serde_json::to_value(progress.operation).unwrap()
        );
        assert_eq!(raw["root"], serde_json::to_value(progress.root).unwrap());
    }
    serde_json::to_value(combined).unwrap();
    if let Ok(path) = std::str::from_utf8(data) {
        if let Ok(key) = FileKey::parse(path) {
            assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
        }
    }
    if let Ok(view) = view {
        let state = state?;
        assert_eq!(state.status, view.status);
        assert_eq!(state.reason, view.reason);
        assert_eq!(state.markdown, view.markdown);
        round_trip(&view, view.reason.as_deref())?;
    }
    // Even malformed frontmatter exercises arbitrary Unicode reason encoding.
    if let Ok(text) = std::str::from_utf8(data) {
        let reason: String = text.chars().take(12).collect();
        let view = GoalView::from_file(file(b"Goal")?, b"Goal", LIMIT)?;
        round_trip(&view, Some(&reason))?;
    }
    Ok(())
}
