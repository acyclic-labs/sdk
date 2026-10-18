#[path = "../../harness/tests/support/client_views.rs"]
mod client_views_fixture;
pub use acyclic_fuzz_support as fuzz_support;
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
        let _ = fuzz_support::round_trip_by(
            &(status, view.markdown.clone(), reason.map(str::to_owned)),
            GoalView::from_file(file(bytes.as_bytes())?, bytes.as_bytes(), LIMIT),
            |rewritten| {
                Ok::<_, std::convert::Infallible>((
                    rewritten.status,
                    rewritten.markdown.clone(),
                    rewritten.reason.clone(),
                ))
            },
        );
    }
    Ok(())
}

pub fn check(fuzz_input: fuzz_support::FuzzInput<'_>) -> Result<()> {
    let data = fuzz_input.bytes();
    if data.len() as u64 > LIMIT {
        return Ok(());
    }
    if let Some(Some(file)) = fuzz_support::FuzzInput::parse(fuzz_input, |bytes| {
        let record = acyclic_stream::Record {
            sequence: 1,
            value: bytes.to_vec().into(),
            commit_id: acyclic_stream::CommitId::from_bytes([1; 32]),
            committed_at_micros: 0,
        };
        FileValue::from_record(&record)
    }) {
        let json = serde_json::to_vec(&file).unwrap();
        let _ = fuzz_support::round_trip(&(file), serde_json::from_slice::<FileValue>(&json));
    }
    let state = fuzz_support::FuzzInput::parse_bounded(fuzz_input, LIMIT, |data, limit| {
        GoalState::from_bytes(data, limit)
    });
    let expected_view = state
        .as_ref()
        .is_some_and(|state| state.markdown.len() + GOAL_METADATA_HEADROOM <= LIMIT as usize);
    let view = fuzz_support::accepts(expected_view, GoalView::from_file(file(data)?, data, LIMIT));
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
        (Some(GoalFileView::Present { goal }), Some(state)) => {
            assert_eq!(goal, state);
            let encoded = serde_json::to_value(state).unwrap();
            let _ = fuzz_support::round_trip(state, serde_json::from_value::<GoalState>(encoded));
        }
        (Some(GoalFileView::Invalid { .. }), None) => {}
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
        let raw: serde_json::Value = fuzz_support::must_decode(serde_json::from_slice(data));
        assert_eq!(raw["automatic_turns"], progress.continuations);
        assert_eq!(
            raw["operation"],
            serde_json::to_value(progress.operation).unwrap()
        );
        assert_eq!(raw["root"], serde_json::to_value(progress.root).unwrap());
    }
    serde_json::to_value(combined).unwrap();
    if let Some(key) = fuzz_support::FuzzInput::parse_text(fuzz_input, FileKey::parse) {
        let _ = fuzz_support::round_trip(&key, FileKey::parse(&key.path()));
    }
    if let Some(view) = view {
        let Some(state) = state else {
            unreachable!("validated view requires an admitted raw state")
        };
        assert_eq!(state.status, view.status);
        assert_eq!(state.reason, view.reason);
        assert_eq!(state.markdown, view.markdown);
        round_trip(&view, view.reason.as_deref())?;
    }
    // Even malformed frontmatter exercises arbitrary Unicode reason encoding.
    if let Some(text) = fuzz_input.text() {
        let reason: String = text.chars().take(12).collect();
        let view = GoalView::from_file(file(b"Goal")?, b"Goal", LIMIT)?;
        round_trip(&view, Some(&reason))?;
    }
    Ok(())
}
