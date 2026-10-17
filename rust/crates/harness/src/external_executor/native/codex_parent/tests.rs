use super::*;
use proptest::prelude::*;
use serde_json::json;
use std::io::Cursor;

fn input(session: Uuid, child: Uuid) -> Value {
    json!({"session_id":session,"agent_id":child})
}

fn record(session: Uuid, child: Uuid, parent: Uuid) -> Value {
    json!({"type":"session_meta","payload":{
        "session_id":session,"id":child,"parent_thread_id":parent,
        "source":{"subagent":{"thread_spawn":{"parent_thread_id":parent}}},
        "base_instructions":{"text":"ignored native instructions"}
    }})
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A child transcript always selects its immediate parent; root children
    // retain the root attachment. The original root and child IDs never change.
    #[test]
    fn ancestry_round_trips_and_selects_only_the_native_parent(
        ids in prop::collection::vec(any::<u128>(), 3),
        root_parent in any::<bool>(),
        parent_file in any::<bool>(),
    ) {
        prop_assume!(ids[0] != ids[1] && ids[0] != ids[2] && ids[1] != ids[2]);
        let ids: Vec<_> = ids.into_iter().map(Uuid::from_u128).collect();
        let (session, child) = (ids[0], ids[1]);
        let parent = if root_parent {session} else {ids[2]};
        let record = if parent_file {record(session,parent,session)} else {record(session,child,parent)};
        let encoded = serde_json::to_vec(&record)?;
        let metadata = decode(encoded.as_slice())?;
        let mut hook = input(session,child);
        bind(&mut hook,metadata,!parent_file)?;
        prop_assert_eq!(&hook["parent_agent_id"],&json!((!root_parent).then(||parent.to_string())));
        prop_assert_eq!(&hook["session_id"],&json!(session));
        prop_assert_eq!(&hook["agent_id"],&json!(child));
        let normalized = crate::external_executor::request(
            crate::external_executor::ExternalHost::Codex,"SubagentStart",hook.clone())?;
        prop_assert_eq!(normalized.caller,(!root_parent).then(||parent.to_string()));
        let replay = serde_json::to_vec(&json!([input(session,child),record,!parent_file]))?;
        fuzz(&replay);
    }
}

#[test]
fn only_the_first_metadata_record_is_read() -> Result<()> {
    let (session, child, parent) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
    let header = serde_json::to_vec(&record(session, child, parent)).unwrap();
    let mut transcript = header.clone();
    transcript.extend_from_slice(b"\nnot a valid later record");
    transcript.resize(header.len() + 1_000_000, b'!');
    let mut cursor = Cursor::new(transcript);
    let metadata = decode(&mut cursor)?;
    assert_eq!(metadata.id, child);
    assert!(cursor.position() <= header.len() as u64 + 1);
    Ok(())
}

#[test]
fn malformed_or_duplicate_metadata_cannot_select_a_parent() {
    for bytes in [
        b"{\"type\":\"event_msg\",\"payload\":{}}".as_slice(),
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"invalid\"}}",
        br#"{"type":"session_meta","payload":{"session_id":"00000000-0000-0000-0000-000000000001","id":"00000000-0000-0000-0000-000000000003","parent_thread_id":"00000000-0000-0000-0000-000000000002","parent_thread_id":"00000000-0000-0000-0000-000000000004"}}"#,
        b"{\"type\":\"session_meta\",\"payload\":null}",
    ] {
        assert!(decode(bytes).is_err());
    }
}

#[test]
fn mismatched_or_cyclic_ancestry_never_changes_the_hook() {
    let (session, child, parent) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
    for metadata in [
        Metadata {
            session_id: parent,
            id: child,
            parent_thread_id: Some(parent),
        },
        Metadata {
            session_id: session,
            id: parent,
            parent_thread_id: Some(session),
        },
        Metadata {
            session_id: session,
            id: child,
            parent_thread_id: Some(child),
        },
        Metadata {
            session_id: session,
            id: child,
            parent_thread_id: None,
        },
    ] {
        let mut hook = input(session, child);
        let original = hook.clone();
        assert!(bind(&mut hook, metadata, true).is_err());
        assert_eq!(hook, original);
    }
    for extra in [
        json!({"parent_agent_id":session}),
        json!({"parentAgentId":session}),
        json!({"parent_agent_id":parent,"parentAgentId":parent}),
    ] {
        let mut hook = input(session, child);
        hook.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let original = hook.clone();
        let metadata = Metadata {
            session_id: session,
            id: child,
            parent_thread_id: Some(parent),
        };
        assert!(bind(&mut hook, metadata, true).is_err());
        assert_eq!(hook, original);
    }
}

#[test]
fn stop_uses_the_child_transcript_and_can_use_the_parent_transcript() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let (session, child, parent) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
    let child_path = directory.path().join("child.jsonl");
    let parent_path = directory.path().join("parent.jsonl");
    std::fs::write(
        &child_path,
        serde_json::to_vec(&record(session, child, parent)).unwrap(),
    )
    .unwrap();
    std::fs::write(
        &parent_path,
        serde_json::to_vec(&record(session, parent, session)).unwrap(),
    )
    .unwrap();
    for child_file in [true, false] {
        let mut hook = input(session, child);
        hook["transcript_path"] = json!(parent_path);
        if child_file {
            hook["agent_transcript_path"] = json!(child_path);
        }
        prepare("SubagentStop", &mut hook)?;
        assert_eq!(hook["parent_agent_id"], json!(parent));
    }
    Ok(())
}
