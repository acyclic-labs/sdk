use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

async fn model<const BOUND: bool>(events: &[(u8, u8, bool)]) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("busy-model").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([12; 16]);
    let mut client = CommandSession::open(volume, session, 65536, 128)
        .await
        .unwrap();
    for (step, &(target, id, outcome)) in events.iter().enumerate() {
        let operation = OperationId::from_bytes([id + 1; 16]);
        let file = if outcome {
            files::TaskFile::ExecutionResult(component(operation.to_string()).unwrap())
        } else {
            files::TaskFile::Request(component(operation.to_string()).unwrap())
        };
        let key = match target {
            0 => files::FileKey::Session {
                session: component(session.to_string()).unwrap(),
                file,
            },
            3 => files::FileKey::Session {
                session: component(uuid::Uuid::from_bytes([13; 16]).to_string()).unwrap(),
                file,
            },
            _ => files::FileKey::Task {
                task: component(TaskId::from_bytes([target; 16]).to_string()).unwrap(),
                file,
            },
        };
        let value = if outcome {
            json!({"sequence":1+u64::from(target)*4+u64::from(id),"ok":{"text":"done"}})
        } else {
            serde_json::to_value(CommandResolution::Prompt {
                text: "hello".into(),
                source: None,
                mentions: vec![],
            })
            .unwrap()
        };
        client
            .fold(files::FileValue::new(
                key,
                serde_json::to_vec(&value).unwrap(),
            ))
            .unwrap();
        let selected = match step % 3 {
            0 => Value::Null,
            n => json!(TaskId::from_bytes([n as u8; 16])),
        };
        let layout = client
            .state_key(CommandFileRoot::Session, "layout.json")
            .unwrap();
        client
            .fold(files::FileValue::new(
                layout,
                serde_json::to_vec(&json!({"selected_task":selected})).unwrap(),
            ))
            .unwrap();
        client.take_request();
        if !BOUND {
            client.pending_session = 0;
            client.pending_tasks.clear();
        }
        assert_eq!(
            client.pending_execution().unwrap(),
            client.pending_execution_scan_control().unwrap(),
            "publication fold must retain exact busy state"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn pending_counts_equal_file_authority_under_replay_and_reordering(events in prop::collection::vec((0u8..4,0u8..4,any::<bool>()),0..32)) {
        futures::executor::block_on(model::<true>(&events));
    }
}
#[test]
#[should_panic(expected = "publication fold must retain exact busy state")]
fn missing_publication_binding_negative_control() {
    futures::executor::block_on(model::<false>(&[(0, 0, false)]));
}
