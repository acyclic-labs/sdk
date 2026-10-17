#![no_main]

#[path = "../../harness/tests/support/client_views.rs"]
mod client_views_fixture;
use acyclic_harness::{
    AgentId, Result,
    client::{ClientSession, HarnessClient, MemoryClientState},
    conversation::{
        Attachment, BoundedContentFuture, ContentResidencyVerifier, ConversationMessage,
        FileDescriptor, FileRef, MessageKind, ReferencedAttachments, VolumeClass, VolumeOwner,
        VolumeRef,
    },
    core::EventPayload,
    filesystem::PreparationIo,
    resources::ProviderRef,
    wire,
    wire_api::{HarnessWireApi, OperationControlRequest, ReplayMode, current_protocol, negotiate},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, OwnedBytes};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

struct Files(OwnedBytes);
impl ContentResidencyVerifier for Files {
    fn read<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        BoundedContentFuture::new(io, || async { Ok(self.0.clone()) })
    }
}

fuzz_target!(|input: &[u8]| {
    if input.is_empty() || input.len() > 4096 {
        return;
    }
    model_file_view(&input[1..]);
    futures::executor::block_on(async {
        let (selector, bytes) = input.split_first().unwrap();
        let files = Files(bytes.to_vec().into());
        let reference = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("fuzz".into(), "memory".into(), "1".into(), &mut None).unwrap(),
                "task".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )
            .unwrap(),
            "artifact.json".into(),
            "v1".into(),
            FileDescriptor::from_bytes(bytes, "application/json", &mut None).unwrap(),
            "artifact.json".into(),
            &mut None,
        )
        .unwrap();
        let mut message = ConversationMessage {
            id: acyclic_harness::conversation::assistant_message_id(
                acyclic_harness::OperationId::from_bytes([1; 16]),
            ),
            sequence: 1,
            kind: match selector % 3 {
                0 => MessageKind::ToolCall,
                1 => MessageKind::ToolResult,
                _ => MessageKind::Assistant,
            },
            content: reference.clone(),
            attachments: ReferencedAttachments::Inline { items: vec![] },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        match message.kind {
            MessageKind::ToolCall => message.tool_call_id = Some("call".into()),
            MessageKind::ToolResult => {
                message.tool_call_id = Some("call".into());
                message.attachments = ReferencedAttachments::Inline {
                    items: vec![Attachment {
                        label: Some("model_projection".into()),
                        file: reference,
                    }],
                };
            }
            _ => {
                message
                    .extensions
                    .insert("acyclic.model.metadata".into(), reference);
            }
        }
        let authority = wire::Authority {
            kind: wire::AggregateKind::Conversation as i32,
            id: "task".into(),
        };
        let delivery = wire::Delivery {
            authority: Some(authority.clone()),
            generation: "g".into(),
            from_revision: 0,
            through_revision: 1,
            live: true,
            events: vec![wire::EventEnvelope {
                protocol: Some(current_protocol()),
                authority: Some(authority.clone()),
                revision: 1,
                operation_id: "00000000-0000-0000-0000-000000000001".into(),
                event_type: "conversation_message_appended".into(),
                canonical_payload_json: serde_json::to_vec(
                    &EventPayload::ConversationMessageAppended {
                        message: Box::new(message),
                    },
                )
                .unwrap(),
                ..Default::default()
            }],
        };
        let mut session = ClientSession::open(MemoryClientState::default()).unwrap();
        session
            .track(wire::ReplayCursor {
                authority: Some(authority.clone()),
                generation: "g".into(),
                revision: 0,
            })
            .unwrap();
        let host = Arc::new(Host(delivery));
        let files = Arc::new(files);
        let mut client = HarnessClient::connect(host.clone(), session)
            .await
            .unwrap()
            .with_files(files.clone());
        client.replay().await.unwrap();
        if client.next_delivery().await.is_ok() {
            let expected = client.session().replay_revision(&authority);
            let reopened = ClientSession::open(client.into_session().into_store()).unwrap();
            let mut reopened = HarnessClient::connect(host, reopened)
                .await
                .unwrap()
                .with_files(files);
            reopened.replay().await.unwrap();
            reopened.next_delivery().await.unwrap();
            assert_eq!(reopened.session().replay_revision(&authority), expected);
        } else {
            assert_eq!(client.session().resume().cursors[0].revision, 0);
            assert_eq!(client.session().replay_revision(&authority), 0);
        }
    });
});

struct Host(wire::Delivery);
impl HarnessWireApi for Host {
    fn handshake(
        &self,
        request: wire::HandshakeRequest,
    ) -> BoxProviderFuture<'_, Result<wire::HandshakeResponse>> {
        Box::pin(async move { negotiate(&request, &wire::CapabilitySet::default()) })
    }
    fn replay(
        &self,
        _: wire::ResumeRequest,
        _: ReplayMode,
    ) -> BoxProviderFuture<'_, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        let page = self.0.clone();
        Box::pin(async move {
            Ok(Box::pin(futures::stream::iter([Ok(page)]))
                as BoxProviderStream<'static, Result<wire::Delivery>>)
        })
    }
    fn acknowledge(&self, _: wire::Acknowledge) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn submit(&self, _: wire::CommandEnvelope) -> BoxProviderFuture<'_, Result<wire::Admission>> {
        panic!("replay must not submit")
    }
    fn authorize_operation_control<'a>(
        &'a self,
        _: &'a OperationControlRequest,
    ) -> BoxProviderFuture<'a, Result<()>> {
        panic!("replay must not authorize operation control")
    }
    fn observe(
        &self,
        _: wire::ObserveRequest,
    ) -> BoxProviderFuture<'_, Result<wire::OperationStatus>> {
        panic!("replay must not observe operations")
    }
    fn watch_operation(
        &self,
        _: wire::ObserveRequest,
    ) -> BoxProviderFuture<'_, Result<BoxProviderStream<'static, Result<wire::OperationStatus>>>>
    {
        panic!("replay must not watch operations")
    }
    fn cancel(
        &self,
        _: wire::CancelRequest,
    ) -> BoxProviderFuture<'_, Result<wire::CancelResponse>> {
        panic!("replay must not cancel")
    }
}

fn model_file_view(input: &[u8]) {
    use acyclic_harness::task_volume::{
        Change, Component, Cursor, FileKey, FileValue, Generation, Query, SCHEMA, Snapshot,
        TaskFile,
    };
    let key = FileKey::Task {
        task: Component::new("task").unwrap(),
        file: TaskFile::TurnRequest(0),
    };
    let mut cursor = Cursor {
        volume: [1; 32],
        epoch: uuid::Uuid::from_bytes([1; 16]),
        schema: SCHEMA,
        query: *blake3::hash(&serde_json::to_vec(&Query::Root).unwrap()).as_bytes(),
        position: 1,
        accepted: 1,
        durable: 1,
        commit: [1; 32],
    };
    let mut views = client_views_fixture::file_views();
    views
        .query_snapshot(
            Query::Root,
            Snapshot {
                generation: Generation {
                    volume: cursor.volume,
                    epoch: cursor.epoch,
                    position: cursor.position,
                    commit: cursor.commit,
                },
                cursor: cursor.clone(),
                files: vec![FileValue::new(key.clone(), input.to_vec())],
            },
            1,
            4096,
        )
        .unwrap();
    let observed = serde_json::to_value(&views).unwrap()["models"].clone();
    assert_eq!(observed.as_array().unwrap().len(), 1);
    let decoded = serde_json::from_slice::<serde_json::Value>(input);
    if decoded.is_err() {
        assert_eq!(
            observed[0]["model"]["kind"], "invalid",
            "malformed skipped JSON must invalidate the identity"
        );
    }
    if observed[0]["model"]["kind"] == "selected" {
        if let Ok(request) = decoded {
            for field in ["provider", "name", "revision"] {
                assert_eq!(observed[0]["model"][field], request["model"][field]);
            }
        }
        let encoded =
            serde_json::to_vec(&serde_json::json!({"model":observed[0]["model"]})).unwrap();
        let mut round_trip = client_views_fixture::file_views();
        round_trip
            .query_snapshot(
                Query::Root,
                Snapshot {
                    generation: Generation {
                        volume: cursor.volume,
                        epoch: cursor.epoch,
                        position: cursor.position,
                        commit: cursor.commit,
                    },
                    cursor: cursor.clone(),
                    files: vec![FileValue::new(key.clone(), encoded)],
                },
                1,
                4096,
            )
            .unwrap();
        assert_eq!(
            views.models().next().unwrap().model,
            round_trip.models().next().unwrap().model
        );
        assert!(observed[0]["model"].get("options").is_none());
    }
    cursor.position = 2;
    cursor.accepted = 2;
    cursor.durable = 2;
    cursor.commit = [2; 32];
    views
        .query_change(Change {
            cursor: cursor.clone(),
            file: None,
            files: vec![],
            removed: vec![key.clone()],
        })
        .unwrap();
    assert_eq!(views.models().count(), 0);
    if input.len() > 512 {
        let bounded_key = FileKey::Task {
            task: Component::new("task").unwrap(),
            file: TaskFile::TurnRequest(1),
        };
        let valid = br#"{"model":{"provider":"openai","name":"x","revision":"1"}}"#;
        cursor.position = 3;
        cursor.accepted = 3;
        cursor.durable = 3;
        cursor.commit = [3; 32];
        let mut bounded = client_views_fixture::file_views();
        bounded
            .query_snapshot(
                Query::Root,
                Snapshot {
                    generation: Generation {
                        volume: cursor.volume,
                        epoch: cursor.epoch,
                        position: cursor.position,
                        commit: cursor.commit,
                    },
                    cursor: cursor.clone(),
                    files: vec![
                        FileValue::new(key.clone(), input.to_vec()),
                        FileValue::new(bounded_key.clone(), valid.to_vec()),
                    ],
                },
                2,
                256,
            )
            .unwrap();
        assert_eq!(bounded.models().count(), 2);
        assert_eq!(
            serde_json::to_value(bounded.models().find(|row| row.turn == 0).unwrap().model)
                .unwrap()["kind"],
            "invalid"
        );
        cursor.position = 4;
        cursor.accepted = 4;
        cursor.durable = 4;
        cursor.commit = [4; 32];
        bounded
            .query_change(Change {
                cursor,
                file: Some(FileValue::new(bounded_key, valid.to_vec())),
                files: vec![],
                removed: vec![key],
            })
            .unwrap();
        assert_eq!(bounded.models().count(), 1);
    }
}
