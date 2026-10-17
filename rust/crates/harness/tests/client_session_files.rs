//! Recovery folds the writer's immutable files, independent of live chunk timing.
use super::*;
use acyclic_harness::{
    OperationId,
    conversation::{ConversationMessage, FileRef, MessageKind, ReferencedAttachments},
    tool::ToolInvocation,
};
use serde_json::json;

async fn json_file(
    fixture: &client_files::Fixture,
    path: &str,
    value: &impl serde::Serialize,
) -> FileRef {
    fixture
        .put_file(
            path,
            &serde_json::to_vec(value).unwrap(),
            "application/json",
            path,
        )
        .await
        .unwrap()
}

fn message(sequence: u64, kind: MessageKind, content: FileRef) -> ConversationMessage {
    ConversationMessage {
        id: uuid::Uuid::from_u128(u128::from(sequence)),
        sequence,
        kind,
        content,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    }
}

fn page(messages: Vec<ConversationMessage>) -> wire::Delivery {
    let mut page = delivery(0, LifecycleState::Active);
    page.through_revision = messages.len() as u64;
    page.events = messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| wire::EventEnvelope {
            revision: index as u64 + 1,
            event_type: "conversation_message_appended".into(),
            canonical_payload_json: serde_json::to_vec(
                &EventPayload::ConversationMessageAppended {
                    message: Box::new(message),
                },
            )
            .unwrap(),
            ..page.events[0].clone()
        })
        .collect();
    page
}

async fn connect(
    state: ClientSession<MemoryClientState>,
    page: wire::Delivery,
    fixture: &client_files::Fixture,
) -> HarnessClient<MemoryClientState> {
    let host = Arc::new(Host::default());
    *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::iter([Ok(page)])));
    let mut client = HarnessClient::connect(host, state)
        .await
        .unwrap()
        .with_files(fixture.reader.clone());
    client.replay().await.unwrap();
    client
}

#[tokio::test]
async fn missing_file_authority_never_commits_or_acknowledges_a_partial_view() {
    let fixture = client_files::Fixture::new(&[]).await.unwrap();
    let turn = OperationId::from_bytes(7u128.to_be_bytes());
    let invocation = ToolInvocation {
        operation_id: turn,
        call_id: "call".into(),
        name: "bash".into(),
        arguments: json!({}),
    };
    let mut call = message(
        1,
        MessageKind::ToolCall,
        json_file(
            &fixture,
            "call.json",
            &json!({"invocation":&invocation,"metadata":null}),
        )
        .await,
    );
    call.tool_call_id = Some("call".into());
    let page = page(vec![call]);
    let host = Arc::new(Host::default());
    *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::iter([Ok(page.clone())])));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    client.replay().await.unwrap();
    assert!(matches!(
        client.next_delivery().await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(client.session().resume().cursors[0].revision, 0);
    assert!(host.acknowledgements.lock().unwrap().is_empty());
    assert_eq!(client.session().replay_revision(&owner()), 0);
    // Recovery can bind the same platform reader and retry the unchanged cursor.
    let mut client = connect(client.into_session(), page, &fixture).await;
    client.next_delivery().await.unwrap().unwrap();
    assert_eq!(client.session().replay_revision(&owner()), 1);
}

#[tokio::test]
async fn invalid_tool_files_never_reach_jsonl_or_the_durable_cursor() {
    for bytes in [
        b"{".as_slice(),
        br#"{"call_id":"other","name":"bash","arguments":{}}"#,
        br#"{"call_id":"call","arguments":{}}"#,
    ] {
        let fixture = client_files::Fixture::new(&[]).await.unwrap();
        let reference = fixture
            .put_file("bad.json", bytes, "application/json", "bad")
            .await
            .unwrap();
        let mut call = message(1, MessageKind::ToolCall, reference);
        call.tool_call_id = Some("call".into());
        let page = page(vec![call]);
        let host = Arc::new(Host::default());
        *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::iter([Ok(page)])));
        let mut client = HarnessClient::connect(host.clone(), session())
            .await
            .unwrap()
            .with_files(fixture.reader.clone());
        let mut output = Vec::new();
        assert!(
            acyclic_harness::client::run_headless(
                &mut client,
                command(),
                headless::target(),
                &mut output
            )
            .await
            .is_err()
        );
        assert_eq!(client.session().resume().cursors[0].revision, 0);
        assert!(host.acknowledgements.lock().unwrap().is_empty());
        assert_eq!(client.session().replay_revision(&owner()), 0);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .lines()
                .all(|line| !matches!(
                    jsonl::parse_server_frame(line).unwrap().frame,
                    Some(wire::server_frame::Frame::Delivery(_))
                ))
        );
    }
}
