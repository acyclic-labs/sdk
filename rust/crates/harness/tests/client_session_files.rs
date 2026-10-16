//! Recovery folds the writer's immutable files, independent of live chunk timing.
use super::*;
use acyclic_harness::{
    OperationId,
    conversation::{Attachment, ConversationMessage, FileRef, MessageKind, ReferencedAttachments},
    model::{ModelEvent, ToolResultContent},
    tool::ToolInvocation,
};
use serde_json::json;

const KEY: &str = "AGGREGATE_KIND_TASK:task";

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

async fn recover_views(chunks: &[String], tokens: &[u16], structured: bool) {
    let fixture = client_files::Fixture::new(&[]).await.unwrap();
    let turn = OperationId::from_bytes(7u128.to_be_bytes());
    let invocation = ToolInvocation {
        operation_id: turn,
        call_id: "call".into(),
        name: "bash".into(),
        arguments: json!({"command":"pwd"}),
    };
    let mut call = message(
        1,
        MessageKind::ToolCall,
        json_file(&fixture, "call.json", &invocation).await,
    );
    call.tool_call_id = Some("call".into());
    let output = if structured {
        ToolResultContent::Json {
            value: json!({"cwd":"/workspace", "exit_code":0}),
        }
    } else {
        tool_text(&chunks.concat())
    };
    let projection = json_file(&fixture, "projection.json", &output).await;
    let mut result = message(
        2,
        MessageKind::ToolResult,
        json_file(
            &fixture,
            "raw.json",
            &json!({"raw":"not the model projection"}),
        )
        .await,
    );
    result.tool_call_id = Some("call".into());
    result.reply_to = Some(call.id);
    result.attachments = ReferencedAttachments::Inline {
        items: vec![Attachment {
            label: Some("model_projection".into()),
            file: projection,
        }],
    };
    let body = fixture
        .put_file("assistant.txt", b"done", "text/plain", "assistant")
        .await
        .unwrap();
    let mut assistant = message(3, MessageKind::Assistant, body.clone());
    assistant.id = acyclic_harness::conversation::assistant_message_id(turn);
    let total: u64 = tokens.iter().map(|&value| u64::from(value)).sum();
    assistant.extensions.insert(
        "acyclic.model.metadata".into(),
        json_file(&fixture, "usage.json", &json!({"tokens":total})).await,
    );
    let next_turn = OperationId::from_bytes(8u128.to_be_bytes());
    let mut next = message(4, MessageKind::Assistant, body);
    next.id = acyclic_harness::conversation::assistant_message_id(next_turn);
    next.extensions.insert(
        "acyclic.model.metadata".into(),
        json_file(&fixture, "next-usage.json", &json!({"tokens":11})).await,
    );
    let page = page(vec![call, result, assistant, next]);
    let mut client = connect(session(), page.clone(), &fixture).await;
    client
        .model_event(
            &owner(),
            turn,
            ModelEvent::ToolCall {
                call_id: invocation.call_id,
                name: invocation.name,
                arguments: invocation.arguments,
            },
        )
        .unwrap();
    for chunk in chunks {
        client.tool_output(&owner(), "call", chunk).unwrap();
    }
    for &tokens in tokens {
        client
            .model_event(
                &owner(),
                turn,
                ModelEvent::Completed {
                    metadata: json!({"tokens":tokens}),
                },
            )
            .unwrap();
    }
    client.next_delivery().await.unwrap().unwrap();
    let expected = client.session().views().clone();
    assert_eq!(expected.tools[KEY]["call"].name, "bash");
    assert_eq!(
        expected.tools[KEY]["call"].arguments,
        json!({"command":"pwd"})
    );
    assert_eq!(expected.tools[KEY]["call"].output, output);
    assert_eq!(expected.usage[KEY], json!({"tokens":total + 11}));
    // Late live records must not overwrite canonical files or double charge usage.
    client.tool_output(&owner(), "call", "late").unwrap();
    for event in [
        ModelEvent::ToolCall {
            call_id: "call".into(),
            name: "wrong".into(),
            arguments: json!(null),
        },
        ModelEvent::Completed {
            metadata: json!({"tokens":999}),
        },
    ] {
        client.model_event(&owner(), turn, event).unwrap();
    }
    assert_eq!(client.session().views(), &expected);
    let mut state = client.into_session();
    state.checkpoint().unwrap();
    let state = ClientSession::open(state.into_store()).unwrap();
    assert_eq!(state.views(), &Default::default());
    let mut recovered = connect(state, page.clone(), &fixture).await;
    recovered.next_delivery().await.unwrap().unwrap();
    assert_eq!(recovered.session().views(), &expected);
    // Same shared fold also tolerates overlap without reading or charging twice.
    let mut recovered = connect(recovered.into_session(), page, &fixture).await;
    recovered.next_delivery().await.unwrap().unwrap();
    assert_eq!(recovered.session().views(), &expected);
    let encoded = serde_json::to_value(&expected.tools[KEY]["call"].output).unwrap();
    assert_eq!(
        serde_json::from_value::<ToolResultContent>(encoded).unwrap(),
        output
    );
}

#[tokio::test]
async fn reopen_rebuilds_tool_details_output_and_usage_from_writer_files() {
    for structured in [false, true] {
        recover_views(&["hello".into(), "\nworld".into()], &[2, 3], structured).await;
    }
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
        json_file(&fixture, "call.json", &invocation).await,
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
    assert_eq!(client.session().views(), &Default::default());
    // Recovery can bind the same platform reader and retry the unchanged cursor.
    let mut client = connect(client.into_session(), page, &fixture).await;
    client.next_delivery().await.unwrap().unwrap();
    assert_eq!(client.session().views().tools[KEY]["call"].name, "bash");
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
        assert_eq!(client.session().views(), &Default::default());
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

#[tokio::test]
async fn replay_usage_overflow_preserves_the_entire_previous_view() {
    let fixture = client_files::Fixture::new(&[]).await.unwrap();
    let metadata = json_file(&fixture, "metadata.json", &json!({"tokens":1})).await;
    let mut assistant = message(1, MessageKind::Assistant, metadata.clone());
    assistant
        .extensions
        .insert("acyclic.model.metadata".into(), metadata);
    let mut client = connect(session(), page(vec![assistant]), &fixture).await;
    client
        .model_event(
            &owner(),
            OperationId::from_bytes([7; 16]),
            ModelEvent::Completed {
                metadata: json!({"tokens":u64::MAX}),
            },
        )
        .unwrap();
    let previous = client.session().views().clone();
    assert!(matches!(
        client.next_delivery().await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(client.session().resume().cursors[0].revision, 0);
    assert_eq!(client.session().views(), &previous);
}

proptest::proptest! {
    // Property: final file projections are independent of chunk boundaries, usage
    // step counts, and output shape; replay and live views are identical.
    #[test]
    fn file_replay_equals_live_for_arbitrary_chunking_and_usage(
        chunks in proptest::collection::vec(".{0,16}", 0..5),
        tokens in proptest::collection::vec(0u16..1000, 0..5),
        structured in proptest::bool::ANY,
    ) {
        futures::executor::block_on(recover_views(&chunks, &tokens, structured));
    }
}
