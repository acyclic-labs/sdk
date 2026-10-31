use super::*;
use crate::{AgentBinding, AgentCall, AgentEvent, AgentId, Phase, StatusRecord};
use acyclic_actors::wire::InvokeActorRequest;
use acyclic_harness_core::{
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use acyclic_stream::{
    AppendRequest, BoxProviderFuture, MemoryStream, StreamError, StreamProvider,
    runtime::PlatformExecutor,
    session::{self, Client, transport},
};
use futures::StreamExt as _;
use prost::Message as _;
use std::{io, sync::Arc};

struct Endpoint(Arc<MemoryStream>);

impl transport::Transport for Endpoint {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            session::serve(
                self.0.clone(),
                session::testing::authority(),
                "node".into(),
                session::Limits::default(),
                source.map(Ok).boxed(),
            )
        })
    }
}

fn attachment(
    memory: Arc<MemoryStream>,
    agent: AgentPath,
    status_from: u64,
) -> Result<AttachedSession, Box<dyn std::error::Error>> {
    let pool = Client::with_transport(
        Arc::new(Endpoint(memory)),
        Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable) })),
        Arc::new(PlatformExecutor),
    );
    pool.register_credential(0, session::testing::credential())?;
    let binding = AgentBinding::new(
        &InvokeActorRequest {
            actor_id: "fixture".into(),
            method: "POST".into(),
            url: "https://fixture.invalid/command".into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )?;
    Ok(AttachedSession::new(
        AgentControls::new(pool, 0, binding),
        AgentSelection {
            agent,
            conversation_from: 0,
            status_from,
        },
    )?)
}

async fn status(editor: &mut AttachedSession) -> Result<StatusRecord, AgentClientError> {
    loop {
        if let AgentEvent::Status { status, .. } = editor.observer().next().await? {
            return Ok(status);
        }
    }
}

fn snapshot() -> Result<FileRef, acyclic_harness_core::Error> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?,
            "editor-context".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        "selection.txt".into(),
        "revision-4".into(),
        FileDescriptor::from_bytes(b"selected source", "text/plain", &mut None)?,
        "selection".into(),
        &mut None,
    )
}

#[tokio::test]
async fn editor_ingress_keeps_original_target_context_and_reconnect_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let memory = Arc::new(MemoryStream::default());
    let agent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let mut editor = attachment(memory.clone(), agent.clone(), 0)?;
    let file = snapshot()?;
    let uri = "editor://document/selection?revision=4";
    // Protocol input, not a captured host-output fixture. The adapter must
    // preserve user order and resolve only the explicitly supplied snapshot.
    let request = serde_json::from_value(serde_json::json!({
        "sessionId": agent.as_str(),
        "prompt": [
            {"type": "text", "text": "explain this selection"},
            {"type": "resource_link", "uri": uri, "name": "selection"}
        ]
    }))?;
    let mut resolutions = Vec::new();
    let prepared = editor
        .prepare_prompt(IntentId::try_from("editor-input-1")?, request, |resource| {
            resolutions.push(resource);
            std::future::ready(Ok::<_, io::Error>(file.clone()))
        })
        .await?;
    assert_eq!(resolutions, [uri]);
    let [invocation] = prepared.invocations() else {
        return Err("small editor prompt was not one original invocation".into());
    };
    let call = AgentCall::decode(invocation)?;
    let AgentCommand::Prompt {
        agent: target,
        id,
        body,
    } = call.command()
    else {
        return Err("editor input lost canonical prompt admission".into());
    };
    assert_eq!(target, &agent);
    assert_eq!(id.as_str(), "editor-input-1");
    assert_eq!(
        body,
        &ModelContent::Parts(vec![
            ModelContentPart::Text {
                text: "explain this selection".into()
            },
            ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::BoundedFull
            },
        ])
    );

    // Visible focus can change without retargeting this attachment's commands.
    editor.observer().set_view(
        AgentSelection {
            agent: AgentPath::root(&AgentId::from_bytes([2; 16])),
            conversation_from: 0,
            status_from: 0,
        },
        std::iter::empty(),
    )?;
    let cancelled = editor.prepare_cancel(
        IntentId::try_from("editor-cancel-1")?,
        serde_json::from_value(serde_json::json!({"sessionId": agent.as_str()}))?,
    )?;
    let call = AgentCall::decode(&cancelled.invocations()[0])?;
    assert!(matches!(call.command(), AgentCommand::Cancel {agent: target, ..} if target == &agent));
    assert_eq!(prepared.invocations()[0], *invocation);

    let outcome = StatusRecord {
        phase: Phase::Running,
        cursor: 1,
        conversation: 0,
    };
    memory
        .append(AppendRequest {
            path: agent.status()?,
            records: vec![outcome.append_record()?],
            if_tail: None,
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    // Reattach directly at the existing stream cursor; no replay cache or new
    // session ID. The next original append is the sole readiness producer.
    drop(editor);
    let mut editor = attachment(memory.clone(), agent.clone(), 1)?;
    let next = StatusRecord {
        phase: Phase::Idle,
        cursor: 2,
        conversation: 0,
    };
    memory
        .append(AppendRequest {
            path: agent.status()?,
            records: vec![next.append_record()?],
            if_tail: Some(1),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    assert_eq!(status(&mut editor).await?, next);

    rejected_context(&editor, &agent, uri).await?;
    Ok(())
}

async fn rejected_context(
    editor: &AttachedSession,
    agent: &AgentPath,
    uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Neither another session nor a failed original document publisher can
    // produce an executable prompt. There is no fallback read of the URI.
    let mismatch = editor
        .prepare_prompt(
            IntentId::try_from("wrong-session")?,
            PromptRequest {
                session_id: "another-session".into(),
                prompt: vec![ContentBlock::ResourceLink {
                    uri: uri.into(),
                    name: "selection".into(),
                }],
            },
            |_| std::future::ready(Err::<FileRef, _>("must not resolve another session")),
        )
        .await;
    assert!(matches!(mismatch, Err(InputError::SessionMismatch)));
    let refused = editor
        .prepare_prompt(
            IntentId::try_from("ungranted-document")?,
            PromptRequest {
                session_id: agent.to_string(),
                prompt: vec![ContentBlock::ResourceLink {
                    uri: "file:///private/document".into(),
                    name: "private".into(),
                }],
            },
            |_| {
                std::future::ready(Err::<FileRef, _>(io::Error::from(
                    io::ErrorKind::PermissionDenied,
                )))
            },
        )
        .await;
    assert!(
        matches!(refused, Err(InputError::Context(cause)) if cause.kind() == io::ErrorKind::PermissionDenied)
    );
    // The same publisher's completion channel supplies a typed terminal when
    // its producer disappears. The adapter must not wait on another event.
    let (producer, completion) = futures::channel::oneshot::channel::<FileRef>();
    drop(producer);
    let mut completion = Some(completion);
    let lost = editor
        .prepare_prompt(
            IntentId::try_from("lost-publisher")?,
            PromptRequest {
                session_id: agent.to_string(),
                prompt: vec![ContentBlock::ResourceLink {
                    uri: uri.into(),
                    name: "selection".into(),
                }],
            },
            |_| {
                let completion = completion.take();
                async move {
                    completion
                        .ok_or_else(|| io::Error::other("publisher completion already consumed"))?
                        .await
                        .map_err(io::Error::other)
                }
            },
        )
        .await;
    assert!(matches!(lost, Err(InputError::Context(_))));
    assert!(
        serde_json::from_value::<PromptRequest>(serde_json::json!({
            "sessionId": agent.as_str(), "prompt": [{"type": "image", "data": "unadmitted"}]
        }))
        .is_err()
    );
    Ok(())
}
