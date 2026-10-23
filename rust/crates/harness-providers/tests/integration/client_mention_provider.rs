//! Composer -> pin -> real Rust provider projection -> scripted model response.
use super::{CommandCatalog, CommandResolution, Fixture, Result, TestResult};
use acyclic_harness::{
    Error, OperationId,
    context::ModelContextCapacity,
    conversation::Limits,
    model::{
        ModelContent, ModelContentPart, ModelDispatch, ModelEvent, ModelMessage, ModelProvider,
        ModelRequest, ModelRole, PreparedModelRequest,
    },
};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, HttpTransport, ModelFeatures,
    ModelSpec, ProviderSpec, Registry, TokenCountField, TokenCounterSpec,
};
use acyclic_stream::BoxProviderFuture;
use futures::TryStreamExt as _;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
};

struct Script(mpsc::Sender<HttpRequest>);
impl HttpTransport for Script {
    fn delay(&self, _: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        Box::pin(futures::future::pending())
    }
    fn now(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            self.0
                .send(request)
                .map_err(|_| Error::Invalid("scripted request receiver dropped".into()))?;
            Ok(HttpResponse {
                retry_after: None,
                status: 200,
                body: Box::pin(futures::stream::iter([Ok(bytes::Bytes::from_static(concat!(
                    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"run\",\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":1,\"total_tokens\":5}}}\n\n"
                ).as_bytes()))])),
            })
        })
    }
}

/// A Responses registry whose transport records each request and answers "ok".
fn scripted_registry(
    reader: Arc<dyn acyclic_harness::conversation::ContentResidencyVerifier>,
    sent: mpsc::Sender<HttpRequest>,
) -> Result<Registry> {
    let mut registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::new()),
        Arc::new(Script(sent)),
    )
    .with_content(reader, 4096);
    registry.register(
        "scripted".into(),
        ProviderSpec {
            adapter: Adapter::Responses,
            authentication: Authentication::Optional,
            base_url: "https://scripted.invalid/v1".into(),
            headers: BTreeMap::new(),
            models: BTreeMap::from([(
                "model".into(),
                ModelSpec {
                    max_context_tokens: None,
                    capacity: ModelContextCapacity {
                        context_tokens: 8192,
                        output_tokens: Some(64),
                    },
                    features: ModelFeatures {
                        tools: false,
                        images: false,
                    },
                },
            )]),
            token_counter: Some(TokenCounterSpec {
                path: "count_tokens".into(),
                adapter: Adapter::Responses,
                response_field: TokenCountField::InputTokens,
                framing_tokens: 0,
            }),
            max_event_bytes: 4096,
            retry: acyclic_harness_providers::RetrySpec::DEFAULT,
            timeouts: acyclic_harness_providers::TimeoutSpec::DEFAULT,
        },
    )?;
    Ok(registry)
}

async fn scenario(
    body: &[u8],
    staged: Option<&str>,
    text: bool,
    drop_attachment: bool,
) -> TestResult {
    let fixture = Fixture::new(&[]).await?;
    if let Some(media) = staged {
        fixture
            .put_file("src/lib.rs", body, media, "source")
            .await?;
    } else {
        fixture
            .put_workspace_file("src/lib.rs", body, "source")
            .await?;
    }
    let page = fixture
        .reader
        .list_private_directory(&fixture.root.volume, "", "", None, None, 16, None, None)?
        .await?;
    let resolved = CommandCatalog::builtins()?
        .resolve_input(
            "Inspect @src/lib.rs @src/lib.rs",
            &fixture.root,
            &page.generation,
            fixture.reader.as_ref(),
            4096,
        )
        .await?;
    let CommandResolution::Prompt { mentions, .. } = &resolved else {
        return Err("mentions did not resolve to a prompt".into());
    };
    let [first, second] = mentions.as_slice() else {
        return Err(format!("expected two mentions, got {}", mentions.len()).into());
    };
    let pinned = first.file.clone();
    assert_eq!(second.file, pinned);
    assert_eq!(
        pinned.descriptor().media_type(),
        if text {
            "text/plain"
        } else {
            "application/octet-stream"
        }
    );
    pinned.descriptor().verify(body, &mut None)?;
    let mut content = resolved.into_model_input()?;
    let ModelContent::Parts(parts) = &mut content else {
        return Err("mentions did not attach file parts".into());
    };
    assert_eq!(parts.len(), 2, "duplicate mentions attach once");
    assert!(matches!(parts.last(), Some(ModelContentPart::File { file, .. }) if file == &pinned));
    if drop_attachment {
        parts.pop();
    }
    // A newer, invalid body at head must never replace the original pinned file.
    fixture
        .put_workspace_file("src/lib.rs", b"changed\xff", "later-head")
        .await?;
    let (sent, received) = mpsc::channel();
    let provider = scripted_registry(fixture.reader.clone(), sent)?.resolve("scripted/model")?;
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content,
            }],
            tools: Vec::new(),
            max_output_tokens: Some(64),
        },
        Limits::default(),
        None,
    )?;
    let dispatch = ModelDispatch {
        operation_id: OperationId::new(),
        step: 0,
        request_digest: request.request_digest(),
    };
    let result = provider
        .generate(request, dispatch)
        .try_collect::<Vec<_>>()
        .await;
    check_dispatch(result, &received, body, text)
}

/// The scripted provider's outcome: pinned text dispatches once as text, binary never dispatches.
fn check_dispatch(
    result: Result<Vec<ModelEvent>>,
    received: &mpsc::Receiver<HttpRequest>,
    body: &[u8],
    text: bool,
) -> TestResult {
    if text {
        let events = result?;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::Content { delta } if delta == "ok"))
        );
        assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
        let request = received.try_recv()?;
        let wire: Value = serde_json::from_slice(&request.body)?;
        let source = wire.pointer("/input/1/content/0");
        if source.and_then(|part| part.get("type")) != Some(&Value::from("input_text"))
            || source
                .and_then(|part| part.get("text"))
                .and_then(Value::as_str)
                != Some(std::str::from_utf8(body)?)
        {
            return Err(format!("provider must receive pinned source as text: {wire}").into());
        }
        assert!(received.try_recv().is_err(), "one provider dispatch");
    } else {
        assert!(
            matches!(result, Err(Error::Unsupported(_))),
            "binary content must be rejected before provider dispatch: {result:?}"
        );
        assert!(
            received.try_recv().is_err(),
            "binary content cannot dispatch"
        );
    }
    Ok(())
}

#[tokio::test]
async fn composer_mentions_reach_the_rust_provider_with_pinned_utf8_and_reject_binary() -> TestResult
{
    for (body, staged, text) in [
        (
            "pub fn answer() -> &'static str { \"\u{8abf}\u{67fb}\" }\n".as_bytes(),
            None,
            true,
        ),
        (b"\0valid UTF-8 but binary".as_slice(), None, false),
        (b"invalid\xff".as_slice(), None, false),
        (
            b"declared binary".as_slice(),
            Some("application/octet-stream"),
            false,
        ),
        (b"declared text".as_slice(), Some("text/plain"), true),
    ] {
        scenario(body, staged, text, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn negative_control_prompt_only_dispatch_fails_the_same_provider_scenario() -> TestResult {
    match scenario(b"pub fn answer() {}", None, true, true).await {
        Err(error)
            if error
                .to_string()
                .contains("provider must receive pinned source as text") =>
        {
            Ok(())
        }
        other => Err(format!("prompt-only dispatch escaped the provider oracle: {other:?}").into()),
    }
}

impl Fixture {
    /// Ordinary workspace mutation: no upload metadata is staged for this path.
    async fn put_workspace_file(&self, path: &str, bytes: &[u8], key: &str) -> Result<()> {
        use acyclic_harness::filesystem::{WorkspaceMutation, workspace_ref};
        let volume = &self.root.volume;
        let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
        let mut mutations = Vec::new();
        if let Some((parent, _)) = path.rsplit_once('/') {
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: format!("/{parent}"),
            });
        }
        mutations.push(WorkspaceMutation::PutFile {
            path: format!("/{path}"),
            bytes: bytes.to_vec().into(),
        });
        self.host
            .apply(
                &workspace,
                None,
                &mutations,
                &acyclic_harness::IdempotencyKey::new(key)?,
            )
            .await?;
        Ok(())
    }
}
