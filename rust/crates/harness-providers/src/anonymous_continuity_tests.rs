//! Recorded signed calls through registry admission, the local host, its real
//! shell and durable conversation continuation. Only HTTP delivery is replayed.
use super::*;
use acyclic_harness::{
    LocalHarness, LocalHarnessConfig,
    approvals::ApprovalPolicy,
    context::CompactionPolicy,
    local::agent_conversation::Conversation,
    test_support::{PromptOutcome, prompt_settled},
};
use acyclic_harness_core::{
    executor::{ExecutionEvent, ExecutionPayloadField},
    model::{NativePart, replay_key},
};
use std::sync::atomic::{AtomicUsize, Ordering};

const CALL: &[u8] = include_bytes!("fixtures/gemini-flash-shell.sse");
const ANSWER: &[u8] = include_bytes!("fixtures/gemini-flash-answer.sse");
const COUNT: &[u8] = include_bytes!("fixtures/gemini-flash-count.json");
const MISSING_SIGNATURE: &[u8] = include_bytes!("fixtures/gemini-flash-missing-signature.body");
const PROMPT: &str = "Use the shell tool to create shell.txt containing exactly the UTF-8 bytes LIVE_dd7e4549fdec4891b0cc299b265e9be8, with no newline or other bytes, then reply DONE.";

struct Capture {
    call: &'static [u8],
    refusal: &'static [u8],
    id: &'static str,
    signature_length: usize,
    skill: bool,
}

const CAPTURES: &[Capture] = &[
    Capture {
        call: CALL,
        refusal: MISSING_SIGNATURE,
        id: "call_81218",
        signature_length: 960,
        skill: false,
    },
    Capture {
        call: include_bytes!("fixtures/gemini-flash-skill.sse"),
        refusal: include_bytes!("fixtures/gemini-flash-skill-missing-signature.body"),
        id: "call_1093319",
        signature_length: 5932,
        skill: true,
    },
];

struct Recorded {
    capture: &'static Capture,
    requests: Mutex<Vec<Bytes>>,
    native: AtomicUsize,
}

impl Recorded {
    fn body(&self, request: HttpRequest) -> (u16, Bytes) {
        assert!(request.headers.keys().all(|key| !matches!(
            key.as_str(),
            "authorization" | "x-api-key" | "x-goog-api-key"
        )));
        if request.url.contains(":countTokens") {
            return (200, Bytes::from_static(COUNT));
        }
        let mut requests = self.requests.lock().unwrap();
        let first = requests.is_empty();
        let missing = !first
            && signed_part(&request.body, self.capture.id)
                .is_none_or(|part| part["thoughtSignature"].as_str().is_none());
        requests.push(request.body);
        if missing {
            (400, Bytes::from_static(self.capture.refusal))
        } else {
            (
                200,
                Bytes::from_static(if first { self.capture.call } else { ANSWER }),
            )
        }
    }
}

impl HttpTransport for Recorded {
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }

    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }

    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        crate::fixtures::native_delay(wait, work)
    }

    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            assert!(
                request.url.contains(":countTokens"),
                "host generation must use its funded native path"
            );
            let (status, body) = self.body(request);
            Ok(HttpResponse {
                status,
                request_id: None,
                retry_after: None,
                body: Box::pin(futures::stream::iter([Ok(body)])),
            })
        })
    }

    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            if !request.url.contains(":countTokens") {
                self.native.fetch_add(1, Ordering::SeqCst);
            }
            let (status, body) = self.body(request);
            HttpResponse::with_native_body(status, futures::stream::iter([Ok(body)]), work)
        })
    }
}

fn resolve(transport: Arc<Recorded>, route: &str) -> Result<ResolvedModel> {
    resolve_with(transport, route, |_| {})
}

fn resolve_with(
    transport: Arc<Recorded>,
    route: &str,
    configure: impl FnOnce(&mut ProviderSpec),
) -> Result<ResolvedModel> {
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport);
    let catalog = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        catalog.path(),
        include_bytes!("fixtures/gemini-flash-catalog.json"),
    )
    .unwrap();
    registry.load_catalog(catalog.path())?;
    let mut spec = registry.catalog()["gemini"].as_ref().clone();
    spec.base_url = route.into();
    configure(&mut spec);
    registry.register("gemini".into(), spec)?;
    registry.resolve("gemini/gemini-3.8-flash")
}

fn signed_part(body: &[u8], id: &str) -> Option<Value> {
    let value: Value = serde_json::from_slice(body).unwrap();
    value["contents"]
        .as_array()?
        .iter()
        .filter_map(|message| message["parts"].as_array())
        .flatten()
        .find(|part| part["functionCall"]["id"] == id)
        .cloned()
}

#[tokio::test]
async fn anonymous_signed_calls_survive_native_shell_and_reopen() -> Result<()> {
    for seed in [
        include_bytes!("../fuzz/corpus/provider_stream/gemini-flash-shell").as_slice(),
        include_bytes!("../fuzz/corpus/provider_stream/gemini-flash-answer").as_slice(),
    ] {
        crate::fuzzing::assert_stream_invariants(seed);
    }
    for capture in CAPTURES {
        replay(capture).await?;
    }
    Ok(())
}

async fn replay(capture: &'static Capture) -> Result<()> {
    let temporary = tempfile::tempdir().unwrap();
    let workspace = temporary.path().join("project");
    std::fs::create_dir(&workspace).unwrap();
    if capture.skill {
        let directory = workspace.join(".agents/skills/inventory");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("SKILL.md"),
            include_bytes!("fixtures/gemini-flash-skill.md"),
        )
        .unwrap();
    }
    let state = temporary.path().join("state");
    let transport = Arc::new(Recorded {
        capture,
        requests: Mutex::new(Vec::new()),
        native: AtomicUsize::new(0),
    });
    let route = "http://127.0.0.1:1234/session/gemini";
    let open = || -> Result<LocalHarnessConfig> {
        let provider = Arc::new(resolve(transport.clone(), route)?);
        Ok(
            LocalHarnessConfig::new(&workspace, (provider.model().clone(), provider))
                .state_directory(&state)
                .hermetic()
                .without_user_settings()
                .approval_policy(ApprovalPolicy::accept_all("recorded-call"))
                .compaction(CompactionPolicy::Disabled),
        )
    };
    let harness = Arc::new(LocalHarness::open(open()?).await?);
    let prompt = if capture.skill {
        "/inventory Follow the selected skill precisely."
    } else {
        PROMPT
    };
    let (entry, output) = match prompt_settled(&harness, "signed-call", prompt).await? {
        PromptOutcome::Settled(entry, output) => (entry, output),
        PromptOutcome::Failed(reason) => panic!("recorded tool turn failed: {reason}"),
    };
    assert_eq!(output.steps, 2);
    let conversation = Conversation::open(harness.connect().await?)?;
    let saved = admitted_requests(&conversation, entry.inbox).await?;
    assert_eq!(saved.len(), 2);
    assert_signed_replay_identity(&saved[1])?;
    if capture.skill {
        assert_authored_replay_identity(&saved[0])?;
    }
    drop(conversation);
    let captured = std::str::from_utf8(capture.call)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|json| serde_json::from_str::<Value>(json).unwrap())
        .find_map(|event| {
            event["candidates"]
                .as_array()?
                .iter()
                .filter_map(|candidate| candidate["content"]["parts"].as_array())
                .flatten()
                .find(|part| part["functionCall"]["id"] == capture.id)
                .cloned()
        })
        .expect("original recorded response contains the signed call");
    let native = output
        .transcript
        .iter()
        .find_map(|message| {
            message.content.parts().iter().find_map(|part| match part {
                ModelContentPart::ToolCall(call) => call.native.clone(),
                _ => None,
            })
        })
        .expect("canonical output retained the signed call");
    let original: Value = serde_json::from_slice(&native.continuity).unwrap();
    assert_eq!(original, captured);
    assert_eq!(
        original["thoughtSignature"].as_str().unwrap().len(),
        capture.signature_length
    );
    let continuation = transport.requests.lock().unwrap()[1].clone();
    assert_eq!(
        signed_part(&continuation, capture.id),
        Some(original.clone())
    );
    assert_eq!(transport.native.load(Ordering::SeqCst), 2);
    if capture.skill {
        let first = transport.requests.lock().unwrap()[0].clone();
        assert!(
            std::str::from_utf8(&first)
                .unwrap()
                .contains("SKILL_LIVE_27ab6f453aef46a389af6e71a68aed4b")
        );
        let request: Value = serde_json::from_slice(&continuation).unwrap();
        assert!(
            request["contents"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| {
                    message["parts"].as_array().unwrap().iter().any(|part| {
                        part["functionResponse"]["id"] == capture.id
                            && part["functionResponse"]["name"] == "read"
                    })
                })
        );
    } else {
        assert_eq!(
            std::fs::read(workspace.join("shell.txt")).unwrap(),
            b"LIVE_dd7e4549fdec4891b0cc299b265e9be8"
        );
    }
    Arc::try_unwrap(harness).ok().unwrap().disconnect().await?;

    let harness = Arc::new(LocalHarness::open(open()?).await?);
    let conversation = Conversation::open(harness.connect().await?)?;
    let restored = admitted_requests(&conversation, entry.inbox).await?;
    assert_eq!(restored, saved, "reopen retains original admitted bytes");
    for bytes in &restored {
        assert_saved_request_key(bytes)?;
    }
    drop(conversation);
    assert!(matches!(
        prompt_settled(&harness, "after-reopen", "continue").await?,
        PromptOutcome::Settled(..)
    ));
    let reopened = transport.requests.lock().unwrap()[2].clone();
    assert_eq!(signed_part(&reopened, capture.id), Some(original));
    assert_eq!(transport.native.load(Ordering::SeqCst), 3);
    Arc::try_unwrap(harness).ok().unwrap().disconnect().await?;

    // Unknown origins, changed route bindings and authenticated accounts may
    // never inherit ownership from the admitted anonymous selection.
    let same = resolve(transport.clone(), route)?;
    let changed = resolve(transport.clone(), "http://127.0.0.1:1234/other/gemini")?;
    let target = |provider: &ResolvedModel| provider.reasoning_origin(None, None);
    assert_eq!(
        crate::project_native(Some(&native), &target(&same)),
        Some(native.continuity.as_slice())
    );
    assert!(crate::project_native(Some(&native), &target(&changed)).is_none());
    let changed_headers = resolve_with(transport.clone(), route, |spec| {
        spec.headers.insert("x-route-revision".into(), "2".into());
    })?;
    assert!(crate::project_native(Some(&native), &target(&changed_headers)).is_none());
    assert!(
        resolve_with(transport.clone(), route, |spec| {
            spec.authentication = Authentication::ApiKey;
        })
        .is_err()
    );
    // Every protocol uses the same admitted owner, without a second renderer
    // or adapter-specific exception for the captured Gemini signature.
    for adapter in [Adapter::Anthropic, Adapter::Responses, Adapter::Chat] {
        let provider = resolve_with(transport.clone(), route, |spec| {
            spec.adapter = adapter;
            spec.token_counter = None;
        })?;
        let origin = target(&provider);
        assert!(origin.credential_scope.is_some());
        assert_ne!(origin.credential_scope, native.origin.credential_scope);
        assert!(crate::project_native(Some(&native), &origin).is_none());
    }
    let unknown = NativePart {
        origin: acyclic_harness_core::model::ReasoningOrigin {
            credential_scope: None,
            ..native.origin.clone()
        },
        continuity: native.continuity.clone(),
    };
    assert!(crate::project_native(Some(&unknown), &target(&same)).is_none());
    assert!(
        crate::project_native(
            Some(&native),
            &same.reasoning_origin(Some("other-account"), None)
        )
        .is_none()
    );
    Ok(())
}

// Read the same authenticated, digest-verified projection used by recording
// export. Both loops consume finite pages/chunks of the settled original input;
// neither waits for a live producer or reconstructs provider wire input.
async fn admitted_requests(conversation: &Conversation, input: u64) -> Result<Vec<Vec<u8>>> {
    let mut requests = Vec::new();
    let mut after = 0;
    loop {
        let query = conversation.execution_page(input, after, false).await?;
        assert_eq!(query.input, input);
        assert!(query.settled);
        let page = query.page.expect("settled input retains its journal");
        for record in page.items {
            if !matches!(record.event, ExecutionEvent::ModelStarted { .. }) {
                continue;
            }
            let mut bytes = Vec::new();
            loop {
                let (chunk, length) = conversation
                    .execution_payload(
                        input,
                        record.sequence,
                        ExecutionPayloadField::AdmittedRequest,
                        bytes.len() as u64,
                    )
                    .await?;
                assert!(!chunk.is_empty(), "original payload must advance");
                bytes.extend_from_slice(&chunk);
                if bytes.len() as u64 >= length {
                    assert_eq!(bytes.len() as u64, length);
                    break;
                }
            }
            requests.push(bytes);
        }
        let Some(next) = page.next else { break };
        assert!(next > after, "original journal page must advance");
        after = next;
    }
    Ok(requests)
}

fn assert_saved_request_key(bytes: &[u8]) -> Result<[u8; 32]> {
    let input: ModelRequest = serde_json::from_slice(bytes).unwrap();
    let admitted = PreparedModelRequest::prepare(input, Limits::default(), None)?;
    let key = admitted.replay_key()?;
    assert_eq!(key, replay_key(bytes)?);
    Ok(key)
}

fn assert_signed_replay_identity(bytes: &[u8]) -> Result<()> {
    let key = assert_saved_request_key(bytes)?;
    let mut input: ModelRequest = serde_json::from_slice(bytes).unwrap();
    let call = input
        .messages
        .iter_mut()
        .flat_map(|message| {
            let parts: &mut [ModelContentPart] = match &mut message.content {
                ModelContent::Text(_) => &mut [],
                ModelContent::Part(part) => std::slice::from_mut(part),
                ModelContent::Parts(parts) => parts,
            };
            parts
        })
        .find_map(|part| match part {
            ModelContentPart::ToolCall(call) if call.native.is_some() => Some(call),
            _ => None,
        })
        .expect("the actual continuation contains its signed call");
    call.native = None;
    assert_ne!(
        key,
        replay_key(&serde_json::to_vec(&input).unwrap())?,
        "losing original native continuity cannot replay the same answer"
    );
    Ok(())
}

fn assert_authored_replay_identity(bytes: &[u8]) -> Result<()> {
    let original: ModelRequest = serde_json::from_slice(bytes).unwrap();
    let mut keys = std::collections::BTreeSet::from([assert_saved_request_key(bytes)?]);
    for suffix in [
        "\nFollow the selected skill and preserve its exact output.",
        "\n{\"kind\":\"generation\",\"key\":\"12345678-1234-1234-1234-123456789abc\"}",
        "\n{\"kind\":\"generation\",\"key\":\"87654321-4321-4321-4321-cba987654321\"}",
    ] {
        let mut changed = original.clone();
        let text = changed
            .messages
            .iter_mut()
            .filter(|message| message.role == ModelRole::System)
            .find_map(|message| match &mut message.content {
                ModelContent::Text(text) if text.contains("inventory") => Some(text),
                _ => None,
            })
            .expect("the admitted skill catalog is original system text");
        text.push_str(suffix);
        let changed = serde_json::to_vec(&changed).unwrap();
        assert!(
            keys.insert(assert_saved_request_key(&changed)?),
            "authored instructions and UUIDs cannot impersonate fresh harness identity"
        );
    }
    Ok(())
}
