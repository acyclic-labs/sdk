//! Recorded signed calls through registry admission, the local host, its real
//! shell and durable conversation continuation. Only HTTP delivery is replayed.
use super::*;
use acyclic_harness::{
    LocalHarness, LocalHarnessConfig,
    approvals::ApprovalPolicy,
    context::CompactionPolicy,
    test_support::{PromptOutcome, prompt_settled},
};
use acyclic_harness_core::model::NativePart;
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
    let output = match prompt_settled(&harness, "signed-call", prompt).await? {
        PromptOutcome::Settled(_, output) => output,
        PromptOutcome::Failed(reason) => panic!("recorded tool turn failed: {reason}"),
    };
    assert_eq!(output.steps, 2);
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
