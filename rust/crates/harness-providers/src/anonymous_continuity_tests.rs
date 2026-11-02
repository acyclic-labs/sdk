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
const PROMPT: &str = "Use the shell tool to create shell.txt containing exactly the UTF-8 bytes LIVE_dd7e4549fdec4891b0cc299b265e9be8, with no newline or other bytes, then reply DONE.";

struct Recorded {
    requests: Mutex<Vec<Bytes>>,
    native: AtomicUsize,
}

impl Recorded {
    fn body(&self, request: HttpRequest) -> Bytes {
        assert!(request.headers.keys().all(|key| !matches!(
            key.as_str(),
            "authorization" | "x-api-key" | "x-goog-api-key"
        )));
        if request.url.contains(":countTokens") {
            return Bytes::from_static(COUNT);
        }
        let mut requests = self.requests.lock().unwrap();
        let first = requests.is_empty();
        requests.push(request.body);
        Bytes::from_static(if first { CALL } else { ANSWER })
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
            Ok(HttpResponse {
                status: 200,
                request_id: None,
                retry_after: None,
                body: Box::pin(futures::stream::iter([Ok(self.body(request))])),
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
            self.native.fetch_add(1, Ordering::SeqCst);
            HttpResponse::with_native_body(
                200,
                futures::stream::iter([Ok(self.body(request))]),
                work,
            )
        })
    }
}

fn resolve(transport: Arc<Recorded>, route: &str) -> Result<ResolvedModel> {
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport);
    let mut spec = crate::default_catalog()["gemini"].clone();
    spec.authentication = Authentication::Optional;
    spec.base_url = route.into();
    registry.register("gemini".into(), spec)?;
    registry.resolve("gemini/gemini-3.8-flash")
}

fn signed_part(body: &[u8]) -> Option<Value> {
    let value: Value = serde_json::from_slice(body).unwrap();
    value["contents"]
        .as_array()?
        .iter()
        .filter_map(|message| message["parts"].as_array())
        .flatten()
        .find(|part| part["functionCall"]["id"] == "call_81218")
        .cloned()
}

#[tokio::test]
async fn anonymous_signed_calls_survive_native_shell_and_reopen() -> Result<()> {
    let temporary = tempfile::tempdir().unwrap();
    let workspace = temporary.path().join("project");
    std::fs::create_dir(&workspace).unwrap();
    let state = temporary.path().join("state");
    let transport = Arc::new(Recorded {
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
    let output = match prompt_settled(&harness, "signed-shell", PROMPT).await? {
        PromptOutcome::Settled(_, output) => output,
        PromptOutcome::Failed(reason) => panic!("recorded shell turn failed: {reason}"),
    };
    assert_eq!(output.steps, 2);
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
    assert_eq!(original["thoughtSignature"].as_str().unwrap().len(), 960);
    let continuation = transport.requests.lock().unwrap()[1].clone();
    assert_eq!(signed_part(&continuation), Some(original.clone()));
    assert!(transport.native.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        std::fs::read(workspace.join("shell.txt")).unwrap(),
        b"LIVE_dd7e4549fdec4891b0cc299b265e9be8"
    );
    Arc::try_unwrap(harness).ok().unwrap().disconnect().await?;

    let harness = Arc::new(LocalHarness::open(open()?).await?);
    assert!(matches!(
        prompt_settled(&harness, "after-reopen", "continue").await?,
        PromptOutcome::Settled(..)
    ));
    let reopened = transport.requests.lock().unwrap()[2].clone();
    assert_eq!(signed_part(&reopened), Some(original));
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
