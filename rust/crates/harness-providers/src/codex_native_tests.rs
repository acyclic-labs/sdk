use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use acyclic_harness_core::NativeFutureBundle;
use futures::StreamExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct NativeServer {
    pub(super) inner: Arc<Server>,
    pub(super) work: NativeMaterializationWork,
    pub(super) sent: AtomicUsize,
}

#[tokio::test]
async fn native_conversation_identity_reaches_codex_across_operations() -> Result<()> {
    use acyclic_harness_core::{ConversationId, OperationId, conversation::Limits, model::*};
    let (_root, login, inner, _) = fixture(vec![(200, RESPONSE.into()), (200, RESPONSE.into())])?;
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let server = Arc::new(NativeServer {
        inner: inner.clone(),
        work: work.clone(),
        sent: AtomicUsize::new(0),
    });
    let provider = Registry::with_transport(Credentials::explicit(BTreeMap::new()), server)
        .with_codex_login(login)
        .resolve("openai/gpt-5")?;
    let conversation = ConversationId::from_bytes([9; 16]);
    let (base, _) = crate::tests::request(&provider)?;
    let mut request = base.request().clone();
    request.conversation_id = Some(conversation);
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    for byte in [1, 2] {
        let dispatch = ModelDispatch {
            operation_id: OperationId::from_bytes([byte; 16]),
            step: 0,
            request_digest: prepared.request_digest(),
        };
        provider
            .generate_with_native_work(&prepared, dispatch, &work)?
            .await?
            .try_collect::<Vec<_>>()
            .await?;
    }
    let requests = inner
        .requests
        .lock()
        .map_err(|_| invalid("fixture poisoned"))?;
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(request.headers["session-id"], conversation.to_string());
        assert_eq!(request.headers["thread-id"], conversation.to_string());
        assert_eq!(
            crate::json(&request.body)?["prompt_cache_key"],
            conversation.to_string()
        );
    }
    assert_eq!(
        requests.first().unwrap().body,
        requests.get(1).unwrap().body
    );
    assert_ne!(
        requests.first().unwrap().headers["x-client-request-id"],
        requests.get(1).unwrap().headers["x-client-request-id"]
    );
    Ok(())
}

impl HttpTransport for NativeServer {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness_core::Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        crate::fixtures::native_delay(wait, work)
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("native login must not fall back to ordinary HTTP")
    }
    fn supports_native(&self, work: &NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse>> {
        assert!(work.shares_allocation_context(&self.work));
        NativeFutureBundle::new(work, || async move {
            self.sent.fetch_add(1, Ordering::SeqCst);
            assert!(
                work.receipt().source_bytes_read > 0,
                "credential reads use this ledger"
            );
            let (status, body) = self.inner.reply(request)?;
            HttpResponse::with_native_body(
                status,
                futures::stream::iter([Ok(Bytes::from(body))]),
                work,
            )
        })
    }
}

#[tokio::test]
async fn native_login_funds_credentials_proactive_refresh_and_unauthorized_recovery() -> Result<()>
{
    let (root, login, inner, _) = fixture(vec![
        (200, json!({"access_token":"proactive-access"}).to_string()),
        (401, String::new()),
        (200, json!({"access_token":"recovered-access"}).to_string()),
        (200, RESPONSE.into()),
    ])?;
    let path = root.path().join("auth.json");
    let mut auth = crate::json(&std::fs::read(&path).expect("fixture credentials"))?;
    *auth
        .pointer_mut("/tokens/access_token")
        .expect("fixture access token") = json!(format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(json!({"exp":0}).to_string())
    ));
    std::fs::write(&path, auth.to_string()).expect("expired fixture credentials");
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let server = Arc::new(NativeServer {
        inner: inner.clone(),
        work: work.clone(),
        sent: AtomicUsize::new(0),
    });
    let provider = Registry::with_transport(Credentials::explicit(BTreeMap::new()), server.clone())
        .with_codex_login(login)
        .resolve("openai/gpt-5")?;
    let (request, dispatch) = crate::tests::request(&provider)?;
    {
        let mut stream = provider
            .generate_with_native_work(&request, dispatch, &work)?
            .await?;
        let mut completed = false;
        while let Some(event) = stream.next().await {
            let (event, custody) = event?;
            assert!(custody.work().shares_allocation_context(&work));
            completed |= matches!(
                event,
                acyclic_harness_core::model::ModelEvent::Completed { .. }
            );
            drop((event, custody));
        }
        assert!(completed);
    }
    assert_eq!(server.sent.load(Ordering::SeqCst), 4);
    let mut sent = inner.requests.lock().expect("fixture requests");
    assert_eq!(
        sent.first().unwrap().url,
        "https://auth.openai.com/oauth/token"
    );
    assert_eq!(
        sent.get(2).unwrap().url,
        "https://auth.openai.com/oauth/token"
    );
    assert_eq!(
        sent.get(1).unwrap().body,
        sent.get(3).unwrap().body,
        "401 retries the same prepared body"
    );
    assert_eq!(
        sent.get(3).unwrap().headers["authorization"],
        "Bearer recovered-access"
    );
    sent.clear();
    assert_eq!(work.receipt().durability_operations, 2);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(
        work.live_allocation_bytes() > baseline,
        "prepared request retains its wire payload"
    );
    drop(request);
    drop(provider);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn unfunded_credential_reads_stop_before_reading_or_http() -> Result<()> {
    for exhausted_memory in [false, true] {
        let (_root, login, inner, _) = fixture(Vec::new())?;
        let mut budget = acyclic_fs::performance::work_budget(100_000, 4_000_000);
        if exhausted_memory {
            budget.peak_allocation_bytes =
                NativeMaterializationWork::accounting_state_bytes() + 1024;
        } else {
            budget.source_bytes_read = 0;
        }
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let mut custody = work.claim(0, 0, WorkCounters::UNCHARGED)?;
        let error = login
            .bearer(inner.as_ref(), None, Some(&mut custody))
            .await
            .expect_err("unfunded credential IO");
        drop((error, custody));
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert!(inner.requests.lock().expect("fixture requests").is_empty());
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
