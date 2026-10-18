use super::*;
use acyclic_fs::NativeMaterializationWork;
use acyclic_harness::NativeFutureBundle;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Backend {
    counts: AtomicUsize,
    generations: AtomicUsize,
}
impl HttpTransport for Backend {
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
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        crate::fixtures::native_delay(wait, work)
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("stock must use original-ledger native HTTP")
    }
    fn supports_native(&self, _: &NativeMaterializationWork) -> Result<bool> {
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse>> {
        NativeFutureBundle::new(work, || async move {
            let (status, body) = if request.url == "https://chatgpt.com/backend-api/codex/responses"
            {
                self.generations.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request.headers["chatgpt-account-id"], "account");
                (200, RESPONSE)
            } else {
                self.counts.fetch_add(1, Ordering::SeqCst);
                (404, "no count route")
            };
            drop(request);
            HttpResponse::with_native_body(
                status,
                futures::stream::iter([Ok(Bytes::from_static(body.as_bytes()))]),
                work,
            )
        })
    }
}

#[tokio::test]
async fn codex_stock_execution_counts_locally_without_a_backend_count_route() -> Result<()> {
    stock(false).await
}
#[tokio::test]
async fn negative_control_inherited_public_counter_rejects_the_same_stock_task() -> Result<()> {
    stock(true).await
}
async fn stock(broken: bool) -> Result<()> {
    let (_root, login, _, _) = fixture(Vec::new())?;
    let backend = Arc::new(Backend {
        counts: AtomicUsize::new(0),
        generations: AtomicUsize::new(0),
    });
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), backend.clone())
            .with_codex_login(login);
    let catalog = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
    std::fs::write(catalog.path(), include_str!("fixtures/luna-catalog.json"))
        .map_err(|_| invalid("fixture catalog"))?;
    registry.load_catalog(catalog.path())?;
    let mut spec = registry.catalog()["openai"].as_ref().clone();
    spec.authentication = Authentication::ApiKeyOrCodexLogin;
    registry.register("openai".into(), spec)?;
    let mut provider = registry.resolve("openai/gpt-6-luna")?;
    assert!(provider.config.token_counter.is_none());
    if broken {
        // The former path retained the public endpoint's counter when changing
        // base_url. Disable the new local choice to expose precisely that route.
        provider.codex_login = None;
        provider.key = Some("fixture-key".into());
        Arc::make_mut(&mut provider.config).token_counter =
            crate::catalog::default_catalog()["openai"]
                .token_counter
                .clone();
    }
    // This fixture declares an 8192-token context in its catalog. Enable
    // automatic counting with explicit compaction budgets inside that window.
    let policy = acyclic_harness::context::CompactionPolicy::Threshold(
        acyclic_harness::context::ThresholdCompaction {
            response_reserve_tokens: 1024,
            recent_tokens: 2048,
            retention: Default::default(),
        },
    );
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let filesystem = acyclic_fs::Fs::new(
        acyclic_fs::MemoryAuthorityBackend::new(Arc::new(acyclic_stream::MemoryStream::default())),
        acyclic_fs::CachedObjectStore::new(
            acyclic_fs::LogicalObjectStore::new(Arc::new(objects), bucket),
            Default::default(),
        )
        .map_err(|_| invalid("fixture cache"))?,
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let result = crate::tests::native_tests::stock::run_stock_turn(
        provider,
        Adapter::Responses,
        policy,
        filesystem,
    )
    .await;
    if broken {
        assert!(result.is_err());
        assert_eq!(backend.counts.load(Ordering::SeqCst), 1);
        assert_eq!(backend.generations.load(Ordering::SeqCst), 0);
    } else {
        result?;
        assert_eq!(backend.counts.load(Ordering::SeqCst), 0);
        assert_eq!(backend.generations.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
