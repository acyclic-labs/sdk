use super::*;
use acyclic_fs::NativeMaterializationWork;
use acyclic_harness_core::NativeFutureBundle;
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
    ) -> acyclic_harness_core::Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
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

#[tokio::test]
async fn local_count_reservation_boundary_65536_and_65537_keeps_the_original_ledger() -> Result<()>
{
    use acyclic_fs::{CancellationToken, WorkCounters, WorkError};
    let (_root, login, _, _) = fixture(Vec::new())?;
    let backend = Arc::new(Backend {
        counts: AtomicUsize::new(0),
        generations: AtomicUsize::new(0),
    });
    let provider =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), backend.clone())
            .with_codex_login(login)
            .resolve("openai/gpt-5")?;
    let (request, _) = crate::tests::request(&provider)?;
    let items = request.request().messages.len() as u64;
    assert_eq!(items, 2);
    // The public wrapper registers one waiter and withholds three items for
    // cancellation delivery/retirement. Ordinary completion uses one of those
    // three cleanup items and refunds the other two; these are distinct costs.
    const REGISTER: u64 = 1;
    const CLEANUP_RESERVED: u64 = 3;
    const RETIRE: u64 = 1;
    let admitted_items = items + REGISTER + CLEANUP_RESERVED;
    let completed_items = items + REGISTER + RETIRE;
    for total in [65_536, 65_537] {
        let work = NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 4 * 1024 * 1024),
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let prior = total - admitted_items;
        work.charge(WorkCounters {
            items_examined: prior,
            ..WorkCounters::UNCHARGED
        })?;
        let baseline = work.live_allocation_bytes();
        let result = provider
            .count_tokens_with_native_work(&request, &work)?
            .await;
        if total == 65_536 {
            let (count, custody) = result?;
            assert!(custody.work().shares_allocation_context(&work));
            assert!(custody.work().shares_ledger(&work));
            assert_eq!(count.request_digest, request.request_digest());
            assert_eq!(count.input_tokens, provider.codex_count.count(&request)?);
            assert_eq!(work.receipt().items_examined, prior + completed_items);
            drop((count, custody));
        } else {
            let Err(Error::NativeWork {
                cause:
                    acyclic_fs::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                        counter: "items_examined",
                        observed,
                        maximum,
                    }),
                ..
            }) = result
            else {
                panic!("count did not preserve its native refusal: {result:?}");
            };
            // Work errors carry the effective ceiling while cleanup is held.
            assert_eq!(observed + CLEANUP_RESERVED, total);
            assert_eq!(maximum + CLEANUP_RESERVED, 65_536);
            assert_eq!(work.receipt().items_examined, prior + REGISTER + RETIRE);
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    assert_eq!(backend.counts.load(Ordering::SeqCst), 0);
    assert_eq!(backend.generations.load(Ordering::SeqCst), 0);
    Ok(())
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
    let policy = acyclic_harness_core::context::CompactionPolicy::Threshold(
        acyclic_harness_core::context::ThresholdCompaction {
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
    .await?;
    if broken {
        // The public counter's refusal is the provider's: the task fails without generating.
        assert!(
            matches!(&result, acyclic_harness_core::Outcome::Failed { .. }),
            "{result:?}"
        );
        assert_eq!(backend.counts.load(Ordering::SeqCst), 1);
        assert_eq!(backend.generations.load(Ordering::SeqCst), 0);
    } else {
        assert!(
            matches!(&result, acyclic_harness_core::Outcome::Succeeded(_)),
            "{result:?}"
        );
        assert_eq!(backend.counts.load(Ordering::SeqCst), 0);
        assert_eq!(backend.generations.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
