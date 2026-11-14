use super::*;
use crate::tests::CountTestExt as _;
use acyclic_fs::NativeMaterializationWork;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Backend {
    counts: AtomicUsize,
    generations: AtomicUsize,
}
impl Backend {
    fn answer(&self, request: &HttpRequest) -> (u16, &'static str) {
        if request.url == "https://chatgpt.com/backend-api/codex/responses" {
            self.generations.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.headers["chatgpt-account-id"], "account");
            (200, RESPONSE)
        } else {
            self.counts.fetch_add(1, Ordering::SeqCst);
            (404, "no count route")
        }
    }
}

impl HttpTransport for Backend {
    fn delay(
        &self,
        wait: std::time::Duration,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness_core::Result<
        acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>,
    > {
        crate::fixtures::native_delay(wait, work)
    }
    /// The agent host's ordinary dispatch.
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let (status, body) = self.answer(&request);
            Ok(HttpResponse {
                request_id: None,
                retry_after: None,
                status,
                body: Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                    body.as_bytes(),
                ))])),
            })
        })
    }
    fn supports_native(&self, _: &NativeMaterializationWork) -> Result<bool> {
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            let (status, body) = self.answer(&request);
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
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), backend.clone())?
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
        let result = provider.counted_with_native_work(&request, 0, &work)?.await;
        if total == 65_536 {
            let (count, custody) = result?;
            assert!(custody.work().shares_allocation_context(&work));
            assert!(custody.work().shares_ledger(&work));
            assert_eq!(count.request_digest, request.request_digest());
            assert_eq!(
                count.input_tokens,
                provider.accounting.usage.codex_estimate(&request)?
            );
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
    let credentials = Credentials::explicit(if broken {
        BTreeMap::from([("openai".into(), "fixture-key".into())])
    } else {
        BTreeMap::new()
    });
    let mut registry =
        Registry::with_transport(credentials, backend.clone())?.with_codex_login(login);
    let catalog = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
    std::fs::write(catalog.path(), include_str!("fixtures/luna-catalog.json"))
        .map_err(|_| invalid("fixture catalog"))?;
    registry.load_catalog(catalog.path())?;
    let mut spec = registry.catalog()["openai"].as_ref().clone();
    spec.authentication = Authentication::ApiKeyOrCodexLogin;
    if broken {
        // The former path retained the public endpoint's counter when changing
        // base_url. Configure that original input before resolution, so each
        // credential refresh admits the same deliberately broken route.
        spec.base_url = "https://chatgpt.com/backend-api/codex".into();
        let mut counter = crate::catalog::default_catalog()?["openai"]
            .token_counter
            .clone()
            .ok_or_else(|| invalid("fixture inherited counter is absent"))?;
        // Generated counter configuration on the original catalog: framing
        // alone fills this context, so the byte shortcut cannot admit the
        // unchanged stock task. It must reach the inherited, absent route.
        counter.framing_tokens = u32::try_from(spec.models["gpt-6-luna"].capacity.context_tokens)
            .map_err(|_| invalid("fixture context exceeds counter framing"))?;
        spec.token_counter = Some(counter);
    }
    registry.register("openai".into(), spec)?;
    let provider = registry.resolve("openai/gpt-6-luna")?;
    assert_eq!(provider.config.token_counter.is_some(), broken);
    // This fixture declares an 8192-token context in its catalog. Enable
    // automatic counting with explicit compaction budgets inside that window.
    let policy = acyclic_harness_core::context::CompactionPolicy::Capacity(
        acyclic_harness_core::context::CapacityCompaction {},
    );
    let objects = acyclic_objects::MemoryObjects::memory();
    let filesystem = acyclic_fs::Fs::new(
        acyclic_fs::MemoryAuthorityBackend::new(Arc::new(acyclic_stream::MemoryStream::default())),
        acyclic_fs::CachedObjectStore::new(
            acyclic_fs::LogicalObjectStore::new(Arc::new(objects)),
            Default::default(),
        )
        .map_err(|_| invalid("fixture cache"))?,
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let result =
        crate::tests::native_tests::stock::run_stock_turn(provider, policy, filesystem).await?;
    if broken {
        // The public counter's refusal is the provider's: the turn fails without generating.
        assert!(
            matches!(
                &result,
                crate::tests::native_tests::stock::StockTurn::Failed(_)
            ),
            "{result:?}"
        );
        assert_eq!(backend.counts.load(Ordering::SeqCst), 1);
        assert_eq!(backend.generations.load(Ordering::SeqCst), 0);
    } else {
        assert_eq!(
            result,
            crate::tests::native_tests::stock::StockTurn::Settled
        );
        assert_eq!(backend.counts.load(Ordering::SeqCst), 0);
        assert_eq!(backend.generations.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
