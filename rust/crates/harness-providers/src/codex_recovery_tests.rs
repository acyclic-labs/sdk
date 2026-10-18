use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::model::{ModelEvent, ModelProvider};
use futures::StreamExt as _;
use std::sync::atomic::AtomicUsize;

const AUTH_ERROR: &str = "data: {\"type\":\"error\",\"error\":{\"code\":\"invalid_api_key\",\"type\":\"authentication_error\"}}\n\n";
const TEXT: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n";

#[tokio::test]
async fn ordinary_and_native_share_one_retry_budget_for_headers_and_preoutput_sse() -> Result<()> {
    for native in [false, true] {
        for (first, second, succeeds, expected) in [
            (
                (200, AUTH_ERROR.to_owned()),
                (200, RESPONSE.to_owned()),
                true,
                3,
            ),
            (
                (200, AUTH_ERROR.to_owned()),
                (200, AUTH_ERROR.to_owned()),
                false,
                3,
            ),
            ((401, String::new()), (200, AUTH_ERROR.to_owned()), false, 3),
            ((200, AUTH_ERROR.to_owned()), (401, String::new()), false, 3),
            (
                (200, format!("{TEXT}{AUTH_ERROR}")),
                (200, RESPONSE.to_owned()),
                false,
                1,
            ),
        ] {
            let (_root, login, server, _) = fixture(vec![
                first,
                (200, json!({"access_token":"rotated-access"}).to_string()),
                second,
            ])?;
            let work = NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let transport: Arc<dyn HttpTransport> = if native {
                Arc::new(super::native::NativeServer {
                    inner: server.clone(),
                    work: work.clone(),
                    sent: AtomicUsize::new(0),
                })
            } else {
                server.clone()
            };
            let provider =
                Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport)
                    .with_codex_login(login)
                    .resolve("openai/gpt-5")?;
            let (request, dispatch) = crate::tests::request(&provider)?;
            let mut completed = false;
            let mut error = None;
            if native {
                let mut events = provider
                    .generate_with_native_work(&request, dispatch, &work)?
                    .await?;
                while let Some(event) = events.next().await {
                    match event {
                        Ok((event, custody)) => {
                            completed |= matches!(event, ModelEvent::Completed { .. });
                            drop((event, custody));
                        }
                        Err(cause) => {
                            error = Some(cause);
                            break;
                        }
                    }
                }
            } else {
                let mut events = provider.generate(request, dispatch);
                while let Some(event) = events.next().await {
                    match event {
                        Ok(event) => completed |= matches!(event, ModelEvent::Completed { .. }),
                        Err(cause) => {
                            error = Some(cause);
                            break;
                        }
                    }
                }
            }
            assert_eq!(completed, succeeds);
            if !succeeds {
                assert_eq!(
                    error.as_ref().unwrap().code(),
                    acyclic_harness::wire::ErrorCode::Unauthorized
                );
            }
            drop(error);
            let mut requests = server.requests.lock().unwrap();
            assert_eq!(requests.len(), expected, "native={native}");
            if expected == 3 {
                assert_eq!(requests[0].body, requests[2].body);
                assert_eq!(
                    requests[2].headers["authorization"],
                    "Bearer rotated-access"
                );
            }
            requests.clear();
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn retry_budget_is_never_replenished_after_output_or_failure(
        trace in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY), 0..64),
    ) {
        let mut recovery = crate::dispatch::Recovery::default();
        let (mut output, mut spent) = (false, false);
        for (emitted, unauthorized) in trace {
            output |= emitted;
            recovery.output |= emitted;
            let error = if unauthorized { Error::Unauthorized("fixture".into()) } else { Error::Invalid("fixture".into()) };
            let expected = unauthorized && !output && !spent;
            proptest::prop_assert_eq!(recovery.retry(&error), expected);
            spent |= expected;
        }
    }
}

struct CancelRefresh {
    cancel: CancellationToken,
    first: (u16, &'static str),
}
impl HttpTransport for CancelRefresh {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("native HTTP required")
    }
    fn supports_native(&self, _: &NativeMaterializationWork) -> Result<bool> {
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            if request.url.ends_with("/oauth/token") {
                self.cancel.cancel();
                std::future::pending().await
            } else {
                drop(request);
                HttpResponse::with_native_body(
                    self.first.0,
                    futures::stream::iter([Ok(Bytes::from_static(self.first.1.as_bytes()))]),
                    work,
                )
            }
        })
    }
}
#[tokio::test]
async fn cancellation_during_authentication_recovery_releases_the_guard_and_all_custody()
-> Result<()> {
    for first in [(401, ""), (200, AUTH_ERROR)] {
        let (_root, login, _, _) = fixture(Vec::new())?;
        let cancel = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
            &cancel,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let provider = Registry::with_transport(
            Credentials::explicit(BTreeMap::new()),
            Arc::new(CancelRefresh { cancel, first }),
        )
        .with_codex_login(login)
        .resolve("openai/gpt-5")?;
        let (request, dispatch) = crate::tests::request(&provider)?;
        {
            let mut events = provider
                .generate_with_native_work(&request, dispatch, &work)?
                .await?;
            let error = events
                .next()
                .await
                .expect("cancellation observed")
                .expect_err("cancelled recovery");
            assert!(error.to_string().contains("cancel"));
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
