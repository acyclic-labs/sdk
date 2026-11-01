use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness_core::model::{ModelEvent, ModelProvider};
use futures::StreamExt as _;
use std::sync::atomic::AtomicUsize;

const AUTH_ERROR: &str = "data: {\"type\":\"error\",\"error\":{\"code\":\"invalid_api_key\",\"type\":\"authentication_error\"}}\n\n";
const TEXT: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n";

#[tokio::test]
async fn credential_read_failures_never_become_terminal_model_refusals() -> Result<()> {
    for native in [false, true] {
        for recovery in [false, true] {
            let (root, login, server, _) = fixture(if recovery {
                vec![(401, String::new())]
            } else {
                Vec::new()
            })?;
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
            let path = root.path().join("auth.json");
            if recovery {
                // Remove only this fixture after the actual first HTTP refusal,
                // before recovery rereads it. No timer or competing filesystem task.
                *server
                    .remove_auth_on_unauthorized
                    .lock()
                    .map_err(|_| invalid("fixture poisoned"))? = Some(path);
            } else {
                std::fs::remove_file(path).map_err(|error| invalid(error.to_string()))?;
            }
            let result: Result<()> = if native {
                async {
                    let mut events = provider
                        .generate_with_native_work(&request, dispatch, &work)?
                        .await?;
                    while let Some(event) = events.next().await {
                        drop(event?);
                    }
                    Ok(())
                }
                .await
            } else {
                provider
                    .generate(&request, dispatch)
                    .try_collect::<Vec<_>>()
                    .await
                    .map(|_| ())
            };
            let error = match result {
                Err(error) => error,
                Ok(()) => {
                    return Err(invalid(
                        "missing fixture credentials unexpectedly succeeded",
                    ));
                }
            };
            assert_eq!(error.code(), acyclic_harness_core::wire::ErrorCode::Invalid);
            if native {
                assert!(
                    matches!(&error, Error::NativeDiagnostic { custody, .. }
                    if custody.work().shares_allocation_context(&work)),
                    "{error:?}"
                );
            } else {
                assert!(matches!(&error, Error::Invalid(_)), "{error:?}");
            }
            drop(error);
            let mut requests = server
                .requests
                .lock()
                .map_err(|_| invalid("fixture poisoned"))?;
            assert_eq!(requests.len(), usize::from(recovery));
            requests.clear();
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_and_native_refresh_only_before_successful_response_admission() -> Result<()> {
    for native in [false, true] {
        for (first, second, succeeds, expected, refresh_failure) in [
            (
                (200, AUTH_ERROR.to_owned()),
                (200, RESPONSE.to_owned()),
                false,
                1,
                None,
            ),
            (
                (401, String::new()),
                (200, RESPONSE.to_owned()),
                true,
                3,
                None,
            ),
            (
                (401, String::new()),
                (200, AUTH_ERROR.to_owned()),
                false,
                3,
                None,
            ),
            (
                (200, format!("{TEXT}{AUTH_ERROR}")),
                (200, RESPONSE.to_owned()),
                false,
                1,
                None,
            ),
        ]
        .into_iter()
        .chain(
            [
                acyclic_harness_core::ModelTransportFailure::Connect,
                acyclic_harness_core::ModelTransportFailure::Timeout,
                acyclic_harness_core::ModelTransportFailure::Interrupted,
                acyclic_harness_core::ModelTransportFailure::Tls,
                acyclic_harness_core::ModelTransportFailure::Status(503),
            ]
            .map(|failure| {
                (
                    (401, String::new()),
                    (200, RESPONSE.to_owned()),
                    false,
                    2,
                    Some(failure),
                )
            }),
        ) {
            let (_root, login, server, _) = fixture(vec![
                first,
                (200, json!({"access_token":"rotated-access"}).to_string()),
                second,
            ])?;
            *server.refresh_failure.lock().unwrap() = refresh_failure;
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
                assert!(events.next().await.is_none(), "one original terminal");
            } else {
                let mut events = provider.generate(&request, dispatch);
                while let Some(event) = events.next().await {
                    match event {
                        Ok(event) => completed |= matches!(event, ModelEvent::Completed { .. }),
                        Err(cause) => {
                            error = Some(cause);
                            break;
                        }
                    }
                }
                assert!(events.next().await.is_none(), "one original terminal");
            }
            assert_eq!(completed, succeeds);
            if let Some(expected) = refresh_failure {
                assert!(
                    matches!(error.as_ref(), Some(Error::ModelUnavailable { failure, .. })
                    if *failure == expected),
                    "{error:?}"
                );
            } else if !succeeds {
                assert_eq!(
                    error.as_ref().unwrap().code(),
                    acyclic_harness_core::wire::ErrorCode::Unauthorized
                );
            }
            drop(error);
            let mut requests = server.requests.lock().unwrap();
            assert_eq!(requests.len(), expected, "native={native}");
            if refresh_failure.is_some() {
                assert_eq!(requests[1].url, "https://auth.openai.com/oauth/token");
                assert!(
                    server.replies.lock().unwrap().len() == 2,
                    "a failed refresh cannot consume the old-policy success replies"
                );
            }
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
    fn recovery_is_never_replenished_after_acceptance_or_failure(
        trace in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY,
            proptest::sample::select(vec![400u16, 401, 403, 408, 413, 429, 503])), 0..64),
    ) {
        let mut attempt = crate::dispatch::GenerationAttempt::new(crate::RetrySpec::DEFAULT,
            acyclic_harness_core::model::ModelDispatch {
                operation_id: acyclic_harness_core::OperationId::from_bytes([3; 16]),
                step: 1,
                request_digest: [0; 32],
            });
        let (mut accepted, mut spent) = (false, false);
        for failure in [acyclic_harness_core::ModelTransportFailure::Interrupted,
            acyclic_harness_core::ModelTransportFailure::Timeout] {
            let error = crate::transport_failure(true, failure, "fixture");
            proptest::prop_assert_eq!(attempt.redeliver(&error, None), None);
        }
        for (admitted, unauthorized, status) in trace {
            accepted |= admitted;
            if admitted { attempt.accept(); }
            if unauthorized { attempt.refuse(status); }
            let error = if unauthorized { Error::Unauthorized("fixture".into()) } else { Error::Invalid("fixture".into()) };
            let expected = unauthorized && status == 401 && !accepted && !spent;
            proptest::prop_assert_eq!(attempt.refresh(&error), expected);
            let interruption = crate::transport_failure(true,
                acyclic_harness_core::ModelTransportFailure::Interrupted, "fixture");
            if accepted { proptest::prop_assert_eq!(attempt.redeliver(&interruption, None), None); }
            spent |= expected;
        }
    }
}

struct CancelRefresh {
    cancel: CancellationToken,
}
impl HttpTransport for CancelRefresh {
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
    ) -> acyclic_harness_core::Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        crate::fixtures::native_delay(wait, work)
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
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            if request.url.ends_with("/oauth/token") {
                self.cancel.cancel();
                std::future::pending().await
            } else {
                drop(request);
                HttpResponse::with_native_body(401, futures::stream::iter([Ok(Bytes::new())]), work)
            }
        })
    }
}
#[tokio::test]
async fn cancellation_during_authentication_recovery_releases_the_guard_and_all_custody()
-> Result<()> {
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
        Arc::new(CancelRefresh { cancel }),
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
    Ok(())
}
