use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::NativeFutureBundle;
use std::sync::atomic::{AtomicUsize, Ordering};
#[path = "native_stock_probe.rs"]
mod probe;
#[path = "native_stock_tests.rs"]
mod stock;

pub(super) struct Fixture {
    pub(super) response: &'static str,
    pub(super) sent: AtomicUsize,
    cancel: Option<CancellationToken>,
    pending_body: bool,
    prefix: Option<&'static str>,
}

impl Fixture {
    pub(super) fn new(response: &'static str) -> Self {
        Self {
            response,
            sent: AtomicUsize::new(0),
            cancel: None,
            pending_body: false,
            prefix: None,
        }
    }
}

impl HttpTransport for Fixture {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("native execution must use its funded HTTP callback")
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
        NativeFutureBundle::new(work, || async move {
            if let Some(response) = fixture_count(&request) {
                return HttpResponse::with_native_body(response.status, response.body, work);
            }
            self.sent.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.headers.contains_key("authorization")
                    || request.headers.contains_key("x-api-key")
                    || request.headers.contains_key("x-goog-api-key")
            );
            drop(request);
            if let Some(cancel) = &self.cancel {
                if self.pending_body {
                    let cancel = cancel.clone();
                    // Unpin stream frame; cancellation is triggered by its first poll.
                    let body = futures::stream::poll_fn(move |_| {
                        cancel.cancel();
                        std::task::Poll::<Option<Result<Bytes>>>::Pending
                    });
                    if let Some(prefix) = self.prefix {
                        return HttpResponse::with_native_body(
                            200,
                            futures::stream::iter([Ok(Bytes::from_static(prefix.as_bytes()))])
                                .chain(body),
                            work,
                        );
                    }
                    return HttpResponse::with_native_body(200, body, work);
                }
                cancel.cancel();
                return futures::future::pending().await;
            }
            HttpResponse::with_native_body(
                200,
                futures::stream::iter([Ok(Bytes::from_static(self.response.as_bytes()))]),
                work,
            )
        })
    }
}

pub(super) fn work(cancel: &CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
        cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[tokio::test]
async fn native_callbacks_stream_tools_usage_and_release_original_custody() -> Result<()> {
    for (adapter, response) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        let fixture = Arc::new(Fixture::new(response));
        provider.transport = fixture.clone();
        let (request, dispatch) = request(&provider)?;
        let work = work(&CancellationToken::new())?;
        let before = work.live_allocation_bytes();
        {
            assert!(provider.supports_native_model(&work)?);
            let (count, custody) = provider
                .count_tokens_with_native_work(&request, &work)?
                .await?;
            assert!(custody.work().shares_allocation_context(&work));
            assert_eq!(count.request_digest, request.request_digest());
            drop((count, custody));
            let mut stream = provider
                .generate_with_native_work(&request, dispatch, &work)?
                .await?;
            let mut calls = 0;
            let mut completed = 0;
            while let Some(event) = stream.next().await {
                let (event, custody) = event?;
                assert!(custody.work().shares_allocation_context(&work));
                assert_eq!(custody.bytes(), event.logical_owned_bytes()?);
                match &event {
                    ModelEvent::ToolCall(call) => {
                        calls += 1;
                        assert_eq!(call.call_id, dispatch.tool_call_id(0));
                        assert_eq!(call.metadata["provider_call"]["id"], "call");
                        assert_eq!(call.arguments, json!({"value":1}));
                    }
                    ModelEvent::Completed { metadata } => {
                        completed += 1;
                        assert!(metadata["usage"].is_object());
                    }
                    _ => {}
                }
                drop((event, custody));
            }
            assert_eq!((calls, completed), (1, 1));
        }
        assert_eq!(fixture.sent.load(Ordering::SeqCst), 1);
        assert_eq!(work.live_allocation_bytes(), before, "{adapter:?}");
    }
    Ok(())
}

#[tokio::test]
async fn native_cancellation_wakes_pending_headers_and_pending_body() -> Result<()> {
    for (pending_body, prefix) in [
        (false, None),
        (true, None),
        (
            true,
            Some("data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n"),
        ),
    ] {
        let (mut provider, _) = provider(Adapter::Chat, 200);
        let cancel = CancellationToken::new();
        provider.transport = Arc::new(Fixture {
            cancel: Some(cancel.clone()),
            pending_body,
            prefix,
            ..Fixture::new(CHAT)
        });
        let (request, dispatch) = request(&provider)?;
        let work = work(&cancel)?;
        let baseline = work.live_allocation_bytes();
        let result = async {
            let mut stream = provider
                .generate_with_native_work(&request, dispatch, &work)?
                .await?;
            if prefix.is_some() {
                let (event, custody) = stream.next().await.expect("content before cancellation")?;
                assert_eq!(
                    event,
                    ModelEvent::Content {
                        delta: "first".into()
                    }
                );
                drop((event, custody));
            }
            stream.next().await.transpose().map(drop)
        }
        .await;
        assert!(result.is_err());
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn native_protocol_errors_keep_diagnostic_custody_and_reject_before_http_when_unfunded()
-> Result<()> {
    let (mut provider, _) = provider(Adapter::Chat, 200);
    provider.transport = Arc::new(Fixture::new("data: invalid-json\n\n"));
    let (request, dispatch) = request(&provider)?;
    let work = work(&CancellationToken::new())?;
    let baseline = work.live_allocation_bytes();
    {
        let mut stream = provider
            .generate_with_native_work(&request, dispatch, &work)?
            .await?;
        let error = stream
            .next()
            .await
            .expect("error event")
            .expect_err("invalid protocol");
        assert!(
            matches!(&error, Error::NativeDiagnostic { code: acyclic_harness::wire::ErrorCode::Invalid, custody, .. }
            if custody.work().shares_allocation_context(&work))
        );
        drop(error);
    }
    assert_eq!(work.live_allocation_bytes(), baseline);
    let transport = Arc::new(Fixture::new(CHAT));
    provider.transport = transport.clone();
    let exhausted = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, baseline),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(
        provider
            .count_tokens_with_native_work(&request, &exhausted)
            .is_err()
    );
    let result = async {
        provider
            .generate_with_native_work(&request, dispatch, &exhausted)?
            .await
            .map(drop)
    }
    .await;
    assert!(result.is_err());
    assert_eq!(transport.sent.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_counting_cancels_in_flight_http_and_returns_all_custody() -> Result<()> {
    let cancel = CancellationToken::new();
    let work = work(&cancel)?;
    let (mut provider, _) = provider(Adapter::Responses, 200);
    Arc::make_mut(&mut provider.config)
        .token_counter
        .as_mut()
        .unwrap()
        .path = "cancel_count".into();
    provider.transport = Arc::new(Fixture {
        response: "",
        sent: AtomicUsize::new(0),
        cancel: Some(cancel),
        pending_body: true,
        prefix: None,
    });
    let (request, _) = request(&provider)?;
    let baseline = work.live_allocation_bytes();
    let result = provider
        .count_tokens_with_native_work(&request, &work)?
        .await;
    assert!(matches!(
        result,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    drop(result);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
