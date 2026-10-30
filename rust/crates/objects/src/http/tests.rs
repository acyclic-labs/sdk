//! The shipping HTTP client against canonical dispatch over a real engine.
use super::*;
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode},
    response::Response as HttpResponse,
};
use futures::{FutureExt as _, StreamExt as _};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone)]
struct Service {
    store: crate::Store<crate::MemoryMedium>,
    granted: Arc<AtomicBool>,
}

async fn dispatch(
    State(service): State<Service>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<HttpResponse, StatusCode> {
    let account = match headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer first") if service.granted.load(Ordering::SeqCst) => b"first".as_slice(),
        Some("Bearer second") => b"second",
        _ => return Err(StatusCode::FORBIDDEN),
    };
    let blobs = service
        .store
        .account(account)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let route = uri
        .path()
        .strip_prefix("/v1/objects/")
        .ok_or(StatusCode::NOT_FOUND)?;
    if let Some((_, input, _)) = crate::HTTP_STREAM_ROUTES
        .iter()
        .find(|(path, ..)| *path == route)
    {
        let bytes =
            json::decode_binary(input, &body, 1024 * 1024).map_err(|_| StatusCode::BAD_REQUEST)?;
        let events = crate::route::invoke_stream(&blobs, route, bytes.into())
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let frames = events.map(move |event| {
            if account == b"first" && !service.granted.load(Ordering::SeqCst) {
                return Err(Error::from(wire::ErrorCode::AccessDenied));
            }
            event
        });
        return HttpResponse::builder()
            .header("content-type", "application/x-protobuf")
            .body(Body::from_stream(frames))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
    }
    let (_, input, output) = crate::HTTP_ROUTES
        .iter()
        .find(|(path, ..)| *path == route)
        .ok_or(StatusCode::NOT_FOUND)?;
    let input =
        json::decode_binary(input, &body, 1024 * 1024).map_err(|_| StatusCode::BAD_REQUEST)?;
    let output = match crate::route::invoke(&blobs, route, input.into()).await {
        Ok(bytes) => json::encode_binary(output, &bytes, 1024 * 1024)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        Err(error) => {
            let bytes = json::encode(
                "ErrorDetail",
                &wire::ErrorDetail {
                    code: error.code as i32,
                    ..Default::default()
                },
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            return HttpResponse::builder()
                .status(400)
                .header("content-type", "application/json")
                .body(Body::from(bytes))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    HttpResponse::builder()
        .header("content-type", "application/json")
        .body(Body::from(output))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[tokio::test]
async fn http_availability_uses_shared_accounts_framing_and_terminal_failure() -> TestResult {
    let service = Service {
        store: crate::Store::new(crate::MemoryMedium::default()),
        granted: Arc::new(AtomicBool::new(true)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:15441").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let router = axum::Router::new()
        .fallback(dispatch)
        .with_state(service.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    struct Stop(tokio::task::AbortHandle);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _stop = Stop(server.abort_handle());
    let first = HttpObjects::new(&endpoint, "first", 1024 * 1024, None)?;
    let second = HttpObjects::new(&endpoint, "second", 1024 * 1024, None)?;
    crate::conformance::verify(&first, &second, "http-availability").await?;
    let digest = Digest::of(b"HTTP physical restoration");
    let mut observation = first.availability(digest).await?;
    let mut isolated = second.availability(digest).await?;
    assert!(
        matches!(HttpObjects::new(&endpoint, "denied", 1024, None)?.availability(digest).await, Err(error) if error.code == wire::ErrorCode::AccessDenied)
    );
    let account = service.store.account(b"first")?;
    account.availability_source().restored(digest)?;
    observation.changed().await?;
    assert!(isolated.changed().now_or_never().is_none());
    assert_eq!(
        first.head(digest).await.unwrap_err().code,
        wire::ErrorCode::NotFound
    );
    account.availability_source().fail();
    assert_eq!(
        observation.changed().await.unwrap_err().code,
        wire::ErrorCode::Unavailable
    );
    service
        .store
        .account(b"second")?
        .availability_source()
        .fail();
    assert_eq!(
        isolated.changed().await.unwrap_err().code,
        wire::ErrorCode::Unavailable
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn http_availability_retains_original_context_at_open_and_delivery() -> TestResult {
    let service = Service {
        store: crate::Store::new(crate::MemoryMedium::default()),
        granted: Arc::new(AtomicBool::new(true)),
    };
    let source = service
        .store
        .account(b"first")?
        .availability_source()
        .clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:15442").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let router = axum::Router::new().fallback(dispatch).with_state(service);
    let (stop, stopped) = futures::channel::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let result = crate::availability::client::context_tests::verify(
        |context, credential| {
            Box::new(
                HttpObjects::with_credential_provider(&endpoint, credential, 1024 * 1024, None)
                    .unwrap()
                    .with_context(context),
            )
        },
        source,
    )
    .await;
    stop.send(()).unwrap();
    server.await??;
    result?;
    Ok(())
}

#[tokio::test]
async fn http_put_batch_splits_by_the_reply_budget_and_every_member_commits() -> TestResult {
    const BUDGET: usize = 1024;
    const MEMBERS: usize = 100;
    let service = Service {
        store: crate::Store::new(crate::MemoryMedium::default()),
        granted: Arc::new(AtomicBool::new(true)),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let router = axum::Router::new()
        .fallback(
            move |state: State<Service>, uri: OriginalUri, headers: HeaderMap, body: Bytes| {
                if uri.0.path().ends_with("/blobs/put-batch") {
                    counted.fetch_add(1, Ordering::SeqCst);
                }
                dispatch(state, uri, headers, body)
            },
        )
        .with_state(service);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    struct Stop(tokio::task::AbortHandle);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _stop = Stop(server.abort_handle());
    let objects = HttpObjects::new(&endpoint, "first", BUDGET, None)?;
    let (single, step) = put_batch_acknowledgement().ok_or("acknowledgement bound")?;
    let per_call = 1 + (BUDGET - single) / step;
    // Negative control: one reply answering every member exceeds the budget.
    assert!(single + (MEMBERS - 1) * step > BUDGET);
    let batch = || {
        (0..MEMBERS as u32)
            .map(|index| {
                let body = OwnedBytes::from(index.to_le_bytes().to_vec());
                (crate::Sealed::new(body), Vec::new())
            })
            .collect::<Vec<_>>()
    };
    let results = objects.put_batch_sealed(batch()).await;
    assert_eq!(results.len(), MEMBERS);
    for (index, result) in results.iter().enumerate() {
        let put = result
            .as_ref()
            .map_err(|error| format!("member {index}: {error}"))?;
        assert_eq!(
            put.verified().map(|verified| verified.digest),
            Some(Digest::of(&(index as u32).to_le_bytes()))
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), MEMBERS.div_ceil(per_call));
    // The repeat is answered in the same number of calls.
    let repeated = objects.put_batch_sealed(batch()).await;
    assert!(repeated.iter().all(Result::is_ok));
    assert_eq!(calls.load(Ordering::SeqCst), 2 * MEMBERS.div_ceil(per_call));
    Ok(())
}
