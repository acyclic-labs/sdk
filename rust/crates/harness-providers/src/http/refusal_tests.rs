//! Regression mutations of captured llama.cpp and Gemini refusals.
use crate::{Error, HttpResponse};
use acyclic_harness_core::ModelTransportFailure;
use bytes::Bytes;

const CONTEXT: &[u8] =
    include_bytes!("../../../conformance/fixtures/inference/llamacpp/context.body");

fn response(status: u16, padding: usize, recorded: &'static [u8]) -> HttpResponse {
    HttpResponse {
        status,
        request_id: None,
        retry_after: None,
        body: Box::pin(futures::stream::iter([
            Ok(Bytes::from(vec![b' '; padding])),
            Ok(Bytes::from_static(recorded)),
        ])),
    }
}

#[tokio::test]
async fn context_classification_uses_protocol_budget_and_preserves_status_classes() {
    assert!(matches!(
        crate::http_failure(response(400, 1024, CONTEXT), 2048, None).await,
        Error::ModelContextOverflow
    ));
    let recorded = CONTEXT;
    let fragmented = HttpResponse {
        status: 400,
        request_id: None,
        retry_after: None,
        body: Box::pin(futures::stream::iter(
            recorded
                .chunks(1)
                .flat_map(|byte| [Ok(Bytes::new()), Ok(Bytes::from_static(byte))]),
        )),
    };
    assert!(matches!(
        crate::http_failure(fragmented, recorded.len(), None).await,
        Error::ModelContextOverflow
    ));
    assert!(matches!(
        crate::http_failure(response(400, 1024, CONTEXT), 512, None).await,
        Error::Invalid(_)
    ));
    assert!(matches!(
        crate::http_failure(response(429, 0, CONTEXT), 2048, None).await,
        Error::ModelUnavailable {
            failure: ModelTransportFailure::Status(429),
            ..
        }
    ));
    for status in [401, 403, 404, 409, 422] {
        let response = HttpResponse {
            status,
            request_id: None,
            retry_after: None,
            body: Box::pin(futures::stream::poll_fn(|_| {
                panic!("status-only refusal polled its discarded body")
            })),
        };
        assert_eq!(
            crate::http_failure(response, 2048, None).await.code(),
            crate::http_error(status).code(),
        );
    }
}

#[tokio::test]
async fn shared_refusal_admission_retains_only_the_original_error() -> crate::Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let baseline = NativeMaterializationWork::accounting_state_bytes();
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 8_000_000),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let gemini = &include_bytes!("../../fuzz/corpus/http_failure/gemini-signature")[2..];
    for (status, recorded) in [400, 401, 403, 404, 408, 413, 429, 503]
        .into_iter()
        .map(|status| (status, CONTEXT))
        .chain([(400, gemini), (413, gemini)])
    {
        let ordinary = crate::diagnostic::rejection(
            crate::http_failure(response(status, 0, recorded), 2048, None).await,
        );
        let native = crate::http_failure(response(status, 0, recorded), 2048, Some(&work)).await;
        assert_eq!(native.code(), ordinary.code());
        assert_eq!(native.to_string(), ordinary.to_string());
        if let Error::ModelDiagnostic {
            message, custody, ..
        } = &native
        {
            if matches!(status, 400 | 413) {
                assert!(message.contains(&status.to_string()), "{message}");
                assert!(
                    message.contains("Function call is missing a thought_signature"),
                    "{message}"
                );
                assert!(message.len() <= crate::EXCERPT_BYTES);
                assert!(message.chars().all(|character| !character.is_control()));
            }
            let hold = custody.as_ref().expect("original diagnostic custody");
            assert!(hold.work().shares_allocation_context(&work));
            assert_eq!(hold.bytes(), message.capacity() as u64);
            assert_eq!(work.live_allocation_bytes(), baseline + hold.bytes());
        } else {
            assert!(matches!(native, Error::ModelContextOverflow));
        }
        drop(native);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    for drop_consumer in [false, true] {
        let (producer, terminal) = futures::channel::oneshot::channel::<()>();
        let response = HttpResponse {
            status: 503,
            request_id: None,
            retry_after: None,
            body: Box::pin(futures::stream::once(async move {
                assert!(terminal.await.is_err());
                Err(Error::NativeBoundary {
                    code: acyclic_harness_core::wire::ErrorCode::Storage,
                    reason: "original refusal body producer lost",
                })
            })),
        };
        let mut refusal = Box::pin(crate::http_failure(response, 2048, Some(&work)));
        assert!(futures::poll!(refusal.as_mut()).is_pending());
        assert!(work.live_allocation_bytes() > baseline);
        if drop_consumer {
            drop(refusal);
            assert!(producer.is_canceled());
        } else {
            drop(producer);
            let error = refusal.await;
            assert!(matches!(
                error,
                Error::NativeBoundary {
                    code: acyclic_harness_core::wire::ErrorCode::Storage,
                    reason: "original refusal body producer lost",
                }
            ));
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    let denied = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, baseline),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let response = HttpResponse {
        status: 503,
        request_id: None,
        retry_after: None,
        body: Box::pin(futures::stream::poll_fn(|_| {
            panic!("unfunded refusal polled its body")
        })),
    };
    assert!(matches!(
        crate::http_failure(response, 2048, Some(&denied)).await,
        Error::NativeWork { .. }
    ));
    assert_eq!(denied.live_allocation_bytes(), baseline);
    Ok(())
}
