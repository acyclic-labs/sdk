//! Regression mutations of the actual llama.cpp refusal, not fabricated fixtures.
use crate::{Error, HttpResponse};
use acyclic_harness_core::ModelTransportFailure;
use bytes::Bytes;

fn response(status: u16, padding: usize) -> HttpResponse {
    let recorded = include_bytes!("../../../conformance/fixtures/inference/llamacpp/context.body");
    HttpResponse {
        status,
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
        crate::http_failure(response(400, 1024), 2048, None).await,
        Error::ModelContextOverflow
    ));
    assert!(matches!(
        crate::http_failure(response(400, 1024), 512, None).await,
        Error::Invalid(_)
    ));
    assert!(matches!(
        crate::http_failure(response(429, 0), 2048, None).await,
        Error::ModelUnavailable {
            failure: ModelTransportFailure::Status(429),
            ..
        }
    ));
    let response = HttpResponse {
        status: 401,
        retry_after: None,
        body: Box::pin(futures::stream::poll_fn(|_| {
            panic!("authentication refusal polled its body")
        })),
    };
    assert!(matches!(
        crate::http_failure(response, 2048, None).await,
        Error::Unauthorized(_)
    ));
}
