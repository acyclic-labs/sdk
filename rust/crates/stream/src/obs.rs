//! Span helpers for the native providers and transports; see `docs/observability.md`.

use tracing::Span;

pub(crate) use acyclic_native_runtime::obs::{OperationSpan, Scoped};

/// Records `outcome` and, on failure, `error.kind` on the owned operation span.
#[cfg(any(test, feature = "http", feature = "local"))]
pub(crate) fn finish<T>(span: &Span, result: Result<T, crate::StreamError>) -> Result<T, crate::StreamError> {
    match &result {
        Ok(_) => { span.record("outcome", "ok"); }
        Err(error) => failed(span, error.code()),
    }
    result
}

/// Records a failure of `kind` on the owned operation span.
pub(crate) fn failed(span: &Span, kind: &'static str) {
    span.record("outcome", "err").record("error.kind", kind);
}

/// Records cancellation if the caller drops an unfinished operation.
#[cfg(any(feature = "http", feature = "local"))]
pub(crate) struct Cancellation(pub(crate) Option<OperationSpan>);
#[cfg(any(feature = "http", feature = "local"))]
impl Drop for Cancellation {
    fn drop(&mut self) {
        if let Some(span) = &self.0 { failed(span, "cancelled"); }
    }
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "the capture registry indexes known live span IDs and assertions require exact recorded fields"
)]
pub(crate) mod tests {
    use acyclic_grpc_observability::test_support::Capture;
    use std::sync::Arc;
    use tracing_subscriber::layer::SubscriberExt as _;

    #[tokio::test]
    async fn scoped_cancellation_closes_nested_spans_under_origin_dispatch() {
        use tracing::Instrument as _;
        let captured = Capture::default();
        let (operation, future) = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = tracing::info_span!("origin_parent");
                let operation =
                    super::OperationSpan::new(tracing::info_span!(parent: &parent, "operation"));
                let nested = operation.in_scope(|| tracing::info_span!("nested"));
                let future = operation.scope(std::future::pending::<()>().instrument(nested));
                (operation, future)
            },
        );
        let other = Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let foreign_parent = tracing::info_span!("foreign_parent");
        let _entered = foreign_parent.enter();
        let mut future = Box::pin(future);
        assert!(futures::poll!(future.as_mut()).is_pending());
        drop(operation);
        assert_eq!(
            captured.closed("origin_parent"),
            0,
            "nested reference retains the actual origin parent"
        );
        drop(future);
        for name in ["nested", "operation", "origin_parent"] {
            assert_eq!(
                captured.closed(name),
                1,
                "{name} closes exactly once on its own registry"
            );
        }
        assert_eq!(other.closed("foreign_parent"), 0);
    }

    #[tokio::test]
    async fn filtered_operation_retains_origin_caller_for_child_poll_and_drop() {
        let captured = Capture::default();
        let future = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default()
                .with(captured.clone())
                .with(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.name() != "filtered_operation"
                })),
            || {
                let parent = tracing::info_span!("origin_caller", outcome = "caller");
                let _entered = parent.enter();
                let operation = super::OperationSpan::new(tracing::info_span!(
                    "filtered_operation",
                    outcome = tracing::field::Empty
                ));
                assert!(operation.is_disabled());
                operation.scope(async {
                    let child = tracing::info_span!("visible_child");
                    std::future::pending::<()>().await;
                    drop(child);
                })
            },
        );
        assert_eq!(
            captured.closed("origin_caller"),
            0,
            "fallback retains caller after its external handle drops"
        );
        let other = Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_caller", outcome = "caller");
        let _entered = parent.enter();
        let mut future = Box::pin(future);
        assert!(futures::poll!(future.as_mut()).is_pending());
        assert_eq!(captured.parent("visible_child"), Some("origin_caller"));
        assert!(other.all("visible_child").is_empty());
        drop(future);
        assert_eq!(captured.closed("visible_child"), 1);
        assert_eq!(captured.closed("origin_caller"), 1);
        assert_eq!(captured.fields("origin_caller")["outcome"], "caller");
        assert_eq!(other.closed("foreign_caller"), 0);
        assert_eq!(other.fields("foreign_caller")["outcome"], "caller");
    }

    #[tokio::test]
    async fn scoped_stream_seals_eof_and_drops_producer_under_origin() {
        use futures::StreamExt as _;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let captured = Capture::default();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut body = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = tracing::info_span!("origin_parent");
                let operation =
                    super::OperationSpan::new(tracing::info_span!(parent: &parent, "operation"));
                let nested = operation.in_scope(|| tracing::info_span!("producer"));
                let count = Arc::clone(&polls);
                operation.scope(futures::stream::poll_fn(move |_| {
                    let _retained = &nested;
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        std::task::Poll::Ready(None)
                    } else {
                        std::task::Poll::Ready(Some(99_u8))
                    }
                }))
            },
        );
        let other = Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_parent");
        let _entered = parent.enter();
        assert!(body.next().await.is_none());
        assert_eq!(
            captured.closed("producer"),
            1,
            "EOF destroys the producer immediately under its origin"
        );
        assert!(body.next().await.is_none());
        assert!(body.next().await.is_none());
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "non-fused producer must never be repolled after EOF"
        );
        drop(body);
        assert_eq!(captured.closed("operation"), 1);
        assert_eq!(captured.closed("origin_parent"), 1);
        assert_eq!(other.closed("foreign_parent"), 0);
    }

    #[test]
    fn filtered_completion_does_not_mutate_visible_parent() {
        let captured = Capture::default();
        let subscriber = tracing_subscriber::Registry::default()
            .with(captured.clone())
            .with(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.name() != "filtered.operation"
            }));
        tracing::subscriber::with_default(subscriber, || {
            let parent =
                tracing::info_span!("caller", outcome = "caller", error.kind = "caller", rev = 7);
            let _entered = parent.enter();
            let child = tracing::info_span!(
                "filtered.operation",
                outcome = tracing::field::Empty,
                error.kind = tracing::field::Empty,
                rev = tracing::field::Empty
            );
            assert!(child.is_disabled());
            child.record("rev", 99);
            super::finish(&child, Ok::<_, crate::StreamError>(())).unwrap();
            assert!(super::finish(&child, Err::<(), _>(crate::StreamError::AccessDenied)).is_err());
            assert_eq!(captured.fields("caller")["outcome"], "caller");
            assert_eq!(captured.fields("caller")["error.kind"], "caller");
            assert_eq!(captured.fields("caller")["rev"], "7");
        });
    }

    #[test]
    fn completion_records_owned_span_without_entering_it() {
        let captured = Capture::default();
        tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let operation = tracing::info_span!(
                    "operation",
                    outcome = tracing::field::Empty,
                    error.kind = tracing::field::Empty
                );
                let caller = tracing::info_span!("caller", outcome = "caller");
                let _entered = caller.enter();
                super::failed(&operation, "access_denied");
                assert_eq!(captured.fields("operation")["outcome"], "err");
                assert_eq!(captured.fields("operation")["error.kind"], "access_denied");
                assert_eq!(captured.fields("caller")["outcome"], "caller");
            },
        );
    }
}
