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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt as _;

    type Fields = HashMap<String, String>;
    #[derive(Default)]
    struct Captured {
        spans: Vec<(&'static str, Fields)>,
        parents: Vec<Option<usize>>,
        live: HashMap<u64, usize>,
    }
    #[derive(Clone, Default)]
    pub(crate) struct Capture(Arc<Mutex<Captured>>);
    struct Visitor<'a>(&'a mut Fields);
    impl tracing::field::Visit for Visitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Fields::new();
            attributes.record(&mut Visitor(&mut fields));
            let parent = attributes.parent().cloned().or_else(|| {
                attributes
                    .is_contextual()
                    .then(|| context.current_span().id().cloned())
                    .flatten()
            });
            let mut captured = self.0.lock().unwrap();
            let index = captured.spans.len();
            let parent = parent.and_then(|parent| captured.live.get(&parent.into_u64()).copied());
            captured.spans.push((attributes.metadata().name(), fields));
            captured.parents.push(parent);
            captured.live.insert(id.into_u64(), index);
        }
        fn on_record(
            &self,
            id: &tracing::span::Id,
            record: &tracing::span::Record<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut captured = self.0.lock().unwrap();
            let index = captured.live[&id.into_u64()];
            record.record(&mut Visitor(&mut captured.spans[index].1));
        }
        fn on_close(&self, id: tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
            self.0.lock().unwrap().live.remove(&id.into_u64());
        }
    }

    impl Capture {
        pub(crate) fn parent(&self, name: &str) -> Option<&'static str> {
            let captured = self.0.lock().unwrap();
            captured
                .spans
                .iter()
                .position(|(found, _)| *found == name)
                .and_then(|index| captured.parents[index])
                .map(|index| captured.spans[index].0)
        }
        pub(crate) fn closed(&self, name: &str) -> usize {
            let captured = self.0.lock().unwrap();
            captured
                .spans
                .iter()
                .enumerate()
                .filter(|(index, (found, _))| {
                    *found == name && !captured.live.values().any(|live| live == index)
                })
                .count()
        }
        pub(crate) fn all(&self, name: &str) -> Vec<Fields> {
            self.0
                .lock()
                .unwrap()
                .spans
                .iter()
                .filter(|(found, _)| *found == name)
                .map(|(_, fields)| fields.clone())
                .collect()
        }
        pub(crate) fn fields(&self, name: &str) -> Fields {
            self.0
                .lock()
                .unwrap()
                .spans
                .iter()
                .find(|(found, _)| *found == name)
                .map(|(_, fields)| fields.clone())
                .unwrap_or_default()
        }
    }

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
