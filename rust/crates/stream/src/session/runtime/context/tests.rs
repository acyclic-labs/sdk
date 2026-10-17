use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tracing_subscriber::{layer::Context as LayerContext, prelude::*};

struct Edges(Arc<AtomicUsize>);
impl tracing_subscriber::Layer<tracing_subscriber::Registry> for Edges {
    fn on_follows_from(
        &self,
        _: &tracing::span::Id,
        _: &tracing::span::Id,
        _: LayerContext<'_, tracing_subscriber::Registry>,
    ) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn check(foreign: bool, broken: bool) {
    let edges = Arc::new(AtomicUsize::new(0));
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Edges(edges.clone())));
    let other = tracing::Dispatch::new(
        tracing_subscriber::registry().with(Edges(Arc::new(AtomicUsize::new(0)))),
    );
    let operation = tracing::dispatcher::with_default(&dispatch, || {
        OperationSpan::new_root(tracing::info_span!(parent: None, "operation"))
    });
    let cause = tracing::dispatcher::with_default(if foreign { &other } else { &dispatch }, || {
        OperationSpan::new(tracing::info_span!("cause"))
    });
    futures::executor::block_on(operation.scope(async {
        if broken {
            operation.span.follows_from(cause.span.id());
        } else {
            operation.follows_from_same_subscriber(&cause);
        }
    }));
    assert_eq!(
        edges.load(Ordering::SeqCst),
        usize::from(!foreign),
        "causal edges cannot cross subscriber identities"
    );
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn portable_context_checks_registry_identity(foreign in any::<bool>()) {
        check(foreign, false);
    }
}
#[test]
#[should_panic(expected = "causal edges cannot cross subscriber identities")]
fn negative_control_aliases_equal_span_ids_from_different_registries() {
    check(true, true);
}
