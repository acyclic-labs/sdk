//! Exact work on the production receive path, with a redundant-lookup control.
use super::*;
use acyclic_test_support::{
    AllocationInfo,
    differential::{pair, record},
    measure,
};
use futures::FutureExt;
use proptest::prelude::*;
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

#[derive(Debug, PartialEq, Eq)]
enum Trace {
    Input(Vec<u8>),
    Field(&'static str, &'static str, String),
    Closed(&'static str),
}
struct Observer;
struct Fields(&'static str);
impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        record(Trace::Field(self.0, field.name(), format!("{value:?}")));
    }
}
impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Observer {
    fn on_record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        values.record(&mut Fields(ctx.span(id).unwrap().name()));
    }
    fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
        record(Trace::Closed(ctx.span(&id).unwrap().name()));
    }
}
struct Immediate;
impl crate::runtime::Executor for Immediate {
    fn spawn(&self, task: BoxProviderFuture<'static, ()>) {
        assert_eq!(
            task.now_or_never(),
            Some(()),
            "terminal delivery must fit the reserved slot"
        );
    }
    fn sleep(&self, _: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(future::pending())
    }
}
struct State {
    pool: Pool,
    input: Vec<wire::SessionFrame>,
    ready: Vec<oneshot::Receiver<Result<(u64, Option<wire::SessionOpened>), StreamError>>>,
    records: Vec<mpsc::Receiver<Result<Delivery, StreamError>>>,
    sender: Arc<Outbound<wire::SessionFrame>>,
    _registration: tracing::Dispatch,
    dispatch: tracing::Dispatch,
}

fn initialize(n: usize, kind: u8) -> State {
    let endpoint =
        tonic::transport::Endpoint::from_static("http://route-lookups.invalid").connect_lazy();
    let (mut pool, _) = fixture::fixture(&endpoint);
    pool.routes.clear();
    pool.executor = std::sync::Arc::new(Immediate);
    let sender = Arc::new(Outbound::new(2 * n + 4, crate::MAX_COMMAND_BYTES));
    pool.links.get_mut("good").unwrap().sender = Arc::clone(&sender);
    // Warm this independently owned queue before measuring receive, as on an
    // established physical session; no candidate/baseline storage is shared.
    for _ in 0..n {
        sender
            .push(frame(0, 0, Body::Window(Default::default())), 0)
            .unwrap();
    }
    for _ in 0..n {
        sender.next().now_or_never().unwrap().unwrap();
    }
    let registration = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Observer));
    let mut state = State {
        pool,
        input: Vec::with_capacity(n),
        ready: Vec::with_capacity(n),
        records: Vec::with_capacity(n),
        sender,
        _registration: registration,
        dispatch,
    };
    for index in 0..n {
        add_route(&mut state, index, kind);
    }
    state
}

fn add_route(state: &mut State, index: usize, kind: u8) {
    let id = u64::try_from(index).unwrap() + 10;
    let (records, receiver) = mpsc::channel(2);
    let (ready, opened) = oneshot::channel();
    let checked = matches!(kind, 2 | 3 | 4 | 9);
    let mut route = fixture::route(
        "good",
        Kind::Follow(wire::SessionFollow {
            opening_cut: false,
            key: None,
            read_limit: 0,
            bootstrap_head: false,
            when_absent: Default::default(),
            delivery: wire::SessionDelivery::All as i32,
            resource: crate::wire_codec::session::stream_resource(&format!("route/{id}")),
            preceding_commit_id: if checked {
                vec![7; 32].into()
            } else {
                Bytes::new()
            },
            until: None,
            at_least: Default::default(),
        }),
        records,
    );
    route.received = u64::from(checked);
    route.opened = false;
    route.credit = Default::default();
    route.ready = if matches!(kind, 8 | 9) {
        None
    } else {
        Some(ready)
    };
    route.observation = tracing::dispatcher::with_default(&state.dispatch, || {
        crate::obs::OperationSpan::new(tracing::info_span!(
            "route_lookup",
            generation = tracing::field::Empty
        ))
    });
    if matches!(kind, 1 | 4 | 8 | 9) {
        route
            .records
            .try_send(Ok(Delivery {
                before: None,
                item: frame(
                    id,
                    1,
                    Body::Watermark(wire::SessionWatermark {
                        accepted: 1,
                        durable: 1,
                        ..Default::default()
                    }),
                )
                .into(),
                sequence: 0,
                generation: 1,
                credit: Credit::new(
                    Arc::clone(&state.pool.release_sender),
                    Released {
                        id,
                        node: "good".into(),
                        bytes: 0,
                        epoch: 2,
                        generation: 1,
                    },
                ),
            }))
            .unwrap();
    }
    let body = if matches!(kind, 1 | 4 | 8 | 9) {
        Body::Closed(wire::SessionClosed {
            code: "not_found".into(),
            message: "server diagnostic".into(),
        })
    } else {
        Body::Opened(fixture::opened(
            super::super::super::cursor(if kind == 5 { 2 } else { route.received }),
            if checked {
                vec![if kind == 3 { 8 } else { 7 }; 32].into()
            } else {
                Bytes::new()
            },
        ))
    };
    if kind == 7 {
        route.node = "bad".into();
    }
    state.pool.routes.insert(id, route);
    state
        .input
        .push(frame(id, if kind == 6 { 2 } else { 1 }, body));
    state.ready.push(opened);
    state.records.push(receiver);
}

type Delivered = Result<(u64, u64, Vec<u8>), StreamError>;

#[derive(Debug, PartialEq, Eq)]
struct Output {
    results: Vec<Result<(), StreamError>>,
    ready: Vec<Result<Result<u64, StreamError>, String>>,
    routes: Vec<String>,
    frames: Vec<Result<Vec<u8>, StreamError>>,
    deliveries: Vec<Vec<Delivered>>,
}

fn run(mut state: State, redundant: bool) -> (Output, usize, AllocationInfo) {
    let mut results = Vec::with_capacity(state.input.len());
    let mut allocations = AllocationInfo::default();
    let before = state.pool.routes.searches();
    for input in state.input {
        record(Trace::Input(input.encode_to_vec()));
        allocations += measure(|| {
            if redundant {
                assert!(state.pool.routes.get_mut(&input.channel_id).is_some());
            }
            results.push(state.pool.receive("good", input));
        });
    }
    let searches = state.pool.routes.searches() - before;
    let routes = state
        .pool
        .routes
        .iter()
        .map(|(id, r)| {
            format!(
                "{id}:{:?}:{:?}",
                (
                    &r.node,
                    &r.kind,
                    r.raw,
                    r.generation,
                    r.placement_epoch,
                    r.received,
                    &r.received_opaque
                ),
                (
                    r.opened,
                    &r.credit,
                    r.acknowledge,
                    r.acknowledged,
                    r.ready.is_some(),
                    r.progress.position()
                )
            )
        })
        .collect();
    let mut frames = Vec::new();
    while let Some(value) = state.sender.next().now_or_never() {
        let failed = value.is_err();
        frames.push(value.map(|frame| frame.encode_to_vec()));
        if failed {
            break;
        }
    }
    let ready = state
        .ready
        .iter_mut()
        .map(|r| {
            r.try_recv()
                .map(|outcome| outcome.map(|(id, _)| id))
                .map_err(|error| error.to_string())
        })
        .collect();
    let deliveries = state
        .records
        .iter_mut()
        .map(|receiver| {
            let mut received = Vec::new();
            while let Ok(value) = receiver.try_recv() {
                received.push(
                    value.map(|d| (d.sequence, d.generation, d.item.into_wire().encode_to_vec())),
                );
            }
            received
        })
        .collect();
    (
        Output {
            results,
            ready,
            routes,
            frames,
            deliveries,
        },
        searches,
        allocations,
    )
}

fn required_searches(n: usize, kind: u8) -> usize {
    n * if matches!(kind, 6 | 7) { 1 } else { 2 }
}

fn redundant_search_is_rejected(n: usize, kind: u8) -> bool {
    let [
        ((before, searches, before_allocations), before_trace),
        ((after, extra_searches, allocations), trace),
    ] = pair::<_, _, Trace>(
        || initialize(n, kind),
        |state, baseline| run(state, !baseline),
    );
    assert_eq!(
        after, before,
        "redundant lookup changes no observable result"
    );
    assert_eq!(trace, before_trace, "operation/observer trace");
    assert_eq!(allocations.count_total, before_allocations.count_total);
    assert_eq!(allocations.bytes_total, before_allocations.bytes_total);
    assert_eq!(searches, required_searches(n, kind));
    extra_searches != required_searches(n, kind)
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn route_dispatch_uses_only_required_searches(n in 1_usize..17, kind in 0_u8..10) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _entered = runtime.enter();
        let (_, searches, _) = run(initialize(n, kind), false);
        prop_assert_eq!(searches, required_searches(n, kind));
    }
}

#[test]
fn redundant_route_search_fails_the_same_contract() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    for kind in 0..10 {
        assert!(
            redundant_search_is_rejected(3, kind),
            "redundant search escaped kind {kind}"
        );
    }
}
