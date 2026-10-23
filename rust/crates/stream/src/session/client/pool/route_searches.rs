//! Independent old handlers plus exact work on the production receive path.
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
impl super::super::super::runtime::Executor for Immediate {
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
    ready: Vec<oneshot::Receiver<Result<u64, StreamError>>>,
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
            delivery: wire::SessionDelivery::All as i32,
            path: format!("route/{id}"),
            preceding_commit_id: if checked {
                vec![7; 32].into()
            } else {
                Bytes::new()
            },
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
                item: Received::Frame(frame(
                    id,
                    1,
                    Body::Watermark(wire::SessionWatermark {
                        accepted: 1,
                        durable: 1,
                        ..Default::default()
                    }),
                )),
                sequence: 0,
                opaque: Bytes::new(),
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
        Body::Opened(wire::SessionOpened {
            resume: Some(super::super::super::cursor(if kind == 5 {
                2
            } else {
                route.received
            })),
            preceding_commit_id: if checked {
                vec![if kind == 3 { 8 } else { 7 }; 32].into()
            } else {
                Bytes::new()
            },
        })
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

type Delivered = Result<(u64, Vec<u8>, u64, Vec<u8>), StreamError>;

#[derive(Debug, PartialEq, Eq)]
struct Output {
    results: Vec<Result<(), StreamError>>,
    ready: Vec<Result<Result<u64, StreamError>, String>>,
    routes: Vec<String>,
    frames: Vec<Result<Vec<u8>, StreamError>>,
    deliveries: Vec<Vec<Delivered>>,
}

fn run(mut state: State, old: bool, redundant: bool) -> (Output, usize, AllocationInfo) {
    let mut results = Vec::with_capacity(state.input.len());
    let mut allocations = AllocationInfo::default();
    let before = state.pool.routes.searches();
    for input in state.input {
        record(Trace::Input(input.encode_to_vec()));
        allocations += measure(|| {
            if redundant {
                assert!(state.pool.routes.get_mut(&input.channel_id).is_some());
            }
            results.push(if old {
                old_receive(&mut state.pool, "good", input)
            } else {
                state.pool.receive("good", input)
            });
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
        .map(|r| r.try_recv().map_err(|error| error.to_string()))
        .collect();
    let deliveries = state
        .records
        .iter_mut()
        .map(|receiver| {
            let mut received = Vec::new();
            while let Ok(value) = receiver.try_recv() {
                received.push(value.map(|d| {
                    (
                        d.sequence,
                        d.opaque.to_vec(),
                        d.generation,
                        match d.item {
                            Received::Frame(frame) => frame.encode_to_vec(),
                            Received::Data(data) => data.encode_to_vec(),
                            Received::Child(child) => child.path.as_str().as_bytes().to_vec(),
                        },
                    )
                }));
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

fn compare(n: usize, kind: u8, redundant: bool) -> bool {
    let [
        ((before, old_searches, old_allocations), old_trace),
        ((after, searches, allocations), trace),
    ] = pair::<_, _, Trace>(
        || initialize(n, kind),
        |state, old| run(state, old, !old && redundant),
    );
    assert_eq!(
        after, before,
        "complete results, readiness, route state, wire bytes and ordered deliveries"
    );
    assert_eq!(trace, old_trace, "operation/observer trace");
    assert_eq!(
        allocations.count_total, old_allocations.count_total,
        "exact allocations"
    );
    assert_eq!(
        allocations.bytes_total, old_allocations.bytes_total,
        "exact allocated bytes"
    );
    let expected = n * if matches!(kind, 5..=7) { 1 } else { 2 };
    assert_eq!(
        old_searches,
        expected + if matches!(kind, 6 | 7) { 0 } else { n }
    );
    searches == expected
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn route_dispatch_uses_only_required_searches(n in 1_usize..17, kind in 0_u8..10) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _entered = runtime.enter();
        prop_assert!(compare(n, kind, false));
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
            !compare(3, kind, true),
            "redundant search escaped kind {kind}"
        );
    }
}

#[test]
fn route_dispatch_paired_measurement() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    for kind in [0, 1] {
        let mut elapsed = [std::time::Duration::ZERO; 2];
        let [
            ((before, old_searches, _), old_trace),
            ((after, searches, _), trace),
        ] = pair::<_, _, Trace>(
            || initialize(256, kind),
            |state, old| {
                let start = std::time::Instant::now();
                let result = run(state, old, false);
                let [before, after] = &mut elapsed;
                *if old { before } else { after } = start.elapsed();
                result
            },
        );
        assert_eq!(after, before);
        assert_eq!(trace, old_trace);
        assert_eq!(old_searches, 768);
        assert_eq!(searches, 512);
        eprintln!(
            "route dispatch kind={kind} n=256 old={:?} new={:?} lookups=768->512",
            elapsed[0], elapsed[1]
        );
    }
}

fn old_receive(pool: &mut Pool, node: &str, input: wire::SessionFrame) -> Result<(), StreamError> {
    let id = input.channel_id;
    if id == 0 {
        return pool.control(node, input.generation, input.body.as_ref());
    }
    if matches!(input.body, Some(Body::Data(_))) {
        let link = pool.links.get_mut(node).ok_or(StreamError::Unavailable)?;
        wire_codec::session::consume_credit(&mut link.credit, &input)?;
    }
    let epoch = pool.links.get(node).ok_or(StreamError::Unavailable)?.epoch;
    let length = input.encoded_len() as u64;
    let Some(route) = pool
        .routes
        .get_mut(&id)
        .filter(|route| route.node == node && route.generation == input.generation)
    else {
        // Return aggregate credit for fenced data without touching its successor.
        if matches!(input.body, Some(Body::Data(_))) {
            pool.release(node, epoch, length)?;
        }
        return Ok(());
    };
    if matches!(input.body, Some(Body::Data(_))) {
        wire_codec::session::consume_credit(&mut route.credit, &input)?;
    }
    if matches!(input.body, Some(Body::Closed(_) | Body::Moved(_)))
        && let Some(link) = pool.links.get(node)
    {
        link.channel.cancel_channel(wire::SessionChannel {
            channel_id: id,
            generation: input.generation,
        });
    }
    match input.body {
        Some(Body::Opened(opened)) => old_opened(pool, id, &opened),
        Some(Body::Data(data)) => pool.data(node, id, input.generation, data, length, epoch),
        Some(Body::Closed(closed)) => old_closed(pool, id, &closed),
        Some(Body::Moved(moved)) => {
            let destination = moved.node.ok_or(StreamError::Unavailable)?;
            if destination.node_id.is_empty()
                || destination.placement_epoch <= route.placement_epoch
            {
                return Err(StreamError::Unavailable);
            }
            route.placement_epoch = destination.placement_epoch;
            route.generation = route.progress.fence()?;
            route.node = destination.node_id.clone();
            route.opened = false;
            if pool.links.contains_key(&destination.node_id) {
                pool.subscribe(id)
            } else {
                pool.dial(destination, None);
                Ok(())
            }
        }
        Some(Body::Watermark(mark)) if mark.durable <= mark.accepted => {
            if route.raw {
                route.watermark = Some(frame(id, input.generation, Body::Watermark(mark)));
                pool.flush_watermark(id)
            } else {
                Ok(())
            }
        }
        _ => Err(StreamError::Unavailable),
    }
}
/// Accept an Open reply that resumes exactly where the route left off.
fn old_opened(pool: &mut Pool, id: u64, opened: &wire::SessionOpened) -> Result<(), StreamError> {
    let route = pool.routes.get_mut(&id).ok_or(StreamError::Unavailable)?;
    if opened.resume.as_ref().is_none_or(|resume| {
        resume.next != route.received || resume.opaque != route.received_opaque
    }) {
        return Err(StreamError::Unavailable);
    }
    if let Kind::Follow(follow) = &route.kind {
        let preceding = wire_codec::session::preceding_follow(
            follow,
            opened.resume.as_ref().ok_or(StreamError::Unavailable)?,
        )?;
        if let Err(error) = wire_codec::session::confirm_follow(preceding, opened) {
            if let Some(ready) = route.ready.take() {
                let _ = ready.send(Err(error.clone()));
            }
            let records = route.records.clone();
            pool.remove(id)?;
            pool.executor.spawn(Box::pin(async move {
                let _ = records.send(Err(error)).await;
            }));
            return Ok(());
        }
    }
    if let Some(ready) = route.ready.take() {
        let _ = ready.send(Ok(id));
    }
    route.observation.record("generation", route.generation);
    route.opened = true;
    pool.grant(id)
}
/// End a route with the server's status after its earlier records.
fn old_closed(pool: &mut Pool, id: u64, closed: &wire::SessionClosed) -> Result<(), StreamError> {
    let route = pool.routes.get_mut(&id).ok_or(StreamError::Unavailable)?;
    let error = StreamError::from_session_close(&closed.code);
    if matches!(&route.kind, Kind::Follow(follow) if !follow.preceding_commit_id.is_empty())
        && let Some(ready) = route.ready.take()
    {
        let _ = ready.send(Err(error));
        pool.routes.remove(&id);
        return Ok(());
    }
    // Errors are delivered after all earlier records, using one reserved slot.
    if let Some(ready) = route.ready.take() {
        let _ = ready.send(Ok(id));
    }
    let route = pool.routes.remove(&id).ok_or(StreamError::Unavailable)?;
    pool.executor.spawn(Box::pin(async move {
        let _ = route.records.send(Err(error)).await;
    }));
    Ok(())
}
