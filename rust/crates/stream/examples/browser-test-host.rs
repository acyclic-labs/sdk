//! Local platform host for browser qualification, using the real service and session.
use acyclic_stream::{StreamError, StreamProvider, grpc, session, wire, wire_codec};
#[path = "browser-test-host/provider.rs"]
mod provider;
use axum::{
    Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    routing::{get, post},
};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use provider::Provider;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tower::Layer as _;
use tower_http::cors::CorsLayer;

#[derive(Clone)]
struct Host {
    streams: Arc<Provider>,
    fail_next: Arc<AtomicUsize>,
    saved: tokio::sync::watch::Sender<Option<String>>,
    consumed: Arc<AtomicU64>,
    outbox_consumed: Arc<AtomicU64>,
    consumed_changed: Arc<Notify>,
    disconnect: Arc<Notify>,
    online: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
    refusals: Arc<AtomicUsize>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = Host {
        streams: Arc::new(Provider::default()),
        fail_next: Arc::new(AtomicUsize::new(0)),
        saved: tokio::sync::watch::channel(None).0,
        consumed: Arc::new(AtomicU64::new(0)),
        outbox_consumed: Arc::new(AtomicU64::new(0)),
        consumed_changed: Arc::new(Notify::new()),
        disconnect: Arc::new(Notify::new()),
        online: Arc::new(AtomicBool::new(true)),
        connections: Arc::new(AtomicUsize::new(0)),
        refusals: Arc::new(AtomicUsize::new(0)),
    };
    let routes = grpc_routes(&host);
    let app = Router::new()
        .route("/v1/stream/session", get(upgrade))
        .route(
            "/fixture/connections",
            get(|State(host): State<Host>| async move {
                host.connections.load(Ordering::Acquire).to_string()
            }),
        )
        .route(
            "/fixture/offline",
            post(|State(host): State<Host>| async move {
                host.online.store(false, Ordering::Release);
            }),
        )
        .route(
            "/fixture/recover",
            post(|State(host): State<Host>| async move {
                host.online.store(true, Ordering::Release);
                host.refusals.store(4, Ordering::Release);
                host.disconnect.notify_waiters();
            }),
        )
        .route(
            "/fixture/fail-next",
            post(|State(host): State<Host>| async move {
                host.fail_next.store(1, Ordering::Release);
            }),
        )
        .route(
            "/fixture/save",
            post(|State(host): State<Host>| async move {
                host.streams.saved.store(true, Ordering::Release);
                let _ = host.saved.send(None);
            }),
        )
        .route("/fixture/save-outbox", post(save_outbox))
        .route(
            "/fixture/outbox-consumed/{position}",
            get(
                |State(host): State<Host>, Path(position): Path<u64>| async move {
                    loop {
                        let changed = host.consumed_changed.notified();
                        if host.outbox_consumed.load(Ordering::Acquire) >= position {
                            break;
                        }
                        changed.await;
                    }
                },
            ),
        )
        .route(
            "/fixture/consumed",
            get(|State(host): State<Host>| async move {
                loop {
                    let changed = host.consumed_changed.notified();
                    if host.consumed.load(Ordering::Acquire) >= 3 {
                        break;
                    }
                    changed.await;
                }
            }),
        )
        .with_state(host)
        .merge(routes)
        .layer(CorsLayer::permissive());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
/// Saves the outbox at its accepted tail, then announces that path's watermarks.
async fn save_outbox(State(host): State<Host>) -> Result<(), (StatusCode, String)> {
    let path = acyclic_stream::StreamPath::new("remote-outbox/files")
        .map_err(|error| fixture_error(&error))?;
    let accepted = host
        .streams
        .tail(path)
        .await
        .map_err(|error| fixture_error(&error))?;
    host.streams.outbox_saved.store(accepted, Ordering::Release);
    let _ = host.saved.send(Some("remote-outbox/files".into()));
    Ok(())
}
fn fixture_error(error: &StreamError) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:?}"))
}
/// The unary gRPC-web service behind the test credential and injected outages.
fn grpc_routes(host: &Host) -> Router {
    let online = host.online.clone();
    let fail_next = host.fail_next.clone();
    let service = wire::stream_service_server::StreamServiceServer::with_interceptor(
        grpc::Service::new(host.streams.clone()),
        move |request: tonic::Request<()>| {
            if request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                != Some("Bearer browser-test")
            {
                return Err(tonic::Status::unauthenticated("test credential"));
            }
            if fail_next
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(tonic::Status::unavailable("one failed HTTP request"));
            }
            if !online.load(Ordering::Acquire) {
                return Err(tonic::Status::unavailable("injected outage"));
            }
            Ok(request)
        },
    );
    tonic::service::Routes::new(tonic_web::GrpcWebLayer::new().layer(service)).into_axum_router()
}
async fn upgrade(
    State(host): State<Host>,
    headers: axum::http::HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<axum::response::Response, StatusCode> {
    if host
        .refusals
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
            left.checked_sub(1)
        })
        .is_ok()
    {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let protocols = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !protocols
        .split(',')
        .map(str::trim)
        .any(|v| v == "acyclic.bearer.YnJvd3Nlci10ZXN0")
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(upgrade
        .protocols(["acyclic.session.v1"])
        .max_message_size(acyclic_stream::MAX_COMMAND_BYTES)
        .on_upgrade(move |socket| serve(host, socket)))
}
/// Each followed channel: generation, path, opened, and the next granted position.
type Routes = BTreeMap<u64, (u64, String, bool, u64)>;
/// Track follows and their consumption from one inbound client frame.
fn observe(routes: &mut Routes, progress: &Host, frame: &wire::SessionFrame) {
    match &frame.body {
        Some(wire::session_frame::Body::Interest(interest)) => {
            for remove in &interest.remove {
                routes.remove(&remove.channel_id);
            }
            for subscription in &interest.upsert {
                if let (Some(channel), Some(wire::session_subscription::Kind::Follow(follow))) =
                    (&subscription.channel, &subscription.kind)
                {
                    let next = subscription.resume.as_ref().map_or(0, |cursor| cursor.next);
                    routes.insert(
                        channel.channel_id,
                        (channel.generation, follow.resource.clone(), false, next),
                    );
                }
            }
        }
        Some(wire::session_frame::Body::Window(window)) => {
            if let Some((_, path, opened, next)) = routes.get_mut(&frame.channel_id) {
                if *opened {
                    *next += u64::from(window.frames);
                    let consumed = match path.as_str() {
                        "streams:/durable/files" => Some(&progress.consumed),
                        "streams:/remote-outbox/files" => Some(&progress.outbox_consumed),
                        _ => None,
                    };
                    if let Some(consumed) = consumed {
                        consumed.fetch_max(*next, Ordering::AcqRel);
                        progress.consumed_changed.notify_waiters();
                    }
                } else {
                    *opened = true;
                }
            }
        }
        Some(wire::session_frame::Body::Ack(ack)) => {
            let consumed = match routes
                .get(&frame.channel_id)
                .map(|(_, path, _, _)| path.as_str())
            {
                Some("durable/files") => &progress.consumed,
                Some("remote-outbox/files") => &progress.outbox_consumed,
                _ => return,
            };
            if let Some(applied) = &ack.applied {
                consumed.fetch_max(applied.next, Ordering::AcqRel);
                progress.consumed_changed.notify_waiters();
            }
        }
        _ => {}
    }
}
async fn serve(host: Host, socket: WebSocket) {
    host.connections.fetch_add(1, Ordering::AcqRel);
    let (mut sender, incoming) = socket.split();
    // Test observation lock only. Never nested or held across I/O/await.
    let routes = Arc::new(Mutex::new(Routes::new()));
    let observing = routes.clone();
    let progress = host.clone();
    let mut saved = host.saved.subscribe();
    let incoming = incoming
        .filter_map(|message| async move {
            match message {
                Ok(Message::Binary(bytes)) => Some(wire_codec::session::decode_frame(&bytes)),
                Ok(Message::Ping(_) | Message::Pong(_)) => None,
                _ => Some(Err(StreamError::Unavailable)),
            }
        })
        .inspect(move |frame| {
            if let Ok(frame) = frame
                && let Ok(mut routes) = observing.lock()
            {
                observe(&mut routes, &progress, frame);
            }
        })
        .boxed();
    let Ok(mut frames) = session::serve(
        host.streams.clone(),
        session::testing::authority(),
        "browser-test".into(),
        session::Limits::default(),
        incoming,
    ) else {
        return;
    };
    loop {
        tokio::select! {
            _ = host.disconnect.notified() => break,
            changed = saved.changed() => {
                if changed.is_err() { break; }
                let only = saved.borrow_and_update().clone();
                let Ok(subscriptions) = routes.lock().map(|routes| routes.clone()) else { break; };
                for (channel_id, (generation, path, _, _)) in subscriptions {
                    if only.as_ref().is_some_and(|only| only != &path) { continue; }
                    let Ok(path) = acyclic_stream::StreamPath::new(path) else { continue; };
                    let Ok(marks) = host.streams.watermarks(path).await else { continue; };
                    let frame = wire::SessionFrame { channel_id, generation,
                        body: Some(wire::session_frame::Body::Watermark(wire::SessionWatermark { accepted: marks.accepted, durable: marks.durable, after: None, preceding_commit_id: Default::default() })) };
                    if sender.send(Message::Binary(frame.encode_to_vec().into())).await.is_err() { return; }
                }
            },
            frame = frames.next() => match frame {
                Some(Ok(frame)) => if sender.send(Message::Binary(frame.encode_to_vec().into())).await.is_err() { break; },
                _ => break,
            }
        }
    }
    let _ = sender.close().await;
}
