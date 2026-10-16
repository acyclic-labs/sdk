//! Local platform host for browser qualification, using the real service and session.
use acyclic_stream::{MemoryStream, StreamError, grpc, session, wire, wire_codec};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    routing::{get, post},
};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tower::Layer as _;
use tower_http::cors::CorsLayer;

#[derive(Clone)]
struct Host {
    streams: Arc<MemoryStream>,
    disconnect: Arc<Notify>,
    online: Arc<AtomicBool>,
    connections: Arc<AtomicUsize>,
    refusals: Arc<AtomicUsize>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = Host {
        streams: Arc::new(MemoryStream::default()),
        disconnect: Arc::new(Notify::new()),
        online: Arc::new(AtomicBool::new(true)),
        connections: Arc::new(AtomicUsize::new(0)),
        refusals: Arc::new(AtomicUsize::new(0)),
    };
    let online = host.online.clone();
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
            if !online.load(Ordering::Acquire) {
                return Err(tonic::Status::unavailable("injected outage"));
            }
            Ok(request)
        },
    );
    let routes = tonic::service::Routes::new(tonic_web::GrpcWebLayer::new().layer(service))
        .into_axum_router();
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
        .with_state(host)
        .merge(routes)
        .layer(CorsLayer::permissive());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
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
async fn serve(host: Host, socket: WebSocket) {
    host.connections.fetch_add(1, Ordering::AcqRel);
    let (mut sender, incoming) = socket.split();
    let incoming = incoming
        .filter_map(|message| async move {
            match message {
                Ok(Message::Binary(bytes)) => Some(wire_codec::session::decode_frame(&bytes)),
                Ok(Message::Ping(_) | Message::Pong(_)) => None,
                _ => Some(Err(StreamError::Unavailable)),
            }
        })
        .boxed();
    let Ok(mut frames) = session::serve(
        host.streams,
        "browser-test".into(),
        session::Limits::default(),
        incoming,
    ) else {
        return;
    };
    loop {
        tokio::select! {
            _ = host.disconnect.notified() => break,
            frame = frames.next() => match frame {
                Some(Ok(frame)) => if sender.send(Message::Binary(frame.encode_to_vec().into())).await.is_err() { break; },
                _ => break,
            }
        }
    }
    let _ = sender.close().await;
}
