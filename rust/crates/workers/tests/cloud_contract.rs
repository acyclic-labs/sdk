//! Proposed SDK Worker deployment contract; historical Cloud provenance pins the code only.
#![cfg(not(target_arch = "wasm32"))]
#![allow(
    clippy::expect_used,
    reason = "test fixtures fail immediately on invalid setup"
)]
use acyclic_actors::{FILE_DESCRIPTOR_SET, http, wire};
use acyclic_objects::{Blobs, ByteRange, MemoryObjects};
use acyclic_workers::deployment::Client;
use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use bytes::Bytes;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use std::sync::{Arc, Mutex};

fn field<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value
        .pointer(pointer)
        .expect("missing golden fixture field")
}

fn golden() -> Value {
    serde_json::from_str(include_str!("fixtures/cloud-workers/golden.json"))
        .expect("valid Cloud fixture")
}

fn decode<T: Message + Default>(name: &str, value: &Value) -> T {
    let pool = DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("valid Cloud fixture");
    let descriptor = pool
        .get_message_by_name(&format!("acyclic.actors.v1.{name}"))
        .expect("valid Cloud fixture");
    let bytes = serde_json::to_vec(value).expect("valid Cloud fixture");
    let mut json = serde_json::Deserializer::from_slice(&bytes);
    DynamicMessage::deserialize(descriptor, &mut json)
        .expect("valid Cloud fixture")
        .transcode_to()
        .expect("valid Cloud fixture")
}

#[derive(Clone)]
struct Fixture {
    golden: Value,
    seen: Arc<Mutex<Vec<String>>>,
}

async fn reply(state: Fixture, headers: HeaderMap, request: Value, operation: &str) -> Json<Value> {
    assert_eq!(
        headers.get("authorization").expect("valid Cloud fixture"),
        "Bearer fixture-token"
    );
    assert_eq!(
        &request,
        field(&state.golden, &format!("/{operation}/request"))
    );
    state
        .seen
        .lock()
        .expect("valid Cloud fixture")
        .push(operation.into());
    Json(field(&state.golden, &format!("/{operation}/response")).clone())
}

async fn deploy(
    State(state): State<Fixture>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    reply(state, headers, request, "deploy").await
}
async fn invoke(
    State(state): State<Fixture>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    reply(state, headers, request, "invoke").await
}
async fn delete(
    State(state): State<Fixture>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    reply(state, headers, request, "delete").await
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "replays the complete Cloud Worker lifecycle"
)]
async fn worker_publication_and_named_deployment_contract() {
    let golden = golden();
    let state = Fixture {
        golden: golden.clone(),
        seen: Arc::default(),
    };
    let router = Router::new()
        .route("/v1/workers/deploy", post(deploy))
        .route("/v1/workers/invoke", post(invoke))
        .route("/v1/workers/delete", post(delete))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("valid Cloud fixture");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("valid Cloud fixture")
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("valid Cloud fixture");
    });
    let objects = std::sync::Arc::new(MemoryObjects::memory());
    let actors =
        http::Client::new(&endpoint, "fixture-token", 1 << 20).expect("valid Cloud fixture");
    let client = Client::new(actors, objects.clone());
    let code = Bytes::from(
        field(&golden, "/code")
            .as_str()
            .expect("valid Cloud fixture")
            .to_owned(),
    );
    let digest = client
        .publish(code.clone())
        .await
        .expect("valid Cloud fixture");
    assert_eq!(
        hex::encode(digest),
        field(&golden, "/publish/sha256")
            .as_str()
            .expect("valid Cloud fixture")
    );
    let published = objects
        .get(
            acyclic_objects::Digest::of(&code),
            ByteRange::FULL,
            4096,
        )
        .await
        .expect("valid Cloud fixture");
    assert_eq!(published.body().as_ref(), code.as_ref());
    let created = client
        .deploy(&decode::<wire::DeploymentRequest>(
            "DeploymentRequest",
            field(&golden, "/deploy/request"),
        ))
        .await
        .expect("valid Cloud fixture");
    assert_eq!(
        created,
        decode::<wire::DeploymentResponse>(
            "DeploymentResponse",
            field(&golden, "/deploy/response")
        )
    );
    let invoked = client
        .invoke(&decode::<wire::InvokeWorkerRequest>(
            "InvokeWorkerRequest",
            field(&golden, "/invoke/request"),
        ))
        .await
        .expect("valid Cloud fixture");
    assert_eq!(
        invoked,
        decode::<wire::InvokeActorResponse>(
            "InvokeActorResponse",
            field(&golden, "/invoke/response")
        )
    );
    let deleted = client
        .delete(&decode::<wire::DeleteWorkerRequest>(
            "DeleteWorkerRequest",
            field(&golden, "/delete/request"),
        ))
        .await
        .expect("valid Cloud fixture");
    assert_eq!(
        deleted,
        decode::<wire::DeleteWorkerResponse>(
            "DeleteWorkerResponse",
            field(&golden, "/delete/response")
        )
    );
    assert_eq!(
        *state.seen.lock().expect("valid Cloud fixture"),
        ["deploy", "invoke", "delete"]
    );
    server.abort();
}
