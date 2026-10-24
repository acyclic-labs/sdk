//! Local transport qualification against the SDK test server.
use acyclic_actors as actors;
use std::io::{self, Read};

#[allow(
    clippy::too_many_lines,
    reason = "enumerates every canonical RPC in the conformance runner"
)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let options: serde_json::Value = serde_json::from_str(&input)?;
    let endpoint = options
        .get("endpoint")
        .and_then(serde_json::Value::as_str)
        .ok_or("endpoint")?;
    let http_endpoint = options
        .get("httpEndpoint")
        .and_then(serde_json::Value::as_str)
        .ok_or("httpEndpoint")?;
    let ca = options
        .get("caCertificate")
        .and_then(serde_json::Value::as_str)
        .ok_or("CA")?;
    let token = options
        .get("token")
        .and_then(serde_json::Value::as_str)
        .ok_or("token")?;
    let mut actors_grpc = actors::grpc::connect(endpoint, token, Some(ca.as_bytes())).await?;
    let actors_http = actors::http::Client::new(http_endpoint, token, 1024 * 1024)?;
    let request = actors::wire::DeploymentRequest {
        name: "example".into(),
        code_sha256: vec![1; 32],
        limits: Some(actors::wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 1,
            checkpoint_bytes: 1,
        }),
        idempotency_key: "deploy-a".into(),
        account_id: Vec::new(),
        jurisdiction: String::new(),
        expected_revision: 0,
        ..Default::default()
    };
    actors_grpc.deploy_namespace(request.clone()).await?;
    actors_http.deploy_namespace(&request).await?;
    let request = actors::wire::DeploymentRequest {
        limits: None,
        worker_limits: Some(actors::wire::WorkerLimits {
            cpu_millis: 1,
            memory_bytes: 1,
            timeout_millis: 1,
        }),
        ..request
    };
    actors_grpc.deploy_worker(request.clone()).await?;
    actors_http.deploy_worker(&request).await?;
    let request = actors::wire::UpdateNamespaceRequest {
        namespace_id: "example".into(),
        code_sha256: vec![1; 32],
        limits: Some(actors::wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 1,
            checkpoint_bytes: 1,
        }),
        idempotency_key: "update-a".into(),
        account_id: Vec::new(),
        ..Default::default()
    };
    actors_grpc.update_namespace(request.clone()).await?;
    actors_http.update_namespace(&request).await?;
    let request = actors::wire::InspectActorRequest {
        actor_id: "actor-a".into(),
        namespace: None,
        account_id: Vec::new(),
    };
    actors_grpc.inspect_actor(request.clone()).await?;
    actors_http.inspect_actor(&request).await?;
    let request = actors::wire::AddSubscriptionRequest {
        namespace: None,
        account_id: Vec::new(),
        actor_id: "actor-a".into(),
        subscription: Some(actors::wire::SubscriptionSpec {
            subscription_id: "input".into(),
            stream_path: "events/input".into(),
            start: Some(actors::wire::SubscriptionStart {
                start: Some(actors::wire::subscription_start::Start::Cursor(
                    9_007_199_254_740_993,
                )),
            }),
            placement_anchor: false,
        }),
        idempotency_key: "subscribe-a".into(),
    };
    actors_grpc.add_subscription(request.clone()).await?;
    actors_http.add_subscription(&request).await?;
    let request = actors::wire::RemoveSubscriptionRequest {
        actor_id: "actor-a".into(),
        subscription_id: "input".into(),
        idempotency_key: "subscription-a".into(),
        namespace: None,
        account_id: Vec::new(),
    };
    actors_grpc.remove_subscription(request.clone()).await?;
    actors_http.remove_subscription(&request).await?;
    let request = actors::wire::ResumeSubscriptionRequest {
        actor_id: "actor-a".into(),
        subscription_id: "input".into(),
        idempotency_key: "subscription-a".into(),
        namespace: None,
        account_id: Vec::new(),
    };
    actors_grpc.resume_subscription(request.clone()).await?;
    actors_http.resume_subscription(&request).await?;
    let request = actors::wire::CheckpointActorRequest {
        namespace: None,
        account_id: Vec::new(),
        actor_id: "actor-a".into(),
        idempotency_key: "checkpoint-a".into(),
    };
    assert_eq!(
        actors_grpc
            .checkpoint_actor(request.clone())
            .await?
            .into_inner()
            .actor
            .ok_or("checkpoint omitted actor")?
            .checkpoint_epoch,
        9
    );
    assert_eq!(
        actors_http
            .checkpoint_actor(&request)
            .await?
            .actor
            .ok_or("checkpoint omitted actor")?
            .checkpoint_epoch,
        9
    );
    let request = actors::wire::InvokeActorRequest {
        actor_id: "actor-a".into(),
        namespace: None,
        account_id: Vec::new(),
        location_hint: None,
        binding_name: None,
        rpc_method: None,
        headers: vec![actors::wire::Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
        ..Default::default()
    };
    actors_grpc.invoke_actor(request.clone()).await?;
    actors_http.invoke_actor(&request).await?;
    let request = actors::wire::DeleteAllRequest {
        namespace: None,
        account_id: Vec::new(),
        actor_id: "actor-a".into(),
        idempotency_key: "delete-a".into(),
    };
    actors_grpc.delete_all(request.clone()).await?;
    actors_http.delete_all(&request).await?;
    let worker = actors::wire::InvokeWorkerRequest {
        worker_name: "example".into(),
        method: "POST".into(),
        url: "/invoke".into(),
        headers: vec![actors::wire::Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
        ..Default::default()
    };
    actors_grpc.invoke_worker(worker.clone()).await?;
    actors_http.invoke_worker(&worker).await?;
    let deleted = actors::wire::DeleteWorkerRequest {
        worker_name: "example".into(),
        idempotency_key: "delete-worker".into(),
        ..Default::default()
    };
    actors_grpc.delete_worker(deleted.clone()).await?;
    actors_http.delete_worker(&deleted).await?;
    let mut denied = actors::grpc::connect(endpoint, "wrong", Some(ca.as_bytes())).await?;
    assert_eq!(
        denied
            .inspect_actor(actors::wire::InspectActorRequest {
                actor_id: "actor-a".into(),
                ..Default::default()
            })
            .await
            .err()
            .ok_or("authentication unexpectedly accepted")?
            .code(),
        tonic::Code::Unauthenticated
    );
    let denied = actors::http::Client::new(http_endpoint, "wrong", 1024)?;
    assert!(matches!(
        denied
            .inspect_actor(&actors::wire::InspectActorRequest {
                actor_id: "actor-a".into(),
                ..Default::default()
            })
            .await,
        Err(actors::http::Error::Service {
            status: 401,
            detail: Some(_),
            ..
        })
    ));
    let bounded = actors::http::Client::new(http_endpoint, token, 8)?;
    assert!(matches!(
        bounded
            .inspect_actor(&actors::wire::InspectActorRequest {
                namespace: None,
                account_id: Vec::new(),
                actor_id: "oversize".into()
            })
            .await,
        Err(actors::http::Error::ResponseTooLarge)
    ));
    println!(
        "Rust: authenticated Actors gRPC and HTTP methods, denied authentication and response bound passed"
    );
    Ok(())
}
