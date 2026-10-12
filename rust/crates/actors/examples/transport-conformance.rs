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
    let mut actors_grpc =
        actors::grpc::connect_with_ca_certificate(endpoint, token, Some(ca.as_bytes())).await?;
    let actors_http = actors::http::Client::new(http_endpoint, token, 1024 * 1024)?;
    let request = actors::wire::CreateActorRequest {
        ..Default::default()
    };
    actors_grpc.create_actor(request.clone()).await?;
    actors_http.create_actor(&request).await?;
    let request = actors::wire::UpdateActorRequest {
        ..Default::default()
    };
    actors_grpc.update_actor(request.clone()).await?;
    actors_http.update_actor(&request).await?;
    let request = actors::wire::InspectActorRequest {
        ..Default::default()
    };
    actors_grpc.inspect_actor(request.clone()).await?;
    actors_http.inspect_actor(&request).await?;
    let request = actors::wire::AddSubscriptionRequest {
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
        ..Default::default()
    };
    actors_grpc.remove_subscription(request.clone()).await?;
    actors_http.remove_subscription(&request).await?;
    let request = actors::wire::ResumeSubscriptionRequest {
        ..Default::default()
    };
    actors_grpc.resume_subscription(request.clone()).await?;
    actors_http.resume_subscription(&request).await?;
    let request = actors::wire::CheckpointActorRequest {
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
        headers: vec![actors::wire::Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
        ..Default::default()
    };
    actors_grpc.invoke_actor(request.clone()).await?;
    actors_http.invoke_actor(&request).await?;
    let request = actors::wire::DeleteActorRequest {
        actor_id: "actor-a".into(),
        idempotency_key: "delete-a".into(),
    };
    actors_grpc.delete_actor(request.clone()).await?;
    actors_http.delete_actor(&request).await?;
    let mut denied =
        actors::grpc::connect_with_ca_certificate(endpoint, "wrong", Some(ca.as_bytes())).await?;
    assert_eq!(
        denied
            .inspect_actor(actors::wire::InspectActorRequest::default())
            .await
            .err()
            .ok_or("authentication unexpectedly accepted")?
            .code(),
        tonic::Code::Unauthenticated
    );
    let denied = actors::http::Client::new(http_endpoint, "wrong", 1024)?;
    assert!(matches!(
        denied
            .inspect_actor(&actors::wire::InspectActorRequest::default())
            .await,
        Err(actors::http::Error::Service {
            status: 401,
            detail: Some(_)
        })
    ));
    let bounded = actors::http::Client::new(http_endpoint, token, 8)?;
    assert!(matches!(
        bounded
            .inspect_actor(&actors::wire::InspectActorRequest {
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
