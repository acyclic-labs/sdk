//! Local transport qualification against the SDK test server.
use acyclic_actors as actors;
use acyclic_workers as workers;
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
    let mut workers_grpc =
        workers::grpc::connect_with_ca_certificate(endpoint, token, Some(ca.as_bytes())).await?;
    let actors_http = actors::http::Client::new(http_endpoint, token, 1024 * 1024)?;
    let workers_http = workers::http::Client::new(http_endpoint, token, 1024 * 1024)?;
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
    let request = workers::wire::PublishVersionRequest {
        ..Default::default()
    };
    workers_grpc.publish_version(request.clone()).await?;
    workers_http.publish_version(&request).await?;
    let request = workers::wire::SelectDeploymentRequest {
        expected_revision: Some(7),
        ..Default::default()
    };
    workers_grpc.select_deployment(request.clone()).await?;
    workers_http.select_deployment(&request).await?;
    let request = workers::wire::SubmitJobRequest {
        target: Some(workers::wire::JobTarget {
            target: Some(workers::wire::job_target::Target::DeploymentAlias(
                "current".into(),
            )),
        }),
        input: Some(workers::wire::Payload {
            source: Some(workers::wire::payload::Source::Object(
                workers::wire::ObjectRef {
                    bucket: "customer-input".into(),
                    key: "video/input.mp4".into(),
                },
            )),
        }),
        limits: Some(workers::wire::JobLimits {
            timeout_millis: 1000,
            memory_bytes: 1024,
            output_bytes: 1024,
        }),
        retry: Some(workers::wire::RetryPolicy {
            max_attempts: 2,
            backoff_millis: 0,
        }),
        idempotency_key: "job-a".into(),
    };
    workers::validate_submit(&request)?;
    assert_eq!(
        workers_grpc
            .submit_job(request.clone())
            .await?
            .into_inner()
            .job
            .ok_or("missing job")?
            .resolved_sha256,
        vec![7; 32]
    );
    assert_eq!(
        workers_http
            .submit_job(&request)
            .await?
            .job
            .ok_or("missing job")?
            .resolved_sha256,
        vec![7; 32]
    );
    let request = workers::wire::InspectJobRequest {
        ..Default::default()
    };
    assert_eq!(
        workers_grpc
            .inspect_job(request.clone())
            .await?
            .into_inner()
            .job
            .ok_or("missing job")?
            .result
            .ok_or("missing result")?
            .body,
        vec![3, 4]
    );
    assert_eq!(
        workers_http
            .inspect_job(&request)
            .await?
            .job
            .ok_or("missing job")?
            .result
            .ok_or("missing result")?
            .body,
        vec![3, 4]
    );
    let request = workers::wire::CancelJobRequest {
        ..Default::default()
    };
    workers_grpc.cancel_job(request.clone()).await?;
    workers_http.cancel_job(&request).await?;
    let request = workers::wire::InvokeVersionRequest {
        version_sha256: vec![1; 32],
        ..Default::default()
    };
    let pinned = workers_grpc
        .invoke_version(request.clone())
        .await?
        .into_inner();
    assert_eq!(pinned.resolved_sha256, vec![1; 32]);
    assert_eq!(pinned.resolved_revision, None);
    let pinned = workers_http.invoke_version(&request).await?;
    assert_eq!(pinned.resolved_sha256, vec![1; 32]);
    assert_eq!(pinned.resolved_revision, None);
    let request = workers::wire::InvokeDeploymentRequest {
        alias: "current".into(),
        ..Default::default()
    };
    let resolved = workers_grpc
        .invoke_deployment(request.clone())
        .await?
        .into_inner();
    assert_eq!(resolved.resolved_sha256, vec![2; 32]);
    assert_eq!(resolved.resolved_revision, Some(8));
    let resolved = workers_http.invoke_deployment(&request).await?;
    assert_eq!(resolved.resolved_sha256, vec![2; 32]);
    assert_eq!(resolved.resolved_revision, Some(8));
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
        "Rust: 15 authenticated gRPC and HTTP methods, denied authentication and response bound passed"
    );
    Ok(())
}
