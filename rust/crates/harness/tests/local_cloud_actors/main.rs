//! SDK Actors operations against the real local-cloud service.
use acyclic_actors::{grpc, wire};
use acyclic_objects::v1::{ObjectsProvider as _, grpc::GrpcObjects, wire as objects};
use acyclic_stream::{AppendOutcome, AppendRequest, StreamPath, StreamProvider as _};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const ACTOR: &str = "e2e-actor";
const EVENTS: &str = "e2e-root/actor-events";
// Exact code/configuration admitted by the cloud gate's locally minted bearer.
const CODE: &str = include_str!("counter.mjs");
const CODE_V2: &str = include_str!("counter-v2.mjs");

fn digest(code: &str) -> Vec<u8> {
    Sha256::digest(code.trim_end_matches('\n').as_bytes()).to_vec()
}

#[derive(Deserialize)]
struct Remote {
    endpoint: String,
    bearer: String,
    ca_file: PathBuf,
}

#[derive(Deserialize)]
struct ActorRemote {
    #[serde(flatten)]
    remote: Remote,
    migration_bearer: String,
}

#[derive(Deserialize)]
struct Endpoints {
    actors: ActorRemote,
    objects: Remote,
    streams: Remote,
}

fn endpoints() -> TestResult<Endpoints> {
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD").ok_or("local-cloud endpoints required")?;
    Ok(serde_json::from_slice(&std::fs::read(file)?)?)
}

fn subscriptions(
    actor: &wire::ActorObservation,
    expected: &[wire::SubscriptionObservation],
) -> TestResult {
    if actor.subscriptions != expected {
        return Err(format!(
            "subscriptions {:?}, expected {expected:?}",
            actor.subscriptions
        )
        .into());
    }
    Ok(())
}

fn subscription(cursor: u64) -> wire::SubscriptionObservation {
    wire::SubscriptionObservation {
        subscription_id: "events".into(),
        stream_path: EVENTS.into(),
        state: wire::SubscriptionState::Active as i32,
        delivered_cursor: cursor,
        completed_cursor: cursor,
        recoverable_cursor: cursor,
        ..Default::default()
    }
}

fn add_subscription(key: &str) -> wire::AddSubscriptionRequest {
    wire::AddSubscriptionRequest {
        actor_id: ACTOR.into(),
        idempotency_key: key.into(),
        subscription: Some(wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: EVENTS.into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        }),
    }
}

async fn append_events(
    streams: &acyclic_stream::grpc::Client,
    records: Vec<acyclic_stream::OwnedBytes>,
) -> TestResult<acyclic_stream::AppendReceipt> {
    match streams
        .append(AppendRequest {
            path: StreamPath::new(EVENTS)?,
            records,
            if_tail: None,
            idempotency_key: None,
        })
        .await?
    {
        AppendOutcome::Committed(receipt) => Ok(receipt),
        other => Err(format!("events append did not commit: {other:?}").into()),
    }
}

fn create() -> wire::CreateActorRequest {
    wire::CreateActorRequest {
        code_sha256: digest(CODE),
        home_region: "la".into(),
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1000,
            memory_bytes: 1 << 20,
            checkpoint_bytes: 1 << 16,
        }),
        idempotency_key: "e2e-create".into(),
        ..Default::default()
    }
}

fn observation(
    actor: Option<wire::ActorObservation>,
    revision: u64,
) -> TestResult<wire::ActorObservation> {
    observation_with_code(actor, revision, CODE)
}

fn observation_with_code(
    actor: Option<wire::ActorObservation>,
    revision: u64,
    code: &str,
) -> TestResult<wire::ActorObservation> {
    let actor = actor.ok_or("response omitted the actor")?;
    if actor.actor_id != ACTOR
        || actor.code_sha256 != digest(code)
        || actor.home_region != "la"
        || actor.configuration_revision != revision
    {
        return Err(format!("unexpected actor identity/configuration: {actor:?}").into());
    }
    Ok(actor)
}

fn state(response: &wire::InvokeActorResponse, counter: i32) -> TestResult {
    state_with_version(response, counter, 1)
}

fn state_with_version(
    response: &wire::InvokeActorResponse,
    counter: i32,
    version: u8,
) -> TestResult {
    state_with_events(response, counter, version, &[])
}

fn state_with_events(
    response: &wire::InvokeActorResponse,
    counter: i32,
    version: u8,
    events: &[String],
) -> TestResult {
    let migration = if version == 2 {
        ",\"migratedFrom\":1"
    } else {
        ""
    };
    let events = serde_json::to_string(events)?;
    let expected =
        format!("{{\"counter\":{counter},\"version\":{version},\"events\":{events}{migration}}}");
    if response.status != 200 || response.body != expected.as_bytes() {
        return Err(format!("unexpected invocation: {response:?}, expected {expected}").into());
    }
    Ok(())
}

async fn invoke(client: &mut grpc::Client, add: Option<i32>, counter: i32) -> TestResult {
    invoke_version(client, add, counter, 1).await
}

async fn invoke_version(
    client: &mut grpc::Client,
    add: Option<i32>,
    counter: i32,
    version: u8,
) -> TestResult {
    let response = invoke_response(client, add).await?;
    state_with_version(&response, counter, version)
}

async fn invoke_response(
    client: &mut grpc::Client,
    add: Option<i32>,
) -> TestResult<wire::InvokeActorResponse> {
    Ok(client
        .invoke_actor(wire::InvokeActorRequest {
            actor_id: ACTOR.into(),
            method: if add.is_some() { "POST" } else { "GET" }.into(),
            url: "https://actor.e2e/state".into(),
            body: add
                .map(|add| format!("{{\"add\":{add}}}").into_bytes())
                .unwrap_or_default(),
            headers: vec![wire::Header {
                name: "content-type".into(),
                value: "application/json".into(),
            }],
        })
        .await?
        .into_inner())
}

#[test]
fn actor_observation_rejects_other_identities_and_revisions() -> TestResult {
    let valid = wire::ActorObservation {
        actor_id: ACTOR.into(),
        code_sha256: create().code_sha256,
        home_region: "la".into(),
        configuration_revision: 1,
        ..Default::default()
    };
    observation(Some(valid.clone()), 1)?;
    assert!(observation(None, 1).is_err());
    for field in 0..4 {
        let mut changed = valid.clone();
        match field {
            0 => changed.actor_id.push_str("-other"),
            1 => changed.code_sha256.clear(),
            2 => changed.home_region.push_str("-other"),
            _ => changed.configuration_revision += 1,
        }
        assert!(observation(Some(changed), 1).is_err());
    }
    Ok(())
}

proptest::proptest! {
    #[test]
    fn subscription_oracle_rejects_changed_identity_path_cursors_and_order(cursor in proptest::prelude::any::<u64>()) {
        let mut expected = vec![subscription(cursor), subscription(cursor)];
        expected[1].subscription_id = "other".into();
        let valid = wire::ActorObservation {
            subscriptions: expected.clone(),
            ..Default::default()
        };
        assert!(subscriptions(&valid, &expected).is_ok());
        for field in 0..10 {
            let mut changed = valid.clone();
            let sub = &mut changed.subscriptions[0];
            match field {
                0 => sub.subscription_id.push_str("-changed"),
                1 => sub.stream_path.push_str("/changed"),
                2 => sub.delivered_cursor = cursor.wrapping_add(1),
                3 => sub.completed_cursor = cursor.wrapping_add(1),
                4 => sub.recoverable_cursor = cursor.wrapping_add(1),
                5 => sub.state = wire::SubscriptionState::Paused as i32,
                6 => sub.placement_anchor = true,
                7 => sub.retry_count = 1,
                8 => sub.failure_code = "customer_exception".into(),
                _ => sub.failed_cursor = Some(cursor),
            }
            assert!(subscriptions(&changed, &expected).is_err());
        }
        let mut reordered = valid;
        reordered.subscriptions.swap(0, 1);
        assert!(subscriptions(&reordered, &expected).is_err());
    }

    #[test]
    fn invocation_oracle_checks_exact_state_and_status(counter in -1000i32..1000) {
        let mut response = wire::InvokeActorResponse {
            status: 200,
            body: format!("{{\"counter\":{counter},\"version\":1,\"events\":[]}}").into_bytes(),
            ..Default::default()
        };
        assert!(state(&response, counter).is_ok());
        // Retaining the v1 worker must fail the migration oracle even with
        // the correct counter and an updated configuration revision.
        assert!(state_with_version(&response, counter, 2).is_err());
        let migrated = wire::InvokeActorResponse {
            status: 200,
            body: format!("{{\"counter\":{counter},\"version\":2,\"events\":[],\"migratedFrom\":1}}").into_bytes(),
            ..Default::default()
        };
        assert!(state_with_version(&migrated, counter, 2).is_ok());
        assert!(state_with_version(&migrated, counter + 1, 2).is_err());
        assert!(state(&migrated, counter).is_err());
        assert!(state(&response, counter + 1).is_err());
        response.status = 503;
        assert!(state(&response, counter).is_err());
        response.status = 200;
        response.body[0] ^= 1;
        assert!(state(&response, counter).is_err());
    }
}

proptest::proptest! {
#[test]
fn delivery_oracle_rejects_omission_duplication_and_reordering(chars in proptest::collection::vec(proptest::prelude::any::<char>(), 0..16)) {
    let tag: String = chars.into_iter().collect();
    let events = vec![format!("v1:1:{tag}"), format!("v1:2:{tag}")];
    for (observed, accepted) in [
        (events.clone(), true),
        (vec![events[0].clone()], false),
        (
            vec![events[0].clone(), events[0].clone(), events[1].clone()],
            false,
        ),
        (vec![events[1].clone(), events[0].clone()], false),
    ] {
        let response = wire::InvokeActorResponse {
            status: 200,
            body: format!(
                "{{\"counter\":0,\"version\":1,\"events\":{}}}",
                serde_json::to_string(&observed).expect("serializable event strings")
            )
            .into_bytes(),
            ..Default::default()
        };
        assert_eq!(
            state_with_events(&response, 0, 1, &events).is_ok(),
            accepted
        );
    }
}
}

async fn published_actor(endpoints: &Endpoints) -> TestResult<grpc::Client> {
    let ca = std::fs::read(&endpoints.objects.ca_file)?;
    let objects = GrpcObjects::connect(
        &endpoints.objects.endpoint,
        &endpoints.objects.bearer,
        Some(&ca),
        None,
    )
    .await?;
    let bucket = objects
        .create_bucket(objects::CreateBucketRequest {
            name: "actors-code".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("code bucket omitted")?;
    for code in [CODE, CODE_V2] {
        let hash: String = digest(code)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        objects
            .put(
                objects::PutObjectHeader {
                    bucket: Some(bucket.clone()),
                    object_key: format!("sha256/{hash}"),
                    ..Default::default()
                },
                acyclic_stream::OwnedBytes::copy_from_slice(code.trim_end_matches('\n').as_bytes()),
            )
            .await?;
    }

    let actor_ca = std::fs::read(&endpoints.actors.remote.ca_file)?;
    let actors = grpc::connect(
        &endpoints.actors.remote.endpoint,
        &endpoints.actors.remote.bearer,
        Some(&actor_ca),
    )
    .await?;
    Ok(actors)
}

async fn event_stream(endpoints: &Endpoints) -> TestResult<(acyclic_stream::grpc::Client, u64)> {
    let ca = std::fs::read(&endpoints.streams.ca_file)?;
    let streams = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        &endpoints.streams.bearer,
        ca,
    )
    .await?;
    let start = append_events(
        &streams,
        vec![acyclic_stream::OwnedBytes::from_static(
            b"before-subscription",
        )],
    )
    .await?
    .end;
    Ok((streams, start))
}

#[tokio::test]
#[ignore = "needs node scripts/local-cloud.mjs --topology full"]
async fn supported_actor_lifecycle_matches_the_counter_oracle() -> TestResult {
    let endpoints = endpoints()?;
    let phase = std::time::Instant::now();
    let (actors, events) = futures::join!(published_actor(&endpoints), event_stream(&endpoints));
    let mut actors = actors?;
    let (_streams, start) = events?;
    println!("Actors setup: {:.3}s", phase.elapsed().as_secs_f64());
    let phase = std::time::Instant::now();
    let created = observation(actors.create_actor(create()).await?.into_inner().actor, 1)?;
    assert_eq!(created.state, wire::ActorState::Paused as i32);
    assert!(created.subscriptions.is_empty());
    let mut retrier = actors.clone();
    let (retry, inspected) = futures::join!(
        retrier.create_actor(create()),
        actors.inspect_actor(wire::InspectActorRequest {
            actor_id: ACTOR.into()
        })
    );
    assert_eq!(observation(retry?.into_inner().actor, 1)?, created);
    assert_eq!(observation(inspected?.into_inner().actor, 1)?, created);
    invoke(&mut actors, None, 0).await?;
    for (add, counter) in [(2, 2), (-1, 1), (4, 5)] {
        invoke(&mut actors, Some(add), counter).await?;
    }
    let checkpoint = wire::CheckpointActorRequest {
        actor_id: ACTOR.into(),
        idempotency_key: "sdk-checkpoint".into(),
    };
    let saved = observation(
        actors
            .checkpoint_actor(checkpoint.clone())
            .await?
            .into_inner()
            .actor,
        1,
    )?;
    assert!(saved.checkpoint_epoch > 0);
    assert!(saved.checkpoint_unix_millis.is_some());
    assert_eq!(saved.state, wire::ActorState::Active as i32);
    let (retry, invoked) = futures::join!(
        retrier.checkpoint_actor(checkpoint),
        invoke(&mut actors, None, 5)
    );
    assert_eq!(observation(retry?.into_inner().actor, 1)?, saved);
    invoked?;
    println!(
        "Actors counter/checkpoint: {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = std::time::Instant::now();

    let add = add_subscription("sdk-add");
    let added = observation(
        actors
            .add_subscription(add.clone())
            .await?
            .into_inner()
            .actor,
        2,
    )?;
    let expected_subscriptions = [subscription(start)];
    subscriptions(&added, &expected_subscriptions)?;
    assert_eq!(
        observation(
            actors
                .add_subscription(add.clone())
                .await?
                .into_inner()
                .actor,
            2
        )?,
        added
    );
    let removed = observation(
        actors
            .remove_subscription(wire::RemoveSubscriptionRequest {
                actor_id: ACTOR.into(),
                subscription_id: "events".into(),
                idempotency_key: "sdk-remove".into(),
            })
            .await?
            .into_inner()
            .actor,
        3,
    )?;
    subscriptions(&removed, &[])?;
    let mut readd = add;
    readd.idempotency_key = "sdk-readd".into();
    let readded = observation(actors.add_subscription(readd).await?.into_inner().actor, 4)?;
    subscriptions(&readded, &expected_subscriptions)?;
    let actor_ca = std::fs::read(&endpoints.actors.remote.ca_file)?;
    actors = grpc::connect(
        &endpoints.actors.remote.endpoint,
        &endpoints.actors.migration_bearer,
        Some(&actor_ca),
    )
    .await?;
    let update = wire::UpdateActorRequest {
        actor_id: ACTOR.into(),
        code_sha256: digest(CODE_V2),
        limits: create().limits,
        bindings: vec![],
        expected_configuration_revision: 4,
        idempotency_key: "sdk-migrate-v2".into(),
    };
    let updated = observation_with_code(
        actors
            .update_actor(update.clone())
            .await?
            .into_inner()
            .actor,
        5,
        CODE_V2,
    )?;
    subscriptions(&updated, &expected_subscriptions)?;
    let mut retrier = actors.clone();
    let (retry, invoked) = futures::join!(retrier.update_actor(update), async {
        invoke_version(&mut actors, None, 5, 2).await?;
        invoke_version(&mut actors, Some(3), 8, 2).await
    });
    assert_eq!(
        observation_with_code(retry?.into_inner().actor, 5, CODE_V2)?,
        updated
    );
    invoked?;
    println!(
        "Actors subscriptions/migration: {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = std::time::Instant::now();
    let deleted = wire::DeleteActorRequest {
        actor_id: ACTOR.into(),
        idempotency_key: "sdk-delete".into(),
    };
    actors.delete_actor(deleted.clone()).await?;
    let (retry, missing) = futures::join!(
        retrier.delete_actor(deleted),
        actors.inspect_actor(wire::InspectActorRequest {
            actor_id: ACTOR.into()
        })
    );
    retry?;
    let missing = missing.expect_err("deleted actor must be absent");
    assert_eq!(
        grpc::error_detail(&missing).map(|error| error.code),
        Some(wire::ErrorCode::ActorNotFound as i32)
    );
    println!("Actors deletion: {:.3}s", phase.elapsed().as_secs_f64());
    println!(
        "local cloud: supported Actor lifecycle and exact v2 migration passed; delivery and paused recovery are separate ignored Cloud request 11 checks"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs Cloud request 11: delivery signals"]
async fn subscription_delivery_advances_cursors_and_preserves_event_order() -> TestResult {
    let endpoints = endpoints()?;
    let mut actors = published_actor(&endpoints).await?;
    observation(actors.create_actor(create()).await?.into_inner().actor, 1)?;
    invoke(&mut actors, None, 0).await?;
    let streams = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        &endpoints.streams.bearer,
        std::fs::read(&endpoints.streams.ca_file)?,
    )
    .await?;
    let start = append_events(
        &streams,
        vec![acyclic_stream::OwnedBytes::from_static(
            b"before-subscription",
        )],
    )
    .await?
    .end;
    let added = observation(
        actors
            .add_subscription(add_subscription("sdk-delivery-add"))
            .await?
            .into_inner()
            .actor,
        2,
    )?;
    subscriptions(&added, &[subscription(start)])?;
    let receipt = append_events(
        &streams,
        vec![
            acyclic_stream::OwnedBytes::from_static(b"event-a"),
            acyclic_stream::OwnedBytes::from_static(b"event-b"),
        ],
    )
    .await?;
    assert_eq!(receipt.start, start);
    assert_eq!(receipt.end.checked_sub(receipt.start), Some(2));
    // Request 11 must supply the pushed completion boundary before these
    // snapshots can be enabled. They are assertions, never readiness polling.
    let delivered = observation(
        actors
            .inspect_actor(wire::InspectActorRequest {
                actor_id: ACTOR.into(),
            })
            .await?
            .into_inner()
            .actor,
        2,
    )?;
    let mut expected = subscription(receipt.end);
    expected.recoverable_cursor = start;
    subscriptions(&delivered, &[expected])?;
    let events = vec![
        format!("v1:{}:event-a", receipt.start),
        format!("v1:{}:event-b", receipt.end - 1),
    ];
    state_with_events(&invoke_response(&mut actors, None).await?, 0, 1, &events)?;
    let saved = observation(
        actors
            .checkpoint_actor(wire::CheckpointActorRequest {
                actor_id: ACTOR.into(),
                idempotency_key: "sdk-delivery-checkpoint".into(),
            })
            .await?
            .into_inner()
            .actor,
        2,
    )?;
    subscriptions(&saved, &[subscription(receipt.end)])?;
    Ok(())
}

#[tokio::test]
#[ignore = "needs Cloud request 11: retired Pause / paused recovery"]
async fn paused_subscription_resumes_with_exact_cursor_preservation() -> TestResult {
    // Cloud must supply a genuinely failed, paused fixture subscription;
    // its retired Pause mutation cannot currently construct that precondition.
    let endpoints = endpoints()?;
    let remote = &endpoints.actors.remote;
    let mut actors = grpc::connect(
        &remote.endpoint,
        &remote.bearer,
        Some(&std::fs::read(&remote.ca_file)?),
    )
    .await?;
    let before = actors
        .inspect_actor(wire::InspectActorRequest {
            actor_id: ACTOR.into(),
        })
        .await?
        .into_inner()
        .actor
        .ok_or("paused fixture actor missing")?;
    assert_eq!(before.actor_id, ACTOR);
    assert_eq!(before.subscriptions.len(), 1);
    let paused = &before.subscriptions[0];
    assert_eq!(paused.subscription_id, "events");
    assert_eq!(paused.stream_path, EVENTS);
    assert_eq!(paused.state, wire::SubscriptionState::Paused as i32);
    assert_eq!(paused.failed_cursor, Some(paused.completed_cursor));
    assert!(!paused.failure_code.is_empty());
    let mut expected = before.subscriptions.clone();
    expected[0].state = wire::SubscriptionState::Active as i32;
    expected[0].failed_cursor = None;
    expected[0].failure_code.clear();
    expected[0].retry_count = 0;
    let request = wire::ResumeSubscriptionRequest {
        actor_id: ACTOR.into(),
        subscription_id: "events".into(),
        idempotency_key: "sdk-paused-resume".into(),
    };
    let resumed = actors
        .resume_subscription(request.clone())
        .await?
        .into_inner()
        .actor
        .ok_or("resume omitted actor")?;
    subscriptions(&resumed, &expected)?;
    let retried = actors
        .resume_subscription(request)
        .await?
        .into_inner()
        .actor
        .ok_or("resume retry omitted actor")?;
    assert_eq!(retried, resumed);
    subscriptions(&retried, &expected)?;
    Ok(())
}
