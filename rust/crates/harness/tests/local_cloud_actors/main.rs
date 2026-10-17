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
    let migration = if version == 2 {
        ",\"migratedFrom\":1"
    } else {
        ""
    };
    let expected =
        format!("{{\"counter\":{counter},\"version\":{version},\"events\":[]{migration}}}");
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
    let response = client
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
        .into_inner();
    state_with_version(&response, counter, version)
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

#[tokio::test]
#[ignore = "needs node scripts/local-cloud.mjs --topology full"]
async fn actors_lifecycle_matches_the_counter_oracle() -> TestResult {
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD").ok_or("local-cloud endpoints required")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
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
    let mut actors = grpc::connect(
        &endpoints.actors.remote.endpoint,
        &endpoints.actors.remote.bearer,
        Some(&actor_ca),
    )
    .await?;
    let created = observation(actors.create_actor(create()).await?.into_inner().actor, 1)?;
    assert_eq!(created.state, wire::ActorState::Paused as i32);
    assert!(created.subscriptions.is_empty());
    assert_eq!(
        observation(actors.create_actor(create()).await?.into_inner().actor, 1)?,
        created
    );
    assert_eq!(
        observation(
            actors
                .inspect_actor(wire::InspectActorRequest {
                    actor_id: ACTOR.into()
                })
                .await?
                .into_inner()
                .actor,
            1
        )?,
        created
    );
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
    assert_eq!(
        observation(
            actors
                .checkpoint_actor(checkpoint)
                .await?
                .into_inner()
                .actor,
            1
        )?,
        saved
    );
    invoke(&mut actors, None, 5).await?;

    let ca = std::fs::read(&endpoints.streams.ca_file)?;
    let streams = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        &endpoints.streams.bearer,
        ca,
    )
    .await?;
    let appended = streams
        .append(AppendRequest {
            path: StreamPath::new(EVENTS)?,
            records: vec![acyclic_stream::OwnedBytes::from_static(
                b"before-subscription",
            )],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let AppendOutcome::Committed(receipt) = appended else {
        return Err("the pre-subscription record was not committed".into());
    };
    let start = receipt.end;
    let add = wire::AddSubscriptionRequest {
        actor_id: ACTOR.into(),
        idempotency_key: "sdk-add".into(),
        subscription: Some(wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: EVENTS.into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        }),
    };
    let added = observation(
        actors
            .add_subscription(add.clone())
            .await?
            .into_inner()
            .actor,
        2,
    )?;
    assert_eq!(added.subscriptions.len(), 1);
    assert_eq!(added.subscriptions[0].subscription_id, "events");
    assert_eq!(added.subscriptions[0].stream_path, EVENTS);
    assert_eq!(
        added.subscriptions[0].state,
        wire::SubscriptionState::Active as i32
    );
    assert_eq!(added.subscriptions[0].delivered_cursor, start);
    assert_eq!(added.subscriptions[0].completed_cursor, start);
    assert_eq!(added.subscriptions[0].recoverable_cursor, start);
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
    let resumed = observation(
        actors
            .resume_subscription(wire::ResumeSubscriptionRequest {
                actor_id: ACTOR.into(),
                subscription_id: "events".into(),
                idempotency_key: "sdk-resume".into(),
            })
            .await?
            .into_inner()
            .actor,
        2,
    )?;
    assert_eq!(
        resumed.subscriptions[0].state,
        wire::SubscriptionState::Active as i32
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
    assert!(removed.subscriptions.is_empty());
    let mut readd = add;
    readd.idempotency_key = "sdk-readd".into();
    observation(actors.add_subscription(readd).await?.into_inner().actor, 4)?;
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
    assert_eq!(updated.subscriptions.len(), 1);
    assert_eq!(
        observation_with_code(
            actors.update_actor(update).await?.into_inner().actor,
            5,
            CODE_V2
        )?,
        updated
    );
    invoke_version(&mut actors, None, 5, 2).await?;
    invoke_version(&mut actors, Some(3), 8, 2).await?;
    let deleted = wire::DeleteActorRequest {
        actor_id: ACTOR.into(),
        idempotency_key: "sdk-delete".into(),
    };
    actors.delete_actor(deleted.clone()).await?;
    actors.delete_actor(deleted).await?;
    let missing = actors
        .inspect_actor(wire::InspectActorRequest {
            actor_id: ACTOR.into(),
        })
        .await
        .expect_err("deleted actor must be absent");
    assert_eq!(
        grpc::error_detail(&missing).map(|error| error.code),
        Some(wire::ErrorCode::ActorNotFound as i32)
    );
    println!(
        "local cloud: all nine SDK Actors operations passed, exact counter state and retries checked"
    );
    Ok(())
}
