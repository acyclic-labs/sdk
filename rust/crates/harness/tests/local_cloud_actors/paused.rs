use super::*;

const ID: &str = "sdk-paused-poison";
const SOURCE: &str = "e2e-root/sdk-paused-poison";
const POISON: &str = include_str!("poison.mjs");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn configuration() -> wire::CreateActorRequest {
    let mut request = create();
    request.code_sha256 = digest(POISON);
    request.idempotency_key = "sdk-create-paused-poison".into();
    request.subscriptions = vec![wire::SubscriptionSpec {
        subscription_id: "events".into(),
        stream_path: SOURCE.into(),
        start: Some(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::CurrentHead(true)),
        }),
        placement_anchor: false,
    }];
    request
}

fn paused() -> wire::SubscriptionObservation {
    wire::SubscriptionObservation {
        stream_path: SOURCE.into(),
        state: wire::SubscriptionState::Paused as i32,
        failed_cursor: Some(1),
        retry_count: 1,
        failure_code: "customer_exception".into(),
        ..subscription(1)
    }
}

// All boundaries use fixture inputs, never a previous remote reply as configuration.
fn observation(
    actor: Option<wire::ActorObservation>,
    request: &wire::CreateActorRequest,
    expected: &[wire::SubscriptionObservation],
) -> TestResult<wire::ActorObservation> {
    let actor = actor_configuration(actor, ID, request, 1)?;
    subscriptions(&actor, expected)?;
    Ok(actor)
}

fn active() -> wire::SubscriptionObservation {
    wire::SubscriptionObservation {
        stream_path: SOURCE.into(),
        ..subscription(1)
    }
}

// Cloud's additive public Configure RPC; no internal mutation or state injection.
#[derive(Clone, PartialEq, prost::Message)]
struct Configure {
    #[prost(string, tag = "1")]
    actor_id: String,
    #[prost(uint32, optional, tag = "2")]
    max_attempts: Option<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Configured {
    #[prost(uint32, tag = "1")]
    max_attempts: u32,
}

async fn configure(remote: &Remote, bearer: &str, max_attempts: u32) -> TestResult {
    let channel = tonic::transport::Endpoint::from_shared(remote.endpoint.clone())?
        .tls_config(tonic::transport::ClientTlsConfig::new().ca_certificate(
            tonic::transport::Certificate::from_pem(std::fs::read(&remote.ca_file)?),
        ))?
        .connect()
        .await?;
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await?;
    let mut request = tonic::Request::new(Configure {
        actor_id: ID.into(),
        max_attempts: Some(max_attempts),
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {bearer}").parse()?);
    let response: tonic::Response<Configured> = client
        .unary(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/acyclic.actors.v1.ActorsService/DeadLetters",
            ),
            tonic_prost::ProstCodec::default(),
        )
        .await?;
    assert_eq!(response.into_inner().max_attempts, max_attempts);
    Ok(())
}

async fn published_poison_actor(
    endpoints: &Endpoints,
    bearer: &str,
    request: &wire::CreateActorRequest,
) -> TestResult<grpc::Client> {
    let objects = GrpcObjects::connect(
        &endpoints.objects.endpoint,
        &endpoints.objects.bearer,
        Some(&std::fs::read(&endpoints.objects.ca_file)?),
        None,
    )
    .await?;
    let bucket = objects
        .create_bucket(objects::CreateBucketRequest {
            name: "actors-code".into(),
            mutation: Some(objects::MutationIdentity {
                idempotency_key: "sdk-actors-code-bucket".into(),
            }),
        })
        .await?
        .bucket
        .ok_or("code bucket omitted")?;
    objects
        .put(
            objects::PutObjectHeader {
                bucket: Some(bucket),
                object_key: format!("sha256/{}", hex(&request.code_sha256)),
                ..Default::default()
            },
            acyclic_stream::OwnedBytes::copy_from_slice(POISON.trim_end_matches('\n').as_bytes()),
        )
        .await?;
    let remote = &endpoints.actors.remote;
    let mut actors = grpc::connect(
        &remote.endpoint,
        bearer,
        Some(&std::fs::read(&remote.ca_file)?),
    )
    .await?;
    observation(
        actors
            .create_actor(request.clone())
            .await?
            .into_inner()
            .actor,
        request,
        &[active()],
    )?;
    Ok(actors)
}

async fn inspect(
    actors: &mut grpc::Client,
    request: &wire::CreateActorRequest,
    expected: &[wire::SubscriptionObservation],
) -> TestResult {
    observation(
        actors
            .inspect_actor(wire::InspectActorRequest {
                actor_id: ID.into(),
            })
            .await?
            .into_inner()
            .actor,
        request,
        expected,
    )?;
    Ok(())
}

pub(super) async fn qualify() -> TestResult {
    let endpoints = endpoints()?;
    let request = configuration();
    let bearer =
        endpoints.actors.paused_bearer.as_deref().ok_or(
            "paused-delivery gap: fixture must supply an independently scoped paused_bearer",
        )?;
    let ca = std::fs::read(&endpoints.streams.ca_file)?;
    let source = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        &endpoints.streams.bearer,
        ca.clone(),
    )
    .await?;
    let observed = acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(
        &endpoints.streams.endpoint,
        bearer,
        ca,
    )
    .await?;
    let source_path = StreamPath::new(SOURCE)?;
    let append = |value: &'static [u8]| AppendRequest {
        path: source_path.clone(),
        records: vec![value.into()],
        if_tail: None,
        idempotency_key: None,
    };
    assert!(matches!(
        source.append(append(b"before-subscription")).await?,
        AppendOutcome::Committed(_)
    ));
    let remote = &endpoints.actors.remote;
    let mut actors = published_poison_actor(&endpoints, bearer, &request).await?;
    configure(remote, bearer, 1).await?;
    inspect(&mut actors, &request, &[active()]).await?;
    let boundary = observed.tail(observation_path(ID)?).await?;
    let AppendOutcome::Committed(poison) = source.append(append(b"poison")).await? else {
        return Err("poison append did not commit".into());
    };
    assert_eq!((poison.start, poison.end), (1, 2));
    let (before, next) = followed_observation(&observed, ID, boundary, |actor| {
        actor
            .subscriptions
            .iter()
            .any(|sub| sub.state == wire::SubscriptionState::Paused as i32)
    })
    .await?;
    observation(Some(before), &request, &[paused()])?;
    configure(remote, bearer, 2).await?;
    // One public read after the acknowledged Configure, never an Inspect polling loop.
    let configured = actors
        .inspect_actor(wire::InspectActorRequest {
            actor_id: ID.into(),
        })
        .await?
        .into_inner()
        .actor
        .ok_or("configured actor missing")?;
    observation(Some(configured), &request, &[paused()])?;
    let mut expected = paused();
    expected.state = wire::SubscriptionState::Active as i32;
    expected.failed_cursor = None;
    expected.failure_code.clear();
    expected.retry_count = 0;
    let resume = wire::ResumeSubscriptionRequest {
        actor_id: ID.into(),
        subscription_id: "events".into(),
        idempotency_key: "sdk-resume-poison".into(),
    };
    let resumed = actors
        .resume_subscription(resume.clone())
        .await?
        .into_inner()
        .actor
        .ok_or("resume omitted actor")?;
    let resumed = observation(Some(resumed), &request, std::slice::from_ref(&expected))?;
    let retried = actors
        .resume_subscription(resume)
        .await?
        .into_inner()
        .actor
        .ok_or("resume retry omitted actor")?;
    let retried = observation(Some(retried), &request, std::slice::from_ref(&expected))?;
    assert_eq!(retried, resumed);
    let (retained, _) = followed_observation(&observed, ID, next, |actor| {
        actor.subscriptions == [expected.clone()]
    })
    .await?;
    let retained = observation(Some(retained), &request, &[expected])?;
    assert_eq!(retained, resumed);
    actors
        .delete_actor(wire::DeleteActorRequest {
            actor_id: ID.into(),
            idempotency_key: "sdk-delete-poison".into(),
        })
        .await?;
    Ok(())
}

#[test]
fn paused_oracle_rejects_active_and_changed_failure_boundaries() -> TestResult {
    let request = configuration();
    let expected = paused();
    let valid = wire::ActorObservation {
        actor_id: ID.into(),
        code_sha256: request.code_sha256.clone(),
        home_region: request.home_region.clone(),
        configuration_revision: 1,
        subscriptions: vec![expected.clone()],
        ..Default::default()
    };
    observation(
        Some(valid.clone()),
        &request,
        std::slice::from_ref(&expected),
    )?;
    for field in 0..10 {
        let mut changed = valid.clone();
        let sub = changed
            .subscriptions
            .first_mut()
            .ok_or("subscription missing")?;
        match field {
            0 => sub.state = wire::SubscriptionState::Active as i32,
            1 => sub.failed_cursor = None,
            2 => sub.retry_count += 1,
            3 => sub.failure_code.clear(),
            4 => sub.completed_cursor += 1,
            5 => sub.delivered_cursor += 1,
            6 => sub.recoverable_cursor += 1,
            7 => sub.stream_path.push_str("-wrong"),
            8 => sub.subscription_id.push_str("-wrong"),
            _ => sub.placement_anchor = true,
        }
        assert!(observation(Some(changed), &request, std::slice::from_ref(&expected)).is_err());
    }
    Ok(())
}

proptest::proptest! {
    #[test]
    fn every_boundary_rejects_matching_corrupt_configuration(
        revision in 2u64..=u64::MAX,
        hash_byte in 0usize..32,
        region_suffix in "[a-z]{1,16}",
    ) {
        let request = configuration();
        // Create, first Configure, Pause, second Configure, Resume, retry, retained follow.
        for expected in [active(), active(), paused(), paused(), active(), active(), active()] {
            let valid = wire::ActorObservation {
                actor_id: ID.into(),
                code_sha256: request.code_sha256.clone(),
                home_region: request.home_region.clone(),
                configuration_revision: 1,
                subscriptions: vec![expected.clone()],
                ..Default::default()
            };
            assert!(observation(Some(valid.clone()), &request, std::slice::from_ref(&expected)).is_ok());
            assert!(observation(None, &request, std::slice::from_ref(&expected)).is_err());
            for field in 0..4 {
                let mut reply = valid.clone();
                match field {
                    0 => reply.actor_id.push_str("-wrong"),
                    1 => *reply.code_sha256.get_mut(hash_byte).ok_or_else(|| proptest::test_runner::TestCaseError::fail("hash byte missing"))? ^= 1,
                    2 => reply.home_region.push_str(&region_suffix),
                    _ => reply.configuration_revision = revision,
                }
                let retained = reply.clone();
                // Negative control: the former subscription/equality oracle admits
                // identical corruption in both remote outputs.
                assert_eq!(reply, retained);
                assert!(subscriptions(&reply, std::slice::from_ref(&expected)).is_ok());
                for actor in [reply, retained] {
                    assert!(observation(Some(actor), &request, std::slice::from_ref(&expected)).is_err());
                }
            }
        }
    }
}
