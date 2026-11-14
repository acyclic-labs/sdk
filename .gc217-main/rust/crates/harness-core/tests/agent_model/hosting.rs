//! The admitted root producer: one placement builds both the agents and their
//! volumes, and a root's fresh volumes and adoption are one commit prepared
//! without stream IO and held until it settles. Refusal vectors run for
//! every host in the agent-host suite.

use super::{Result, id, intent};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_core::{
    OperationId,
    agent::*,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, TurnAdmission, TurnAuthority, TurnExecutors, TurnLimits,
        attestation::{Attempt, Attestation, Attested, AttestedProvider, Attestor},
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamError, StreamProvider};
use std::sync::{Arc, atomic::Ordering};

/// Never builds an executor: these tests only create roots.
struct Never;

impl TurnExecutors for Never {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async {
            Err(acyclic_harness_core::Error::Unsupported(
                "no turns in this test".into(),
            ))
        })
    }
}

type Objects = LogicalObjectStore<acyclic_objects::MemoryObjects>;

fn provider() -> Result<ProviderRef> {
    Ok(ProviderRef::new(
        "agents".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?)
}

#[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
#[tokio::test]
async fn guest_admission_refuses_wrong_volume_provider_and_hosted_mapping_before_io() -> Result {
    use super::wired::Refusing;
    use acyclic_fs::native_mount::GuestPosixPolicy;
    use acyclic_harness_core::{
        contract::Capabilities,
        filesystem::hosting::{LocalGuestEnrollment, VerifiedCaller},
    };
    acyclic_native_runtime::initialize_native_io_services()?;
    acyclic_fs::initialize_native_mount_services()?;
    let stream = Arc::new(Refusing::default());
    let (placed, bucket) = placed(&stream)?;
    let reference = placed.provider.clone();
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "guest".into(),
    };
    let issuer = AuthorityRoot::new("agents", [7; 32], audience.clone());
    let scope = issuer.bootstrap_scope("guest", &Capabilities::default(), None, &mut None)?;
    let hosted = Bound::admitted(
        issuer.clone(),
        VerifiedCaller { audience, scope },
        placed,
        turns()?,
    )?;
    let local = Bound::local_root(issuer, placement(&stream)?, turns()?)?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let enrollment = |provider: &ProviderRef| -> Result<LocalGuestEnrollment> {
        Ok(LocalGuestEnrollment {
            volume: VolumeRef::new(
                provider.clone(),
                "agent-guest".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(id(7).to_bytes())),
                &mut None,
            )?,
            posix: GuestPosixPolicy {
                identities: vec![(0, 0)],
                metadata: true,
            },
            writable: true,
        })
    };
    assert!(matches!(
        {
            let selected = enrollment(&reference)?;
            hosted
                .guest_session(
                    selected.volume,
                    selected.posix,
                    selected.writable,
                    None,
                    &work,
                )
                .await
        },
        Err(acyclic_harness_core::Error::Unsupported(_))
    ));
    let other = ProviderRef::new("other".into(), "filesystem".into(), "2".into(), &mut None)?;
    assert!(
        local
            .guest_session(
                enrollment(&other)?.volume,
                enrollment(&other)?.posix,
                true,
                None,
                &work
            )
            .await
            .is_err()
    );
    assert_eq!(stream.calls.load(Ordering::SeqCst), 0);
    assert_eq!(bucket.objects().await?, 0);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

/// The volumes' blobs, to observe what preparation writes.
struct Bucket(Arc<acyclic_objects::MemoryObjects>);

impl Bucket {
    /// How many blobs the account holds.
    async fn objects(&self) -> Result<usize> {
        use acyclic_objects::Blobs as _;
        Ok(self.0.list(None, 1).await?.digests.len())
    }
}

/// The one placement over `stream`.
fn placement<P: 'static>(stream: &Arc<P>) -> Result<Placement<P, Objects>> {
    Ok(placed(stream)?.0)
}

/// The one placement over `stream`, and its blobs.
fn placed<P: 'static>(stream: &Arc<P>) -> Result<(Placement<P, Objects>, Bucket)> {
    let objects = acyclic_objects::MemoryObjects::memory();
    let objects = Arc::new(objects);
    let observed = Bucket(objects.clone());
    Ok((
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(objects),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: provider()?,
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        observed,
    ))
}

fn turns() -> Result<Turns> {
    Ok(Turns {
        executors: Arc::new(Never),
        limits: TurnLimits {
            maximum_bytes: Some(64 * 1024),
            history_turns: 8,
        },
        actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        clock: Arc::new(SystemClock),
        approval_policy: Default::default(),
    })
}

fn volume(name: &str, agent: AgentId) -> Result<WorkspaceEntry> {
    Ok(WorkspaceEntry::initial_volume(
        WorkspaceRole::Private,
        name.into(),
        VolumeRef::new(
            provider()?,
            format!("agent-{name}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    ))
}

fn root(agent: AgentId, fresh: Vec<WorkspaceEntry>) -> RootRequest {
    RootRequest {
        peer: true,
        agent,
        fresh,
        retained: Vec::new(),
        brief: None,
        policy: None,
    }
}

// Pending inbox envelopes are host-owned journal content, not published client
// output. Inspect the original reference under the fixture owner's authority.
async fn read_host_input(
    bound: &Bound<MemoryStream, Objects>,
    issuer: &AuthorityRoot,
    agent: &AgentPath,
    file: &acyclic_harness_core::conversation::FileRef,
) -> Result<Vec<u8>> {
    use acyclic_harness_core::conversation::{ContentResidencyVerifier, VolumeOperation};
    let journal = acyclic_harness_core::conversation::journal_volume(provider()?, &agent.id())?;
    let scope = issuer.bootstrap_scope(
        "control-input",
        &acyclic_harness_core::Capabilities::new([
            journal.capability(VolumeOperation::Read, &mut None)?,
            acyclic_harness_core::contract::capability::HARNESS_HOST.to_owned(),
        ]),
        Some(acyclic_harness_core::AgentId::from_bytes(agent.id_bytes())),
        &mut None,
    )?;
    let reader = acyclic_harness_core::filesystem::FilesystemContentVerifier::new(
        bound.volumes().clone(),
        &issuer.verifier(&mut None)?,
        &scope,
        None,
        None,
    )?;
    Ok(reader
        .read(file, file.descriptor().byte_length(), None, None)?
        .await?
        .to_vec())
}

/// Drive the real filesystem producer, shared dispatcher and ordinary client
/// files. Every wait is on the original event or inbox append, never a timer.
#[tokio::test]
async fn file_controls_pause_resume_append_steering_and_stop_through_shared_dispatch() -> Result {
    for guest in [false, true] {
        for maximum in [None, Some(64 * 1024)] {
            file_control_journey(guest, maximum).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn rejected_steering_cannot_block_owner_pause_or_stop() -> Result {
    use acyclic_harness_core::{
        filesystem::{WorkspaceMutation, workspace_ref},
        supervision::{Event, Lifecycle},
    };
    use futures::TryStreamExt as _;
    for (bytes, rejected) in [
        (vec![0xff], true),
        (vec![b'x'; 64 * 1024 + 1], true),
        // The file fits, but its escaped input envelope exceeds the selected
        // per-message capacity. Refuse the input without discarding the pause.
        (vec![b'"'; 32 * 1024], true),
        (b"healthy steering".to_vec(), false),
    ] {
        let stream = Arc::new(MemoryStream::default());
        let issuer = AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        );
        let bound = Bound::local_root(issuer.clone(), placement(&stream)?, turns()?)?;
        let private = volume(PRIVATE_VOLUME, id(125))?;
        let RootSubmission::Created(agent) = bound
            .create_root(
                &intent("bad-steering"),
                root(id(125), vec![private.clone()]),
            )
            .await?
        else {
            return Err("root uncertain".into());
        };
        let actor = bound.agents().actor(&agent)?;
        let store = acyclic_harness_core::agent::actor_store(&stream)?;
        let workspace = workspace_ref(
            private.volume()?.provider().clone(),
            &private.volume()?.storage_name(&mut None)?,
        )?;
        let mut request = acyclic_stream::FollowRequest::new(
            acyclic_harness_core::supervision::streams::path(&agent)?,
            0,
        );
        request.when_absent = acyclic_stream::WhenAbsent::Wait;
        let mut feed = acyclic_stream::records_only(stream.follow(request).await?);
        let exercise = async {
            // Use the ordinary filesystem producer: control files may be
            // authored through any filesystem client, including binary writes.
            bound
                .volumes()
                .apply(
                    &workspace,
                    None,
                    &[
                        WorkspaceMutation::CreateDirectory {
                            path: "/control".into(),
                        },
                        WorkspaceMutation::PutFile {
                            path: "/control/steer.md".into(),
                            bytes: bytes::Bytes::from(bytes).into(),
                        },
                        WorkspaceMutation::PutFile {
                            path: "/control/pause".into(),
                            bytes: bytes::Bytes::new().into(),
                        },
                    ],
                    &acyclic_harness_core::IdempotencyKey("pause-with-steering".into()),
                )
                .await?;
            let paused = feed.try_next().await?.ok_or("control feed ended")?;
            let paused_event: Event = serde_json::from_slice(&paused.value)?;
            assert!(matches!(
                paused_event.event,
                Lifecycle::Paused { signal: None }
            ));
            if rejected {
                let refusal = feed.try_next().await?.ok_or("rejection missing")?;
                let refusal_event: Event = serde_json::from_slice(&refusal.value)?;
                assert!(matches!(refusal_event.event,
                    Lifecycle::ControlRejected { path } if path == "control/steer.md"));
                assert_eq!(refusal.commit_id, paused.commit_id);
                assert_eq!(refusal_event.source, paused_event.source);
            }
            bound.agents().runtime().complete().await?;
            assert!(
                store
                    .load(&actor)
                    .await?
                    .state()
                    .ok_or("target missing")?
                    .paused()
            );
            assert_eq!(
                stream.tail(agent.inbox()?).await?,
                if rejected { 1 } else { 2 }
            );
            assert_eq!(
                stream
                    .tail(acyclic_harness_core::supervision::streams::path(&agent)?)
                    .await?,
                if rejected { 2 } else { 1 }
            );
            if rejected {
                let mut messages = acyclic_stream::records_only(
                    stream
                        .follow(acyclic_stream::FollowRequest::new(agent.inbox()?, 1))
                        .await?,
                );
                bound
                    .volumes()
                    .apply(
                        &workspace,
                        None,
                        &[WorkspaceMutation::PutFile {
                            path: "/control/steer.md".into(),
                            bytes: bytes::Bytes::from_static(b"corrected steering").into(),
                        }],
                        &acyclic_harness_core::IdempotencyKey("correct-steering".into()),
                    )
                    .await?;
                // A rejected candidate must not consume any prompt prefix.
                // Wait on either original output, so the negative fails on its
                // explicit rejection rather than waiting for a missing message.
                let record = tokio::select! {
                    message = messages.try_next() => message?.ok_or("steering inbox ended")?,
                    refusal = feed.try_next() => {
                        let _ = refusal?.ok_or("control feed ended")?;
                        return Err("corrected steering was rejected".into());
                    }
                };
                let InboxRecord::Message(message) = InboxRecord::decode(&record.value)? else {
                    return Err("corrected steering did not append a message".into());
                };
                let bytes = read_host_input(&bound, &issuer, &agent, &message.body).await?;
                assert_eq!(
                    serde_json::from_slice::<ModelContent>(&bytes)?,
                    ModelContent::Text("corrected steering".into())
                );
            }
            bound
                .volumes()
                .apply(
                    &workspace,
                    None,
                    &[WorkspaceMutation::PutFile {
                        path: "/control/stop".into(),
                        bytes: bytes::Bytes::new().into(),
                    }],
                    &acyclic_harness_core::IdempotencyKey("stop-with-steering".into()),
                )
                .await?;
            let stopped: Event =
                serde_json::from_slice(&feed.try_next().await?.ok_or("stop missing")?.value)?;
            assert!(matches!(stopped.event, Lifecycle::Stopped { signal: None }));
            assert_eq!(stream.tail(agent.closed()?).await?, 1);
            bound.agents().runtime().complete().await?;
            assert!(bound.agents().state(&agent).await?.closed);
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let result: Result = tokio::select! {
            result = bound.agents().run(|actor, error| panic!("file control delivery {actor:?}: {error}")) => match result { Err(error) => Err(error.into()), Ok(never) => match never {} },
            result = exercise => result,
        };
        result?;
    }
    Ok(())
}

/// Real filesystem output, shared actor alarms and lifecycle feeds. Advance
/// only the injected clock; every readiness wait follows committed actor state.
#[tokio::test]
async fn quiet_tool_alarm_rearms_on_output_and_refuses_completed_jobs() -> Result {
    use acyclic_actors::local::ControlledClock;
    use acyclic_harness_core::{
        client_files::{ClientFileChange, FileRoot},
        supervision::{Action, Kind, Lifecycle},
    };
    use acyclic_stream::{
        AppendRecord, AppendRequest, IdempotencyKey, MemoryLimits, RecordHeaders, StreamPath,
        UnixMillisClock,
    };
    use futures::TryStreamExt as _;
    use std::sync::atomic::AtomicU64;
    struct CommitClock(AtomicU64);
    impl UnixMillisClock for CommitClock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    for completion in [0, 1, 2, 3] {
        let finish = completion != 0;
        let commit_clock = Arc::new(CommitClock(AtomicU64::new(100)));
        let stream = Arc::new(MemoryStream::new_with_clock(
            MemoryLimits::default(),
            commit_clock.clone(),
        ));
        let (clock, mut time) = ControlledClock::new(100);
        let mut execution = turns()?;
        execution.clock = Arc::new(clock);
        let placed = placement(&stream)?;
        let within = placed.within.clone();
        let issuer = AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "quiet".into(),
            },
        );
        let bound = Bound::local_root(issuer.clone(), placed, execution)?;
        let RootSubmission::Created(agent) = bound
            .create_root(
                &intent("quiet-root"),
                root(id(126), vec![volume(PRIVATE_VOLUME, id(126))?]),
            )
            .await?
        else {
            return Err("uncertain root".into());
        };
        let run = OperationId::from_bytes([231; 16]);
        let execution = OperationId::from_bytes([232; 16]);
        let call = OperationId::from_bytes([233; 16]);
        stream
            .append(AppendRequest {
                path: acyclic_harness_core::supervision::streams::path(&agent)?,
                records: vec![AppendRecord::latest(
                    serde_json::to_vec(&acyclic_harness_core::supervision::Event {
                        version: 1,
                        agent: agent.clone(),
                        run: Some(run),
                        source: acyclic_harness_core::supervision::Position {
                            stream: agent.inbox()?,
                            sequence: 0,
                        },
                        event: Lifecycle::Started { input: 0 },
                    })?,
                    acyclic_harness_core::supervision::RUN_KEY,
                    false,
                )?],
                if_tail: None,
                idempotency_key: IdempotencyKey::new("quiet-run")?,
            })
            .await?;
        let jobs = StreamPath::new(format!("{agent}/supervision/jobs"))?;
        let mut keys = vec![
            call.into_bytes().to_vec().into(),
            b"started".to_vec().into(),
        ];
        keys.sort_unstable();
        let started = serde_json::json!({"version":1,"call":call,"event":{
            "kind":"started","run":run,"execution":execution,"quiet_ms":if completion == 3 { u64::MAX } else { 10 },
            "action":Action::Pause,"at_ms":100,"source":{"stream":format!("{agent}/execution"),"sequence":0}
        }});
        stream
            .append(AppendRequest {
                path: jobs.clone(),
                records: vec![AppendRecord {
                    value: serde_json::to_vec(&started)?.into(),
                    headers: RecordHeaders::new(keys, vec![])?,
                }],
                if_tail: None,
                idempotency_key: IdempotencyKey::new("quiet-start")?,
            })
            .await?;
        let actor = ActorReference::new(
            within.account().as_bytes(),
            within.namespace().as_str().as_bytes(),
            format!("tool-quiet-{call}").as_bytes(),
        )?;
        let store = actor_store(&stream)?;
        let mut changes = acyclic_stream::FollowRequest::new(store.path(&actor)?, 0);
        changes.when_absent = acyclic_stream::WhenAbsent::Wait;
        let mut states = acyclic_stream::records_only(stream.follow(changes).await?);
        let wait = async |states: &mut acyclic_stream::RecordStream,
                          deadline: Option<u64>,
                          retired: bool|
               -> Result {
            loop {
                states
                    .try_next()
                    .await?
                    .ok_or("quiet state source closed")?;
                let snapshot = store.load(&actor).await?;
                if let Some(state) = snapshot.state()
                    && state.paused() == retired
                    && state.alarm().deadline_unix_ms() == deadline
                {
                    return Ok(());
                }
            }
        };
        let agent_failure = bound
            .agents()
            .runtime()
            .delivery_failure(&bound.agents().actor(&agent)?)?;
        let exercise = async {
            if completion == 3 {
                let failure = agent_failure.await;
                assert!(
                    failure.to_string().contains("quiet deadline overflow"),
                    "{failure}"
                );
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            wait(&mut states, Some(110), false).await?;
            commit_clock.0.store(105, Ordering::SeqCst);
            let path = acyclic_harness_core::runtime::tool_output_activity_path(execution, call);
            let cursor =
                serde_json::to_string(&acyclic_harness_core::runtime::ToolOutputActivity {
                    version: 1,
                    turn: execution,
                    call,
                    through: 1,
                })?;
            // A model-writable lookalike cannot postpone the host's alarm.
            bound
                .publish_files(
                    &agent,
                    FileRoot::Private {},
                    OperationId::from_bytes([234; 16]),
                    &[ClientFileChange {
                        path: path.clone(),
                        expected: None,
                        text: Some(cursor.clone()),
                    }],
                )
                .await?;
            bound.agents().runtime().complete().await?;
            let completed_path =
                acyclic_harness_core::runtime::tool_output_completion_path(execution, call);
            bound
                .publish_files(
                    &agent,
                    FileRoot::Private {},
                    OperationId::from_bytes([238; 16]),
                    &[ClientFileChange {
                        path: completed_path.clone(),
                        expected: None,
                        text: Some("{}".into()),
                    }],
                )
                .await?;
            bound.agents().runtime().complete().await?;
            assert_eq!(
                store
                    .load(&actor)
                    .await?
                    .state()
                    .ok_or("quiet actor missing")?
                    .alarm()
                    .deadline_unix_ms(),
                Some(110),
            );
            use acyclic_harness_core::conversation::{
                ContentAccess, ContentPublisher, VolumeOperation,
            };
            let journal =
                acyclic_harness_core::conversation::journal_volume(provider()?, &agent.id())?;
            let scope = issuer.bootstrap_scope(
                "quiet-output",
                &acyclic_harness_core::Capabilities::new([
                    journal.capability(VolumeOperation::Read, &mut None)?,
                    journal.capability(VolumeOperation::Write, &mut None)?,
                    acyclic_harness_core::contract::capability::HARNESS_HOST.to_owned(),
                ]),
                Some(acyclic_harness_core::AgentId::from_bytes(agent.id_bytes())),
                &mut None,
            )?;
            let publisher = acyclic_harness_core::filesystem::FilesystemContentPublisher::new(
                bound.volumes().clone(),
                &journal,
                &issuer.verifier(&mut None)?,
                &scope,
                65_536,
                None,
            )?;
            // A different call publishes into the same host journal. Neither
            // its output nor its completion may deliver to this quiet actor.
            let before_unrelated = stream.tail(store.path(&actor)?).await?;
            let other_call = OperationId::from_bytes([240; 16]);
            for unrelated in [
                acyclic_harness_core::runtime::tool_output_activity_path(execution, other_call),
                acyclic_harness_core::runtime::tool_output_completion_path(execution, other_call),
            ] {
                publisher
                    .stage_once(
                        OperationId::new(),
                        &unrelated,
                        b"{}",
                        "application/json",
                        "unrelated.json",
                        ContentAccess::default(),
                    )?
                    .await?;
            }
            bound.agents().runtime().complete().await?;
            assert_eq!(
                stream.tail(store.path(&actor)?).await?,
                before_unrelated,
                "unrelated journal output woke the quiet actor",
            );
            publisher
                .stage_once(
                    OperationId::from_bytes([236; 16]),
                    &path,
                    cursor.as_bytes(),
                    "application/json",
                    "activity.json",
                    ContentAccess::default(),
                )?
                .await?;
            wait(&mut states, Some(115), false).await?;
            time.advance_to(110)?;
            bound.agents().runtime().complete().await?;
            let queries =
                acyclic_harness_core::supervision::streams::Queries::new(&*stream, &agent, run);
            assert!(
                queries.read_signal(Kind::Stuck).await?.is_none(),
                "old alarm ignored fresh output"
            );
            if completion == 1 {
                // Completion may be committed while its delivery is still queued.
                stream.append(AppendRequest { path: jobs.clone(), records: vec![AppendRecord {
                    value: serde_json::to_vec(&serde_json::json!({"version":1,"call":call,"event":{"kind":"finished"}}))?.into(),
                    headers: RecordHeaders::new(vec![call.into_bytes().to_vec().into()], vec![])?,
                }], if_tail: None, idempotency_key: IdempotencyKey::new("quiet-finish")? }).await?;
            } else if completion == 2 {
                let completed =
                    serde_json::to_vec(&acyclic_harness_core::runtime::ToolOutputCompletion {
                        version: 1,
                        turn: execution,
                        call,
                    })?;
                publisher
                    .stage_once(
                        OperationId::from_bytes([239; 16]),
                        &completed_path,
                        &completed,
                        "application/json",
                        "completed.json",
                        ContentAccess::default(),
                    )?
                    .await?;
                bound.agents().runtime().complete().await?;
                let physical = store.load(&actor).await?;
                let physical = physical
                    .state()
                    .ok_or("quiet actor missing after completion")?;
                assert!(
                    physical.paused() && physical.alarm().deadline_unix_ms().is_none(),
                    "physical completion did not retire quiet supervision",
                );
            }
            // B starts after A's tool admission; A's later pause still owns resume.
            let independent = OperationId::from_bytes([237; 16]);
            stream
                .append(AppendRequest {
                    path: acyclic_harness_core::supervision::streams::path(&agent)?,
                    records: vec![AppendRecord::latest(
                        serde_json::to_vec(&acyclic_harness_core::supervision::Event {
                            version: 1,
                            agent: agent.clone(),
                            run: Some(independent),
                            source: acyclic_harness_core::supervision::Position {
                                stream: agent.inbox()?,
                                sequence: 1,
                            },
                            event: Lifecycle::Started { input: 1 },
                        })?,
                        acyclic_harness_core::supervision::RUN_KEY,
                        false,
                    )?],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new("independent-run-before-pause")?,
                })
                .await?;
            assert_eq!(
                acyclic_harness_core::supervision::streams::active_run(&*stream, &agent)
                    .await?
                    .0,
                Some(independent)
            );
            time.advance_to(115)?;
            if finish {
                wait(&mut states, None, true).await?;
                assert!(
                    queries.read_signal(Kind::Stuck).await?.is_none(),
                    "completed job signalled stuck"
                );
            } else {
                let mut request = acyclic_stream::FollowRequest::new(
                    acyclic_harness_core::supervision::streams::path(&agent)?,
                    0,
                );
                request.when_absent = acyclic_stream::WhenAbsent::Wait;
                let mut feed = acyclic_stream::records_only(stream.follow(request).await?);
                loop {
                    let event: acyclic_harness_core::supervision::Event = serde_json::from_slice(
                        &feed.try_next().await?.ok_or("quiet feed ended")?.value,
                    )?;
                    if matches!(
                        event.event,
                        Lifecycle::Paused {
                            signal: Some(Kind::Stuck)
                        }
                    ) {
                        break;
                    }
                }
                assert!(queries.read_signal(Kind::Stuck).await?.is_some());
                assert!(
                    store
                        .load(&bound.agents().actor(&agent)?)
                        .await?
                        .state()
                        .ok_or("target missing")?
                        .paused()
                );
                // The explicit owner pause revision and its deletion form a
                // resume after a policy pause. Query admission and the target
                // actor reopen in the same filesystem-control publication.
                bound
                    .publish_files(
                        &agent,
                        FileRoot::Private {},
                        OperationId::from_bytes([235; 16]),
                        &[ClientFileChange {
                            path: "control/pause".into(),
                            expected: None,
                            text: Some(String::new()),
                        }],
                    )
                    .await?;
                loop {
                    let event: acyclic_harness_core::supervision::Event = serde_json::from_slice(
                        &feed
                            .try_next()
                            .await?
                            .ok_or("owner pause source ended")?
                            .value,
                    )?;
                    if matches!(event.event, Lifecycle::Paused { signal: None }) {
                        break;
                    }
                }
                let files = bound
                    .files(
                        &agent,
                        FileRoot::Private {},
                        &["control/pause".into()],
                        None,
                    )
                    .await?;
                bound
                    .publish_files(
                        &agent,
                        FileRoot::Private {},
                        OperationId::from_bytes([236; 16]),
                        &[ClientFileChange {
                            path: "control/pause".into(),
                            expected: Some(
                                files.files["control/pause"]
                                    .as_ref()
                                    .ok_or("owner pause file missing")?
                                    .original()
                                    .unwrap()
                                    .clone(),
                            ),
                            text: None,
                        }],
                    )
                    .await?;
                let (resumed, publication) = loop {
                    let record = feed.try_next().await?.ok_or("owner resume source ended")?;
                    let event: acyclic_harness_core::supervision::Event =
                        serde_json::from_slice(&record.value)?;
                    if matches!(event.event, Lifecycle::Resumed {}) {
                        break (event, record.commit_id);
                    }
                };
                assert!(
                    queries
                        .admission(&acyclic_harness_core::supervision::Policy::default())
                        .await?
                        .control
                        .is_none()
                );
                assert_eq!(
                    queries
                        .checkpoint()
                        .await?
                        .ok_or("resumed query missing")?
                        .source,
                    resumed.source
                );
                assert!(
                    acyclic_harness_core::supervision::streams::Queries::new(
                        &*stream,
                        &agent,
                        independent,
                    )
                    .checkpoint()
                    .await?
                    .is_none(),
                    "resume changed the independent run"
                );
                let checkpoint = stream
                    .latest(
                        StreamPath::new(format!("{agent}/supervision/runs/{run}/checkpoint"))?,
                        bytes::Bytes::from_static(b"state").into(),
                        Default::default(),
                    )
                    .await?
                    .ok_or("query checkpoint record missing")?;
                assert_eq!(
                    checkpoint.commit_id, publication,
                    "query resume was published separately from owner control"
                );
                assert!(
                    !store
                        .load(&bound.agents().actor(&agent)?)
                        .await?
                        .state()
                        .ok_or("target missing after resume")?
                        .paused()
                );
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        tokio::select! {
            result = bound.agents().run(|actor, error| { if completion != 3 { panic!("quiet actor {actor:?}: {error}"); } }) => match result {
                Err(error) => return Err(error.into()), Ok(never) => match never {},
            },
            result = exercise => result?,
        }
    }
    Ok(())
}

async fn file_control_journey(guest: bool, maximum: Option<u64>) -> Result {
    use acyclic_harness_core::{
        client_files::{ClientFileChange, FileRoot},
        filesystem::{WorkspaceMutation, workspace_ref},
        supervision::{Event, Lifecycle},
    };
    use futures::TryStreamExt as _;
    let stream = Arc::new(MemoryStream::default());
    let mut options = turns()?;
    options.limits.maximum_bytes = maximum;
    let issuer = AuthorityRoot::new(
        "agents",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "local".into(),
        },
    );
    let bound = Bound::local_root(issuer.clone(), placement(&stream)?, options)?;
    let guest_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bound = if guest {
        use acyclic_actors::guest::{GuestFuture, wire};
        let native = bound.handler();
        let interrupt_handler = native.clone();
        let manifest = wire::decode_manifest(&wire::encode_manifest(native.manifest()))?;
        let calls = guest_calls.clone();
        bound.hosting(
            acyclic_actors::guest::Requests::default(),
            Arc::new(wire::Remote::new(
                manifest,
                move |bytes, cancellation| -> GuestFuture<'static, Vec<u8>> {
                    let native = native.clone();
                    let calls = calls.clone();
                    Box::pin(async move {
                        let (delivery, checkpoint) = wire::decode_delivery(&bytes)?;
                        assert!(
                            !delivery.actor.id().starts_with("file-controls-"),
                            "host-owned control crossed the guest boundary"
                        );
                        calls.fetch_add(1, Ordering::SeqCst);
                        wire::encode_transition(
                            &native
                                .on_record(delivery, checkpoint.as_ref(), cancellation)
                                .await?,
                        )
                    })
                },
                move |query| -> GuestFuture<'static, bool> {
                    let handler = interrupt_handler.clone();
                    Box::pin(async move { wire::accepts_interrupt(handler.as_ref(), &query).await })
                },
            )),
        )?
    } else {
        bound
    };
    let RootSubmission::Created(agent) = bound
        .create_root(
            &intent("file-controls"),
            root(id(123), vec![volume(PRIVATE_VOLUME, id(123))?]),
        )
        .await?
    else {
        return Err("root uncertain".into());
    };
    let store = acyclic_harness_core::agent::actor_store(&stream)?;
    let actor = bound.agents().actor(&agent)?;
    // All three revisions commit while the dispatcher is dormant. A later
    // steering key must not overtake an earlier stop request and its deletion.
    let RootSubmission::Created(stopped) = bound
        .create_root(
            &intent("stop-before-steer"),
            root(id(124), vec![volume(PRIVATE_VOLUME, id(124))?]),
        )
        .await?
    else {
        return Err("second root uncertain".into());
    };
    bound
        .publish_files(
            &stopped,
            FileRoot::Private {},
            OperationId::from_bytes([201; 16]),
            &[ClientFileChange {
                path: "control/stop".into(),
                expected: None,
                text: Some(String::new()),
            }],
        )
        .await?;
    let stop = bound
        .files(
            &stopped,
            FileRoot::Private {},
            &["control/stop".into()],
            None,
        )
        .await?
        .files["control/stop"]
        .as_ref()
        .ok_or("stop missing")?
        .original()
        .unwrap()
        .clone();
    bound
        .publish_files(
            &stopped,
            FileRoot::Private {},
            OperationId::from_bytes([202; 16]),
            &[ClientFileChange {
                path: "control/stop".into(),
                expected: Some(stop),
                text: None,
            }],
        )
        .await?;
    bound
        .publish_files(
            &stopped,
            FileRoot::Private {},
            OperationId::from_bytes([203; 16]),
            &[ClientFileChange {
                path: "control/steer.md".into(),
                expected: None,
                text: Some("later input".into()),
            }],
        )
        .await?;
    let mut stop_request = acyclic_stream::FollowRequest::new(
        acyclic_harness_core::supervision::streams::path(&stopped)?,
        0,
    );
    stop_request.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut stop_events = acyclic_stream::records_only(stream.follow(stop_request).await?);
    let mut request = acyclic_stream::FollowRequest::new(
        acyclic_harness_core::supervision::streams::path(&agent)?,
        0,
    );
    request.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut events = acyclic_stream::records_only(stream.follow(request).await?);
    let mut inbox = acyclic_stream::records_only(
        stream
            .follow(acyclic_stream::FollowRequest::new(agent.inbox()?, 1))
            .await?,
    );
    let write = async |path: &str, text: Option<&str>, number: u8| -> Result {
        let current = bound
            .files(&agent, FileRoot::Private {}, &[path.to_owned()], None)
            .await?;
        let expected = current
            .files
            .get(path)
            .and_then(Option::as_ref)
            .and_then(|file| file.original().cloned());
        bound
            .publish_files(
                &agent,
                FileRoot::Private {},
                OperationId::from_bytes([number; 16]),
                &[ClientFileChange {
                    path: path.to_owned(),
                    expected,
                    text: text.map(str::to_owned),
                }],
            )
            .await?;
        Ok(())
    };
    let event = async |events: &mut acyclic_stream::RecordStream| -> Result<Event> {
        let record = events.try_next().await?.ok_or("event producer closed")?;
        let event: Event = serde_json::from_slice(&record.value)?;
        assert_eq!(event.version, 1);
        assert_eq!(event.agent, agent);
        assert_eq!(
            event.run, None,
            "owner controls must not invent a run identity"
        );
        Ok(event)
    };
    let exercise = async {
        let stopped_event: Event = serde_json::from_slice(
            &stop_events
                .try_next()
                .await?
                .ok_or("stop producer closed")?
                .value,
        )?;
        assert!(matches!(
            stopped_event.event,
            Lifecycle::Stopped { signal: None }
        ));
        assert_eq!(stream.tail(stopped.closed()?).await?, 1);
        bound.agents().runtime().complete().await?;
        assert!(
            !store
                .load(&actor)
                .await?
                .state()
                .ok_or("missing target")?
                .paused()
        );
        write("control/pause", Some(""), 1).await?;
        assert!(matches!(
            event(&mut events).await?.event,
            Lifecycle::Paused { signal: None }
        ));
        assert!(
            store
                .load(&actor)
                .await?
                .state()
                .ok_or("missing target")?
                .paused()
        );
        write("control/pause", None, 2).await?;
        assert!(matches!(
            event(&mut events).await?.event,
            Lifecycle::Resumed {}
        ));
        assert!(
            !store
                .load(&actor)
                .await?
                .state()
                .ok_or("missing target")?
                .paused()
        );
        let control = ActorReference::new(
            actor.account().as_bytes(),
            actor.namespace().as_str().as_bytes(),
            format!("file-controls-{}", agent.id()).as_bytes(),
        )?;
        let control_state = store.load(&control).await?;
        let source = control_state
            .state()
            .and_then(|state| state.subscriptions().get("file-pause"))
            .ok_or("control source missing")?
            .path
            .clone();
        let from = stream.tail(source.clone()).await?;
        let mut expected_inbox = stream.tail(agent.inbox()?).await?;
        // The original producer chunks these paths: pause is in an earlier
        // record than steering, but both share one authority revision.
        let mut batch = vec![ClientFileChange {
            path: "control/pause".into(),
            expected: None,
            text: Some(String::new()),
        }];
        batch.extend((0..80).map(|number| ClientFileChange {
            path: format!("control/q{number:03}"),
            expected: None,
            text: Some(String::new()),
        }));
        batch.push(ClientFileChange {
            path: "control/steer.md".into(),
            expected: None,
            text: Some("alpha".into()),
        });
        bound
            .publish_files(
                &agent,
                FileRoot::Private {},
                OperationId::from_bytes([3; 16]),
                &batch,
            )
            .await?;
        let source_tail = stream.tail(source.clone()).await?;
        let records: Vec<_> = stream
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: source,
                from,
                limit: u32::try_from(source_tail - from)?,
                at_least: Default::default(),
            })
            .await?
            .try_collect()
            .await?;
        let keyed = |key: &[u8]| {
            records.iter().find(|record| {
                record
                    .headers
                    .keys()
                    .iter()
                    .any(|value| value.as_ref() == key)
            })
        };
        let pause = keyed(b"control/pause").ok_or("pause record missing")?;
        let steer = keyed(b"control/steer.md").ok_or("steer record missing")?;
        assert!(
            pause.sequence < steer.sequence,
            "batch did not split its controls"
        );
        assert_eq!(pause.commit_id, steer.commit_id);
        assert!(matches!(
            event(&mut events).await?.event,
            Lifecycle::Paused { .. }
        ));
        bound.agents().runtime().complete().await?;
        assert_eq!(
            stream.tail(agent.inbox()?).await?,
            expected_inbox + 1,
            "split publication lost steering after applying pause"
        );
        for (text, delta, number) in [("alpha", "alpha", 4), ("alpha beta", " beta", 5)] {
            if number != 4 {
                write("control/steer.md", Some(text), number).await?;
            }
            loop {
                let record = inbox.try_next().await?.ok_or("inbox producer closed")?;
                if let InboxRecord::Message(message) = InboxRecord::decode(&record.value)? {
                    let bytes = read_host_input(&bound, &issuer, &agent, &message.body).await?;
                    assert_eq!(
                        serde_json::from_slice::<ModelContent>(&bytes)?,
                        ModelContent::Text(delta.into())
                    );
                    expected_inbox = record.sequence + 1;
                    break;
                }
            }
        }
        let sources = bound
            .files(
                &agent,
                FileRoot::Private {},
                &["control/pause".into(), "control/steer.md".into()],
                None,
            )
            .await?;
        // One original publication resumes and rejects a rewrite. Both
        // lifecycle records must use one captured supervision append cut.
        bound
            .publish_files(
                &agent,
                FileRoot::Private {},
                OperationId::from_bytes([6; 16]),
                &[
                    ClientFileChange {
                        path: "control/pause".into(),
                        expected: Some(
                            sources.files["control/pause"]
                                .as_ref()
                                .ok_or("pause missing")?
                                .original()
                                .unwrap()
                                .clone(),
                        ),
                        text: None,
                    },
                    ClientFileChange {
                        path: "control/steer.md".into(),
                        expected: Some(
                            sources.files["control/steer.md"]
                                .as_ref()
                                .ok_or("steer missing")?
                                .original()
                                .unwrap()
                                .clone(),
                        ),
                        text: Some("rewritten".into()),
                    },
                ],
            )
            .await?;
        let resumed = event(&mut events).await?;
        assert!(matches!(resumed.event, Lifecycle::Resumed {}));
        let rejected = event(&mut events).await?;
        assert!(
            matches!(rejected.event, Lifecycle::ControlRejected { path } if path == "control/steer.md")
        );
        assert_eq!(resumed.source, rejected.source);
        assert_eq!(
            stream.tail(agent.inbox()?).await?,
            expected_inbox,
            "rejected rewrite appended model input"
        );
        let private = volume(PRIVATE_VOLUME, id(123))?;
        let workspace = workspace_ref(
            private.volume()?.provider().clone(),
            &private.volume()?.storage_name(&mut None)?,
        )?;
        let mut steering = "alpha beta".to_owned();
        // Ordinary filesystem publications can grow the file beyond the
        // per-input capacity. Each new suffix is still one bounded message.
        // The rejected rewrite above must not have replaced its admitted base.
        for number in 7..13 {
            let delta = format!(
                "{number}{}",
                "x".repeat(if maximum.is_none() {
                    80 * 1024
                } else {
                    16 * 1024
                })
            );
            steering.push_str(&delta);
            bound
                .volumes()
                .apply(
                    &workspace,
                    None,
                    &[WorkspaceMutation::PutFile {
                        path: "/control/steer.md".into(),
                        bytes: bytes::Bytes::copy_from_slice(steering.as_bytes()).into(),
                    }],
                    &acyclic_harness_core::IdempotencyKey(format!("large-steer-{number}")),
                )
                .await?;
            loop {
                let record = inbox.try_next().await?.ok_or("inbox producer closed")?;
                if let InboxRecord::Message(message) = InboxRecord::decode(&record.value)? {
                    let bytes = read_host_input(&bound, &issuer, &agent, &message.body).await?;
                    assert_eq!(
                        serde_json::from_slice::<ModelContent>(&bytes)?,
                        ModelContent::Text(delta)
                    );
                    break;
                }
            }
        }
        write("control/stop", Some(""), 13).await?;
        assert!(matches!(
            event(&mut events).await?.event,
            Lifecycle::Stopped { signal: None }
        ));
        assert_eq!(stream.tail(agent.closed()?).await?, 1);
        bound.agents().runtime().complete().await?;
        assert!(bound.agents().state(&agent).await?.closed);
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let result: Result = tokio::select! {
        result = bound.agents().run(|actor, error| panic!("file control delivery {actor:?}: {error}")) => match result { Err(error) => Err(error.into()), Ok(never) => match never {} },
        result = exercise => result,
    };
    result?;
    assert_eq!(guest_calls.load(Ordering::SeqCst) > 0, guest);
    Ok(())
}

#[tokio::test]
async fn root_admission_requires_the_implicit_journal_before_io() -> Result {
    use super::wired::Refusing;
    use acyclic_harness_core::{
        Capabilities, conversation::VolumeOperation, filesystem::hosting::VerifiedCaller,
    };

    let stream = Arc::new(Refusing::default());
    let private = volume("interface", id(1))?;
    let journal = acyclic_harness_core::conversation::journal_volume(provider()?, &id(1))?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "admission".into(),
    };
    let issuer = AuthorityRoot::new("agents", [7; 32], audience.clone());
    for grants_journal in [false, true] {
        let (placement, bucket) = placed(&stream)?;
        let private_write = private
            .volume()?
            .capability(VolumeOperation::Write, &mut None)?;
        let mut grants = vec![private_write];
        if grants_journal {
            grants.push(journal.capability(VolumeOperation::Write, &mut None)?);
        }
        let scope =
            issuer.bootstrap_scope("caller", &Capabilities::new(grants), None, &mut None)?;
        let bound = Bound::admitted(
            issuer.clone(),
            VerifiedCaller {
                audience: audience.clone(),
                scope,
            },
            placement,
            turns()?,
        )?;
        let request = root(id(1), vec![private.clone()]);
        assert_eq!(bound.admit_root(&request).is_ok(), grants_journal);
        if grants_journal {
            let mut aliased = root(id(1), vec![private.clone()]);
            aliased.retained.push(WorkspaceEntry::initial_volume(
                WorkspaceRole::User,
                "ordinary".into(),
                journal.clone(),
            ));
            assert!(bound.admit_root(&aliased).is_err());
        }
        assert_eq!(bucket.objects().await?, 0);
    }
    assert_eq!(stream.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn create_root_preparation_and_intent_replay_are_exact() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement(&stream)?,
        turns()?,
    )?;
    // A display name never reserves storage or selects harness internals.
    let private = volume("journal", id(1))?;
    let journal = acyclic_harness_core::conversation::journal_volume(provider()?, &id(1))?;
    let journal_ref = acyclic_harness_core::filesystem::workspace_ref(
        provider()?,
        &journal.storage_name(&mut None)?,
    )?;
    let first = bound
        .prepare_root(&intent("root"), root(id(1), vec![private.clone()]))
        .await?;
    let again = bound
        .prepare_root(&intent("root"), root(id(1), vec![private.clone()]))
        .await?;
    assert_eq!(first.request(), again.request(), "preparation is exact");
    let other = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement(&Arc::new(MemoryStream::default()))?,
        turns()?,
    )?;
    let misplaced = bound
        .prepare_root(&intent("root"), root(id(1), vec![private.clone()]))
        .await?;
    assert!(matches!(
        misplaced.submit(bound.volumes(), other.agents()).await,
        Err(Error::Record("root activation uses another provider"))
    ));
    assert!(bound.volumes().resolve(&journal_ref, None).await.is_err());
    let child = first.child().clone();
    let RootSubmission::Created(created) = bound.submit_root(first).await? else {
        return Err("the root's submission was uncertain".into());
    };
    assert_eq!(created, child);
    bound.volumes().resolve(&journal_ref, None).await?;
    assert_eq!(
        bound.agents().state(&created).await?.volumes,
        vec![private.clone()]
    );
    // The identical prepared commit and the original intent both replay the
    // same creation.
    assert!(matches!(
        bound.submit_root(again).await?,
        RootSubmission::Created(replayed) if replayed == child
    ));
    assert!(matches!(
        bound
            .create_root(&intent("root"), root(id(1), vec![private.clone()]))
            .await?,
        RootSubmission::Created(replayed) if replayed == child
    ));
    let (alice, bob) = (
        Sender::User {
            principal: "alice".into(),
        },
        Sender::User {
            principal: "bob".into(),
        },
    );
    // Input publication shares the journal created atomically with the root,
    // without adding it to the model's bound volumes.
    let staged = bound
        .stage_text(
            &created,
            "message",
            &alice,
            &intent("m1"),
            ("/command", "hello"),
        )
        .await?;
    assert_eq!(staged.volume(), &journal);
    let (scope, verifier) = bound.volume_scope(&created).await?;
    assert!(
        acyclic_harness_core::conversation::ContentGrant::verify(
            &verifier,
            &scope,
            &journal,
            acyclic_harness_core::conversation::VolumeOperation::Write,
            &mut None,
        )
        .is_err()
    );
    assert!(bound.read_file(&created, &staged, 0, 64).await.is_err());
    let prompt = bound
        .stage_prompt(
            &created,
            &alice,
            &intent("prompt"),
            &ModelContent::Text("hello".into()),
        )
        .await?;
    assert_eq!(prompt.volume(), &journal);
    bound
        .publish_files(
            &created,
            acyclic_harness_core::client_files::FileRoot::Private {},
            OperationId::from_bytes([174; 16]),
            &[acyclic_harness_core::client_files::ClientFileChange {
                path: "attachment.txt".into(),
                expected: None,
                text: Some("visible input".into()),
            }],
        )
        .await?;
    let files = bound
        .files(
            &created,
            acyclic_harness_core::client_files::FileRoot::Private {},
            &[
                "attachment.txt".into(),
                "status/session-directory.json".into(),
            ],
            None,
        )
        .await?;
    assert!(
        files.files["status/session-directory.json"].is_none(),
        "ordinary binding advertised an uninitialized directory"
    );
    assert_eq!(files.cut.queries.len(), 1);
    assert_eq!(files.cut.queries.values().next(), Some(&0));
    let watched_paths = [
        "attachment.txt".to_owned(),
        "status/session-directory.json".to_owned(),
    ];
    let watching = bound.files(
        &created,
        acyclic_harness_core::client_files::FileRoot::Private {},
        &watched_paths,
        Some(&files.cut),
    );
    tokio::pin!(watching);
    assert!(futures::poll!(&mut watching).is_pending());
    bound.initialize_sessions().await?;
    let initialized = watching.await?;
    let directory = initialized.files["status/session-directory.json"]
        .as_ref()
        .ok_or("initialization did not wake the pending directory file watch")?;
    assert!(matches!(
        directory.source,
        acyclic_harness_core::client_files::FileSource::Query { .. }
    ));
    assert_eq!(
        initialized.cut.generation, files.cut.generation,
        "directory initialization changed the authored private filesystem"
    );
    let attached = files.files["attachment.txt"]
        .as_ref()
        .and_then(|file| file.original())
        .ok_or("bound attachment missing")?;
    let envelope = bound
        .stage_prompt(
            &created,
            &alice,
            &intent("attached"),
            &ModelContent::Part(acyclic_harness_core::model::ModelContentPart::File {
                file: attached.clone(),
                policy: acyclic_harness_core::model::FileProjectionPolicy::BoundedFull,
            }),
        )
        .await?;
    assert_eq!(envelope.volume(), &journal);
    // Staging an envelope does not authorize a reference to hidden journal
    // content: embedded files still need the ordinary content grant.
    assert!(
        bound
            .stage_prompt(
                &created,
                &alice,
                &intent("hidden"),
                &ModelContent::Part(acyclic_harness_core::model::ModelContentPart::File {
                    file: staged.clone(),
                    policy: acyclic_harness_core::model::FileProjectionPolicy::BoundedFull,
                }),
            )
            .await
            .is_err()
    );
    assert_eq!(
        bound
            .staged_text(&created, "message", &alice, &intent("m1"), "/command")
            .await?,
        Some(staged)
    );
    // Equal expanded bytes do not make a changed original request a replay.
    assert!(matches!(
        bound
            .staged_text(&created, "message", &alice, &intent("m1"), "/changed")
            .await,
        Err(acyclic_harness_core::agent::Error::Conflict(_))
    ));
    assert!(
        bound
            .stage_text(
                &created,
                "message",
                &alice,
                &intent("m1"),
                ("/changed", "hello")
            )
            .await
            .is_err()
    );
    // Another sender's same intent with different text stages its own input.
    bound
        .stage_text(&created, "message", &bob, &intent("m1"), ("hi", "hi"))
        .await?;
    Ok(())
}

/// The guest has no host companion factory. The host derives the child's
/// control actor from the validated spawn, in its original atomic publication.
#[tokio::test]
async fn guest_spawn_creates_host_controls_with_original_child_and_volume_publication() -> Result {
    use acyclic_actors::guest::{GuestFuture, Handler as _, wire};
    use acyclic_harness_core::{
        client_files::{ClientFileChange, FileRoot},
        filesystem::FilesystemSpawnVolumes,
        supervision::{Event, Lifecycle},
    };
    use futures::TryStreamExt as _;
    struct SpawnOne;
    impl TurnDriver for SpawnOne {
        fn step<'a>(
            &'a self,
            agent: &'a AgentState,
            _: u64,
            _: Option<Queued>,
            _: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move {
                Ok(TurnStep {
                    checkpoint: None,
                    outs: if agent.parent.is_none() {
                        vec![Out::Spawn {
                            profile: None,
                            history: None,
                            intent: "worker".into(),
                            brief: None,
                            volumes: Vec::new(),
                        }]
                    } else {
                        Vec::new()
                    },
                    wait: Wait::Input,
                    drops: None,
                })
            })
        }
    }
    let stream = Arc::new(MemoryStream::default());
    let placement = placement(&stream)?;
    let within = placement.within.clone();
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement,
        turns()?,
    )?;
    let guest = Arc::new(
        AgentHandler::new(actor_store(&stream)?, SpawnOne, turns()?.actors)?.with_spawn_volumes(
            Arc::new(FilesystemSpawnVolumes::new(bound.volumes().clone())),
        ),
    );
    let manifest = wire::decode_manifest(&wire::encode_manifest(guest.manifest()))?;
    let interrupt_handler = guest.clone();
    let bound = bound.hosting(
        acyclic_actors::guest::Requests::default(),
        Arc::new(wire::Remote::new(
            manifest,
            move |bytes, cancellation| -> GuestFuture<'static, Vec<u8>> {
                let guest = guest.clone();
                Box::pin(async move {
                    let (delivery, checkpoint) = wire::decode_delivery(&bytes)?;
                    assert!(!delivery.actor.id().starts_with("file-controls-"));
                    wire::encode_transition(
                        &guest
                            .on_record(delivery, checkpoint.as_ref(), cancellation)
                            .await?,
                    )
                })
            },
            move |query| -> GuestFuture<'static, bool> {
                let handler = interrupt_handler.clone();
                Box::pin(async move { wire::accepts_interrupt(handler.as_ref(), &query).await })
            },
        )),
    )?;
    let RootSubmission::Created(root) = bound
        .create_root(
            &intent("guest-parent"),
            root(id(125), vec![volume(PRIVATE_VOLUME, id(125))?]),
        )
        .await?
    else {
        return Err("root uncertain".into());
    };
    bound.agents().drain(&root).await?;
    let child = root.child(&AgentId::derive(&root.id(), "worker"))?;
    bound
        .agents()
        .send(
            &root,
            &super::user(),
            &InboxRecord::ApprovalRequested(ApprovalRequested {
                child: child.clone(),
                id: intent("wake-parent"),
            }),
        )
        .await?;
    bound.agents().drain(&root).await?;
    let store = actor_store(&stream)?;
    let control = ActorReference::new(
        within.account().as_bytes(),
        within.namespace().as_str().as_bytes(),
        format!("file-controls-{}", child.id()).as_bytes(),
    )?;
    let child_actor = bound.agents().actor(&child)?;
    let creation = async |actor: &ActorReference| -> Result<acyclic_stream::Record> {
        let mut records = stream
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: store.path(actor)?,
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        Ok(records.try_next().await?.ok_or("actor was not created")?)
    };
    let child_creation = creation(&child_actor).await?;
    let control_creation = creation(&control).await?;
    assert_eq!(child_creation.commit_id, control_creation.commit_id);
    let publication = stream.read_commit(child_creation.commit_id).await?;
    assert!(
        publication.mutations.iter().any(|mutation| {
            let records = match mutation {
                acyclic_stream::CommittedMutation::Append(append) => &append.records,
                acyclic_stream::CommittedMutation::Fork(fork) => &fork.records,
            };
            records.iter().any(|record| {
                matches!(InboxRecord::decode(&record.value),
            Ok(InboxRecord::ForkAdopted(adopted)) if adopted.agent == child.id())
            })
        }),
        "child adoption was outside its control creation publication"
    );
    // Resolve the real new private volume before its child has taken a step.
    let files = bound
        .files(
            &child,
            FileRoot::Private {},
            &["control/pause".into()],
            None,
        )
        .await?;
    assert!(files.files["control/pause"].is_none());
    let mut follow = acyclic_stream::FollowRequest::new(
        acyclic_harness_core::supervision::streams::path(&child)?,
        0,
    );
    follow.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut events = acyclic_stream::records_only(stream.follow(follow).await?);
    bound
        .publish_files(
            &child,
            FileRoot::Private {},
            OperationId::from_bytes([204; 16]),
            &[ClientFileChange {
                path: "control/pause".into(),
                expected: None,
                text: Some(String::new()),
            }],
        )
        .await?;
    let exercise = async {
        let event: Event =
            serde_json::from_slice(&events.try_next().await?.ok_or("child event missing")?.value)?;
        assert_eq!(event.agent, child);
        assert!(matches!(event.event, Lifecycle::Paused { signal: None }));
        assert!(
            store
                .load(&child_actor)
                .await?
                .state()
                .ok_or("child state missing")?
                .paused()
        );
        assert!(
            !store
                .load(&bound.agents().actor(&root)?)
                .await?
                .state()
                .ok_or("parent state missing")?
                .paused()
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        result = bound.agents().run(|actor, error| panic!("guest child control {actor:?}: {error}")) => match result { Err(error) => Err(error.into()), Ok(never) => match never {} },
        result = exercise => result,
    }
}

#[tokio::test]
async fn staged_waking_notices_keep_entries_bounded_and_replay_exact() -> Result {
    use acyclic_harness_core::{
        conversation::{ContentResidencyVerifier as _, wire::Notice},
        filesystem::{FilesystemContentVerifier, agent_authority},
        model::ModelContentPart,
    };
    let stream = Arc::new(MemoryStream::default());
    let mut capacity = turns()?;
    capacity.limits.maximum_bytes = Some(1 << 20);
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement(&stream)?,
        capacity,
    )?;
    let RootSubmission::Created(agent) = bound
        .create_root(
            &intent("root"),
            root(id(1), vec![volume(PRIVATE_VOLUME, id(1))?]),
        )
        .await?
    else {
        return Err("uncertain root".into());
    };
    let event = |body| Notice {
        source: "filesystem".into(),
        kind: "files.changed".into(),
        body: serde_json::Value::String(body),
        wake: true,
        coalesce_key: None,
    };
    // Escaping makes this payload larger than an entire persisted record.
    let notice = event("\0".repeat(32 * 1024));
    let original = acyclic_harness_core::contract::canonical_json_bytes(&notice)?;
    assert!(original.len() > MAX_RECORD_BYTES);
    let request = intent("notice");
    let staged = bound
        .stage_notice(&agent, &super::user(), &request, &notice)
        .await?;
    assert_eq!(
        staged,
        bound
            .stage_notice(&agent, &super::user(), &request, &notice)
            .await?
    );
    assert!(
        bound
            .stage_notice(&agent, &super::user(), &request, &event("changed".into()))
            .await
            .is_err()
    );
    assert!(
        InboxRecord::Notice {
            id: request.clone(),
            notice: NoticePayload::Quiet(notice.clone())
        }
        .encode()
        .is_err(),
        "an inline waking body must not reenter the queue"
    );
    let InboxRecord::Notice {
        notice: NoticePayload::Waking(file),
        ..
    } = &staged
    else {
        return Err("waking notice was not staged".into());
    };
    assert_eq!(
        file.volume(),
        &acyclic_harness_core::conversation::journal_volume(provider()?, &agent.id())?
    );
    assert!(
        bound
            .read_file(&agent, file, 0, original.len() as u64)
            .await
            .is_err()
    );
    let input = TurnInput::Notice {
        id: request,
        notice: file.clone(),
    };
    let entry = ConversationEntry {
        observed_through: 0,
        inbox: 1,
        input: input.clone(),
        output: file.as_ref().clone(),
        outcome: TurnOutcome::Completed,
        goal: None,
        announcements: Vec::new(),
        tool_results: None,
        steering: None,
        notices: None,
    };
    assert!(staged.encode()?.len() < 4096);
    assert!(entry.encode()?.len() < 8192);
    assert_eq!(ConversationEntry::decode(&entry.encode()?)?, entry);
    let state = bound.agents().state(&agent).await?;
    let authority = agent_authority(
        bound.issuer(),
        bound.admission(),
        &agent,
        &state.volumes,
        std::slice::from_ref(file.as_ref()),
    )
    .await?;
    let reader = FilesystemContentVerifier::new(
        bound.volumes().clone(),
        &authority.verifier,
        &authority.scope,
        1 << 20,
        None,
    )?;
    // Exact content grants do not confer custody of host-owned envelopes.
    assert!(
        reader
            .read(file, original.len() as u64, None, None)?
            .await
            .is_err()
    );
    let scope = bound
        .issuer()
        .issue(
            None,
            "notice-host",
            &acyclic_harness_core::Capabilities::new([
                acyclic_harness_core::contract::capability::HARNESS_HOST.to_owned(),
                file.volume().capability(
                    acyclic_harness_core::conversation::VolumeOperation::Write,
                    &mut None,
                )?,
                file.volume().capability(
                    acyclic_harness_core::conversation::VolumeOperation::Read,
                    &mut None,
                )?,
            ]),
            Some(acyclic_harness_core::AgentId::from_bytes(agent.id_bytes())),
        )
        .await?;
    // An undelivered operation retains its publication when producer placement
    // changes. Its immutable source and bytes must still match on every retry.
    let saved_request = intent("saved-notice");
    let Sender::User { principal } = super::user() else {
        return Err("expected user sender".into());
    };
    let sender = format!("user:{principal}");
    let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.hosting.input.v1");
    for part in [
        agent.as_str(),
        "notice",
        sender.as_str(),
        saved_request.as_str(),
    ] {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    let operation = acyclic_harness_core::OperationId::from_bytes(identity);
    let saved_path = format!("inputs/{operation}-{}.json", blake3::hash(&original));
    let publisher = acyclic_harness_core::filesystem::FilesystemContentPublisher::new(
        bound.volumes().clone(),
        file.volume(),
        &bound.issuer().verifier(&mut None)?,
        &scope,
        1 << 20,
        None,
    )?;
    use acyclic_harness_core::conversation::ContentPublisher as _;
    let (saved, _) = publisher
        .stage_once(
            operation,
            &saved_path,
            &original,
            acyclic_harness_core::agent::NOTICE_MEDIA_TYPE,
            "notice",
            Default::default(),
        )?
        .await?;
    let replayed = bound
        .stage_notice(&agent, &super::user(), &saved_request, &notice)
        .await?;
    let InboxRecord::Notice {
        notice: NoticePayload::Waking(replayed),
        ..
    } = replayed
    else {
        return Err("saved waking notice was not replayed".into());
    };
    assert_eq!(replayed.as_ref(), &saved);
    assert!(
        bound
            .stage_notice(
                &agent,
                &super::user(),
                &saved_request,
                &event("changed".into())
            )
            .await
            .is_err()
    );
    let reader = FilesystemContentVerifier::new(
        bound.volumes().clone(),
        &bound.issuer().verifier(&mut None)?,
        &scope,
        1 << 20,
        None,
    )?;
    assert_eq!(
        reader
            .read(file, original.len() as u64, None, None)?
            .await?
            .as_ref(),
        original
    );
    let expanded = expand_attachments(model_input(&input)?, reader.as_ref()).await?;
    assert_eq!(
        expanded,
        ModelContent::Part(ModelContentPart::Notices {
            notices: vec![notice]
        })
    );
    Ok(())
}

/// A provider that admits a write only with valid evidence: it counts
/// publications, records each presented attestation, and can lose the
/// first commit's acknowledgement after applying it.
#[derive(Default)]
pub(super) struct Evidence {
    missing: std::sync::Mutex<Option<(acyclic_stream::StreamPath, StreamError)>>,
    inner: MemoryStream,
    valid: bool,
    unknown_first: std::sync::atomic::AtomicBool,
    presented: std::sync::Mutex<Vec<Attestation>>,
    published: std::sync::atomic::AtomicUsize,
}

impl Evidence {
    pub(super) fn missing(&self, path: acyclic_stream::StreamPath, error: StreamError) {
        *self.missing.lock().expect("missing diagnostic") = Some((path, error));
    }
}

pub(super) fn missing_refusal() -> StreamError {
    let mut metadata = http::HeaderMap::new();
    metadata.insert(
        "acyclic-request-id",
        "missing-record".parse().expect("header"),
    );
    StreamError::NotFound.with_refusal(
        acyclic_auth::refusal::Refusal::capture(
            5,
            "not published yet",
            b"retained detail",
            &metadata,
            acyclic_auth::refusal::Canonical {
                code: 5,
                message: "not_found",
                details: &[],
                owned: &[],
            },
            None,
        )
        .expect("noncanonical status"),
    )
}

impl AttestedProvider for Evidence {
    fn bind(
        &self,
        attestation: &Attestation,
    ) -> std::result::Result<Option<Arc<Self>>, StreamError> {
        self.presented
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .push(attestation.clone());
        if !self.valid || attestation.0.len() != 32 {
            return Err(StreamError::AccessDenied);
        }
        Ok(None)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl StreamProvider for Evidence {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        let observed = self.inner.heads(request.clone()).await?;
        if let Some((path, error)) = &*self.missing.lock().expect("missing diagnostic") {
            if request
                .paths
                .iter()
                .zip(&observed.heads)
                .any(|(requested, head)| requested == path && head.is_none())
            {
                return Err(error.clone());
            }
        }
        Ok(observed)
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        match request {
            acyclic_stream::WriteRequest::CommitAccepted(_) => {
                Box::pin(async { Err(acyclic_stream::StreamError::Unsupported) })
            }
            acyclic_stream::WriteRequest::Append(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.published.fetch_add(1, Ordering::SeqCst);
                        self.inner.append(request).await
                    },
                    acyclic_stream::WriteOutcome::Append,
                ))
            }
            acyclic_stream::WriteRequest::Fork(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    self.published.fetch_add(1, Ordering::SeqCst);
                    self.inner.fork(request).await
                },
                acyclic_stream::WriteOutcome::Fork,
            )),
            acyclic_stream::WriteRequest::Commit(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.published.fetch_add(1, Ordering::SeqCst);
                        let outcome = self.inner.commit(request).await?;
                        if self.unknown_first.swap(false, Ordering::SeqCst) {
                            return Err(StreamError::Unavailable);
                        }
                        Ok(outcome)
                    },
                    acyclic_stream::WriteOutcome::Commit,
                ))
            }
        }
    }

    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        let path = request.path.clone();
        match self.inner.read(request).await {
            Err(StreamError::NotFound) => {
                if let Some((selected, error)) = &*self.missing.lock().expect("missing diagnostic")
                {
                    if &path == selected {
                        return Err(error.clone());
                    }
                }
                Err(StreamError::NotFound)
            }
            result => result,
        }
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }

    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for Evidence {}

/// Attests each digest with itself, recording every request for evidence.
#[derive(Default)]
struct Digests(std::sync::Mutex<Vec<[u8; 32]>>);

impl Attestor for Digests {
    fn attest<'a>(
        &'a self,
        attempt: Attempt<'a>,
    ) -> BoxProviderFuture<'a, std::result::Result<Attestation, StreamError>> {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .push(attempt.digest);
            Ok(Attestation(attempt.digest.to_vec().into()))
        })
    }
}

/// A self-host whose provider requires evidence composes its placement stream
/// as [`Attested`], under the same admission its binding acts with.
fn attested(
    stream: &Arc<Evidence>,
    attestor: &Arc<Digests>,
) -> Result<Bound<Attested<Evidence>, Objects>> {
    let stream = Arc::new(Attested::new(
        stream.clone(),
        Some(attestor.clone() as Arc<dyn Attestor>),
        TurnAdmission::LocalRoot,
    ));
    Ok(Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement(&stream)?,
        turns()?,
    )?)
}

#[tokio::test]
async fn every_sealed_write_is_attested_and_an_unknown_retry_presents_the_same_evidence() -> Result
{
    let stream = Arc::new(Evidence {
        valid: true,
        unknown_first: true.into(),
        ..Evidence::default()
    });
    let attestor = Arc::new(Digests::default());
    let bound = attested(&stream, &attestor)?;
    let private = volume(PRIVATE_VOLUME, id(2))?;
    let prepared = bound
        .prepare_root(&intent("root"), root(id(2), vec![private.clone()]))
        .await?;
    let request = prepared.request().clone();
    // The original provider applies the whole sealed commit before losing
    // its reply. The uncertain owner comes back with the identical request.
    let RootSubmission::Unknown(held) = bound.submit_root(prepared).await? else {
        return Err("the lost acknowledgement was not uncertain".into());
    };
    assert_eq!(held.request(), &request);
    let original = stream
        .inner
        .inspect_idempotency(request.idempotency_key.clone())
        .await?
        .ok_or("the unknown result must already be committed")?;
    let acyclic_stream::IdempotencyOutcome::Commit(envelope) = &original.outcome else {
        return Err("the original result is not the root commit".into());
    };
    assert_eq!(
        stream.inner.read_commit(envelope.commit_id).await?,
        *envelope
    );
    assert_eq!(
        bound.agents().state(held.child()).await?.volumes,
        vec![private],
        "the root and its selected volume exist before retry"
    );
    let paths = request.effects[0]
        .mutations
        .iter()
        .map(|mutation| mutation.path().clone())
        .collect::<acyclic_stream::HeadSet<_>>();
    let heads = acyclic_stream::HeadsRequest {
        limit: u32::try_from(paths.len())?,
        paths,
        retained: Default::default(),
        durable_through: None,
        projection: Default::default(),
        children: None,
        at_least: Default::default(),
    };
    let before = stream.inner.heads(heads.clone()).await?;
    assert!(before.heads.iter().all(Option::is_some));
    assert!(matches!(
        bound.submit_root(held).await?,
        RootSubmission::Created(created) if created == AgentPath::root(&id(2))
    ));
    assert_eq!(
        stream
            .inner
            .inspect_idempotency(request.idempotency_key)
            .await?,
        Some(original),
        "retry retains the original terminal envelope"
    );
    assert_eq!(
        stream.inner.heads(heads).await?.heads,
        before.heads,
        "retry appends no second volume, adoption, actor state or activation"
    );
    let digests = attestor.0.lock().map_err(|_| "poisoned")?.clone();
    let presented = stream.presented.lock().map_err(|_| "poisoned")?.clone();
    // Every write presented exactly the evidence attested for its request.
    assert_eq!(presented.len(), digests.len(), "one attestation per write");
    for attestation in &presented {
        assert!(
            digests
                .iter()
                .any(|digest| digest.as_slice() == attestation.0.as_ref())
        );
    }
    // The root's lost attempt and its exact retry are the same sealed
    // request, so they present the identical attestation.
    assert_eq!(presented.len(), 2);
    assert_eq!(
        presented[0], presented[1],
        "the uncertain retry presents the identical attestation"
    );
    Ok(())
}

#[tokio::test]
async fn a_write_without_valid_evidence_publishes_nothing() -> Result {
    let stream = Arc::new(Evidence::default());
    let attestor = Arc::new(Digests::default());
    let bound = attested(&stream, &attestor)?;
    let private = volume(PRIVATE_VOLUME, id(3))?;
    assert!(!matches!(
        bound
            .create_root(&intent("root"), root(id(3), vec![private]))
            .await,
        Ok(RootSubmission::Created(_))
    ));
    assert_eq!(stream.published.load(Ordering::SeqCst), 0);
    assert_eq!(
        stream.presented.lock().map_err(|_| "poisoned")?.len(),
        1,
        "the root's one request was presented once"
    );
    Ok(())
}

#[tokio::test]
async fn trees_sharing_a_stream_each_claim_only_their_own_agents() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let bind = |namespace: &[u8]| -> Result<Bound<MemoryStream, Objects>> {
        let mut placement = placement(&stream)?;
        placement.within = ActorReference::new(b"account", namespace, b"owner")?;
        Ok(Bound::local_root(
            AuthorityRoot::new(
                "agents",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "local".into(),
                },
            ),
            placement,
            turns()?,
        )?)
    };
    let create = async |bound: &Bound<MemoryStream, Objects>, agent: u8| -> Result<AgentPath> {
        let request = root(id(agent), vec![volume(PRIVATE_VOLUME, id(agent))?]);
        match bound
            .create_root(&intent(&format!("root-{agent}")), request)
            .await?
        {
            RootSubmission::Created(created) => Ok(created),
            RootSubmission::Unknown(_) => Err("the root's submission was uncertain".into()),
        }
    };
    // Two servers, one per tree, on one stream; each tree's later roots come
    // from another binding of the same tree, so its server discovers them.
    let (a, b) = (bind(b"tree-a")?, bind(b"tree-b")?);
    let (a2, b2) = (bind(b"tree-a")?, bind(b"tree-b")?);
    let root_b = create(&b, 2).await?;
    create(&a, 1).await?;
    let foreign = b.agents().actor(&root_b)?;
    assert!(a.agents().runtime().track(foreign).is_err());
    // The accepted actor checkpoint is the completion event. A runtime may
    // cooperatively yield before it publishes, even with memory-backed I/O.
    let observed = async |bound: &Bound<MemoryStream, Objects>, path: &AgentPath| -> Result {
        use futures::TryStreamExt as _;
        let actor = bound.agents().actor(path)?;
        let store = bound.agents().runtime().store();
        let mut changes = acyclic_stream::records_only(
            stream
                .follow(acyclic_stream::FollowRequest::new(store.path(&actor)?, 1))
                .await?,
        );
        loop {
            let record = changes
                .try_next()
                .await?
                .ok_or("actor state producer ended")?;
            let state = acyclic_actors::local::State::decode(&record.value)?;
            if state.checkpoint().is_some() {
                break;
            }
        }
        Ok(())
    };
    let (failed, failure) = tokio::sync::oneshot::channel();
    let failed = std::sync::Mutex::new(Some(failed));
    let report = |actor: &ActorReference, error: &acyclic_actors::local::Error| {
        if let Some(failed) = failed.lock().expect("failure sender").take() {
            let _ = failed.send(format!("{actor:?}: {error}"));
        }
    };
    let exercise = async {
        observed(&a, &AgentPath::root(&id(1))).await?;
        observed(&b, &root_b).await?;
        assert_eq!(a.agents().runtime().counts().0, 1);
        assert_eq!(b.agents().runtime().counts().0, 1);
        // These roots are committed by different hosts after discovery starts.
        // Interleaving their activations also exercises foreign-tree exclusion.
        let later_b = create(&b2, 3).await?;
        let later_a = create(&a2, 4).await?;
        let last_b = create(&b2, 5).await?;
        observed(&b, &later_b).await?;
        observed(&a, &later_a).await?;
        observed(&b, &last_b).await?;
        assert_eq!(
            (
                a.agents().runtime().counts().0,
                b.agents().runtime().counts().0
            ),
            (2, 3),
            "activation producers discovered exactly their own roots"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        result = a.agents().run(report) => return match result {
            Err(error) => Err(error.into()),
            Ok(never) => match never {},
        },
        result = b.agents().run(report) => return match result {
            Err(error) => Err(error.into()),
            Ok(never) => match never {},
        },
        result = failure => return Err(result.map_err(|_| "actor failure producer ended")?.into()),
        result = exercise => result?,
    }
    Ok(())
}

#[tokio::test]
async fn conversation_creation_wait_retains_member_absence_diagnostics() -> Result {
    use futures::TryStreamExt as _;
    let provider = Arc::new(Evidence::default());
    let host = AgentHost::new(
        provider.clone(),
        super::script(vec![TurnStep {
            checkpoint: None,
            outs: vec![Out::Conversation(b"answer".to_vec())],
            wait: Wait::Input,
            drops: None,
        }]),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host
        .create(&intent("diagnostic-create"), super::adopted(id(1)))
        .await?;
    assert!(host.runtime().drain().await.is_empty());
    let diagnostic = missing_refusal();
    provider.missing(root.conversation()?, diagnostic.clone());
    assert!(matches!(host.follow_conversation(&root, 1).await,
        Err(Error::Stream(error)) if error == diagnostic));
    provider.missing(root.conversation()?, StreamError::AccessDenied);
    assert!(matches!(
        host.follow_conversation(&root, 0).await,
        Err(Error::Stream(StreamError::AccessDenied))
    ));
    provider.missing(root.conversation()?, diagnostic);
    let waiting = host.follow_conversation(&root, 0);
    futures::pin_mut!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    host.send(
        &root,
        &super::user(),
        &InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2))?,
            id: intent("publish-conversation"),
        }),
    )
    .await?;
    assert!(host.runtime().drain().await.is_empty());
    let mut conversation = waiting.await?;
    assert_eq!(
        conversation
            .try_next()
            .await?
            .ok_or("missing conversation")?
            .value
            .as_ref(),
        b"answer"
    );
    Ok(())
}
