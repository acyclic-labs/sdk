//! Required Streams contracts since Cloud #5322 (857d16149af8ee24b307ad4627878c700dbce860).
use super::*;
use acyclic_harness::task_volume::{FileKey, TaskVolume, Write};
use acyclic_stream::{CommitConflict, CommitMutation, OwnedBytes, wire};
use futures::StreamExt as _;
use prost::Message as _;

struct Connection {
    client: Arc<Client>,
    config: Remote,
    _loopback: Option<Loopback>,
}

impl std::ops::Deref for Connection {
    type Target = Arc<Client>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

async fn remote() -> TestResult<Connection> {
    if let Some(file) = acyclic_harness::test_support::environment("ACYCLIC_LOCAL_CLOUD") {
        let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
        return Ok(Connection {
            client: endpoints.streams.connect().await?,
            config: endpoints.streams,
            _loopback: None,
        });
    }
    let local = loopback(Arc::new(MemoryStream::default())).await?;
    Ok(Connection {
        client: local.client.clone(),
        config: local.remote.clone(),
        _loopback: Some(local),
    })
}

fn report<T>(name: &str, result: TestResult<T>, errors: &mut Vec<String>) {
    match result {
        Ok(_) => eprintln!("Cloud5322 PASS {name}"),
        Err(error) => {
            let error = format!("{name}: {error}");
            eprintln!("Cloud5322 MISMATCH {error}");
            errors.push(error);
        }
    }
}

pub(super) async fn required_pass() -> TestResult {
    let remote = remote().await?;
    let cases = scenarios()?;
    let mut errors = Vec::new();
    // The hierarchy conformance phase needs an otherwise stable catalog.
    report(
        "Stream conformance",
        timed("Streams conformance", async {
            acyclic_stream::conformance::verify(remote.as_ref()).await?;
            TestResult::Ok(())
        })
        .await,
        &mut errors,
    );
    let probes = timed("required Session pins", async {
        let refused = refusals(remote.clone(), &remote, "probe").await?;
        if !refused.is_empty() {
            return Err(format!("expected no Unsupported pins, observed {refused:?}").into());
        }
        TestResult::Ok(())
    });
    let compared = async {
        futures::future::join_all(cases.iter().map(|(path, case)| {
            let remote = remote.clone();
            async move {
                let relative = path.strip_prefix(root()).unwrap_or(path);
                let scenario_root = format!("cloud5322-cases/{}", relative.display())
                    .replace('\\', "/")
                    .replace('.', "-");
                let phase = format!("scenario {}", relative.display());
                let result = timed(
                    &phase,
                    compare(
                        relative,
                        case,
                        Arc::new(MemoryStream::default()),
                        remote,
                        &scenario_root,
                    ),
                )
                .await;
                (relative.display().to_string(), result)
            }
        }))
        .await
    };
    let swarm = timed("required SwarmClient::follow", async {
        let session = remote.session_client()?;
        let baseline = Arc::new(MemoryStream::default());
        let executor = Arc::new(acyclic_stream::session::runtime::PlatformExecutor);
        let local = acyclic_stream::session::Client::in_process(
            baseline.clone(),
            acyclic_stream::session::testing::authority(),
            executor,
        );
        local.register_credential(0, acyclic_stream::session::testing::credential())?;
        let expected = remote_swarm::follow(baseline, &local, "swarm", "Do it.").await?;
        let observed = remote_swarm::follow(remote.clone(), &session, "swarm", "Do it.").await?;
        if observed != expected {
            return Err(format!("Swarm views expected {expected}, observed {observed}").into());
        }
        TestResult::Ok(())
    });
    let resumed = timed("kill / resume / completed reopen", async {
        let local = loopback(Arc::new(MemoryStream::default())).await?;
        let (cloud, baseline, unjournaled) = futures::join!(
            resume::kill_and_resume(&remote.config, remote.clone()),
            resume::kill_and_resume(&local.remote, local.client.clone()),
            resume::resume_unjournaled(remote.clone()),
        );
        let (cloud, baseline) = (cloud?, baseline?);
        unjournaled?;
        if cloud != baseline {
            return Err(format!("resumed journals differ: {cloud:?} vs {baseline:?}").into());
        }
        TestResult::Ok(())
    });
    let (probes, compared, swarm, resumed) = futures::join!(probes, compared, swarm, resumed);
    report(
        "Head publication / checked follow / Children / LATEST pins",
        probes,
        &mut errors,
    );
    report("SwarmClient::follow", swarm, &mut errors);
    report("kill / resume / completed reopen", resumed, &mut errors);
    for (name, result) in compared {
        report(&name, result, &mut errors);
    }
    if !errors.is_empty() {
        return Err(errors.join("\n").into());
    }
    Ok(())
}

#[tokio::test]
async fn head_fenced_task_volume_publication_rejects_a_forged_head() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-fence/task")?;
    let volume = TaskVolume::open(remote.clone(), path.clone()).await?;
    let first = Write::new(FileKey::Goal, b"published".to_vec());
    let head = volume.append(volume.head().await?, &first).await?;
    let forged = CommitId::from_bytes([255; 32]);
    let request = CommitRequest {
        conditions: vec![CommitCondition::Head {
            path: path.clone(),
            expected: head.position,
            commit_id: forged,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"must-not-commit").into()],
        }],
        idempotency_key: IdempotencyKey::new(b"cloud5322-forged-head".to_vec())?,
    };
    let observed = remote.commit(request.clone()).await?;
    let expected = CommitOutcome::Conflict(vec![CommitConflict::Head {
        path: path.clone(),
        expected: forged,
        actual: CommitId::from_bytes(head.commit),
    }]);
    if observed != expected {
        return Err(format!("Head conflict expected {expected:?}, observed {observed:?}").into());
    }
    if remote.commit(request).await? != expected {
        return Err("Head conflict retry changed".into());
    }
    if remote.tail(path).await? != head.position {
        return Err("forged Head changed the tail".into());
    }
    let second = Write::new(FileKey::Goal, b"accepted".to_vec());
    let next = volume.append(head, &second).await?;
    if next.position != head.position + 1 {
        return Err("valid fenced publication did not advance once".into());
    }
    eprintln!(
        "Cloud5322 PASS exact Head conflict, retry, unchanged tail and valid task publication"
    );
    Ok(())
}

/// Lease-fenced publication (`TaskVolume::append_if`/`create_if` with a
/// deadline) commits through `commit_before`. Each step's exact outcome: an
/// elapsed deadline commits nothing, an open one commits once.
async fn deadline_trace<C, F>(
    commit: C,
    path: &StreamPath,
) -> TestResult<Vec<Result<bool, StreamError>>>
where
    C: Fn(CommitRequest, u64) -> F,
    F: std::future::Future<Output = Result<CommitOutcome, StreamError>>,
{
    let mut trace = Vec::new();
    for (step, condition, deadline) in [
        (0u8, CommitCondition::Absent { path: path.clone() }, 1),
        (1, CommitCondition::Absent { path: path.clone() }, u64::MAX),
        (
            2,
            CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            },
            1,
        ),
        (
            3,
            CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            },
            u64::MAX,
        ),
    ] {
        let request = CommitRequest {
            conditions: vec![condition],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![OwnedBytes::copy_from_slice(&[step]).into()],
            }],
            idempotency_key: IdempotencyKey::new(format!("{}/{step}", path.as_str()).into_bytes())?,
        };
        trace.push(match commit(request, deadline).await {
            Ok(CommitOutcome::Committed(_)) => Ok(true),
            Ok(CommitOutcome::Conflict(_)) => Ok(false),
            Err(error) => Err(error),
        });
    }
    Ok(trace)
}

fn deadline_oracle() -> Vec<Result<bool, StreamError>> {
    let elapsed = Err(StreamError::DeadlineElapsed);
    vec![elapsed.clone(), Ok(true), elapsed, Ok(true)]
}

#[tokio::test]
async fn deadline_oracle_matches_memory_and_rejects_an_ignored_deadline() -> TestResult {
    let path = StreamPath::new("cloud5322-deadline/task")?;
    let memory = MemoryStream::default();
    let honoured = deadline_trace(
        |request, deadline| memory.commit_before(request, deadline),
        &path,
    )
    .await?;
    assert_eq!(honoured, deadline_oracle());
    // Negative control: a provider that drops the deadline commits every step.
    let memory = MemoryStream::default();
    let ignored = deadline_trace(|request, _| memory.commit(request), &path).await?;
    assert_ne!(ignored, deadline_oracle());
    Ok(())
}

#[tokio::test]
async fn deadline_fenced_commits_match_the_oracle() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-deadline/task")?;
    let observed = deadline_trace(
        |request, deadline| remote.commit_before(request, deadline),
        &path,
    )
    .await?;
    if observed != deadline_oracle() {
        return Err(format!(
            "deadline commits expected {:?}, observed {observed:?}",
            deadline_oracle()
        )
        .into());
    }
    eprintln!(
        "Cloud5322 PASS exact deadline-fenced commits; watermarks {:?}",
        remote.watermarks(path).await?
    );
    Ok(())
}

#[tokio::test]
async fn latest_payload_matches_exactly() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-errors/status")?;
    let latest = wire::SessionLatestValue {
        key: b"agent".to_vec().into(),
        value: b"ready".to_vec().into(),
        attention: true,
    };
    remote
        .append(AppendRequest {
            path: path.clone(),
            records: vec![latest.encode_to_vec().into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let mut data = remote
        .follow_data(
            wire::SessionFollow {
                resource: acyclic_stream::wire_codec::session::stream_resource(path.as_str()),
                delivery: wire::SessionDelivery::Latest as i32,
                preceding_commit_id: Default::default(),
            },
            0,
            acyclic_stream::FollowFilter::default(),
        )
        .await?;
    let observed = wait(&mut 1, "LATEST value", data.next())
        .await?
        .ok_or("LATEST ended")??;
    if observed.item != Some(wire::session_data::Item::LatestValue(latest.clone())) {
        return Err(format!("LATEST expected {latest:?}, observed {observed:?}").into());
    }
    eprintln!("Cloud5322 PASS exact LATEST value");
    Ok(())
}

#[tokio::test]
async fn checked_follow_close_codes() -> TestResult {
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-errors/checked")?;
    remote
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"anchor").into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let session = remote.session_client()?;
    let mut errors = Vec::new();
    for (path, from, preceding, expected) in [
        (
            path.clone(),
            1,
            Some(CommitId::from_bytes([255; 32])),
            StreamError::PrefixNotRetained,
        ),
        (path, 2, None, StreamError::OutOfRange),
        (
            StreamPath::new("cloud5322-errors/missing")?,
            0,
            None,
            StreamError::NotFound,
        ),
    ] {
        let opened = match preceding {
            Some(preceding) => {
                session
                    .follow_checked(
                        path,
                        from,
                        preceding,
                        acyclic_stream::FollowFilter::default(),
                    )
                    .await
            }
            None => {
                session
                    .follow(path, from, acyclic_stream::FollowFilter::default())
                    .await
            }
        };
        let observed = match opened {
            Err(error) => Some(error),
            Ok(mut follow) => {
                let item = wait(&mut 1, "checked follow close", follow.next()).await?;
                match item {
                    Some(Err(error)) => Some(error),
                    Some(Ok(record)) => {
                        eprintln!("unexpected checked-follow record: {record:?}");
                        Some(StreamError::Unavailable)
                    }
                    None => None,
                }
            }
        };
        if observed == Some(expected.clone()) {
            eprintln!("Cloud5322 PASS Closed {} -> {expected:?}", expected.code());
        } else {
            errors.push(format!(
                "Closed {} must map to {expected:?}, observed {observed:?}",
                expected.code()
            ));
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("\n").into());
    }
    Ok(())
}

#[tokio::test]
async fn raw_session_closed_frames_use_stream_error_codes() -> TestResult {
    use wire::{session_frame::Body, session_subscription::Kind};
    let remote = remote().await?;
    let path = StreamPath::new("cloud5322-raw/checked")?;
    remote
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"anchor").into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let channel = tonic::transport::Endpoint::from_shared(remote.config.endpoint.clone())?
        .tls_config(tonic::transport::ClientTlsConfig::new().ca_certificate(
            tonic::transport::Certificate::from_pem(std::fs::read(&remote.config.ca_file)?),
        ))?
        .connect()
        .await?;
    let mut client = wire::stream_session_service_client::StreamSessionServiceClient::new(channel);
    let (send, receive) = tokio::sync::mpsc::channel(8);
    let frame = |body| wire::SessionFrame {
        channel_id: 0,
        generation: 0,
        body: Some(body),
    };
    send.send(frame(Body::Hello(wire::SessionHello {
        credentials: vec![remote.config.workload.clone().into()],
        version: 1,
        client_id: b"cloud-local-5322".to_vec().into(),
        destination_node: String::new(),
        receive_bytes: 1 << 20,
        receive_frames: 100,
    })))
    .await?;
    let input = futures::stream::unfold(receive, |mut receive| async move {
        receive.recv().await.map(|frame| (frame, receive))
    });
    let mut request = tonic::Request::new(input);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", remote.config.bearer).parse()?,
    );
    let mut output = client.session(request).await?.into_inner();
    let welcome = wait(&mut 1, "Session Welcome", output.message())
        .await??
        .ok_or("raw Session ended before Welcome")?;
    if !matches!(welcome.body, Some(Body::Welcome(_))) {
        return Err(format!("expected Welcome, observed {welcome:?}").into());
    }
    let subscriptions = [
        (1, path.as_str(), 1, vec![255; 32]),
        (3, path.as_str(), 2, vec![]),
        (5, "cloud5322-raw/missing", 0, vec![]),
        (7, path.as_str(), 0, vec![]),
    ]
    .into_iter()
    .map(|(id, path, next, preceding)| wire::SessionSubscription {
        credential: 0,
        filter: None,
        channel: Some(wire::SessionChannel {
            channel_id: id,
            generation: 1,
        }),
        resume: Some(wire::SessionCursor {
            next,
            opaque: Default::default(),
        }),
        kind: Some(Kind::Follow(wire::SessionFollow {
            resource: acyclic_stream::wire_codec::session::stream_resource(path),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: preceding.into(),
        })),
    })
    .collect();
    send.send(frame(Body::Interest(wire::SessionInterest {
        revision: 1,
        upsert: subscriptions,
        remove: vec![],
    })))
    .await?;
    let mut siblings = Siblings::default();
    let mut remaining = Siblings::FRAMES;
    while !siblings.complete() {
        let frame = wait(&mut remaining, "Session sibling outcomes", output.message())
            .await??
            .ok_or("raw Session ended before Closed frames")?;
        eprintln!("Cloud5322 raw frame {frame:?}");
        if let Some(window) = siblings.accept(&frame)? {
            send.send(window).await?;
        }
    }
    eprintln!("Cloud5322 PASS exact raw Closed frames and healthy sibling Opened/data");
    Ok(())
}

/// Exact outcome of the raw Interest: one `InterestApplied` for revision 1, one
/// Closed per refused sibling with its exact code, and the healthy sibling's
/// Opened followed by its anchor record. Any other or repeated frame fails.
#[derive(Default)]
struct Siblings {
    applied: bool,
    closed: std::collections::BTreeMap<u64, String>,
    opened: bool,
    delivered: bool,
}

impl Siblings {
    const REFUSED: [(u64, &str); 3] = [
        (1, "prefix_not_retained"),
        (3, "out_of_range"),
        (5, "not_found"),
    ];
    const HEALTHY: u64 = 7;
    /// Acknowledgement, refused closes, then healthy open and data frames.
    const FRAMES: usize = 1 + Self::REFUSED.len() + 2;

    fn complete(&self) -> bool {
        self.applied && self.closed.len() == Self::REFUSED.len() && self.delivered
    }

    /// Admits one frame; returns the `Window` to send when the healthy sibling opens.
    fn accept(&mut self, frame: &wire::SessionFrame) -> TestResult<Option<wire::SessionFrame>> {
        use wire::session_frame::Body;
        let refused = |id| Self::REFUSED.iter().find(|(channel, _)| *channel == id);
        match (&frame.body, frame.channel_id, frame.generation) {
            (Some(Body::InterestApplied(applied)), 0, 0)
                if !self.applied && applied.revision == 1 =>
            {
                self.applied = true;
            }
            (Some(Body::Closed(closed)), id, 1)
                if refused(id).is_some_and(|(_, code)| *code == closed.code)
                    && !self.closed.contains_key(&id) =>
            {
                self.closed.insert(id, closed.code.clone());
            }
            (Some(Body::Opened(_)), Self::HEALTHY, 1) if !self.opened => {
                self.opened = true;
                return Ok(Some(wire::SessionFrame {
                    channel_id: Self::HEALTHY,
                    generation: 1,
                    body: Some(Body::Window(wire::SessionWindow {
                        bytes: 1 << 20,
                        frames: 10,
                    })),
                }));
            }
            (Some(Body::Data(data)), Self::HEALTHY, 1)
                if self.opened
                    && !self.delivered
                    && data.after.as_ref().map(|cursor| cursor.next) == Some(1)
                    && matches!(&data.item, Some(wire::session_data::Item::Record(record))
                        if record.sequence == 0 && record.value.as_ref() == b"anchor") =>
            {
                self.delivered = true;
            }
            _ => return Err(format!("unexpected Session sibling frame: {frame:?}").into()),
        }
        Ok(None)
    }
}

fn frame(channel_id: u64, generation: u64, body: wire::session_frame::Body) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id,
        generation,
        body: Some(body),
    }
}

#[test]
fn sibling_oracle_requires_each_exact_frame_once() -> TestResult {
    use wire::session_frame::Body;
    let applied = |revision| {
        frame(
            0,
            0,
            Body::InterestApplied(wire::SessionInterestApplied { revision }),
        )
    };
    let closed = |id, code: &str| {
        frame(
            id,
            1,
            Body::Closed(wire::SessionClosed {
                code: code.into(),
                ..Default::default()
            }),
        )
    };
    let opened = frame(7, 1, Body::Opened(Default::default()));
    let data = |sequence, value: &'static [u8], next| {
        frame(
            7,
            1,
            Body::Data(wire::SessionData {
                after: Some(wire::SessionCursor {
                    next,
                    opaque: Default::default(),
                }),
                item: Some(wire::session_data::Item::Record(wire::Record {
                    sequence,
                    value: value.to_vec().into(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
        )
    };
    let valid = vec![
        applied(1),
        closed(1, "prefix_not_retained"),
        closed(3, "out_of_range"),
        opened.clone(),
        closed(5, "not_found"),
        data(0, b"anchor", 1),
    ];
    // Some(index) rejects at that frame; None leaves the exchange incomplete.
    let run = |frames: &[wire::SessionFrame]| -> Result<(), Option<usize>> {
        let mut siblings = Siblings::default();
        for (index, frame) in frames.iter().enumerate() {
            let window = siblings.accept(frame).map_err(|_| Some(index))?;
            assert_eq!(window.is_some(), frame == &opened);
        }
        if siblings.complete() {
            Ok(())
        } else {
            Err(None)
        }
    };
    assert_eq!(valid.len(), Siblings::FRAMES);
    assert_eq!(run(&valid), Ok(()));
    let mut reordered = valid.clone();
    reordered.swap(1, 4);
    assert_eq!(run(&reordered), Ok(()));
    // Negative controls: every omitted frame leaves the exchange incomplete.
    for omitted in 0..valid.len() {
        let mut frames = valid.clone();
        frames.remove(omitted);
        let expected = if omitted == 3 {
            Err(Some(4))
        } else {
            Err(None)
        };
        assert_eq!(run(&frames), expected, "omitted frame {omitted}");
    }
    // Each repeated or altered frame is rejected where it arrives.
    for (index, replacement) in [
        (1, applied(1)),
        (1, applied(2)),
        (1, closed(1, "out_of_range")),
        (1, closed(7, "not_found")),
        (2, closed(1, "prefix_not_retained")),
        (3, frame(1, 1, Body::Opened(Default::default()))),
        (3, frame(7, 2, Body::Opened(Default::default()))),
        (4, opened.clone()),
        (5, data(1, b"anchor", 2)),
        (5, data(0, b"changed", 1)),
        (5, data(0, b"anchor", 0)),
    ] {
        let mut frames = valid.clone();
        *frames.get_mut(index).ok_or("replacement frame missing")? = replacement.clone();
        assert_eq!(run(&frames), Err(Some(index)), "accepted {replacement:?}");
    }
    let mut early = valid.clone();
    early.swap(3, 5);
    assert_eq!(run(&early), Err(Some(3)), "accepted Data before Opened");
    let mut repeated = valid;
    repeated.push(data(0, b"anchor", 1));
    assert_eq!(run(&repeated), Err(Some(6)), "accepted a repeated record");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum WaitError {
    #[error("{operation}: Session response frame budget exhausted")]
    FramesExhausted { operation: &'static str },
    #[error("{operation}: {detail}")]
    Stalled {
        operation: &'static str,
        detail: String,
    },
}

/// Each expected response consumes a protocol frame, independently of host
/// speed. The shared runner guard attributes missing wakeups/frames and drains
/// accepted I/O; scenario semantics carry no clock or per-response deadline.
async fn wait<T>(
    remaining: &mut usize,
    operation: &'static str,
    response: impl std::future::Future<Output = T>,
) -> Result<T, WaitError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(WaitError::FramesExhausted { operation })?;
    super::super::stall::guarded(
        Path::new("cloud5322"),
        super::super::stall::waiting(operation, response),
    )
    .await
    .map_err(|detail| WaitError::Stalled { operation, detail })
}

#[tokio::test(start_paused = true)]
async fn response_waits_fail_with_typed_stalls_and_reject_an_unguarded_wait() {
    fn typed_stall(result: Result<Result<(), WaitError>, String>, operation: &str) -> bool {
        matches!(result, Ok(Err(WaitError::Stalled { operation: actual, detail }))
            if actual == operation && detail == format!("stalled: cloud5322 at {operation}"))
    }
    for operation in [
        "LATEST value",
        "checked follow close",
        "Session Welcome",
        "Session sibling outcomes",
    ] {
        // Register the independent observer before either wait starts. Both
        // guards then observe initial admission, rather than an empty outer
        // registry mistaking the inner guard's first quiet period for a stall.
        for guarded in [true, false] {
            let mut guard = super::super::progress_guard::Guard::new();
            let registry = guard.registry.clone();
            let result = guard
                .run(async {
                    let registration = registry.register(Path::new("wait-negative-control"));
                    drop(registration.polling());
                    if guarded {
                        wait(&mut 1, operation, std::future::pending::<()>()).await
                    } else {
                        // Removing the response guard must fail this same
                        // typed-error oracle without an external termination.
                        std::future::pending::<()>().await;
                        Ok(())
                    }
                })
                .await;
            assert_eq!(typed_stall(result, operation), guarded);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn response_budget_is_exact_and_rejects_a_restored_frame() -> TestResult {
    fn exhausted(result: &Result<(), WaitError>) -> bool {
        matches!(
            result,
            Err(WaitError::FramesExhausted {
                operation: "frames"
            })
        )
    }
    for frames in 0..=8 {
        let mut remaining = frames;
        for delivered in 0..frames {
            assert_eq!(
                wait(&mut remaining, "frames", std::future::ready(delivered)).await?,
                delivered
            );
        }
        assert!(exhausted(
            &wait(&mut remaining, "frames", std::future::ready(())).await
        ));
        // Restoring a consumed frame must fail the same exact-budget oracle.
        remaining += 1;
        assert!(!exhausted(
            &wait(&mut remaining, "frames", std::future::ready(())).await
        ));
    }
    Ok(())
}
