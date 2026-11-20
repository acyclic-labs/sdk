//! Seeded deterministic simulation of the one reader.
//!
//! The reader is sans-I/O, so a world is a real memory provider plus a seeded
//! transport: each frame plans wants, polls the reader, executes the ops on
//! the provider, and folds the outcomes back in a seeded order with seeded
//! faults. The oracle compares what the reader serves with the provider's own
//! history after every frame, so a served byte can never disagree with the
//! stream at the cut the reader claims. A second world drives the real
//! [`Driver`] over the same provider, and a third drives source custody and
//! the live projections.

use super::driver;
use super::live::trace;
use super::*;
use acyclic_auth::credential::{
    CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity,
};
use acyclic_stream::{
    AppendRequest, FollowRequest, ForkRequest, IdempotencyKey, MemoryStream, Record,
    StreamProvider as _, StreamProviderExt as _,
};
use futures::StreamExt as _;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// A tiny deterministic generator, so a world is a pure function of its seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    /// A draw below a small `bound`, in the type the caller counts in.
    fn small<T: TryFrom<u64>>(&mut self, bound: u64) -> T
    where
        T::Error: std::fmt::Debug,
    {
        T::try_from(self.below(bound)).unwrap()
    }
}

/// One provider plus the authoritative history the oracle compares against.
struct World {
    provider: MemoryStream,
    /// Committed suffix per stream; a fork's inherited prefix lives in its source.
    history: BTreeMap<StreamPath, Vec<Vec<u8>>>,
    /// Immediate fork provenance: child -> (source, exclusive inherited cut).
    forks: BTreeMap<StreamPath, (StreamPath, u64)>,
    keys: u64,
}

impl World {
    fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let mut world = Self {
            provider: MemoryStream::new_with_clock(
                acyclic_stream::MemoryLimits::default(),
                Arc::new(std::sync::atomic::AtomicU64::new(1_700_000_000_000)),
            ),
            history: BTreeMap::new(),
            forks: BTreeMap::new(),
            keys: 0,
        };
        for name in ["agents/a", "agents/b", "agents/c"] {
            let path = StreamPath::new(name).unwrap();
            for index in 0..2 + rng.below(3) {
                world.append(&path, format!("{name}#{index}").into_bytes());
            }
            let status = StreamPath::new(format!("{name}/status")).unwrap();
            world.append(&status, format!("{name} status").into_bytes());
        }
        let source = StreamPath::new("agents/a").unwrap();
        let child = StreamPath::new("agents/a/child").unwrap();
        world.fork(&child, &source, 1);
        world.append(&child, b"child#1".to_vec());
        world
    }

    fn append(&mut self, path: &StreamPath, bytes: Vec<u8>) {
        let tail = self.tail(path);
        self.keys += 1;
        futures::executor::block_on(self.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![bytes.clone().into()],
            if_tail: Some(tail),
            idempotency_key:
                IdempotencyKey::new(format!("dst-{}", self.keys).into_bytes()).unwrap(),
        }))
        .unwrap();
        self.history.entry(path.clone()).or_default().push(bytes);
    }

    fn fork(&mut self, child: &StreamPath, source: &StreamPath, cut: u64) {
        self.keys += 1;
        futures::executor::block_on(self.provider.fork(ForkRequest {
            exclude: Vec::new(),
            source: source.clone(),
            destination: child.clone(),
            at_tail: Some(cut),
            idempotency_key:
                IdempotencyKey::new(format!("dst-fork-{}", self.keys).into_bytes()).unwrap(),
        }))
        .unwrap();
        self.forks.insert(child.clone(), (source.clone(), cut));
        self.history.insert(child.clone(), Vec::new());
    }

    fn tail(&self, path: &StreamPath) -> u64 {
        let suffix = self
            .history
            .get(path)
            .map_or(0, |records| records.len() as u64);
        self.forks.get(path).map_or(suffix, |(_, cut)| cut + suffix)
    }

    /// The bytes at one position, following a fork's inherited prefix.
    fn bytes(&self, path: &StreamPath, sequence: u64) -> Option<&[u8]> {
        if let Some((source, cut)) = self.forks.get(path) {
            if sequence < *cut {
                return self.bytes(source, sequence);
            }
            return self
                .history
                .get(path)?
                .get(usize::try_from(sequence - cut).ok()?)
                .map(Vec::as_slice);
        }
        self.history
            .get(path)?
            .get(usize::try_from(sequence).ok()?)
            .map(Vec::as_slice)
    }

    fn record(&self, path: &StreamPath, sequence: u64, commit: CommitId) -> Option<Record> {
        Some(Record {
            sequence,
            value: self.bytes(path, sequence)?.to_vec().into(),
            commit_id: commit,
            committed_at_micros: 0,
            headers: Default::default(),
            origin: path.clone(),
        })
    }
}

/// Transport faults for one frame. A fault names the stream it targets.
#[derive(Clone, Debug, Default)]
struct Faults {
    path: Option<StreamPath>,
    /// The provider refuses the read with an authorization failure.
    deny: bool,
    /// The provider no longer retains the requested prefix.
    drop_prefix: bool,
    /// Replies are held until the next frame.
    withhold: bool,
    /// Every reply is delivered twice.
    duplicate: bool,
    /// Replies are delivered in reverse order.
    reorder: bool,
    /// The follow ends and must be reopened.
    follow_loss: bool,
}

/// The properties every frame must preserve, checked against the provider.
#[derive(Default)]
struct Oracle {
    /// Highest tail the reader has ever observed per stream.
    observed: BTreeMap<StreamPath, u64>,
    /// Visible wants that are not yet served, and the frame they appeared.
    visible: BTreeMap<Want, u64>,
    max_visible: u64,
}

impl Oracle {
    fn check(&mut self, reader: &Reader, world: &World, frame: u64, wants: &[(Want, Priority)]) {
        for path in world.history.keys() {
            let Some(tail) = reader.cache().tail(path) else {
                continue;
            };
            let previous = self.observed.get(path).copied().unwrap_or(0);
            let replaced = matches!(reader.issue(path), Some(ReadIssue::RetentionLost));
            assert!(
                tail >= previous || replaced,
                "frame {frame}: {path} cut rolled back from {previous} to {tail}"
            );
            self.observed.insert(path.clone(), tail.max(previous));
            for (sequence, bytes) in reader.cache().records(path, 0, tail) {
                let Some(bytes) = bytes else { continue };
                assert_eq!(
                    Some(bytes.as_ref()),
                    world.bytes(path, sequence),
                    "frame {frame}: {path}:{sequence} served bytes differ from the stream"
                );
            }
        }
        let residency = reader.residency();
        assert!(
            residency.cache_bytes <= reader.policy().cache_bytes,
            "frame {frame}: cache charge {} exceeds budget {}",
            residency.cache_bytes,
            reader.policy().cache_bytes
        );
        assert!(
            residency.requests <= reader.policy().requests,
            "frame {frame}: {} requests exceed {}",
            residency.requests,
            reader.policy().requests
        );
        for (want, priority) in wants {
            if *priority != Priority::Visible {
                continue;
            }
            let path = want_path(want);
            let settled = reader.resolve(want).is_none() || reader.issue(path).is_some();
            match self.visible.get(want).copied() {
                None if !settled => {
                    self.visible.insert(want.clone(), frame);
                }
                Some(_) if settled => {
                    self.visible.remove(want);
                }
                Some(since) => assert!(
                    frame - since <= self.max_visible,
                    "frame {frame}: visible want {want:?} unsettled for {} frames",
                    frame - since
                ),
                None => {}
            }
        }
    }
}

fn want_path(want: &Want) -> &StreamPath {
    match want {
        Want::Range { stream, .. } | Want::Last { stream, .. } => stream,
        Want::Listing { parent, .. } => parent,
    }
}

fn op_path(op: &Op) -> &StreamPath {
    match op {
        Op::Read { stream, .. } | Op::Head { stream, .. } => stream,
        Op::Children { parent, .. } => parent,
    }
}

fn op_key(op: &Op) -> OpKey {
    match op {
        Op::Read { stream, from, .. } => OpKey::Read(stream.clone(), *from),
        Op::Head { stream, .. } => OpKey::Head(stream.clone()),
        Op::Children {
            parent,
            index,
            limit,
            tail,
            ..
        } => OpKey::children(parent, *index, *limit, tail.clone()),
    }
}

/// One seeded world: the reader, its transport and the oracle.
struct Sim {
    world: World,
    reader: Reader,
    oracle: Oracle,
    now: u64,
    frame: u64,
    held: Vec<(u64, Outcome)>,
    follows: BTreeMap<StreamPath, acyclic_stream::RecordStream>,
    trace: Vec<String>,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x5DEE_CE66_D1CE_4E5B);
        let policy = ReaderPolicy {
            cache_bytes: 64 + 64 * rng.small::<usize>(8),
            streams: 2 + rng.small::<usize>(3),
            requests: 2 + rng.small::<usize>(3),
            prefetch_requests: 1,
            follow_requests: 1 + rng.small::<usize>(2),
            read_limit: 1 + rng.small::<u32>(3),
            listing_limit: 1 + rng.small::<u32>(3),
            lag: 1 + rng.below(3),
            deliveries: 1 + rng.small::<usize>(3),
            ..ReaderPolicy::default()
        };
        Self {
            world: World::new(seed),
            reader: Reader::new(policy).unwrap(),
            oracle: Oracle {
                max_visible: 12,
                ..Oracle::default()
            },
            now: 0,
            frame: 0,
            held: Vec::new(),
            follows: BTreeMap::new(),
            trace: Vec::new(),
        }
    }

    fn frame(&mut self, wants: &[(Want, Priority)], faults: &Faults) {
        self.frame += 1;
        self.now += 1;
        self.reader.begin_frame(self.now);
        self.reader.plan(wants.iter().cloned());
        let ops = self.reader.poll(self.now);
        let mut interests = BTreeSet::new();
        for op in &ops {
            assert!(
                interests.insert(op_key(op)),
                "frame {}: duplicate interest dispatched: {op:?}",
                self.frame
            );
        }
        let mut replies = Vec::with_capacity(ops.len());
        for op in &ops {
            replies.push((op.id(), self.execute(op, faults)));
        }
        let mut due = std::mem::take(&mut self.held);
        if faults.withhold {
            self.held = replies;
        } else {
            due.extend(replies);
        }
        if faults.reorder {
            due.reverse();
        }
        if faults.duplicate {
            due = due
                .iter()
                .flat_map(|reply| [reply.clone(), reply.clone()])
                .collect();
        }
        let mut line = format!(
            "{}|{}|{}|{}",
            self.frame,
            self.reader.residency().cache_bytes,
            self.reader.residency().requests,
            self.reader.generation()
        );
        for op in &ops {
            line.push_str(&format!("|{:?}", op_key(op)));
        }
        for (id, outcome) in &due {
            line.push_str(&format!("|{id}:{outcome:?}"));
        }
        self.trace.push(line);
        for (id, outcome) in due {
            self.reader.complete(id, outcome, self.now);
        }
        self.drive_follows(faults);
        self.oracle
            .check(&self.reader, &self.world, self.frame, wants);
    }

    fn execute(&self, op: &Op, faults: &Faults) -> Outcome {
        if faults.path.as_ref() == Some(op_path(op)) {
            if faults.deny {
                return Outcome::Failed(StreamError::AccessDenied);
            }
            if faults.drop_prefix {
                return Outcome::Failed(StreamError::PrefixNotRetained);
            }
        }
        futures::executor::block_on(driver::execute(&self.world.provider, op.clone()))
    }

    fn drive_follows(&mut self, faults: &Faults) {
        let wanted: Vec<(StreamPath, u64)> = self
            .reader
            .follows()
            .map(|(path, from)| (path.clone(), from))
            .collect();
        for (path, from) in wanted {
            if faults.follow_loss && faults.path.as_ref() == Some(&path) {
                self.follows.remove(&path);
                self.reader.follow_failed(Failure::Unavailable, self.now);
                continue;
            }
            if !self.follows.contains_key(&path) {
                match futures::executor::block_on(
                    self.world
                        .provider
                        .follow(FollowRequest::new(path.clone(), from)),
                ) {
                    Ok(stream) => {
                        self.follows
                            .insert(path.clone(), acyclic_stream::records_only(stream));
                    }
                    Err(error) => {
                        self.reader.follow_failed(Failure::of(&error), self.now);
                        continue;
                    }
                }
            }
            let accepted = self.world.tail(&path);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let polled = self
                .follows
                .get_mut(&path)
                .map(|stream| stream.poll_next_unpin(&mut cx));
            match polled {
                Some(Poll::Ready(Some(Ok(record)))) => {
                    self.reader
                        .deliver(&path, driver::fetched(record), accepted);
                }
                Some(Poll::Ready(Some(Err(error)))) => {
                    self.reader.follow_failed(Failure::of(&error), self.now);
                    self.follows.remove(&path);
                }
                Some(Poll::Ready(None)) => {
                    self.reader.follow_failed(Failure::Unavailable, self.now);
                    self.follows.remove(&path);
                }
                Some(Poll::Pending) | None => {}
            }
        }
    }

    fn append(&mut self, path: &StreamPath, bytes: &[u8]) {
        self.world.append(path, bytes.to_vec());
        self.trace
            .push(format!("append:{path}:{}", self.world.tail(path)));
    }

    fn follow(&mut self, paths: &[StreamPath]) {
        self.reader.set_follows(paths.iter().cloned());
        self.trace.push(format!("follow:{}", paths.len()));
    }

    fn acknowledge(&mut self, path: &StreamPath) {
        self.reader.acknowledge_issue(path);
        self.trace.push(format!("acknowledge:{path}"));
    }

    /// Fold a head whose cut moved backwards and whose anchor changed, as a
    /// republished fork would. The reader must retire the old bytes.
    fn replace(&mut self, path: &StreamPath) {
        let tail = self.world.tail(path);
        let observed = self.reader.cache().observation(path);
        let commit = CommitId::from_bytes([0xAB; 32]);
        let head = PathHead {
            changed_at: 0,
            retained_through: None,
            tail: tail.saturating_sub(1),
            durable: 0,
            origin: None,
            last: tail
                .checked_sub(2)
                .and_then(|sequence| self.world.record(path, sequence, commit)),
        };
        let observation = self.reader.observed_head(path, head, observed);
        self.trace.push(format!("replace:{path}:{observation:?}"));
    }
}

/// One scripted step. The script is a pure function of the seed.
enum Step {
    Frame(Vec<(Want, Priority)>, Faults),
    Append(StreamPath, Vec<u8>),
    Follow(Vec<StreamPath>),
    Acknowledge(StreamPath),
    Replace(StreamPath),
}

fn script(seed: u64) -> Vec<Step> {
    let mut rng = Rng::new(seed ^ 0xA5A5_5A5A_1234_5678);
    let a = StreamPath::new("agents/a").unwrap();
    let b = StreamPath::new("agents/b").unwrap();
    let c = StreamPath::new("agents/c").unwrap();
    let child = StreamPath::new("agents/a/child").unwrap();
    let parent = StreamPath::new("agents").unwrap();
    let status = StreamPath::new("status").unwrap();
    let last = |stream: &StreamPath, count: u32| Want::Last {
        stream: stream.clone(),
        count,
    };
    let listing = Want::Listing {
        parent: parent,
        index: 0,
        limit: 2,
        tail: Some(status),
        witness: None,
    };
    let visible = |wants: Vec<Want>| {
        wants
            .into_iter()
            .map(|want| (want, Priority::Visible))
            .collect::<Vec<_>>()
    };
    let mut steps = vec![
        Step::Frame(
            visible(vec![last(&a, 2), last(&b, 1), listing.clone()]),
            Faults::default(),
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&b, 1), listing.clone()]),
            Faults::default(),
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&c, 1), listing]),
            Faults::default(),
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&c, 1)]),
            Faults {
                withhold: true,
                ..Faults::default()
            },
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&c, 1)]),
            Faults {
                duplicate: true,
                ..Faults::default()
            },
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&c, 1)]),
            Faults {
                reorder: true,
                ..Faults::default()
            },
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&c, 1)]),
            Faults {
                path: Some(c.clone()),
                deny: true,
                ..Faults::default()
            },
        ),
        Step::Acknowledge(c.clone()),
        Step::Frame(visible(vec![last(&a, 2), last(&c, 1)]), Faults::default()),
        Step::Frame(
            visible(vec![last(&a, 2), last(&child, 2)]),
            Faults {
                path: Some(a.clone()),
                drop_prefix: true,
                ..Faults::default()
            },
        ),
        Step::Frame(
            visible(vec![last(&a, 2), last(&child, 2)]),
            Faults::default(),
        ),
        Step::Replace(child.clone()),
        Step::Frame(visible(vec![last(&child, 2)]), Faults::default()),
        Step::Follow(vec![a.clone()]),
        Step::Append(a.clone(), b"followed#0".to_vec()),
        Step::Frame(visible(vec![last(&a, 2)]), Faults::default()),
        Step::Append(a.clone(), b"followed#1".to_vec()),
        Step::Frame(visible(vec![last(&a, 2)]), Faults::default()),
        Step::Frame(vec![(last(&a, 2), Priority::Prefetch)], Faults::default()),
        Step::Frame(visible(vec![last(&a, 2)]), Faults::default()),
        Step::Frame(
            visible(vec![last(&a, 2)]),
            Faults {
                path: Some(a.clone()),
                follow_loss: true,
                ..Faults::default()
            },
        ),
        Step::Frame(visible(vec![last(&a, 2)]), Faults::default()),
    ];
    for _ in 0..rng.below(4) {
        let path = [&a, &b, &c][usize::try_from(rng.below(3)).unwrap()].clone();
        steps.push(Step::Append(
            path.clone(),
            format!("extra-{}", rng.next()).into_bytes(),
        ));
        steps.push(Step::Frame(
            visible(vec![last(&path, 2)]),
            Faults::default(),
        ));
    }
    steps
}

fn run(seed: u64) -> Vec<String> {
    let mut sim = Sim::new(seed);
    for step in script(seed) {
        match step {
            Step::Frame(wants, faults) => sim.frame(&wants, &faults),
            Step::Append(path, bytes) => sim.append(&path, &bytes),
            Step::Follow(paths) => sim.follow(&paths),
            Step::Acknowledge(path) => sim.acknowledge(&path),
            Step::Replace(path) => sim.replace(&path),
        }
    }
    sim.trace
}

#[test]
fn seeded_reader_worlds_hold_the_oracle() {
    for seed in 0..8 {
        let trace = run(seed);
        assert!(!trace.is_empty(), "seed {seed} produced no frames");
    }
}

#[test]
fn seeded_reader_worlds_replay_identically() {
    for seed in 0..8 {
        assert_eq!(run(seed), run(seed), "seed {seed} diverged");
    }
}

struct Ready(std::sync::atomic::AtomicBool);

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn driver_world_serves_every_visible_want_and_follows_new_records() {
    use std::sync::atomic::Ordering;

    for seed in 0..4 {
        let mut world = World::new(seed);
        let provider = Arc::new(world.provider.clone());
        let mut driver = Driver::new(provider);
        let mut reader = Reader::new(ReaderPolicy {
            requests: 2,
            prefetch_requests: 1,
            read_limit: 2,
            deliveries: 1,
            ..ReaderPolicy::default()
        })
        .unwrap();
        let ready = Arc::new(Ready(std::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(ready.clone());
        let mut cx = Context::from_waker(&waker);
        let mut now = 0;
        driver.attach(&mut reader, now);
        let a = StreamPath::new("agents/a").unwrap();
        let b = StreamPath::new("agents/b").unwrap();
        let wants = vec![
            (
                Want::Last {
                    stream: a.clone(),
                    count: 2,
                },
                Priority::Visible,
            ),
            (
                Want::Last {
                    stream: b.clone(),
                    count: 1,
                },
                Priority::Visible,
            ),
        ];
        let mut step = |reader: &mut Reader, driver: &mut Driver<MemoryStream>, now: &mut u64| {
            *now += 1;
            reader.begin_frame(*now);
            reader.plan(wants.iter().cloned());
            ready.wake_by_ref();
            while ready.0.swap(false, Ordering::SeqCst) {
                driver.step(reader, *now, &mut cx);
            }
        };
        for _ in 0..6 {
            step(&mut reader, &mut driver, &mut now);
        }
        for (want, _) in &wants {
            assert!(
                reader.resolve(want).is_none(),
                "seed {seed}: {want:?} unserved"
            );
        }
        // A selected tail opens one follow and delivers each new record.
        reader.set_follows([a.clone()]);
        let before = world.tail(&a);
        for _ in 0..4 {
            step(&mut reader, &mut driver, &mut now);
        }
        assert_eq!(reader.follows().count(), 1, "seed {seed}: no follow opened");
        world.append(&a, b"driver#0".to_vec());
        for _ in 0..4 {
            step(&mut reader, &mut driver, &mut now);
        }
        assert_eq!(
            reader
                .cache()
                .record(&a, before)
                .map(|bytes| bytes.as_ref().to_vec()),
            Some(b"driver#0".to_vec()),
            "seed {seed}: follow did not deliver the appended record"
        );
    }
}

#[test]
fn source_change_hides_retained_data_before_it_is_served() {
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    let world = World::new(7);
    let path = StreamPath::new("agents/a").unwrap();
    let validity = Arc::new(Mutex::new(CredentialValidity {
        now_unix_seconds: 10,
        revision: Some(CredentialRevision::from([1; 32])),
    }));
    let observed = validity.clone();
    let refreshed = validity.clone();
    let source = CredentialCache::new(
        move |_| {
            let revision = refreshed.lock().unwrap().revision.clone().unwrap();
            async move { CredentialSnapshot::new("credential".into(), revision, 20) }
        },
        move || observed.lock().unwrap().clone(),
    );
    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    reader.credentials = Some(Credentials::new(source.clone().into(), 0, Some(10_000)));
    reader.reset_source();
    let ready = Arc::new(Ready(std::sync::atomic::AtomicBool::new(false)));
    let waker = Waker::from(ready.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(reader.advance_credentials(0, &mut cx));
    assert_eq!(reader.credential_status(), CredentialStatus::Current);
    assert!(reader.readable());

    let want = Want::Last {
        stream: path.clone(),
        count: 1,
    };
    let last = world.tail(&path) - 1;
    reader.begin_frame(1);
    reader.plan([(want, Priority::Visible)]);
    for op in reader.poll(1) {
        let outcome = futures::executor::block_on(driver::execute(&world.provider, op.clone()));
        reader.complete(op.id(), outcome, 1);
    }
    assert!(reader.cache().record(&path, last).is_some());
    let snapshot = reader.snapshot().unwrap();
    assert!(!snapshot.is_empty());

    // A source change hides every retained byte before the watch is driven.
    validity.lock().unwrap().revision = Some(CredentialRevision::from([2; 32]));
    source.invalidate().unwrap();
    assert!(!reader.readable());
    assert!(reader.cache().record(&path, last).is_none());
    assert!(reader.snapshot().unwrap().is_empty());
    assert!(reader.restore(&snapshot).is_err());
    assert!(ready.0.load(Ordering::SeqCst));
    assert!(reader.advance_credentials(2, &mut cx));
    assert_eq!(reader.credential_status(), CredentialStatus::Current);
    assert!(reader.readable());
    assert!(reader.cache().record(&path, last).is_none());
}

/// Drive one frame of the reader against the world's provider.
fn load(reader: &mut Reader, provider: &MemoryStream, want: Want, now: u64) {
    reader.begin_frame(now);
    reader.plan([(want, Priority::Visible)]);
    for op in reader.poll(now) {
        let outcome = futures::executor::block_on(driver::execute(provider, op.clone()));
        reader.complete(op.id(), outcome, now);
    }
}

#[test]
fn live_projection_follows_the_original_source_and_retires_on_replacement() {
    use acyclic_stream::trace::{
        Mechanism, Outcome as TraceOutcome, SpanEvent, SpanId, TraceRecord,
    };

    let mut world = World::new(11);
    let path = StreamPath::new("agents/a/trace").unwrap();
    let id = SpanId {
        process: [1; 16],
        sequence: 0,
    };
    let start = TraceRecord::V1(SpanEvent::Started {
        id,
        parent: None,
        name: "wait".into(),
        mechanism: Mechanism::Wait,
        at_ns: 10,
        guarantee: None,
    });
    world.append(&path, serde_json::to_vec(&start).unwrap());

    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    let mut file = trace::File::new(&mut reader, path.clone(), 0, BTreeMap::new()).unwrap();
    let page = NonZeroU32::new(1).unwrap();
    load(&mut reader, &world.provider, file.want(page).unwrap(), 1);
    assert_eq!(file.advance(&reader, page).unwrap(), 1);
    let value = file.value(&reader).unwrap();
    assert!(value.caught_up);
    assert_eq!(
        (
            value.active_spans,
            value.active_waits,
            value.missing_guarantees
        ),
        (1, 1, 1)
    );

    // A new original record arrives on the source's follow, and the same
    // projection consumes it. Growth past the known tail is the follow's: a
    // range read never extends it.
    reader.set_follows([path.clone()]);
    let mut follow = acyclic_stream::records_only(
        futures::executor::block_on(world.provider.follow(FollowRequest::new(path.clone(), 1)))
            .unwrap(),
    );
    let finish = TraceRecord::V1(SpanEvent::Finished {
        id,
        at_ns: 30,
        bytes: None,
        result: TraceOutcome::Unsettled,
    });
    world.append(&path, serde_json::to_vec(&finish).unwrap());
    let record = futures::executor::block_on(follow.next()).unwrap().unwrap();
    reader.deliver(&path, driver::fetched(record), world.tail(&path));
    assert_eq!(file.advance(&reader, page).unwrap(), 1);
    let value = file.value(&reader).unwrap();
    assert_eq!(value.active_waits, 0);
    assert_eq!(value.mechanisms[&Mechanism::Wait].elapsed_ns, 20);

    // A replacement retires the projection's custody.
    reader.replaced(&path);
    assert!(matches!(
        file.value(&reader),
        Err(live::Error::ContinuityLost)
    ));
}

#[test]
fn live_source_borrows_only_the_requested_original_window() {
    let world = World::new(13);
    let path = StreamPath::new("agents/a").unwrap();
    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    let source = live::Source::new(path.clone());
    assert!(matches!(source.latest(&reader), Err(live::Error::Pending)));

    load(&mut reader, &world.provider, source.want(), 1);
    let tail = world.tail(&path);
    assert_eq!(
        source.latest(&reader).unwrap().as_ref(),
        world.bytes(&path, tail - 1).unwrap()
    );

    // Only the observed head is loaded, so the earlier gap stays Pending.
    let page = NonZeroU32::new(2).unwrap();
    {
        let mut window = source.page(&reader, tail - 2, page).unwrap();
        assert_eq!(window.end, tail);
        assert!(matches!(
            window.next().unwrap().1,
            Err(live::Error::Pending)
        ));
        assert!(window.next().unwrap().1.is_ok());
    }

    load(
        &mut reader,
        &world.provider,
        source.want_page(0, page).unwrap(),
        2,
    );
    let mut window = source.page(&reader, 0, page).unwrap();
    assert!(window.all(|(_, row)| row.is_ok()));
    assert!(matches!(
        source.page(&reader, tail + 1, page),
        Err(live::Error::Range)
    ));
}
