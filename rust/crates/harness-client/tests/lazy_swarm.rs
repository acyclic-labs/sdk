//! A 10^5-agent swarm explored through the one reader under a virtual clock:
//! 300 ms round trips, packet loss and an offline phase. Every bound and
//! latency is measured on the real `Explorer`/`Reader`; each has a negative
//! control that the same oracle rejects.
#![cfg(feature = "agents")]

use acyclic_harness_client::explorer::{Explorer, Row};
use acyclic_harness_client::reader::{
    Connectivity, Failure, Fetched, ListedChild, Op, Outcome, Priority, Reader, ReaderPolicy,
    Residency, Want,
};
use acyclic_harness_core::agent::{
    AGENTS, AgentId, AgentPath, AgentRecord, ForkAdopted, InboxRecord, Phase, StatusRecord,
};
use acyclic_stream::{CommitId, ForkOrigin, PathHead, Record, StreamPath};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// The fixture's tree never changes shape after creation.
const HIERARCHY: CommitId = CommitId::from_bytes([7; 32]);

const ROOTS: u64 = 100;
const CHILDREN: u64 = 999;
const AGENTS_TOTAL: u64 = ROOTS * (CHILDREN + 1);
const RTT: u64 = 300;
const FRAME: u64 = 16;
const HEIGHT: usize = 32;
const ROOT_HISTORY: u64 = 5_000;
const INHERITED: u64 = 4_000;
const OWN_HISTORY: u64 = 20;
const FORKED_AT: u64 = 7;

fn commit(stream: &StreamPath, sequence: u64) -> CommitId {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&sequence.to_be_bytes());
    let name = stream.as_str().as_bytes();
    let tail = &name[name.len().saturating_sub(24)..];
    bytes[8..8 + tail.len()].copy_from_slice(tail);
    CommitId::from_bytes(bytes)
}

fn root_id(root: u64) -> AgentId {
    AgentId::try_from(format!("{:032x}", root + 1)).unwrap()
}

fn child_id(root: u64, child: u64) -> AgentId {
    AgentId::try_from(format!("{:032x}", ((root + 1) << 32) | (child + 1))).unwrap()
}

/// Agent number from its id: roots are `< 2^32`, children carry their root.
fn decode_id(id: &str) -> Option<(u64, Option<u64>)> {
    let value = u128::from_str_radix(id, 16).ok()?;
    let root = (value >> 32) as u64;
    let low = (value & 0xffff_ffff) as u64;
    if root == 0 {
        Some((low - 1, None))
    } else {
        Some((root - 1, Some(low - 1)))
    }
}

fn agent(root: u64, child: Option<u64>) -> AgentPath {
    let parent = AgentPath::root(&root_id(root));
    match child {
        None => parent,
        Some(child) => parent.child(&child_id(root, child)).unwrap(),
    }
}

/// Committed history of the fixture, computed rather than stored.
struct Swarm {
    /// Agents whose status advances once a second.
    hot_every: u64,
}

enum Kind {
    Status(u64, Option<u64>),
    Conversation(u64, Option<u64>),
    Inbox(u64, Option<u64>),
}

fn kind(stream: &StreamPath) -> Option<Kind> {
    let text = stream.as_str();
    if let Some(id) = text.strip_prefix("harness/status/") {
        let (root, child) = decode_id(id)?;
        return Some(Kind::Status(root, child));
    }
    let (base, conversation) = match text.strip_suffix("/conversation") {
        Some(base) => (base, true),
        None => (text, false),
    };
    let path = AgentPath::try_from(base.to_owned()).ok()?;
    let (root, child) = decode_id(path.id().as_str())?;
    Some(if conversation {
        Kind::Conversation(root, child)
    } else {
        Kind::Inbox(root, child)
    })
}

impl Swarm {
    /// Primitive path origin, with a distinct cut for inbox and conversation.
    fn origin(&self, stream: &StreamPath) -> Option<ForkOrigin> {
        let (root, child, conversation) = match kind(stream)? {
            Kind::Conversation(root, Some(child)) => (root, child, true),
            Kind::Inbox(root, Some(child)) => (root, child, false),
            _ => return None,
        };
        Some(ForkOrigin {
            source: if conversation {
                agent(root, None).conversation().unwrap()
            } else {
                agent(root, None).inbox().unwrap()
            },
            forked_at: if conversation { INHERITED } else { FORKED_AT },
            commit_id: commit(&agent(root, Some(child)).inbox().unwrap(), FORKED_AT),
        })
    }

    fn hot(&self, root: u64, child: Option<u64>) -> bool {
        (root * 1_000 + child.map_or(0, |c| c + 1)) % self.hot_every == 0
    }

    fn tail(&self, stream: &StreamPath, now: u64) -> u64 {
        match kind(stream) {
            Some(Kind::Status(root, child)) => {
                2 + if self.hot(root, child) {
                    now / 1_000
                } else {
                    0
                }
            }
            Some(Kind::Conversation(_, None)) => ROOT_HISTORY,
            Some(Kind::Conversation(_, Some(_))) => INHERITED + OWN_HISTORY,
            Some(Kind::Inbox(..)) => FORKED_AT + 2,
            None => 0,
        }
    }

    fn value(&self, stream: &StreamPath, sequence: u64) -> Bytes {
        match kind(stream) {
            Some(Kind::Status(..)) => Bytes::from(
                StatusRecord {
                    phase: if sequence % 2 == 1 {
                        Phase::Running
                    } else {
                        Phase::Idle
                    },
                    cursor: if sequence == 0 {
                        FORKED_AT
                    } else {
                        FORKED_AT + sequence
                    },
                }
                .encode()
                .unwrap(),
            ),
            // A child's conversation prefix is its parent's records.
            Some(Kind::Conversation(root, Some(_))) if sequence < INHERITED => {
                self.value(&agent(root, None).conversation().unwrap(), sequence)
            }
            Some(Kind::Inbox(root, Some(_))) if sequence < FORKED_AT => {
                self.value(&agent(root, None).inbox().unwrap(), sequence)
            }
            Some(Kind::Inbox(root, Some(child))) if sequence == FORKED_AT => Bytes::from(
                InboxRecord::ForkAdopted(Box::new(ForkAdopted {
                    agent: child_id(root, child),
                    parent: Some(agent(root, None)),
                    forked_at: FORKED_AT,
                    conversation_tail: INHERITED,
                    volumes: Vec::new(),
                    brief: None,
                    policy: None,
                }))
                .encode()
                .unwrap(),
            ),
            _ => Bytes::from(format!(
                "{:<120}",
                format!("{}#{sequence}", stream.as_str())
            )),
        }
    }

    fn children(
        &self,
        parent: &StreamPath,
        after: Option<&StreamPath>,
        limit: u32,
    ) -> (Vec<AgentPath>, bool) {
        let (count, make): (u64, Box<dyn Fn(u64) -> AgentPath>) = if parent.as_str() == AGENTS {
            (ROOTS, Box::new(|index| agent(index, None)))
        } else {
            match parent
                .as_str()
                .strip_suffix("/children")
                .and_then(|owner| AgentPath::try_from(owner.to_owned()).ok())
            {
                Some(owner) if owner.depth() == 0 => {
                    let (root, _) = decode_id(owner.id().as_str()).unwrap();
                    (CHILDREN, Box::new(move |index| agent(root, Some(index))))
                }
                _ => (0, Box::new(|_| unreachable!())),
            }
        };
        let start = after.map_or(0, |after| {
            let path = AgentPath::try_from(after.as_str().to_owned()).unwrap();
            let (root, child) = decode_id(path.id().as_str()).unwrap();
            child.unwrap_or(root) + 1
        });
        let end = (start + u64::from(limit)).min(count);
        ((start..end).map(make).collect(), end < count)
    }

    /// The record at `sequence` with a commit id unique to its position.
    fn fetch(&self, stream: &StreamPath, sequence: u64) -> Fetched {
        let inherited = self
            .origin(stream)
            .filter(|origin| sequence < origin.forked_at);
        Fetched {
            sequence,
            value: self.value(stream, sequence),
            commit: commit(
                inherited.as_ref().map_or(stream, |origin| &origin.source),
                sequence,
            ),
        }
    }

    fn answer(&self, op: &Op, now: u64) -> Outcome {
        match op {
            Op::Read {
                stream,
                from,
                limit,
                ..
            } => {
                let tail = self.tail(stream, now);
                Outcome::Records(
                    (*from..tail.min(from + u64::from(*limit)))
                        .map(|sequence| self.fetch(stream, sequence))
                        .collect(),
                )
            }
            Op::Head { stream, .. } => {
                let tail = self.tail(stream, now);
                Outcome::Head(PathHead {
                    tail,
                    durable: 0,
                    origin: self.origin(stream),
                    last: tail.checked_sub(1).map(|sequence| {
                        let fetched = self.fetch(stream, sequence);
                        Record {
                            sequence,
                            value: fetched.value.into(),
                            commit_id: fetched.commit,
                            committed_at_micros: 0,
                            headers: Default::default(),
                        }
                    }),
                })
            }
            Op::Children {
                parent,
                after,
                limit,
                tail,
                version,
                ..
            } => {
                // As every provider: a cursor is only valid at a version.
                assert!(
                    after.is_none() || *version == Some(HIERARCHY),
                    "continuation page without the hierarchy version"
                );
                let (children, more) = self.children(parent, after.as_ref(), *limit);
                Outcome::Children {
                    next: children
                        .last()
                        .filter(|_| more)
                        .map(|last| StreamPath::new(last.as_str()).unwrap()),
                    version: HIERARCHY,
                    children: children
                        .into_iter()
                        .map(|child| {
                            let path = StreamPath::new(child.as_str()).unwrap();
                            let tail = tail.as_ref().and_then(|relative| {
                                let stream =
                                    StreamPath::new(format!("{child}/{}", relative.as_str()))
                                        .unwrap();
                                let last = self.tail(&stream, now).checked_sub(1)?;
                                Some(self.fetch(&stream, last))
                            });
                            let origin = self.origin(&path);
                            ListedChild { path, origin, tail }
                        })
                        .collect(),
                }
            }
        }
    }
}

/// The network between one client and the server.
struct Net {
    swarm: Swarm,
    latency: u64,
    /// Every n-th request is lost (never answered).
    drop_every: Option<u64>,
    offline: Option<(u64, u64)>,
    pending: Vec<(u64, u64, Outcome)>,
    sent: u64,
    /// Reads per stream, for reuse assertions.
    reads: BTreeMap<String, u64>,
    /// Reads answered successfully, and reads that asked for one again.
    answered: BTreeSet<(String, u64)>,
    inflight: BTreeMap<u64, (String, u64)>,
    refetched: u64,
    sent_offline: u64,
}

impl Net {
    fn new(latency: u64) -> Self {
        Self {
            swarm: Swarm { hot_every: 7 },
            latency,
            drop_every: None,
            offline: None,
            pending: Vec::new(),
            sent: 0,
            reads: BTreeMap::new(),
            answered: BTreeSet::new(),
            inflight: BTreeMap::new(),
            refetched: 0,
            sent_offline: 0,
        }
    }

    fn is_offline(&self, now: u64) -> bool {
        self.offline
            .is_some_and(|(from, to)| (from..to).contains(&now))
    }

    fn issue(&mut self, op: Op, now: u64) {
        self.sent += 1;
        if let Op::Read { stream, from, .. } = &op {
            *self.reads.entry(stream.as_str().to_owned()).or_default() += 1;
            let key = (stream.as_str().to_owned(), *from);
            if self.answered.contains(&key) {
                self.refetched += 1;
            }
            self.inflight.insert(op.id(), key);
        }
        if self.is_offline(now) {
            self.sent_offline += 1;
            self.pending
                .push((now + 1, op.id(), Outcome::Failed(Failure::Unavailable)));
            return;
        }
        if self.drop_every.is_some_and(|every| self.sent % every == 0) {
            return;
        }
        let outcome = self.swarm.answer(&op, now);
        self.pending.push((now + self.latency, op.id(), outcome));
    }

    fn cancel(&mut self, ids: &[u64]) {
        self.pending.retain(|(_, id, _)| !ids.contains(id));
    }

    fn due(&mut self, now: u64) -> Vec<(u64, Outcome)> {
        let (due, rest) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|(at, ..)| *at <= now);
        self.pending = rest;
        due.into_iter()
            .map(|(_, id, outcome)| {
                if let Some(key) = self.inflight.remove(&id)
                    && matches!(outcome, Outcome::Records(_))
                {
                    self.answered.insert(key);
                }
                (id, outcome)
            })
            .collect()
    }
}

/// Bounds observed over a run.
#[derive(Default, Debug)]
struct Peak {
    cache_bytes: usize,
    entries: usize,
    requests: usize,
    follows: usize,
    demand: usize,
    timers: usize,
}

impl Peak {
    fn observe(&mut self, residency: Residency, timers: usize) {
        self.cache_bytes = self.cache_bytes.max(residency.cache_bytes);
        self.entries = self.entries.max(residency.cache_entries);
        self.requests = self.requests.max(residency.requests);
        self.follows = self.follows.max(residency.follows);
        self.demand = self.demand.max(residency.demand);
        self.timers = self.timers.max(timers);
    }
}

/// The oracle: footprint independent of the swarm size. Follows: one
/// status per visible row, at most one witness per displayed page (no more
/// pages than rows) and the open history.
fn bounded(peak: &Peak, policy: &ReaderPolicy) -> bool {
    peak.cache_bytes <= policy.cache_bytes
        && peak.follows <= 2 * HEIGHT + 1
        && peak.follows <= policy.follows
        && peak.requests <= policy.requests
        && peak.timers <= 1
}

struct Client {
    explorer: Explorer,
    net: Net,
    now: u64,
    peak: Peak,
}

impl Client {
    fn new(policy: ReaderPolicy, latency: u64) -> Self {
        let mut explorer = Explorer::new(policy).unwrap();
        explorer.set_viewport(HEIGHT, 16, FRAME);
        Self {
            explorer,
            net: Net::new(latency),
            now: 0,
            peak: Peak::default(),
        }
    }

    /// One frame: fold due completions and follow deliveries, lay out, issue.
    fn frame(&mut self) {
        let now = self.now;
        for (id, outcome) in self.net.due(now) {
            self.explorer.reader().complete(id, outcome, now);
        }
        if !self.net.is_offline(now) {
            let follows: Vec<(StreamPath, u64)> = self
                .explorer
                .reader_ref()
                .follows()
                .map(|(path, from)| (path.clone(), from))
                .collect();
            for (stream, from) in follows {
                let tail = self
                    .net
                    .swarm
                    .tail(&stream, now.saturating_sub(self.net.latency / 2));
                for sequence in from..tail {
                    let reader = self.explorer.reader();
                    if !reader.accepting() {
                        break;
                    }
                    reader.deliver(&stream, self.net.swarm.fetch(&stream, sequence), tail);
                }
            }
        }
        self.explorer.frame(now);
        let reader = self.explorer.reader();
        let cancelled = reader.take_cancelled();
        self.net.cancel(&cancelled);
        for op in reader.poll(now) {
            self.net.issue(op, now);
        }
        let timers = usize::from(self.explorer.reader_ref().next_deadline().is_some());
        self.peak
            .observe(self.explorer.reader_ref().residency(), timers);
        self.now += FRAME;
    }

    fn run_until(&mut self, limit: u64, done: impl Fn(&Self) -> bool) -> Option<u64> {
        let start = self.now;
        while self.now - start <= limit {
            self.frame();
            if done(self) {
                return Some(self.now - FRAME - start);
            }
        }
        None
    }

    fn rows(&self) -> &[(acyclic_harness_client::explorer::Pos, Row)] {
        self.explorer.rows()
    }
}

/// Every visible row shows committed content: its path and latest record.
fn meaningful(client: &Client) -> bool {
    client.rows().len() == HEIGHT
        && client.rows().iter().all(|(_, row)| {
            matches!(
                row,
                Row::Agent {
                    conversation: Some(_),
                    ..
                }
            )
        })
}

fn statuses(client: &Client) -> bool {
    meaningful(client)
        && client.rows().iter().all(|(_, row)| {
            matches!(
                row,
                Row::Agent {
                    status: Some(_),
                    ..
                }
            )
        })
}

fn row_agent(client: &Client, index: usize) -> AgentPath {
    match &client.rows()[index].1 {
        Row::Agent { path, .. } => path.clone(),
        Row::Loading { .. } => panic!("row {index} is loading"),
    }
}

fn policy() -> ReaderPolicy {
    ReaderPolicy {
        timeout_ms: 1_500,
        ..ReaderPolicy::default()
    }
}

#[test]
fn first_render_cold_warm_and_offline() {
    // Cold: one listing round trip shows every row; statuses follow per row.
    let mut cold = Client::new(policy(), RTT);
    let first = cold.run_until(5_000, meaningful).expect("cold render");
    let full = cold
        .run_until(5_000, statuses)
        .map(|t| t + first)
        .expect("statuses");
    eprintln!("cold TTFMR {first} ms, statuses {full} ms");
    assert!(
        (RTT..=RTT + 2 * FRAME).contains(&first),
        "cold TTFMR {first} ms"
    );
    assert!(full <= 3 * RTT + 4 * FRAME, "cold status {full} ms");
    cold.run_until(2_000, |_| false);
    let snapshot = cold.explorer.reader_ref().snapshot();

    // Warm: the persisted cache renders the first frame; revalidation follows.
    let mut warm = Client::new(policy(), RTT);
    warm.explorer.reader().restore(&snapshot).unwrap();
    assert_eq!(warm.run_until(0, statuses), Some(0), "warm TTFMR");
    assert!(warm.net.sent > 0, "displayed restored pages revalidate");
    // Negative control: without the persisted cache the first frame is skeletons.
    let mut unrestored = Client::new(policy(), RTT);
    assert_eq!(unrestored.run_until(0, meaningful), None);

    // Offline: the cached view stays complete and says it is stale; probes
    // back off instead of flooding; reconnecting refetches nothing displayed.
    warm.run_until(1_000, |_| false);
    let outage = (warm.now, warm.now + 5_000);
    warm.net.offline = Some(outage);
    warm.explorer.reader().network_changed(false, warm.now);
    // Scrolling into uncached rows while offline asks for reads it cannot get.
    warm.explorer.scroll(HEIGHT as i64 * 2);
    warm.run_until(2_500, |_| false);
    warm.explorer.scroll(-(HEIGHT as i64) * 2);
    warm.run_until(2_400, |_| false);
    assert!(matches!(
        warm.explorer.reader_ref().connectivity(),
        Connectivity::Offline { .. }
    ));
    assert!(statuses(&warm), "offline view keeps every cached row");
    let probes = warm.net.sent_offline;
    eprintln!("offline probes {probes}");
    assert!(probes <= 8, "offline probes {probes}");
    warm.run_until(outage.1 - warm.now, |_| false);
    warm.explorer.reader().network_changed(true, warm.now);
    warm.run_until(1_000, |_| false);
    assert_eq!(
        warm.explorer.reader_ref().connectivity(),
        Connectivity::Online
    );
    // Nothing answered before is ever read again: reconnect resumes, the
    // cache only extends.
    assert_eq!(warm.net.refetched, 0, "refetched reads");
    assert!(bounded(&warm.peak, &policy()), "{:?}", warm.peak);

    // Negative control: retrying without backoff floods the offline server.
    let naive_policy = ReaderPolicy {
        backoff_min_ms: 1,
        backoff_max_ms: 1,
        offline_after: u32::MAX,
        ..policy()
    };
    let mut naive = Client::new(naive_policy, RTT);
    naive.run_until(2_000, statuses).unwrap();
    naive.net.offline = Some((naive.now, naive.now + 5_000));
    naive.explorer.scroll(HEIGHT as i64 * 4);
    naive.run_until(5_000, |_| false);
    assert!(
        naive.net.sent_offline > 20 * probes.max(1),
        "naive {} vs backoff {probes}",
        naive.net.sent_offline
    );
}

/// Hover for a typical dwell, then expand and open: report p99 latency to
/// content and how often content was already there.
fn intents(prefetch: bool) -> (u64, f64, Client) {
    let mut client = Client::new(
        ReaderPolicy {
            prefetch,
            ..policy()
        },
        RTT,
    );
    client.run_until(5_000, statuses).unwrap();
    let mut latencies = Vec::new();
    let mut hits = 0;
    for _ in 0..50 {
        // A root never acted on before: the top row, one further each round.
        let target = row_agent(&client, 0);
        client.explorer.hover(Some(target.clone()));
        client.run_until(400, |_| false);
        // Expand: the first child row shows content.
        client.explorer.expand(&target);
        let shown = |client: &Client| {
            client.rows().iter().any(|(_, row)| {
                matches!(row, Row::Agent { path, conversation: Some(_), .. }
                    if path.parent().as_ref() == Some(&target))
            })
        };
        let latency = client
            .run_until(3_000, shown)
            .expect("expand shows children");
        hits += usize::from(latency == 0);
        latencies.push(latency);
        client.explorer.collapse(&target);
        // Open: the history window shows records.
        client.explorer.open(target.clone(), 40);
        let full = |client: &Client| {
            let rows = client.explorer.history();
            rows.len() == 40 && rows.iter().all(|row| row.value.is_some())
        };
        let latency = client.run_until(3_000, full).expect("open shows history");
        hits += usize::from(latency == 0);
        latencies.push(latency);
        client.explorer.close();
        client.explorer.hover(None);
        client.explorer.scroll(1);
        client.run_until(200, |_| false);
    }
    latencies.sort_unstable();
    eprintln!("prefetch={prefetch} latencies {latencies:?}");
    let p99 = latencies[(latencies.len() * 99).div_ceil(100) - 1];
    (p99, hits as f64 / latencies.len() as f64, client)
}

#[test]
fn intent_prefetch_hides_latency() {
    let (p99, hit, client) = intents(true);
    eprintln!("prefetch p99 {p99} ms, hit ratio {hit}");
    // Faster than any uncached read could be.
    assert!(p99 < RTT, "p99 expand/open {p99} ms");
    assert!(hit >= 0.9, "prefetch hit ratio {hit}");
    assert!(bounded(&client.peak, &policy()), "{:?}", client.peak);
    // Negative control: without likely-next reads every action waits a trip.
    let (p99, hit, _) = intents(false);
    eprintln!("no-prefetch p99 {p99} ms, hit ratio {hit}");
    assert!(p99 >= RTT, "no-prefetch p99 {p99} ms");
    assert!(hit < 0.1, "no-prefetch hit ratio {hit}");
}

#[test]
fn swarm_scroll_and_history_stay_bounded() {
    let mut client = Client::new(policy(), RTT);
    client.net.drop_every = Some(25);
    client.run_until(5_000, meaningful).unwrap();
    // Expand every root as it scrolls by and fling through the tree.
    let mut expanded = 0;
    let mut deepest = 0;
    for step in 0..900 {
        let roots: Vec<AgentPath> = client
            .rows()
            .iter()
            .filter_map(|(_, row)| match row {
                Row::Agent {
                    path,
                    expanded: false,
                    ..
                } if path.depth() == 0 => Some(path.clone()),
                _ => None,
            })
            .collect();
        for root in roots {
            client.explorer.expand(&root);
            expanded += 1;
        }
        if step == 400 {
            client.net.offline = Some((client.now, client.now + 2_000));
        }
        client
            .explorer
            .scroll(if step % 50 < 45 { 96 } else { -32 });
        client.frame();
        deepest = deepest.max(
            client
                .rows()
                .iter()
                .filter_map(|(_, row)| match row {
                    Row::Agent { path, .. } => decode_id(path.id().as_str()).map(|(root, _)| root),
                    Row::Loading { .. } => None,
                })
                .max()
                .unwrap_or(0),
        );
    }
    assert!(expanded >= 50, "expanded {expanded} roots");
    assert!(deepest >= 50, "scrolled to root {deepest}");
    assert!(client.explorer.expanded() <= 256);
    assert!(bounded(&client.peak, &policy()), "{:?}", client.peak);

    // Huge history: jump to the end and seek anywhere with one read each.
    let root = agent(3, None);
    client.explorer.open(root.clone(), 40);
    let full = |client: &Client| {
        let rows = client.explorer.history();
        rows.len() == 40 && rows.iter().all(|row| row.value.is_some())
    };
    client.net.drop_every = None;
    // After the outage the client resumes within its backoff ceiling.
    let recovered = client
        .run_until(policy().backoff_max_ms, |client| {
            client.explorer.reader_ref().connectivity() == Connectivity::Online
        })
        .expect("recovers after the outage");
    assert!(
        recovered <= policy().backoff_max_ms * 5 / 4 + RTT,
        "recovered {recovered} ms after the outage"
    );
    client.run_until(3_000, full).unwrap();
    assert_eq!(client.net.refetched, 0, "refetched reads while scrolling");
    let history = root.conversation().unwrap();
    for seek in [0, 2_500, 3_960] {
        let before = client.net.reads.get(history.as_str()).copied().unwrap_or(0);
        client.explorer.seek(seek);
        let latency = client.run_until(3_000, full).unwrap();
        let reads = client.net.reads[history.as_str()] - before;
        assert!(latency <= RTT + FRAME, "seek {seek}: {latency} ms");
        // The window and the page before it: O(1) in the history length.
        assert!(reads <= 2, "seek {seek}: {reads} reads");
    }
    // Fork = cache hit: a child's inherited prefix is served from the
    // parent's cached records once the primitive origin is read.
    let child = agent(3, Some(5));
    client.explorer.open(child.clone(), 40);
    client.run_until(3_000, full).unwrap();
    client.run_until(2 * RTT + 2 * FRAME, |_| false);
    let conversation = child.conversation().unwrap();
    let before = client
        .net
        .reads
        .get(conversation.as_str())
        .copied()
        .unwrap_or(0);
    client.explorer.seek(3_960);
    assert_eq!(
        client.run_until(0, full),
        Some(0),
        "inherited prefix is a cache hit"
    );
    assert_eq!(
        client
            .net
            .reads
            .get(conversation.as_str())
            .copied()
            .unwrap_or(0),
        before,
        "no child read for the inherited prefix"
    );
    assert!(bounded(&client.peak, &policy()), "{:?}", client.peak);
    eprintln!("recovered {recovered} ms after the outage");
    eprintln!(
        "10^5 agents: peak cache {} B / {} entries, {} requests, {} follows, {} timers",
        client.peak.cache_bytes,
        client.peak.entries,
        client.peak.requests,
        client.peak.follows,
        client.peak.timers
    );
}

/// The open history is followed before tree rows spend the follow budget,
/// so its new records stay live. Control: following it after every visible
/// status left it outside a full budget.
#[test]
fn the_open_history_keeps_its_follow_under_a_full_follow_budget() {
    let policy = ReaderPolicy {
        follows: 2,
        ..policy()
    };
    let mut client = Client::new(policy, RTT);
    client.run_until(5_000, statuses).expect("render");
    let root = agent(1, None);
    client.explorer.open(root.clone(), 40);
    client.frame();
    let history = root.conversation().unwrap();
    let follows: Vec<StreamPath> = client
        .explorer
        .reader_ref()
        .follows()
        .map(|(path, _)| path.clone())
        .collect();
    assert_eq!(follows.len(), 2, "{follows:?}");
    assert!(follows.contains(&history), "{follows:?}");
}

/// The eager variant — every listing page read, every status followed and
/// retained — fails the same oracle at 10^5 agents.
#[test]
fn eager_variant_fails_the_oracle() {
    let swarm = Swarm { hot_every: 7 };
    let mut bytes = 0usize;
    let mut subscriptions = 0usize;
    let mut stack = vec![StreamPath::new(AGENTS).unwrap()];
    while let Some(parent) = stack.pop() {
        let mut after = None;
        loop {
            let (children, more) = swarm.children(&parent, after.as_ref(), 1_024);
            for child in &children {
                let status = child.status().unwrap();
                bytes += swarm.value(&status, 1).len() + status.as_str().len() + 96;
                subscriptions += 1;
                stack.push(child.children().unwrap());
            }
            after = children
                .last()
                .map(|last| StreamPath::new(last.as_str()).unwrap());
            if !more {
                break;
            }
        }
    }
    assert_eq!(subscriptions as u64, AGENTS_TOTAL);
    let peak = Peak {
        cache_bytes: bytes,
        follows: subscriptions,
        ..Peak::default()
    };
    assert!(
        !bounded(&peak, &policy()),
        "eager {bytes} B, {subscriptions} follows"
    );
}

/// Cached records above the durable watermark are provisional: a resumed
/// follow is anchored on the last cached commit, and a replaced suffix is
/// dropped rather than joined. Control: an unanchored resume has no check.
#[test]
fn provisional_suffix_is_anchored_and_dropped_on_replacement() {
    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    let stream = StreamPath::new("harness/agents/x/conversation").unwrap();
    let record = |sequence| Fetched {
        sequence,
        value: Bytes::from_static(b"r"),
        commit: commit(&stream, sequence),
    };
    reader.set_follows([stream.clone()]);
    assert_eq!(
        reader.follows().count(),
        0,
        "no tail known: nothing to follow"
    );
    reader.cache_mut().observe_tail(&stream, 0);
    reader.set_follows([stream.clone()]);
    for sequence in 0..3 {
        reader.deliver(&stream, record(sequence), sequence + 1);
    }
    reader.set_follows([stream.clone()]);
    let (_, from) = reader.follows().next().unwrap();
    assert_eq!(from, 3);
    assert_eq!(
        reader.follow_anchor(&stream, from),
        Some(commit(&stream, 2))
    );
    // Control: a position the cache never anchored resumes unchecked.
    assert_eq!(reader.follow_anchor(&stream, 2), None);
    reader.replaced(&stream);
    assert!(reader.cache().record(&stream, 0).is_none());
    assert_eq!(reader.cache().tail(&stream), None);
    assert_eq!(reader.follows().count(), 0);
    // The anchor survives persistence.
    reader.cache_mut().observe_tail(&stream, 0);
    reader.set_follows([stream.clone()]);
    reader.deliver(&stream, record(0), 1);
    let mut restored = Reader::new(ReaderPolicy::default()).unwrap();
    restored.restore(&reader.snapshot()).unwrap();
    assert_eq!(restored.follow_anchor(&stream, 1), Some(commit(&stream, 0)));
}

#[test]
fn listings_follow_server_cursors_versions_and_witnesses() {
    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    let parent = StreamPath::new("harness/agents/a/children").unwrap();
    let witness = StreamPath::new("harness/status/a").unwrap();
    let child = |name: &str| StreamPath::new(format!("{}/{name}", parent.as_str())).unwrap();
    let want = |index, witness: Option<&StreamPath>| {
        (
            Want::Listing {
                parent: parent.clone(),
                index,
                tail: None,
                witness: witness.cloned(),
            },
            Priority::Visible,
        )
    };
    let version = |byte| CommitId::from_bytes([byte; 32]);
    let one = |reader: &mut Reader, wants: Vec<(Want, Priority)>| {
        reader.plan(wants);
        let ops = reader.poll(0);
        assert!(ops.len() <= 1, "{ops:?}");
        ops.into_iter().next().map(|op| match op {
            Op::Children {
                id,
                index,
                after,
                version,
                ..
            } => (id, index, after, version),
            other => panic!("unexpected {other:?}"),
        })
    };
    let page = |children: &[&str], next: Option<&str>, at| Outcome::Children {
        children: children
            .iter()
            .map(|name| ListedChild {
                path: child(name),
                origin: None,
                tail: None,
            })
            .collect(),
        next: next.map(child),
        version: version(at),
    };
    reader.cache_mut().observe_tail(&witness, 1);
    // An empty first page that continues: everything on it was unlistable.
    let (id, index, after, at) = one(&mut reader, vec![want(0, Some(&witness))]).unwrap();
    assert_eq!((index, after, at), (0, None, None));
    reader.complete(id, page(&[], Some("m"), 1), 0);
    // The continuation starts at the server's cursor, at the page's version.
    let (id, index, after, at) = one(&mut reader, vec![want(1, Some(&witness))]).unwrap();
    assert_eq!((index, after, at), (1, Some(child("m")), Some(version(1))));
    // A path was created meanwhile: re-learn the version from a first page.
    reader.complete(id, Outcome::Failed(Failure::Changed), 0);
    assert!(matches!(reader.connectivity(), Connectivity::Online));
    let (id, index, after, at) = one(&mut reader, vec![want(1, Some(&witness))]).unwrap();
    assert_eq!((index, after, at), (0, None, None));
    reader.complete(id, page(&[], Some("m"), 2), 0);
    let (id, index, after, at) = one(&mut reader, vec![want(1, Some(&witness))]).unwrap();
    assert_eq!((index, after, at), (1, Some(child("m")), Some(version(2))));
    reader.complete(id, page(&["n"], None, 2), 0);
    // Fresh while the witness holds still.
    assert_eq!(one(&mut reader, vec![want(1, Some(&witness))]), None);
    // The parent committed a step (a spawn writes its status): revalidate.
    reader.cache_mut().observe_tail(&witness, 2);
    let (id, index, after, at) = one(&mut reader, vec![want(1, Some(&witness))]).unwrap();
    assert_eq!((index, after, at), (1, Some(child("m")), Some(version(2))));
    // Control: without a witness the same page is never revalidated.
    reader.complete(id, page(&["n", "o"], None, 2), 0);
    reader.cache_mut().observe_tail(&witness, 3);
    assert_eq!(one(&mut reader, vec![want(1, None)]), None);
    assert_eq!(
        reader.cache().listing(&parent, 1).unwrap().children.len(),
        2
    );
}

#[test]
fn a_failed_follow_keeps_the_one_timer_armed() {
    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    let stream = StreamPath::new("harness/status/a").unwrap();
    reader.cache_mut().observe_tail(&stream, 1);
    reader.set_follows([stream.clone()]);
    reader.plan(Vec::new());
    assert!(reader.poll(10).is_empty());
    let before = reader.generation();
    reader.follow_failed(Failure::Unavailable, 10);
    assert!(reader.generation() > before, "failures wake the renderer");
    let retry = reader.next_deadline().expect("a reopen deadline");
    assert!(retry > 10);
    // Control: once the follow reopens nothing is waiting on the timer.
    reader.follow_opened(&stream, 1);
    assert_eq!(reader.next_deadline(), None);
}

#[test]
fn a_shifted_page_boundary_drops_later_pages_and_reads_stop_at_the_end() {
    let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
    let parent = StreamPath::new("harness/agents/a/children").unwrap();
    let witness = StreamPath::new("harness/status/a").unwrap();
    let child = |name: &str| StreamPath::new(format!("{}/{name}", parent.as_str())).unwrap();
    let want = |index| {
        vec![(
            Want::Listing {
                parent: parent.clone(),
                index,
                tail: None,
                witness: Some(witness.clone()),
            },
            Priority::Visible,
        )]
    };
    let page = |names: &[&str], next: Option<&str>| Outcome::Children {
        children: names
            .iter()
            .map(|name| ListedChild {
                path: child(name),
                origin: None,
                tail: None,
            })
            .collect(),
        next: next.map(child),
        version: HIERARCHY,
    };
    let issue = |reader: &mut Reader, wants| {
        reader.plan(wants);
        let ops = reader.poll(0);
        assert!(ops.len() <= 1, "{ops:?}");
        ops.into_iter().next()
    };
    reader.cache_mut().observe_tail(&witness, 1);
    let op = issue(&mut reader, want(0)).unwrap();
    reader.complete(op.id(), page(&["b", "c"], Some("c")), 0);
    let op = issue(&mut reader, want(1)).unwrap();
    reader.complete(op.id(), page(&["d", "e"], None), 0);
    // `a` is inserted: the first page now ends at `b`, so `c` moved to page 1.
    reader.cache_mut().observe_tail(&witness, 2);
    let op = issue(&mut reader, want(0)).unwrap();
    reader.complete(op.id(), page(&["a", "b"], Some("b")), 0);
    assert!(
        reader.cache().listing(&parent, 1).is_none(),
        "stale page 1 kept"
    );
    match issue(&mut reader, want(1)) {
        Some(Op::Children { index, after, .. }) => {
            assert_eq!((index, after), (1, Some(child("b"))))
        }
        other => panic!("unexpected {other:?}"),
    }

    // Reads stop at the stream's end instead of re-reading past it.
    let stream = StreamPath::new("harness/agents/a/conversation").unwrap();
    let range = |from, to| {
        vec![(
            Want::Range {
                stream: stream.clone(),
                from,
                to,
            },
            Priority::Visible,
        )]
    };
    let op = issue(&mut reader, range(0, 40)).unwrap();
    reader.complete(op.id(), Outcome::Records(Vec::new()), 0);
    assert_eq!(
        issue(&mut reader, range(0, 40)),
        None,
        "empty stream re-read"
    );
    reader.cache_mut().observe_tail(&stream, 5_000);
    let Some(Op::Read { id, from, .. }) = issue(&mut reader, range(4_990, 5_030)) else {
        panic!("expected a read");
    };
    let records = (from..5_000)
        .map(|sequence| Fetched {
            sequence,
            value: Bytes::from_static(b"r"),
            commit: commit(&stream, sequence),
        })
        .collect();
    reader.complete(id, Outcome::Records(records), 0);
    assert_eq!(
        issue(&mut reader, range(4_990, 5_030)),
        None,
        "read past the tail"
    );
}

#[test]
fn cached_records_do_not_end_a_stream() {
    let policy = ReaderPolicy::default();
    let mut reader = Reader::new(policy).unwrap();
    let stream = StreamPath::new("harness/agents/a/conversation").unwrap();
    let want = vec![(
        Want::Range {
            stream: stream.clone(),
            from: 0,
            to: u64::from(policy.read_limit) * 3,
        },
        Priority::Visible,
    )];
    let mut starts = Vec::new();
    for _ in 0..3 {
        reader.plan(want.clone());
        let Some(Op::Read {
            id, from, limit, ..
        }) = reader.poll(0).into_iter().next()
        else {
            panic!("expected a read");
        };
        starts.push(from);
        let records = (from..from + u64::from(limit))
            .map(|sequence| Fetched {
                sequence,
                value: Bytes::from_static(b"r"),
                commit: commit(&stream, sequence),
            })
            .collect();
        reader.complete(id, Outcome::Records(records), 0);
    }
    let step = u64::from(policy.read_limit);
    assert_eq!(starts, [0, step, 2 * step], "a full read is not the end");
    assert_eq!(reader.cache().tail(&stream), None, "no end was observed");
}
