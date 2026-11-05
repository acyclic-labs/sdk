//! Bounded exhaustive model check of the call protocol over a real
//! `MemoryStream`.
//!
//! The caller's request races with four other participants on the call:
//! the executor (accept, chunk, `ok`), a second executor (accept), a
//! canceller and the executor's session end (its fence).
//! Every interleaving is run through the real commit builders. After every
//! step the journal must fold, its settled marker must agree with it, and the
//! outcome a reader observes, once present, must never change. Cancellation
//! commits in its one step or finds the call already ended, and the call is
//! settled right after it (it never waits). Independent controls remove
//! settlement guards, session guards or the original retry identity, or
//! incorrectly fence cancellation behind the executor's ended session.
//!
//! A fork of the caller's subtree at every reachable prefix copies the call.
//! The copy settles exactly once: with its inherited terminal, or
//! `indeterminate` when it was open, and never changes as the source goes on.
//! The control removes the copy settlement's guards.
use super::*;
use acyclic_stream::{CommitOutcome, FollowRequest, FollowUpdate, MemoryStream, StreamProvider};
use futures::StreamExt as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Hop {
    Request,
    Executor,
    Rival,
    Cancel,
    Fence,
    Chunk,
    Replay,
    /// Fork the caller's whole subtree, actor-blind, as a branch does.
    Fork,
    /// The copy's owner observes its copied call.
    Copy,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    Settlement,
    Fence,
    Retry,
    Producer,
    Cancel,
    Copy,
}

/// The branch's copy of the call.
struct Copied {
    call: CallPath,
    /// The source's terminal, when the fork copied one.
    inherited: Option<Outcome>,
    /// The copy's settled outcome, once its owner observed it.
    expected: Option<Outcome>,
    journal: Journal,
    terminals: u64,
}

struct Model {
    provider: MemoryStream,
    call: CallPath,
    fence: StreamPath,
    fault: Fault,
    request: bool,
    requested: bool,
    executor: usize,
    stopped: bool,
    observed: Option<Outcome>,
    ended: bool,
    retry: Option<(CommitRequest, CommitOutcome)>,
    result: Outcome,
    journal: Journal,
    terminals: u64,
    accepted: bool,
    expected: Option<Outcome>,
    chunks: u64,
    read_chunks: u64,
    copy: Option<Copied>,
}

impl Model {
    fn new(fault: Fault, result: Outcome) -> Self {
        let root = StreamPath::new("caller").expect("root");
        Self {
            provider: MemoryStream::default(),
            call: CallPath::new(&root, "c1").expect("call"),
            fence: StreamPath::new("executor/sessions/1/ended").expect("fence"),
            fault,
            request: false,
            requested: true,
            executor: 0,
            stopped: false,
            observed: None,
            ended: false,
            retry: None,
            result,
            journal: Journal::default(),
            terminals: 0,
            accepted: false,
            expected: None,
            chunks: 0,
            read_chunks: 0,
            copy: None,
        }
    }

    fn guard(&self, mut commit: Commit) -> Commit {
        commit.conditions.retain(|condition| match condition {
            CommitCondition::Absent { path } => {
                !(self.fault == Fault::Settlement && path == self.call.settled()
                    || self.fault == Fault::Fence && path == &self.fence)
            }
            _ => true,
        });
        commit
    }

    async fn replay(&self) -> Result<(), String> {
        if let Some((request, expected)) = &self.retry {
            let mut request = request.clone();
            if self.fault == Fault::Retry {
                request.idempotency_key = IdempotencyKey::unique();
            }
            let actual = self
                .provider
                .commit(request)
                .await
                .map_err(|error| error.to_string())?;
            if &actual != expected {
                return Err("retry lost the original committed receipt".into());
            }
        }
        Ok(())
    }

    async fn publish(&mut self, commit: Commit) -> Result<bool, String> {
        let request = commit.request(IdempotencyKey::unique());
        let outcome = self
            .provider
            .commit(request.clone())
            .await
            .map_err(|error| error.to_string())?;
        let committed = matches!(outcome, CommitOutcome::Committed(_));
        // Replaying the previous successful write after this operation must
        // retain its receipt even if its original conditions no longer hold.
        self.replay().await?;
        if committed {
            self.retry = Some((request, outcome));
            self.replay().await?;
        }
        Ok(committed)
    }

    async fn step(&mut self, hop: Hop) -> Result<(), String> {
        let accepted = |executor: &'static [u8], fence: &StreamPath| Accepted {
            executor: OwnedBytes::from(executor),
            fence: Some(fence.clone()),
        };
        match hop {
            Hop::Request => {
                if self
                    .publish(
                        self.call
                            .request(OwnedBytes::from(&b"run"[..]))
                            .map_err(|error| error.to_string())?,
                    )
                    .await?
                {
                    self.request = true;
                }
            }
            Hop::Replay => self.replay().await?,
            Hop::Chunk if self.executor == 0 || self.stopped => {}
            Hop::Executor if self.stopped => {}
            Hop::Executor | Hop::Chunk => {
                let fence = Some(&self.fence);
                let stage = if hop == Hop::Chunk { 1 } else { self.executor };
                let commit = match stage {
                    0 => self
                        .call
                        .accept(&accepted(b"e1", &self.fence), self.requested),
                    1 => self.call.chunk(
                        fence,
                        OwnedBytes::from(self.chunks.to_be_bytes().as_slice()),
                    ),
                    _ => self.call.settle(fence, &self.result),
                }
                .map_err(|error| error.to_string())?;
                let commit = self.guard(commit);
                if hop == Hop::Executor {
                    self.executor += 1;
                }
                // A refused step stops the executor: it was cancelled,
                // fenced or never accepted.
                self.stopped = !self.publish(commit).await?;
                if !self.stopped {
                    match stage {
                        0 => self.accepted = true,
                        1 => self.chunks += 1,
                        _ => {
                            self.expected = Some(match &self.result {
                                Outcome::Cancelled(_) if self.accepted => {
                                    Outcome::Indeterminate("cancelled".into())
                                }
                                outcome => outcome.clone(),
                            })
                        }
                    }
                }
                if self.ended && !self.stopped {
                    return Err("ended executor committed to the call".into());
                }
            }
            Hop::Fork => {
                // A caller's root exists, as an agent's inbox does; the fork
                // takes it at its tail with the whole subtree beneath.
                let source = StreamPath::new("caller").map_err(|error| error.to_string())?;
                let root = StreamPath::new("branch/".repeat(40) + "leaf")
                    .map_err(|error| error.to_string())?;
                let at_tail = match self.provider.head(source.clone()).await {
                    Ok(head) => head.tail,
                    Err(StreamError::NotFound) => {
                        let root = Commit {
                            conditions: vec![CommitCondition::Absent {
                                path: source.clone(),
                            }],
                            mutations: vec![CommitMutation::Append {
                                path: source.clone(),
                                records: vec![b"root".to_vec().into()],
                            }],
                        };
                        self.provider
                            .commit(root.request(IdempotencyKey::unique()))
                            .await
                            .map_err(|error| error.to_string())?;
                        1
                    }
                    Err(error) => return Err(error.to_string()),
                };
                let fork = Commit {
                    conditions: vec![CommitCondition::Absent { path: root.clone() }],
                    mutations: vec![CommitMutation::Fork {
                        exclude: Vec::new(),
                        source,
                        destination: root.clone(),
                        at_tail,
                        records: Vec::new(),
                    }],
                };
                match self
                    .provider
                    .commit(fork.request(IdempotencyKey::unique()))
                    .await
                    .map_err(|error| error.to_string())?
                {
                    CommitOutcome::Committed(_) => {}
                    outcome => return Err(format!("fork refused: {outcome:?}")),
                }
                self.copy = Some(Copied {
                    call: CallPath::new(&root, "c1").map_err(|error| error.to_string())?,
                    inherited: (self.terminals > 0)
                        .then(|| self.journal.outcome().cloned())
                        .flatten(),
                    expected: None,
                    journal: Journal::default(),
                    terminals: 0,
                });
            }
            Hop::Copy => {
                let Some(copy) = &self.copy else {
                    return self.check(hop).await;
                };
                let (call, inherited) = (copy.call.clone(), copy.inherited.clone());
                let forked_at = match self.provider.head(call.log().clone()).await {
                    // The fork preceded the call's first record.
                    Err(StreamError::NotFound) => return self.check(hop).await,
                    Err(error) => return Err(error.to_string()),
                    Ok(head) => {
                        head.origin
                            .ok_or("a copied journal has no origin")?
                            .inherited
                            .forked_at
                    }
                };
                let committed = if self.fault == Fault::Copy {
                    let mut commit = call
                        .settle_copy(forked_at)
                        .map_err(|error| error.to_string())?;
                    commit.conditions.clear();
                    self.publish(commit).await?
                } else {
                    // The owner's one production observation of its copy.
                    call.settle_if_copied(&self.provider)
                        .await
                        .map_err(|error| error.to_string())?
                };
                if committed == inherited.is_some() {
                    return Err(format!(
                        "copy settlement committed={committed} with inherited {inherited:?}"
                    ));
                }
                if let Some(copy) = self.copy.as_mut() {
                    copy.expected =
                        Some(inherited.unwrap_or_else(|| Outcome::Indeterminate("copied".into())));
                }
            }
            Hop::Rival => {
                let commit = self
                    .call
                    .accept(&accepted(b"e2", &self.fence), self.requested)
                    .map_err(|error| error.to_string())?;
                if self.publish(self.guard(commit)).await? {
                    self.accepted = true;
                    if self.ended {
                        return Err("ended executor accepted the call".into());
                    }
                }
            }
            Hop::Cancel => {
                let mut commit = self
                    .call
                    .cancel("stop")
                    .map_err(|error| error.to_string())?;
                if self.fault == Fault::Cancel {
                    commit.conditions.push(CommitCondition::Absent {
                        path: self.fence.clone(),
                    });
                }
                let commit = self.guard(commit);
                if self.publish(commit).await? && self.expected.is_none() {
                    self.expected = Some(if self.accepted {
                        Outcome::Indeterminate("cancelled".into())
                    } else {
                        Outcome::Cancelled("stop".into())
                    });
                }
            }
            Hop::Fence => {
                self.publish(Commit {
                    conditions: Vec::new(),
                    mutations: vec![append(&self.fence, AppendRecord::from(&b"lost"[..]))],
                })
                .await?;
                self.ended = true;
                if self.accepted && self.expected.is_none() {
                    self.expected = Some(Outcome::Indeterminate("lost".into()));
                }
            }
        }
        self.check(hop).await
    }

    async fn tail(&self, path: &StreamPath) -> Result<u64, String> {
        match self.provider.head(path.clone()).await {
            Ok(head) => Ok(head.tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(error.to_string()),
        }
    }

    async fn records(&self, path: &StreamPath, from: u64) -> Result<Vec<Record>, String> {
        let tail = self.tail(path).await?;
        if tail == from {
            return Ok(Vec::new());
        }
        let follow = self
            .provider
            .follow(FollowRequest::new(path.clone(), from))
            .await
            .map_err(|error| error.to_string())?;
        let mut follow = if self.fault == Fault::Producer {
            // Close the original delivery source after its commit. Its
            // receiver must return a typed refusal rather than wait forever.
            futures::future::Either::Left(follow.take(0))
        } else {
            futures::future::Either::Right(follow)
        };
        let mut records = Vec::new();
        while from + (records.len() as u64) < tail {
            match follow.next().await.ok_or("follow ended")? {
                Ok(FollowUpdate::Record(record)) => records.push(record),
                Ok(_) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(records)
    }

    async fn check(&mut self, hop: Hop) -> Result<(), String> {
        for record in self.records(self.call.log(), self.journal.next()).await? {
            match self
                .journal
                .apply(&record)
                .map_err(|error| error.to_string())?
            {
                Entry::Terminal(_) => self.terminals += 1,
                Entry::Chunk(body) => {
                    if body.as_ref() != self.read_chunks.to_be_bytes() {
                        return Err("chunks lost their committed order".into());
                    }
                    self.read_chunks += 1;
                }
                _ => {}
            }
        }
        if self.chunks != self.read_chunks {
            return Err("a chunk was duplicated or lost".into());
        }
        let markers = self.tail(self.call.settled()).await?;
        if markers != self.terminals {
            return Err(format!(
                "{markers} settled markers for {} terminals",
                self.terminals
            ));
        }
        // Keep the raw record cursor across steps. A later explicit cancel
        // can materialize an already observed virtual fence terminal.
        let mut journal = self.journal.clone();
        if self.ended {
            journal.fenced("lost");
        }
        if let Some(receipt) = journal.receipt(self.call.log()) {
            // The receipt is the committed request plus the one terminal
            // record, or the fence that settled the accepted call.
            if receipt.request.is_some() != self.request
                || receipt.terminal.is_none() == receipt.fence.is_none()
            {
                return Err(format!("incomplete receipt {receipt:?}"));
            }
        }
        let outcome = journal.outcome().cloned();
        if match (&self.expected, &outcome) {
            (Some(expected), Some(actual)) => !same(expected, actual),
            (None, None) => false,
            _ => true,
        } {
            return Err(format!(
                "terminal differs from committed operation: {:?} != {outcome:?}",
                self.expected
            ));
        }
        match (&self.observed, &outcome) {
            (Some(before), Some(now)) if !same(before, now) => {
                return Err(format!("outcome changed from {before:?} to {now:?}"));
            }
            (Some(before), None) => return Err(format!("outcome {before:?} was withdrawn")),
            _ => self.observed = outcome,
        }
        if hop == Hop::Cancel && self.terminals != 1 {
            return Err("cancel did not retain exactly one terminal record".into());
        }
        self.check_copy().await
    }

    /// Once observed, the copy holds exactly one terminal, its settled
    /// outcome, whatever the source does after the fork.
    async fn check_copy(&mut self) -> Result<(), String> {
        let Some(mut copy) = self.copy.take() else {
            return Ok(());
        };
        let result = self.fold_copy(&mut copy).await;
        self.copy = Some(copy);
        result
    }

    async fn fold_copy(&self, copy: &mut Copied) -> Result<(), String> {
        let Some(expected) = copy.expected.clone() else {
            return Ok(());
        };
        for record in self.records(copy.call.log(), copy.journal.next()).await? {
            if let Entry::Terminal(_) = copy
                .journal
                .apply(&record)
                .map_err(|error| error.to_string())?
            {
                copy.terminals += 1;
            }
        }
        let markers = self.tail(copy.call.settled()).await?;
        if copy.terminals != 1 || markers != 1 {
            return Err(format!(
                "copy has {} terminals and {markers} settled markers",
                copy.terminals
            ));
        }
        match copy.journal.outcome() {
            Some(outcome) if same(&expected, outcome) => Ok(()),
            outcome => Err(format!("copy settled {outcome:?}, not {expected:?}")),
        }
    }
}

/// One outcome as a caller acts on it: an `indeterminate` reason (fence or
/// cancel after acceptance) is diagnostic only.
fn same(before: &Outcome, now: &Outcome) -> bool {
    match (before, now) {
        (Outcome::Indeterminate(_), Outcome::Indeterminate(_)) => true,
        _ => before == now,
    }
}

#[cfg(test)]
fn schedules(remaining: &mut Vec<Hop>, prefix: &mut Vec<Hop>, out: &mut Vec<Vec<Hop>>) {
    if remaining.is_empty() {
        out.push(prefix.clone());
        return;
    }
    let mut tried = Vec::new();
    for (index, hop) in remaining.clone().into_iter().enumerate() {
        if tried.contains(&hop) {
            continue;
        }
        tried.push(hop);
        remaining.remove(index);
        prefix.push(hop);
        schedules(remaining, prefix, out);
        prefix.pop();
        remaining.insert(index, hop);
    }
}

#[cfg(test)]
async fn explore(fault: Fault) -> Result<usize, String> {
    let mut all = Vec::new();
    schedules(
        &mut vec![
            Hop::Request,
            Hop::Executor,
            Hop::Executor,
            Hop::Executor,
            Hop::Rival,
            Hop::Cancel,
            Hop::Fence,
        ],
        &mut Vec::new(),
        &mut all,
    );
    assert_eq!(all.len(), 840);
    let mut violations = 0;
    for requested in [false, true] {
        for schedule in &all {
            let mut model = Model::new(fault, Outcome::Ok(OwnedBytes::from(&b"done"[..])));
            model.requested = requested;
            for hop in schedule {
                if let Err(violation) = model.step(*hop).await {
                    if fault == Fault::None {
                        return Err(format!("requested={requested} {schedule:?}: {violation}"));
                    }
                    violations += 1;
                    break;
                }
            }
        }
    }
    Ok(violations)
}

/// Forks the caller at every distinct reachable prefix, observes the copy at
/// once, then lets the source run to the end of one schedule from there.
#[cfg(test)]
async fn explore_copies(fault: Fault) -> Result<usize, String> {
    let mut all = Vec::new();
    schedules(
        &mut vec![
            Hop::Request,
            Hop::Executor,
            Hop::Executor,
            Hop::Executor,
            Hop::Rival,
            Hop::Cancel,
            Hop::Fence,
        ],
        &mut Vec::new(),
        &mut all,
    );
    let mut forks = std::collections::HashSet::new();
    let (mut violations, mut copied) = (0, 0);
    for schedule in &all {
        for at in 0..=schedule.len() {
            if !forks.insert(schedule[..at].to_vec()) {
                continue;
            }
            for requested in [false, true] {
                let mut model = Model::new(fault, Outcome::Ok(OwnedBytes::from(&b"done"[..])));
                model.requested = requested;
                let hops = schedule[..at]
                    .iter()
                    .chain(&[Hop::Fork, Hop::Copy])
                    .chain(&schedule[at..]);
                for hop in hops {
                    if let Err(violation) = model.step(*hop).await {
                        if fault == Fault::None {
                            return Err(format!(
                                "requested={requested} fork at {at} of {schedule:?}: {violation}"
                            ));
                        }
                        violations += 1;
                        break;
                    }
                }
                copied += usize::from(model.copy.is_some_and(|copy| copy.expected.is_some()));
            }
        }
    }
    // The exploration is vacuous unless forks actually copied the call.
    if copied == 0 {
        return Err("no fork copied the call".into());
    }
    Ok(violations)
}

/// An executor terminal names the original acceptance's session. Other hops
/// may settle without a session, including before any executor accepts.
#[cfg(test)]
async fn terminal_sessions() -> Result<(), String> {
    for accepted in [false, true] {
        for session in [None, Some(true), Some(false)] {
            for terminal in [
                Outcome::Ok(OwnedBytes::from(&b"done"[..])),
                Outcome::Error("failed".into()),
                Outcome::Cancelled("stop".into()),
                Outcome::Indeterminate("lost".into()),
            ] {
                let mut model =
                    Model::new(Fault::None, Outcome::Ok(OwnedBytes::from(&b"done"[..])));
                model.requested = false;
                let mut journal = Journal::default();
                if accepted {
                    model.step(Hop::Executor).await?;
                    for record in model.records(model.call.log(), 0).await? {
                        journal.apply(&record).map_err(|error| error.to_string())?;
                    }
                    model.step(Hop::Executor).await?;
                    let records = model.records(model.call.log(), 0).await?;
                    let chunk = records.last().ok_or("missing committed chunk")?;
                    let work = acyclic_test_support::measure(|| {
                        assert!(matches!(journal.apply(chunk), Ok(Entry::Chunk(_))));
                    });
                    assert_eq!(work.count_total, 0, "a chunk must borrow its session");
                    assert_eq!(work.bytes_total, 0);
                    if session.is_none() && matches!(terminal, Outcome::Ok(_)) {
                        // Restoring the second path decode to the original chunk
                        // check fails the zero-allocation assertion above.
                        let duplicate = acyclic_test_support::measure(|| {
                            assert_eq!(fence(chunk).unwrap().as_ref(), Some(&model.fence));
                        });
                        assert!(duplicate.count_total > 0);
                        println!(
                            "chunk_session_allocations={} chunk_session_bytes={} duplicate_decode_allocations={} duplicate_decode_bytes={}",
                            work.count_total,
                            work.bytes_total,
                            duplicate.count_total,
                            duplicate.bytes_total
                        );
                    }
                }
                let original = journal.clone();
                let other = StreamPath::new("executor/sessions/other/ended")
                    .map_err(|error| error.to_string())?;
                let fence = session.map(|same| if same { &model.fence } else { &other });
                let publication = model
                    .call
                    .settle(fence, &terminal)
                    .map_err(|error| error.to_string())?;
                assert!(model.publish(publication).await?);
                let records = model.records(model.call.log(), 0).await?;
                let record = records.last().ok_or("missing committed terminal")?;
                let outcome = journal.apply(record);
                if session.is_none() || (accepted && session == Some(true)) {
                    let expected = match terminal {
                        Outcome::Cancelled(reason) if accepted => {
                            Outcome::Indeterminate(format!("cancelled after acceptance: {reason}"))
                        }
                        terminal => terminal,
                    };
                    assert_eq!(
                        outcome.map_err(|error| error.to_string())?,
                        Entry::Terminal(expected)
                    );
                    assert_eq!(journal.next(), record.sequence + 1);
                    assert_eq!(
                        journal
                            .receipt(model.call.log())
                            .map(|receipt| receipt.terminal),
                        Some(Some(Committed::of(record)))
                    );
                } else {
                    // Removing only the terminal's session check admits this real
                    // committed record and fails the original-reader assertion.
                    assert!(matches!(
                        outcome,
                        Err(Error::Malformed("call terminal from another session"))
                    ));
                    assert_eq!(journal, original, "refusal changed the original reader");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn every_interleaving_settles_once_and_cancel_never_waits() -> Result<(), String> {
    assert_eq!(explore_copies(Fault::None).await?, 0);
    assert_eq!(explore(Fault::None).await?, 0);
    // Without its guards a copy's settlement adds a second terminal to a
    // copy that inherited one.
    assert!(explore_copies(Fault::Copy).await? > 0);
    terminal_sessions().await?;
    // Preserve the owning acceptance control: only its settled guard is
    // removed from the original requestless, already cancelled call.
    let mut model = Model::new(Fault::None, Outcome::Ok(OwnedBytes::from(&b"done"[..])));
    model.step(Hop::Cancel).await?;
    let mut acceptance = model
        .call
        .accept(
            &Accepted {
                executor: OwnedBytes::from(&b"e1"[..]),
                fence: Some(model.fence.clone()),
            },
            true,
        )
        .map_err(|error| error.to_string())?;
    acceptance.conditions.retain(|condition| {
        !matches!(condition, CommitCondition::Absent { path } if path == model.call.settled())
    });
    assert!(model.publish(acceptance).await?);
    assert!(model.check(Hop::Rival).await.is_err());
    for fault in [
        Fault::Settlement,
        Fault::Fence,
        Fault::Retry,
        Fault::Producer,
    ] {
        assert!(explore(fault).await? > 0);
    }
    // A virtual fence outcome alone must not satisfy cancellation's durable
    // terminal guarantee. Wrongly tying cancel to the executor's session
    // refuses this commit even though that virtual outcome is already known.
    let mut model = Model::new(Fault::Cancel, Outcome::Ok(OwnedBytes::from(&b"done"[..])));
    for hop in [Hop::Request, Hop::Executor, Hop::Fence] {
        model.step(hop).await?;
    }
    assert_eq!(
        model.step(Hop::Cancel).await.unwrap_err(),
        "cancel did not retain exactly one terminal record"
    );
    // Select each terminal kind independently of the schedule. Acceptance,
    // multiple chunks, settlement and a later replay/cancel/fence all use the
    // same generated entry point as arbitrary fuzz inputs.
    for terminal in [6, 13, 20, 27] {
        check(&[terminal, 0, 1, 5, 1, 1, 6, 3, 4]).await?;
    }
    Ok(())
}

/// Runs arbitrary caller/executor/recovery schedules through the original
/// builders, provider and fold. Each committed record is folded once.
/// A final cancellation supplies a terminal even when no executor accepted.
pub async fn check(bytes: &[u8]) -> Result<(), String> {
    let result = match bytes.first().copied().unwrap_or_default() % 4 {
        0 => Outcome::Ok(OwnedBytes::from(&b"done"[..])),
        1 => Outcome::Error("refused".into()),
        2 => Outcome::Cancelled("cancelled".into()),
        _ => Outcome::Indeterminate("unknown".into()),
    };
    let mut model = Model::new(Fault::None, result);
    model.requested = bytes.last().is_none_or(|byte| byte & 8 == 0);
    for byte in bytes {
        let hop = match byte % 7 {
            0 => Hop::Request,
            1 => Hop::Executor,
            2 => Hop::Rival,
            3 => Hop::Cancel,
            4 => Hop::Fence,
            5 => Hop::Chunk,
            _ => Hop::Replay,
        };
        model.step(hop).await?;
    }
    model.step(Hop::Cancel).await
}
