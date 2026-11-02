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
//! settlement guards, session guards or the original retry identity.
use super::*;
use acyclic_stream::{
    CommitOutcome, FollowRequest, FollowUpdate, MemoryStream, StreamProvider,
    StreamProviderExt as _,
};
use futures::StreamExt as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hop {
    Request,
    Executor,
    Rival,
    Cancel,
    Fence,
    Chunk,
    Replay,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    Settlement,
    Fence,
    Retry,
    Producer,
}

struct Model {
    provider: MemoryStream,
    call: CallPath,
    fence: StreamPath,
    fault: Fault,
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
}

impl Model {
    fn new(fault: Fault, result: Outcome) -> Self {
        let root = StreamPath::new("caller").expect("root");
        Self {
            provider: MemoryStream::default(),
            call: CallPath::new(&root, "c1").expect("call"),
            fence: StreamPath::new("executor/sessions/1/ended").expect("fence"),
            fault,
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
                self.publish(
                    self.call
                        .request(OwnedBytes::from(&b"run"[..]))
                        .map_err(|error| error.to_string())?,
                )
                .await?;
            }
            Hop::Replay => self.replay().await?,
            Hop::Chunk if self.executor == 0 || self.stopped => {}
            Hop::Executor if self.stopped => {}
            Hop::Executor | Hop::Chunk => {
                let fence = Some(&self.fence);
                let stage = if hop == Hop::Chunk { 1 } else { self.executor };
                let commit = match stage {
                    0 => self.call.accept(&accepted(b"e1", &self.fence), true),
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
            Hop::Rival => {
                let commit = self
                    .call
                    .accept(&accepted(b"e2", &self.fence), true)
                    .map_err(|error| error.to_string())?;
                if self.publish(self.guard(commit)).await? {
                    self.accepted = true;
                    if self.ended {
                        return Err("ended executor accepted the call".into());
                    }
                }
            }
            Hop::Cancel => {
                let commit = self
                    .call
                    .cancel("stop")
                    .map_err(|error| error.to_string())?;
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
        let mut follow = self
            .provider
            .follow(FollowRequest::new(path.clone(), from))
            .await
            .map_err(|error| error.to_string())?;
        if self.fault == Fault::Producer {
            // Close the original delivery source after its commit. Its
            // receiver must return a typed refusal rather than wait forever.
            follow = Box::pin(follow.take(0));
        }
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
            if receipt.terminal.is_none() == receipt.fence.is_none() {
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
        if hop == Hop::Cancel && self.observed.is_none() {
            return Err("a cancel left the call open".into());
        }
        Ok(())
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
    for schedule in all {
        let mut model = Model::new(fault, Outcome::Ok(OwnedBytes::from(&b"done"[..])));
        for hop in &schedule {
            if let Err(violation) = model.step(*hop).await {
                if fault == Fault::None {
                    return Err(format!("{schedule:?}: {violation}"));
                }
                violations += 1;
                break;
            }
        }
    }
    Ok(violations)
}

#[tokio::test]
async fn every_interleaving_settles_once_and_cancel_never_waits() -> Result<(), String> {
    assert_eq!(explore(Fault::None).await?, 0);
    for fault in [
        Fault::Settlement,
        Fault::Fence,
        Fault::Retry,
        Fault::Producer,
    ] {
        assert!(explore(fault).await? > 0);
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
