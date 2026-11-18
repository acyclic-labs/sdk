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
//! acceptance guards, settlement guards, session guards or the original retry
//! identity. Recovery publishes the original call's sole terminal.
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
    Acceptance,
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
}

impl Model {
    fn new(fault: Fault, result: Outcome) -> Result<Self, String> {
        Self::with_provider(MemoryStream::default(), fault, result)
    }

    fn with_provider(
        provider: MemoryStream,
        fault: Fault,
        result: Outcome,
    ) -> Result<Self, String> {
        let root = StreamPath::new("caller").map_err(|error| error.to_string())?;
        let call = CallPath::new(&root, "c1").map_err(|error| error.to_string())?;
        let journal = Journal::new(call.log());
        Ok(Self {
            provider,
            call,
            fence: StreamPath::new("executor/sessions/1/ended")
                .map_err(|error| error.to_string())?,
            fault,
            request: false,
            requested: true,
            executor: 0,
            stopped: false,
            observed: None,
            ended: false,
            retry: None,
            result,
            journal,
            terminals: 0,
            accepted: false,
            expected: None,
            chunks: 0,
            read_chunks: 0,
        })
    }

    fn guard(&self, mut commit: Commit) -> Commit {
        commit.conditions.retain(|condition| match condition {
            CommitCondition::Absent { path } => {
                !(self.fault == Fault::Settlement && path == self.call.settled()
                    || self.fault == Fault::Acceptance && path == self.call.accepted()
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
                            .request(
                                OwnedBytes::from(&b"run"[..]),
                                crate::escrow::Escrow::new(Some(5)),
                            )
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
            super::codec_fuzz::check_record(&record, &self.journal)
                .map_err(|error| error.to_string())?;
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
        if self.ended
            && let Some(recovery) = self
                .journal
                .recover(&self.call, "lost")
                .map_err(|error| error.to_string())?
        {
            if !self.publish(recovery).await? {
                return Err("original lost-session recovery did not settle".into());
            }
            for record in self.records(self.call.log(), self.journal.next()).await? {
                super::codec_fuzz::check_record(&record, &self.journal)
                    .map_err(|error| error.to_string())?;
                if matches!(
                    self.journal
                        .apply(&record)
                        .map_err(|error| error.to_string())?,
                    Entry::Terminal(_)
                ) {
                    self.terminals += 1;
                }
            }
        }
        let markers = self.tail(self.call.settled()).await?;
        if markers != self.terminals {
            return Err(format!(
                "{markers} settled markers for {} terminals",
                self.terminals
            ));
        }
        if let Some(receipt) = self.journal.receipt(self.call.log()) {
            // The receipt is the committed request plus the one terminal
            // record, including the original session-loss terminal.
            if receipt.request.is_some() != self.request {
                return Err(format!("incomplete receipt {receipt:?}"));
            }
        }
        let outcome = self.journal.outcome().cloned();
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
mod tests;

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
    let mut model = Model::new(Fault::None, result)?;
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

/// Capture the original committed call records, with independent decoded and
/// contextual dispositions. Provider metadata comes from real commits.
pub async fn codec_seeds(
    clock: std::sync::Arc<dyn acyclic_stream::UnixMillisClock>,
) -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    use acyclic_stream::{MemoryLimits, wire_codec};
    use prost::Message as _;
    use std::sync::Arc;

    let mut seeds = Vec::new();
    for (name, outcome, requested, accepted, fenced) in [
        (
            "call-v2-ok",
            Outcome::Ok(OwnedBytes::from(&b"done"[..])),
            true,
            true,
            false,
        ),
        (
            "call-v2-error",
            Outcome::Error("refused: λ".into()),
            false,
            true,
            true,
        ),
        (
            "call-v2-cancelled-accepted",
            Outcome::Cancelled("stop".into()),
            true,
            true,
            true,
        ),
        (
            "call-v2-cancelled",
            Outcome::Cancelled("stop".into()),
            true,
            false,
            false,
        ),
        (
            "call-v2-indeterminate",
            Outcome::Indeterminate("unknown".into()),
            false,
            false,
            false,
        ),
    ] {
        let model = Model::with_provider(
            MemoryStream::new_with_clock(MemoryLimits::default(), Arc::clone(&clock)),
            Fault::None,
            outcome.clone(),
        )?;
        let call = &model.call;
        let fence = StreamPath::new("codec/executor/ended").map_err(Error::Stream)?;
        let acceptance = Accepted {
            executor: OwnedBytes::from(&b"executor"[..]),
            fence: fenced.then_some(fence),
        };
        let mut steps = Vec::new();
        if requested {
            let body = OwnedBytes::from(&b"request"[..]);
            let hold = crate::escrow::Escrow::new(Some(7));
            steps.push((
                call.request(body.clone(), hold)?,
                vec![
                    ("call-v2-request", Entry::Request(body)),
                    ("call-v2-hold", Entry::Hold(hold)),
                ],
            ));
        }
        if accepted {
            steps.push((
                call.accept(&acceptance, requested)?,
                vec![(
                    if fenced {
                        "call-v2-accepted-fenced"
                    } else {
                        "call-v2-accepted"
                    },
                    Entry::Accepted(acceptance.clone()),
                )],
            ));
            let body = OwnedBytes::from(&b"\x00\xffchunk"[..]);
            steps.push((
                call.chunk(acceptance.fence.as_ref(), body.clone())?,
                vec![(
                    if fenced {
                        "call-v2-chunk-fenced"
                    } else {
                        "call-v2-chunk"
                    },
                    Entry::Chunk(body),
                )],
            ));
        }
        steps.push((
            call.settle(acceptance.fence.as_ref(), &outcome)?,
            vec![(name, Entry::Terminal(outcome.clone()))],
        ));
        let mut journal = Journal::new(call.log());
        for (index, (commit, expected)) in steps.into_iter().enumerate() {
            let key = IdempotencyKey::new(format!("{name}/{index}")).map_err(Error::Stream)?;
            assert!(matches!(
                model
                    .provider
                    .commit(commit.request(key))
                    .await
                    .map_err(Error::Stream)?,
                CommitOutcome::Committed(_)
            ));
            let records = model.records(call.log(), journal.next()).await?;
            assert_eq!(
                records.len(),
                expected.len(),
                "original co-committed call records"
            );
            for (record, (label, expected)) in records.into_iter().zip(expected) {
                let bytes = wire_codec::record_wire(record, None).encode_to_vec();
                let record = wire_codec::record(
                    acyclic_stream_wire::wire::Record::decode(bytes.as_slice())?,
                    None,
                )
                .map_err(Error::Stream)?;
                assert_eq!(
                    Entry::decode(&record)?,
                    expected,
                    "call writer must admit its committed record"
                );
                let contextual = match &expected {
                    Entry::Terminal(Outcome::Cancelled(reason)) if accepted => Entry::Terminal(
                        Outcome::Indeterminate(format!("cancelled after acceptance: {reason}")),
                    ),
                    _ => expected,
                };
                assert_eq!(
                    journal.apply(&record)?,
                    contextual,
                    "call reader must retain its acceptance context"
                );
                // Keep one original capture per wire shape. Other topologies still
                // pass through the same independent producer admission above.
                if !seeds.iter().any(|(name, _)| *name == label) {
                    seeds.push((label, bytes));
                }
            }
        }
        let receipt = journal
            .receipt(call.log())
            .ok_or("original terminal receipt is absent")?;
        assert_eq!(receipt.request.is_some(), requested);
        assert_eq!(
            receipt.outcome,
            if accepted && matches!(outcome, Outcome::Cancelled(_)) {
                Outcome::Indeterminate("cancelled after acceptance: stop".into())
            } else {
                outcome
            }
        );
    }
    Ok(seeds)
}
