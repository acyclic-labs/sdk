//! Bounded exhaustive model check of the call protocol over a real
//! `MemoryStream`.
//!
//! The request is either in the journal or on the executor's queue. Four hops
//! then race on the call, including a stale request-location flag:
//! the executor (accept, chunk, `ok`), a second executor (accept), a
//! canceller and the executor's session end (its fence).
//! Every interleaving is run through the real commit builders. After every
//! step the journal must fold, its settled marker must agree with it, and the
//! outcome a reader observes, once present, must never change. Cancellation
//! commits in its one step or finds the call already ended, and the call is
//! settled right after it (it never waits). Unguarded chunks and terminals are
//! the negative control: some interleaving must break an invariant.
use super::*;
use acyclic_stream::{
    CommitOutcome, FollowRequest, FollowUpdate, MemoryStream, StreamProvider,
    StreamProviderExt as _,
};
use futures::StreamExt as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hop {
    Executor,
    Rival,
    Cancel,
    Fence,
}

struct Model {
    provider: MemoryStream,
    call: CallPath,
    fence: StreamPath,
    guarded: bool,
    request: bool,
    requested: bool,
    executor: usize,
    stopped: bool,
    observed: Option<Outcome>,
}

impl Model {
    fn new(guarded: bool, request: bool, requested: bool) -> Self {
        let root = StreamPath::new("caller").expect("root");
        Self {
            provider: MemoryStream::default(),
            call: CallPath::new(&root, "c1").expect("call"),
            fence: StreamPath::new("executor/sessions/1/ended").expect("fence"),
            guarded,
            request,
            requested,
            executor: 0,
            stopped: false,
            observed: None,
        }
    }

    fn guard(&self, mut commit: Commit) -> Commit {
        if !self.guarded {
            commit.conditions.clear();
        }
        commit
    }

    async fn publish(&self, commit: Commit) -> Result<bool, String> {
        match self
            .provider
            .commit(commit.request(IdempotencyKey::unique()))
            .await
            .map_err(|error| error.to_string())?
        {
            CommitOutcome::Committed(_) => Ok(true),
            CommitOutcome::Conflict(_) => Ok(false),
        }
    }

    async fn step(&mut self, hop: Hop) -> Result<(), String> {
        let accepted = |executor: &'static [u8], fence: &StreamPath| Accepted {
            executor: OwnedBytes::from(executor),
            fence: Some(fence.clone()),
        };
        match hop {
            Hop::Executor if self.stopped => {}
            Hop::Executor => {
                let fence = Some(&self.fence);
                let commit = match self.executor {
                    0 => self
                        .call
                        .accept(&accepted(b"e1", &self.fence), self.requested),
                    1 => self.call.chunk(fence, OwnedBytes::from(&b"part"[..])),
                    _ => self
                        .call
                        .settle(fence, &Outcome::Ok(OwnedBytes::from(&b"done"[..]))),
                }
                .map_err(|error| error.to_string())?;
                let commit = if self.executor == 0 {
                    commit
                } else {
                    self.guard(commit)
                };
                self.executor += 1;
                // A refused step stops the executor: it was cancelled,
                // fenced or never accepted.
                self.stopped = !self.publish(commit).await?;
            }
            Hop::Rival => {
                let commit = self
                    .call
                    .accept(&accepted(b"e2", &self.fence), self.requested)
                    .map_err(|error| error.to_string())?;
                self.publish(commit).await?;
            }
            Hop::Cancel => {
                let commit = self
                    .call
                    .cancel("stop")
                    .map_err(|error| error.to_string())?;
                let commit = self.guard(commit);
                self.publish(commit).await?;
            }
            Hop::Fence => {
                self.publish(Commit {
                    conditions: Vec::new(),
                    mutations: vec![append(&self.fence, AppendRecord::from(&b"lost"[..]))],
                })
                .await?;
            }
        }
        self.check(hop).await
    }

    async fn records(&self, path: &StreamPath) -> Result<Vec<Record>, String> {
        let tail = match self.provider.head(path.clone()).await {
            Ok(head) => head.tail,
            Err(StreamError::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error.to_string()),
        };
        let mut follow = self
            .provider
            .follow(FollowRequest::new(path.clone(), 0))
            .await
            .map_err(|error| error.to_string())?;
        let mut records = Vec::new();
        while (records.len() as u64) < tail {
            match follow.next().await.ok_or("follow ended")? {
                Ok(FollowUpdate::Record(record)) => records.push(record),
                Ok(_) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(records)
    }

    async fn check(&mut self, hop: Hop) -> Result<(), String> {
        let mut journal = Journal::default();
        let mut terminals = 0;
        for record in self.records(self.call.log()).await? {
            if let Entry::Terminal(_) = journal.apply(&record).map_err(|error| error.to_string())? {
                terminals += 1;
            }
        }
        let markers = self.records(self.call.settled()).await?.len();
        if markers != terminals {
            return Err(format!(
                "{markers} settled markers for {terminals} terminals"
            ));
        }
        if !self.records(&self.fence).await?.is_empty() {
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

async fn explore(guarded: bool) -> Result<usize, String> {
    let mut all = Vec::new();
    schedules(
        &mut vec![
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
    assert_eq!(all.len(), 120);
    let mut violations = 0;
    for request in [false, true] {
        for requested in [false, true] {
            for schedule in &all {
                let mut model = Model::new(guarded, request, requested);
                if request {
                    let request = model
                        .call
                        .request(OwnedBytes::from(&b"run"[..]))
                        .map_err(|error| error.to_string())?;
                    if !model.publish(request).await? {
                        return Err("the caller's request was refused".into());
                    }
                }
                for hop in schedule {
                    if let Err(violation) = model.step(*hop).await {
                        if guarded {
                            return Err(format!(
                                "request={request} requested={requested} {schedule:?}: {violation}"
                            ));
                        }
                        violations += 1;
                        break;
                    }
                }
            }
        }
    }
    Ok(violations)
}

/// An executor terminal names the original acceptance's session. Other hops
/// may settle without a session, including before any executor accepts.
async fn terminal_sessions() -> Result<(), String> {
    for accepted in [false, true] {
        for session in [None, Some(true), Some(false)] {
            for terminal in [
                Outcome::Ok(OwnedBytes::from(&b"done"[..])),
                Outcome::Error("failed".into()),
                Outcome::Cancelled("stop".into()),
                Outcome::Indeterminate("lost".into()),
            ] {
                let mut model = Model::new(true, false, false);
                let mut journal = Journal::default();
                if accepted {
                    model.step(Hop::Executor).await?;
                    for record in model.records(model.call.log()).await? {
                        journal.apply(&record).map_err(|error| error.to_string())?;
                    }
                    model.step(Hop::Executor).await?;
                    let records = model.records(model.call.log()).await?;
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
                let records = model.records(model.call.log()).await?;
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
    assert_eq!(explore(true).await?, 0);
    terminal_sessions().await?;
    // Remove only acceptance's settled guard from the original cancelled,
    // requestless call. The stale flag then permits a post-terminal append.
    let mut model = Model::new(true, false, true);
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
    // Negative control: without the settled and fence guards, a late
    // executor terminal or chunk lands after a cancel or a fence.
    assert!(explore(false).await? > 0);
    Ok(())
}
