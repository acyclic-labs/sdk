//! Bounded exhaustive model check of the call protocol over a real
//! `MemoryStream`.
//!
//! The caller's request opens the journal. Four hops then race on the call:
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
    executor: usize,
    stopped: bool,
    observed: Option<Outcome>,
}

impl Model {
    fn new(guarded: bool) -> Self {
        let root = StreamPath::new("caller").expect("root");
        Self {
            provider: MemoryStream::default(),
            call: CallPath::new(&root, "c1").expect("call"),
            fence: StreamPath::new("executor/sessions/1/ended").expect("fence"),
            guarded,
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
                    0 => self.call.accept(&accepted(b"e1", &self.fence), true),
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
                    .accept(&accepted(b"e2", &self.fence), true)
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
            if receipt.request.is_none() || receipt.terminal.is_none() == receipt.fence.is_none() {
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
    for schedule in all {
        let mut model = Model::new(guarded);
        let request = model
            .call
            .request(OwnedBytes::from(&b"run"[..]))
            .map_err(|error| error.to_string())?;
        if !model.publish(request).await? {
            return Err("the caller's request was refused".into());
        }
        for hop in &schedule {
            if let Err(violation) = model.step(*hop).await {
                if guarded {
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
    assert_eq!(explore(true).await?, 0);
    // Negative control: without the settled and fence guards, a late
    // executor terminal or chunk lands after a cancel or a fence.
    assert!(explore(false).await? > 0);
    Ok(())
}
