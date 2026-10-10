//! Independent bounded specification replayed against the production coordinator.
//! Scope: atomic CAS, two owners, two expiry tokens (10/20), deterministic IDs,
//! two reconciliation tickets, clock 0/10/20, serialized public calls. Complete
//! action-order exploration through depth four plus selected longer traces.
//! Keys retain durable revision/phase/owners/expiry/pending/subsequent/ticket,
//! issued identities and clock: no unsafe order reduction or UUID quotienting.
//! Reconciling fences opening; parent observations cannot change pinned_parent.
//! This generic adapter does NOT prove authority publication gates, Stream IO,
//! provider transactions, wall-clock accuracy, kernel/FFI, or native mounts.
#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing, clippy::panic,
    reason = "bounded executable specification emits minimized deterministic traces")]
use acyclic_fs::{Digest, GenerationId, OperationId, OperationLease, OperationLeaseId,
    OperationWindowCoordinator, OperationWindowError, OperationWindowFinish, OperationWindowLease,
    OperationWindowPhase, OperationWindowSnapshot, OperationWindowStore, WorkspaceId};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
fn workspace() -> WorkspaceId { WorkspaceId::from_bytes([61;16]) }
fn generation(n: u8) -> GenerationId { GenerationId::new(Digest::from_bytes([n;32])) }
fn identity(owner: usize) -> OperationLeaseId { OperationLeaseId::from_bytes([owner as u8 + 1;16]) }
fn ticket(n: u8) -> OperationId { OperationId::from_bytes([n;16]) }
fn owner(n: usize) -> String { format!("owner-{n}") }
#[derive(Debug, thiserror::Error)]
#[error("one-shot CAS interruption")]
struct Lost;
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
enum Loss { None, Before, After }
#[derive(Clone, Default)]
struct Store { state: Arc<Mutex<Option<OperationWindowSnapshot>>>, fault: Arc<Mutex<(Option<bool>, bool)>> }
impl Store {
    fn arm(&self, loss: Loss) {
        *self.fault.lock() = (match loss { Loss::None => None, Loss::Before => Some(false), Loss::After => Some(true) }, false);
    }
}
impl OperationWindowStore for Store {
    type Error = Lost;
    async fn load(&self, _: WorkspaceId) -> Result<Option<OperationWindowSnapshot>, Lost> { Ok(self.state.lock().clone()) }
    async fn compare_and_swap(&self, _: WorkspaceId, expected: u64, next: OperationWindowSnapshot) -> Result<bool, Lost> {
        let mut state = self.state.lock();
        if state.as_ref().map_or(0, |s| s.revision) != expected { return Ok(false); }
        let mut fault = self.fault.lock();
        if fault.0 == Some(false) { *fault = (None, true); return Err(Lost); }
        *state = Some(next);
        if fault.0 == Some(true) { *fault = (None, true); return Err(Lost); }
        Ok(true)
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum Phase { Idle, Active { expiry: [u8;2], pending: bool },
    Reconcile { ticket: u8, pending: bool, subsequent: bool } }
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Model { revision: u64, phase: Phase, issued: [bool;2], now: u8 }
impl Default for Model {
    fn default() -> Self { Self { revision: 0, phase: Phase::Idle, issued: [false;2], now: 0 } }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
enum Call { Open(usize, bool), Renew(usize), Close(usize, u8), Observe,
    Claim, Complete(u8), Tick, Reopen }
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct Action { call: Call, loss: Loss }
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Reply { Lease(u8), Active(u8), Ticket(u8, bool), Closed, Empty, Bool(bool),
    Ok, Identity, Expiry, StaleLease, StaleTicket, Reconciling, Lost }
impl Model {
    fn candidate(&self, call: Call) -> (Self, Reply, bool) {
        let mut next = self.clone(); let mut write = false;
        let reply = match call {
            Call::Reopen => Reply::Ok,
            Call::Tick => { next.now = if self.now == 0 { 10 } else { 20 }; Reply::Ok },
            Call::Observe => match &mut next.phase {
                Phase::Idle => Reply::Bool(false),
                Phase::Active { pending, .. } => { *pending = true; write = true; Reply::Bool(true) },
                Phase::Reconcile { subsequent, .. } => { *subsequent = true; write = true; Reply::Bool(true) },
            },
            Call::Open(i, mismatch) => {
                if self.now >= 10 { Reply::Expiry }
                else {
                    match &mut next.phase {
                        Phase::Idle => {
                            let mut expiry = [0;2]; expiry[i] = 10;
                            next.phase = Phase::Active { expiry, pending: false }; next.issued[i] = true;
                            write = true; Reply::Lease(10)
                        },
                        Phase::Active { expiry, .. } => {
                            if expiry[i] != 0 {
                                if mismatch || expiry[i] != 10 { Reply::Identity } else { Reply::Lease(10) }
                            } else { expiry[i] = 10; next.issued[i] = true; write = true; Reply::Lease(10) }
                        },
                        Phase::Reconcile { .. } => Reply::Reconciling,
                    }
                }
            },
            Call::Renew(i) => {
                if self.now >= 20 { Reply::Expiry }
                else { match &mut next.phase {
                    Phase::Active { expiry, .. } if expiry[i] == 10 && self.now < 10 => {
                        expiry[i] = 20; write = true; Reply::Lease(20)
                    }, _ => Reply::StaleLease,
                } }
            },
            Call::Close(i, token) => match &mut next.phase {
                Phase::Active { expiry, pending } => {
                    if expiry[i] != 0 && expiry[i] != token { Reply::Closed }
                    else {
                        let live = expiry[i] == token && token > self.now;
                        let before = *expiry; expiry[i] = 0;
                        for e in expiry.iter_mut() { if *e <= self.now { *e = 0; } }
                        let remaining = expiry.iter().filter(|e| **e != 0).count() as u8;
                        write = before != *expiry || remaining == 0;
                        if remaining == 0 {
                            let pending = *pending;
                            next.phase = Phase::Reconcile { ticket: 101, pending, subsequent: false };
                            if live { Reply::Ticket(101, pending) } else { Reply::Closed }
                        } else if live { Reply::Active(remaining) } else { Reply::Closed }
                    }
                }, _ => Reply::Closed,
            },
            Call::Claim => match next.phase.clone() {
                Phase::Idle => Reply::Empty,
                Phase::Reconcile { ticket, pending, .. } => Reply::Ticket(ticket, pending),
                Phase::Active { expiry, pending } => {
                    // A partial expiry sweep in claim is intentionally not persisted.
                    if expiry.iter().all(|e| *e <= self.now) {
                        next.phase = Phase::Reconcile { ticket: 101, pending, subsequent: false };
                        write = true; Reply::Ticket(101, pending)
                    } else { Reply::Empty }
                },
            },
            Call::Complete(t) => match next.phase {
                Phase::Reconcile { ticket, subsequent, .. } if t == ticket => {
                    next.phase = if subsequent { Phase::Reconcile { ticket: 102, pending: true, subsequent: false } }
                        else { Phase::Idle };
                    write = true; Reply::Ok
                }, _ => Reply::StaleTicket,
            },
        };
        (next, reply, write)
    }
    fn step(&mut self, action: Action) -> (Reply, bool) {
        let (mut next, reply, write) = self.candidate(action.call);
        if write {
            if action.loss == Loss::Before { return (Reply::Lost, true); }
            next.revision += 1; *self = next;
            if action.loss == Loss::After { return (Reply::Lost, true); }
        } else { *self = next; }
        (reply, false)
    }
    fn actions(&self) -> Vec<Action> {
        let mut calls = vec![Call::Observe, Call::Claim, Call::Complete(101), Call::Complete(102), Call::Reopen];
        if self.now < 20 { calls.push(Call::Tick); }
        for i in 0..2 {
            // Each owner opens at most once, but exact and conflicting retries
            // remain in the action alphabet for the lifetime of its active ID.
            if !self.issued[i] || matches!(self.phase, Phase::Active { expiry, .. } if expiry[i] != 0) {
                calls.push(Call::Open(i, false));
                if self.issued[i] { calls.push(Call::Open(i, true)); }
            }
            if self.issued[i] { calls.extend([Call::Renew(i), Call::Close(i,10), Call::Close(i,20)]); }
        }
        calls.into_iter().flat_map(|call| [Loss::None, Loss::Before, Loss::After].map(|loss| Action { call, loss })).collect()
    }
    fn snapshot(&self) -> OperationWindowSnapshot {
        let phase = match self.phase {
            Phase::Idle => OperationWindowPhase::Idle,
            Phase::Active { expiry, pending } => OperationWindowPhase::Active {
                pinned_parent: generation(1), pending_parent: pending.then(|| generation(2)),
                leases: expiry.into_iter().enumerate().filter(|(_,e)| *e != 0).map(|(i,e)| {
                    (identity(i), OperationLease { id: identity(i), owner: owner(i), expires_at_millis: u64::from(e) })
                }).collect::<BTreeMap<_,_>>(),
            },
            Phase::Reconcile { ticket: t, pending, subsequent } => OperationWindowPhase::Reconciling {
                ticket: ticket(t), pinned_parent: generation(1), pending_parent: pending.then(|| generation(2)),
                subsequent_parent: subsequent.then(|| generation(2)),
            },
        };
        OperationWindowSnapshot { version: 1, revision: self.revision, workspace_id: workspace(), phase }
    }
}
fn lease(i: usize, expiry: u8) -> OperationWindowLease {
    OperationWindowLease { workspace_id: workspace(), lease_id: identity(i), pinned_parent: generation(1), expires_at_millis: u64::from(expiry) }
}
fn error(e: OperationWindowError<Lost>) -> Reply { match e {
    OperationWindowError::Store(_) => Reply::Lost,
    OperationWindowError::InvalidExpiry => Reply::Expiry,
    OperationWindowError::LeaseIdentityConflict => Reply::Identity,
    OperationWindowError::StaleLease => Reply::StaleLease,
    OperationWindowError::StaleTicket => Reply::StaleTicket,
    OperationWindowError::Reconciling => Reply::Reconciling,
    other => panic!("outside bounded outcomes: {other:?}"),
} }
async fn replay(trace: &[Action]) -> Result<usize, String> {
    let store = Store::default(); let mut engine = OperationWindowCoordinator::new(store.clone());
    let mut model = Model::default(); let mut consumed = 0;
    for (index, action) in trace.iter().copied().enumerate() {
        if !model.actions().contains(&action) { return Ok(0); } // minimization preserves admitted traces
        let now = u64::from(model.now); let (expected, fired) = model.step(action); store.arm(action.loss);
        let actual = match action.call {
            Call::Tick => Reply::Ok,
            Call::Reopen => { engine = OperationWindowCoordinator::new(store.clone()); Reply::Ok },
            Call::Open(i, conflict) => engine.begin_with_lease_id(workspace(), generation(1),
                if conflict { "different-owner".into() } else { owner(i) }, now, 10, identity(i)).await
                .map(|l| Reply::Lease(l.expires_at_millis as u8)).unwrap_or_else(error),
            Call::Renew(i) => engine.renew(&lease(i,10), now,20).await
                .map(|l| Reply::Lease(l.expires_at_millis as u8)).unwrap_or_else(error),
            Call::Close(i,t) => engine.finish_with_ticket(&lease(i,t), now,ticket(101)).await.map(|f| match f {
                OperationWindowFinish::AlreadyClosed => Reply::Closed,
                OperationWindowFinish::StillActive { remaining } => Reply::Active(remaining as u8),
                OperationWindowFinish::Reconcile(r) => Reply::Ticket(r.ticket.into_bytes()[0], r.pending_parent.is_some()),
            }).unwrap_or_else(error),
            Call::Observe => engine.observe_parent(workspace(), generation(2)).await.map(Reply::Bool).unwrap_or_else(error),
            Call::Claim => engine.claim_reconcile_with_ticket(workspace(),now,ticket(101)).await.map(|r| r.map_or(Reply::Empty,
                |r| Reply::Ticket(r.ticket.into_bytes()[0], r.pending_parent.is_some()))).unwrap_or_else(error),
            Call::Complete(t) => engine.complete_reconcile_with_next_ticket(workspace(),ticket(t),ticket(102)).await.map(|()| Reply::Ok).unwrap_or_else(error),
        };
        if actual != expected || store.fault.lock().1 != fired { return Err(format!("at {index}: reply {actual:?}/{expected:?}; fired {}/{fired}", store.fault.lock().1)); }
        let expected_state = (model.revision != 0).then(|| model.snapshot());
        let durable = store.state.lock().clone();
        if durable != expected_state { return Err(format!("at {index}: durable {durable:?} != {expected_state:?}")); }
        if engine.inspect(workspace()).await.map_err(|e| format!("inspect {e:?}"))? != model.snapshot() {
            return Err(format!("at {index}: reopened coordinator view differs from independent state"));
        }
        consumed += usize::from(fired);
    }
    Ok(consumed)
}
async fn checked(trace: &[Action]) -> usize {
    match replay(trace).await { Ok(count) => count, Err(mut failure) => {
        let mut reduced = trace.to_vec(); let mut index = 0;
        while index < reduced.len() {
            let mut candidate = reduced.clone(); candidate.remove(index);
            if let Err(next) = replay(&candidate).await { reduced = candidate; failure = next; } else { index += 1; }
        }
        let json = serde_json::to_string(&reduced).expect("serialize replay actions");
        let path = std::env::temp_dir().join("omp-operation-window-refinement-counterexample.json");
        std::fs::write(&path, &json).expect("persist minimized counterexample");
        panic!("operation-window replay {json}\n{failure}\nOMP_WINDOW_REPLAY={}", path.display());
    } }
}
#[tokio::test]
async fn bounded_operation_window_refinement() {
    if let Some(path) = std::env::var_os("OMP_WINDOW_REPLAY") {
        let trace: Vec<Action> = serde_json::from_slice(&std::fs::read(path).expect("read replay file")).expect("parse replay actions");
        checked(&trace).await;
        return;
    }
    const DEPTH: usize = 4;
    let mut queue = VecDeque::from([(Model::default(),Vec::new())]);
    let mut seen = HashSet::from([(0usize,Model::default())]); let mut edges = 0; let mut consumed = 0;
    while let Some((state, trace)) = queue.pop_front() {
        if trace.len() == DEPTH { continue; }
        for action in state.actions() {
            let mut next = state.clone(); next.step(action);
            let mut path = trace.clone(); path.push(action);
            consumed += checked(&path).await; edges += 1;
            if seen.insert((path.len(),next.clone())) { queue.push_back((next,path)); }
        }
    }
    let cases = [
        vec![Call::Open(0,false), Call::Open(1,false), Call::Observe, Call::Close(0,10), Call::Close(1,10), Call::Observe, Call::Complete(101), Call::Complete(101), Call::Claim, Call::Complete(102)],
        vec![Call::Open(0,false), Call::Renew(0), Call::Close(0,10), Call::Renew(0), Call::Tick, Call::Close(0,10), Call::Tick, Call::Renew(0), Call::Close(0,20), Call::Reopen, Call::Claim, Call::Complete(101)],
        vec![Call::Open(0,false), Call::Open(1,false), Call::Renew(0), Call::Tick, Call::Claim, Call::Close(1,10), Call::Tick, Call::Reopen, Call::Claim, Call::Complete(101)],
    ];
    for calls in cases {
        checked(&calls.into_iter().map(|call| Action { call, loss: Loss::None }).collect::<Vec<_>>()).await;
    }
    // A durable successful open whose reply was lost is retried with its exact
    // identity, then a mismatched owner is rejected without advancing state.
    checked(&[Action { call: Call::Open(0,false), loss: Loss::After },
        Action { call: Call::Reopen, loss: Loss::None }, Action { call: Call::Open(0,false), loss: Loss::None },
        Action { call: Call::Open(0,true), loss: Loss::None }]).await;
    for seed in [1u64,7,41,97,0x5eed,0xc0ffee,0x12345678,0xdeadbeef] {
        let mut random = seed; let mut model = Model::default(); let mut trace = Vec::new();
        for _ in 0..16 {
            random ^= random << 13; random ^= random >> 7; random ^= random << 17;
            let actions = model.actions(); let action = actions[(random % actions.len() as u64) as usize];
            model.step(action); trace.push(action);
        }
        checked(&trace).await;
    }
    assert!(consumed > 0, "failure points must be consumed, not merely armed");
    eprintln!("window bound: depth={DEPTH}, owners=2, expiry epochs=2, tickets=2; canonical states={}, replayed edges={edges}, consumed CAS interruptions={consumed}; fixed traces=8x16 plus 4 directed traces",seen.len());
}
