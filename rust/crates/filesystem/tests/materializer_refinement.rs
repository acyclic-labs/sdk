//! Bounded refinement of the public journal engine, not a proof of kernels/FFI/providers.
//! Assumptions: one executor owns a journal at a time, atomic durable CAS, two disjoint
//! edits, complete byte preimages, endpoint observations are truthful. All action
//! orders through depth three are explored; no commutativity/order reduction is used.
//! State keys retain phase, revision, progress, every endpoint, and mutation counts.
//! Faults are one-shot before/after each CAS or physical edit. A returned error models
//! process loss; it does not claim power-loss/fsync behavior. Reopen discards engines.
#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing, clippy::panic,
    reason = "bounded executable model uses fatal assertions with replay traces")]

use acyclic_fs::{Digest, GenerationId, JournaledMaterializer, MaterializationBackend,
    MaterializationEdit, MaterializationError, MaterializationJournal, MaterializationJournalStore,
    MaterializationObservation, MaterializationPhase, MaterializationPlan, MaterializationPreimage,
    MaterializationRecovery, OperationId};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use parking_lot::Mutex;
const OLD_FROM: &[u8] = b"from:original:41";
const OLD_TO: &[u8] = b"to:original:97";
const OLD_REMOVE: &[u8] = b"remove:original:23";
const OUTSIDE: &[u8] = b"unrelated:never-touch:83";
const EXTERNAL: &[u8] = b"external-writer:59";
fn generation(n: u8) -> GenerationId { GenerationId::new(Digest::from_bytes([n; 32])) }
fn plan() -> MaterializationPlan {
    MaterializationPlan { operation_id: OperationId::from_bytes([31; 16]),
        from: generation(1), to: generation(2), edits: vec![
            MaterializationEdit::Rename { from: "from".into(), to: "to".into() },
            MaterializationEdit::Remove { path: "remove".into() },
        ] }
}
fn original() -> BTreeMap<String, Vec<u8>> {
    [("from", OLD_FROM), ("to", OLD_TO), ("remove", OLD_REMOVE), ("outside", OUTSIDE)]
        .into_iter().map(|(k,v)| (k.into(), v.to_vec())).collect()
}
#[derive(Debug, thiserror::Error)]
#[error("injected interruption or fixture IO failure")]
struct Interrupted;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
enum Event { Record, Mutation }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
struct Fault { event: Event, ordinal: u8, after: bool }
#[derive(Default)]
struct GateState { fault: Option<Fault>, records: u8, mutations: u8, consumed: bool }
#[derive(Clone, Default)]
struct Gate(Arc<Mutex<GateState>>);
impl Gate {
    fn arm(&self, fault: Option<Fault>) { *self.0.lock() = GateState { fault, ..Default::default() }; }
    fn hit(&self, event: Event, after: bool) -> Result<(), Interrupted> {
        let mut g = self.0.lock();
        let ordinal = match event {
            Event::Record => { if !after { g.records += 1; } g.records },
            Event::Mutation => { if !after { g.mutations += 1; } g.mutations },
        };
        if g.fault == Some(Fault { event, ordinal, after }) {
            g.fault = None; g.consumed = true; return Err(Interrupted);
        }
        Ok(())
    }
    fn consumed(&self) -> bool { self.0.lock().consumed }
}
#[derive(Clone, Default)]
struct Store { journal: Arc<Mutex<Option<MaterializationJournal>>>, gate: Gate }
impl MaterializationJournalStore for Store {
    type Error = Interrupted;
    async fn load(&self, _: OperationId) -> Result<Option<MaterializationJournal>, Interrupted> {
        Ok(self.journal.lock().clone())
    }
    async fn compare_and_swap(&self, _: OperationId, revision: u64, next: MaterializationJournal)
        -> Result<bool, Interrupted> {
        let mut current = self.journal.lock();
        if current.as_ref().map_or(0, |j| j.revision) != revision { return Ok(false); }
        self.gate.hit(Event::Record, false)?;
        *current = Some(next);
        self.gate.hit(Event::Record, true)?;
        Ok(true)
    }
}
type Image = [Option<Vec<u8>>; 2];
fn paths(edit: &MaterializationEdit) -> (&str, Option<&str>) {
    match edit {
        MaterializationEdit::Rename { from, to } => (from, Some(to)),
        MaterializationEdit::Remove { path } => (path, None),
        _ => panic!("outside bounded alphabet"),
    }
}
fn encode(image: &Image) -> MaterializationPreimage {
    MaterializationPreimage { image: serde_json::to_vec(image).unwrap() }
}
fn decode(image: &MaterializationPreimage) -> Image { serde_json::from_slice(&image.image).unwrap() }
fn desired(edit: &MaterializationEdit, before: &Image) -> Image {
    match edit { MaterializationEdit::Rename { .. } => [None, before[0].clone()],
        MaterializationEdit::Remove { .. } => [None, None], _ => panic!("bounded alphabet") }
}
fn classify(current: Image, before: Image, after: Image) -> MaterializationObservation {
    if current == before { MaterializationObservation::Preimage }
    else if current == after { MaterializationObservation::Postimage }
    else { MaterializationObservation::Diverged }
}
#[derive(Clone)]
struct Backend { tree: Arc<Mutex<BTreeMap<String, Vec<u8>>>>, counts: Arc<Mutex<[u16; 2]>>, gate: Gate }
impl Backend {
    fn new(gate: Gate) -> Self { Self { tree: Arc::new(Mutex::new(original())),
        counts: Arc::new(Mutex::new([0;2])), gate } }
    fn image(&self, edit: &MaterializationEdit) -> Image {
        let tree = self.tree.lock(); let (a,b) = paths(edit);
        [tree.get(a).cloned(), b.and_then(|b| tree.get(b).cloned())]
    }
    fn set(&self, edit: &MaterializationEdit, image: Image) {
        let (a,b) = paths(edit); let mut tree = self.tree.lock();
        for (path,value) in [(Some(a), image[0].clone()), (b, image[1].clone())] {
            if let Some(path) = path { match value { Some(bytes) => { tree.insert(path.into(), bytes); },
                None => { tree.remove(path); } } }
        }
    }
}
impl MaterializationBackend for Backend {
    type Error = Interrupted;
    async fn capture(&self, edit: &MaterializationEdit) -> Result<MaterializationPreimage, Interrupted> {
        Ok(encode(&self.image(edit)))
    }
    async fn observe(&self, edit: &MaterializationEdit, pre: &MaterializationPreimage)
        -> Result<MaterializationObservation, Interrupted> {
        let before = decode(pre); Ok(classify(self.image(edit), before.clone(), desired(edit, &before)))
    }
    async fn apply(&self, edit: &MaterializationEdit, pre: &MaterializationPreimage) -> Result<(), Interrupted> {
        self.gate.hit(Event::Mutation, false)?; self.set(edit, desired(edit, &decode(pre)));
        self.counts.lock()[0] += 1; self.gate.hit(Event::Mutation, true)
    }
    async fn restore(&self, edit: &MaterializationEdit, pre: &MaterializationPreimage) -> Result<(), Interrupted> {
        self.gate.hit(Event::Mutation, false)?; self.set(edit, decode(pre));
        self.counts.lock()[1] += 1; self.gate.hit(Event::Mutation, true)
    }
}
// Independent specification uses endpoint symbols, not backend images/receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Endpoint { Original, Target, Foreign }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase { Prepared, Forward, Done, Reverse, Undone }
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Journal { revision: u64, phase: Phase, applied: u8, restored: u8 }
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Model { journal: Option<Journal>, endpoints: [Endpoint;2], mutations: [u16;2] }
impl Default for Model {
    fn default() -> Self { Self { journal: None, endpoints: [Endpoint::Original;2], mutations: [0;2] } }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reply { Ok, Absent, Conflict, Incompatible, External, Lost }
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Action { Forward(Option<Fault>), Reverse(Option<Fault>), Conflict, Diverge(u8), Reopen }
struct ModelFault { fault: Option<Fault>, ordinals: [u8;2], consumed: bool }
impl ModelFault {
    fn hit(&mut self, event: Event, after: bool) -> bool {
        let index = if event == Event::Record { 0 } else { 1 };
        if !after { self.ordinals[index] += 1; }
        if self.fault == Some(Fault { event, ordinal: self.ordinals[index], after }) {
            self.fault = None; self.consumed = true; true
        } else { false }
    }
}
impl Model {
    fn record(&mut self, mut next: Journal, fault: &mut ModelFault) -> bool {
        if fault.hit(Event::Record, false) { return false; }
        next.revision = self.journal.as_ref().map_or(1, |j| j.revision + 1);
        self.journal = Some(next); !fault.hit(Event::Record, true)
    }
    fn mutate(&mut self, index: usize, target: Endpoint, fault: &mut ModelFault) -> bool {
        if fault.hit(Event::Mutation, false) { return false; }
        self.endpoints[index] = target;
        self.mutations[usize::from(target == Endpoint::Original)] += 1;
        !fault.hit(Event::Mutation, true)
    }
    fn step(&mut self, action: Action) -> (Reply, bool) {
        let f = match action { Action::Forward(f) | Action::Reverse(f) => f, _ => None };
        let mut fault = ModelFault { fault: f, ordinals: [0;2], consumed: false };
        let reply = self.run(action, &mut fault); (reply, fault.consumed)
    }
    fn run(&mut self, action: Action, fault: &mut ModelFault) -> Reply {
        match action {
            Action::Reopen => return Reply::Ok,
            Action::Diverge(i) => { self.endpoints[usize::from(i)] = Endpoint::Foreign; return Reply::Ok; },
            Action::Conflict => return Reply::Conflict,
            _ => {}
        }
        let reverse = matches!(action, Action::Reverse(_));
        if self.journal.is_none() {
            if reverse { return Reply::Absent; }
            if !self.record(Journal { revision: 0, phase: Phase::Prepared, applied: 0, restored: 0 }, fault) {
                return Reply::Lost;
            }
        }
        let mut j = self.journal.clone().unwrap();
        if !reverse {
            if j.phase == Phase::Undone { return Reply::Conflict; }
            if j.phase == Phase::Reverse || j.restored != 0 { return Reply::Incompatible; }
            if j.phase == Phase::Done {
                for i in 0..2 {
                    if self.endpoints[i] == Endpoint::Foreign { return Reply::External; }
                    if self.endpoints[i] == Endpoint::Original && !self.mutate(i, Endpoint::Target, fault) { return Reply::Lost; }
                }
                return Reply::Ok;
            }
            j.phase = Phase::Forward;
            if !self.record(j.clone(), fault) { return Reply::Lost; }
            while j.applied < 2 {
                let i = usize::from(j.applied);
                if self.endpoints[i] == Endpoint::Foreign { return Reply::External; }
                if self.endpoints[i] == Endpoint::Original && !self.mutate(i, Endpoint::Target, fault) { return Reply::Lost; }
                j.applied += 1;
                if !self.record(j.clone(), fault) { return Reply::Lost; }
            }
            j.phase = Phase::Done;
        } else {
            if j.phase == Phase::Undone { return Reply::Ok; }
            // The undo set includes a possibly-unrecorded physical forward edit.
            if j.phase == Phase::Forward && j.applied < 2 { j.applied += 1; }
            j.phase = Phase::Reverse;
            if !self.record(j.clone(), fault) { return Reply::Lost; }
            while j.restored < j.applied {
                let i = usize::from(j.applied - j.restored - 1);
                if self.endpoints[i] == Endpoint::Foreign { return Reply::External; }
                if self.endpoints[i] == Endpoint::Target && !self.mutate(i, Endpoint::Original, fault) { return Reply::Lost; }
                j.restored += 1;
                if !self.record(j.clone(), fault) { return Reply::Lost; }
            }
            j.phase = Phase::Undone;
        }
        if self.record(j, fault) { Reply::Ok } else { Reply::Lost }
    }
    fn actions(&self) -> Vec<Action> {
        let mut actions = vec![Action::Reopen];
        // Forward means exact original-plan retry or Complete recovery; reverse
        // cannot be changed back to forward after a rollback decision.
        for reverse in [false, true] {
            actions.push(if reverse { Action::Reverse(None) } else { Action::Forward(None) });
            for (event, count) in [(Event::Record, if reverse { 4 } else { 5 }), (Event::Mutation, 2)] {
                for ordinal in 1..=count { for after in [false, true] {
                    let fault = Some(Fault { event, ordinal, after });
                    actions.push(if reverse { Action::Reverse(fault) } else { Action::Forward(fault) });
                } }
            }
        }
        if self.journal.is_some() {
            actions.push(Action::Conflict);
            for i in 0..2 { actions.push(Action::Diverge(i)); }
        }
        actions
    }
}
fn phase(p: Phase) -> MaterializationPhase { match p {
    Phase::Prepared => MaterializationPhase::Prepared, Phase::Forward => MaterializationPhase::Applying,
    Phase::Done => MaterializationPhase::Applied, Phase::Reverse => MaterializationPhase::RollingBack,
    Phase::Undone => MaterializationPhase::RolledBack,
} }
fn expected_tree(model: &Model) -> BTreeMap<String, Vec<u8>> {
    let mut tree = original();
    match model.endpoints[0] {
        Endpoint::Original => {}, Endpoint::Target => { tree.remove("from"); tree.insert("to".into(), OLD_FROM.to_vec()); },
        Endpoint::Foreign => { tree.insert("to".into(), EXTERNAL.to_vec()); },
    }
    match model.endpoints[1] {
        Endpoint::Original => {}, Endpoint::Target => { tree.remove("remove"); },
        Endpoint::Foreign => { tree.insert("remove".into(), EXTERNAL.to_vec()); },
    }
    tree
}
// Foreign rename endpoint must retain whether the source had already disappeared.
// Divergence is applied to both endpoints so its canonical symbol is lossless.
fn diverge_tree(tree: &mut BTreeMap<String, Vec<u8>>, i: u8) {
    if i == 0 { tree.insert("from".into(), OLD_FROM.to_vec()); tree.insert("to".into(), EXTERNAL.to_vec()); }
    else { tree.insert("remove".into(), EXTERNAL.to_vec()); }
}
async fn replay(trace: &[Action]) -> Result<(usize, usize), String> {
    let gate = Gate::default(); let store = Store { gate: gate.clone(), ..Default::default() };
    let backend = Backend::new(gate.clone());
    let mut engine = JournaledMaterializer::new(store.clone(), backend.clone());
    let mut model = Model::default(); let mut faults = 0; let mut committed_losses = 0;
    for (position, action) in trace.iter().copied().enumerate() {
        // Deletion minimization must not introduce actions outside the bound.
        if matches!(action, Action::Conflict | Action::Diverge(_)) && model.journal.is_none() {
            return Ok((0, 0));
        }
        let (expected_reply, consumed) = model.step(action);
        gate.arm(match action { Action::Forward(f) | Action::Reverse(f) => f, _ => None });
        let result = match action {
            Action::Reopen => { engine = JournaledMaterializer::new(store.clone(), backend.clone()); Ok(None) },
            Action::Diverge(i) => { diverge_tree(&mut backend.tree.lock(), i); Ok(None) },
            Action::Conflict => { let mut p = plan(); p.to = generation(9); engine.apply(p).await.map(Some) },
            Action::Forward(_) => {
                if store.journal.lock().as_ref().is_some_and(|j| j.phase == MaterializationPhase::RollingBack) {
                    engine.recover(plan().operation_id, MaterializationRecovery::Complete).await
                } else { engine.apply(plan()).await.map(Some) }
            },
            Action::Reverse(_) => engine.recover(plan().operation_id, MaterializationRecovery::RollBack).await,
        };
        let reply = match result {
            Ok(None) if matches!(action, Action::Reverse(_)) => Reply::Absent,
            Ok(_) => Reply::Ok,
            Err(MaterializationError::PlanConflict) => Reply::Conflict,
            Err(MaterializationError::IncompatibleJournal) => Reply::Incompatible,
            Err(MaterializationError::ExternalMutation) => Reply::External,
            Err(MaterializationError::Store(_) | MaterializationError::Backend(_)) => Reply::Lost,
            Err(other) => return Err(format!("unexpected engine error {other:?} at {position}")),
        };
        if reply != expected_reply || gate.consumed() != consumed {
            return Err(format!("at {position}: reply {reply:?}/{expected_reply:?}; consumed {}/{}", gate.consumed(), consumed));
        }
        let actual = store.journal.lock().clone();
        let expected = model.journal.as_ref().map(|j| MaterializationJournal {
            version: 1, revision: j.revision, plan: plan(), phase: phase(j.phase),
            applied: u32::from(j.applied), restored: u32::from(j.restored), preimages: vec![
                encode(&[Some(OLD_FROM.to_vec()), Some(OLD_TO.to_vec())]),
                encode(&[Some(OLD_REMOVE.to_vec()), None]),
            ],
        });
        if actual != expected { return Err(format!("at {position}: durable journal {actual:?} != {expected:?}")); }
        let tree = backend.tree.lock().clone();
        if tree != expected_tree(&model) || *backend.counts.lock() != model.mutations {
            return Err(format!("at {position}: physical endpoints/counts differ: {tree:?}, model {model:?}"));
        }
        faults += usize::from(consumed);
        committed_losses += usize::from(reply == Reply::Lost && model.journal.as_ref().is_some_and(|j| j.phase == Phase::Done));
    }
    Ok((faults, committed_losses))
}
async fn checked(trace: &[Action]) -> (usize, usize) {
    match replay(trace).await { Ok(counts) => counts, Err(mut failure) => {
        // Deterministic deletion minimization retains a directly executable trace.
        let mut reduced = trace.to_vec(); let mut i = 0;
        while i < reduced.len() {
            let mut candidate = reduced.clone(); candidate.remove(i);
            if let Err(next) = replay(&candidate).await { reduced = candidate; failure = next; } else { i += 1; }
        }
        let json = serde_json::to_string(&reduced).expect("serialize replay actions");
        let path = std::env::temp_dir().join("omp-materializer-refinement-counterexample.json");
        std::fs::write(&path, &json).expect("persist minimized counterexample");
        panic!("materializer replay {json}\n{failure}\nOMP_MATERIALIZER_REPLAY={}", path.display());
    } }
}
#[tokio::test]
async fn bounded_materializer_refinement() {
    if let Some(path) = std::env::var_os("OMP_MATERIALIZER_REPLAY") {
        let trace: Vec<Action> = serde_json::from_slice(&std::fs::read(path).expect("read replay file")).expect("parse replay actions");
        checked(&trace).await;
        return;
    }
    const DEPTH: usize = 3;
    let mut queue = VecDeque::from([(Model::default(), Vec::new())]);
    let mut seen = HashSet::from([(0usize, Model::default())]);
    let mut edges = 0; let mut consumed = 0; let mut committed_losses = 0;
    while let Some((state, trace)) = queue.pop_front() {
        if trace.len() == DEPTH { continue; }
        for action in state.actions() {
            let mut next = state.clone(); next.step(action);
            let mut path = trace.clone(); path.push(action);
            let counts = checked(&path).await; edges += 1; consumed += counts.0; committed_losses += counts.1;
            if seen.insert((path.len(), next.clone())) { queue.push_back((next, path)); }
        }
    }
    // Longer deterministic property traces, including repeated reply loss/reopen.
    for seed in [1u64, 7, 41, 97, 0x5eed, 0xc0ffee, 0x12345678, 0xdeadbeef] {
        let mut random = seed; let mut state = Model::default(); let mut trace = Vec::new();
        for _ in 0..16 {
            random ^= random << 13; random ^= random >> 7; random ^= random << 17;
            let actions = state.actions(); let action = actions[(random % actions.len() as u64) as usize];
            state.step(action); trace.push(action);
        }
        checked(&trace).await;
    }
    assert!(consumed > 0 && committed_losses > 0, "fault and committed-reply-loss coverage must be nonempty");
    eprintln!("materializer bound: depth={DEPTH}, edits=2; canonical states={}, replayed edges={edges}, consumed faults={consumed}, committed-loss observations={committed_losses}, fixed traces=8x16", seen.len());
}

#[cfg(feature = "local")]
mod physical {
    use super::*;
    use acyclic_fs::LocalCoreStateStore;
    use std::path::{Path, PathBuf};
    struct Files { root: PathBuf, gate: Gate }
    fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, Interrupted> {
        match std::fs::read(path) { Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None), Err(_) => Err(Interrupted) }
    }
    impl Files {
        fn image(&self, edit: &MaterializationEdit) -> Result<Image, Interrupted> {
            let (a,b) = paths(edit); Ok([read_optional(&self.root.join(a))?,
                match b { Some(b) => read_optional(&self.root.join(b))?, None => None }])
        }
    }
    impl MaterializationBackend for Files {
        type Error = Interrupted;
        async fn capture(&self, e: &MaterializationEdit) -> Result<MaterializationPreimage, Interrupted> { Ok(encode(&self.image(e)?)) }
        async fn observe(&self, e: &MaterializationEdit, p: &MaterializationPreimage) -> Result<MaterializationObservation, Interrupted> {
            let pre = decode(p); Ok(classify(self.image(e)?, pre.clone(), desired(e, &pre)))
        }
        async fn apply(&self, e: &MaterializationEdit, _: &MaterializationPreimage) -> Result<(), Interrupted> {
            self.gate.hit(Event::Mutation, false)?;
            match e {
                MaterializationEdit::Rename { from, to } => {
                    // Windows rename does not replace an existing destination.
                    std::fs::remove_file(self.root.join(to)).map_err(|_| Interrupted)?;
                    std::fs::rename(self.root.join(from), self.root.join(to)).map_err(|_| Interrupted)?;
                },
                MaterializationEdit::Remove { path } => { std::fs::remove_file(self.root.join(path)).map_err(|_| Interrupted)?; },
                _ => return Err(Interrupted),
            }
            self.gate.hit(Event::Mutation, true)
        }
        async fn restore(&self, e: &MaterializationEdit, p: &MaterializationPreimage) -> Result<(), Interrupted> {
            self.gate.hit(Event::Mutation, false)?;
            let (a,b) = paths(e); let pre = decode(p);
            for (path, bytes) in [(Some(a), &pre[0]), (b, &pre[1])] {
                if let Some(path) = path {
                    match bytes {
                        Some(bytes) => std::fs::write(self.root.join(path), bytes).map_err(|_| Interrupted)?,
                        None => { if self.root.join(path).exists() { std::fs::remove_file(self.root.join(path)).map_err(|_| Interrupted)?; } },
                    }
                }
            }
            self.gate.hit(Event::Mutation, true)
        }
    }
    // Oracle does not use capture, observe, fingerprints or backend proof output.
    fn disk(root: &Path) -> BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(root).expect("direct directory listing").map(|entry| {
            let entry = entry.expect("entry"); (entry.file_name().into_string().expect("portable name"),
                std::fs::read(entry.path()).expect("direct byte read"))
        }).collect()
    }
    #[tokio::test]
    async fn physical_filesystem_reopen_oracle() {
        for ordinal in 1..=2 { for reverse in [false, true] { for external in [false, true] {
            let temporary = tempfile::tempdir().unwrap(); let root = temporary.path().join("checkout");
            let journals = temporary.path().join("journals"); std::fs::create_dir(&root).unwrap();
            for (name, bytes) in original() { std::fs::write(root.join(name), bytes).unwrap(); }
            let gate = Gate::default(); gate.arm(Some(Fault { event: Event::Mutation, ordinal, after: true }));
            let engine = JournaledMaterializer::new(LocalCoreStateStore::new(&journals), Files { root: root.clone(), gate: gate.clone() });
            assert!(matches!(engine.apply(plan()).await, Err(MaterializationError::Backend(_))));
            assert!(gate.consumed(), "requested physical failure must actually fire");
            let mut expected = original(); expected.remove("from"); expected.insert("to".into(), OLD_FROM.to_vec());
            if ordinal == 2 { expected.remove("remove"); }
            assert_eq!(disk(&root), expected, "physical effect exists before durable progress");
            drop(engine);
            if external {
                // Diverge the *next unrecorded* edit so rollback must not touch it.
                let name = if ordinal == 1 { "to" } else { "remove" };
                std::fs::write(root.join(name), EXTERNAL).unwrap(); expected.insert(name.into(), EXTERNAL.to_vec());
            }
            let fresh_store = LocalCoreStateStore::new(&journals);
            let before = fresh_store.load(plan().operation_id).await.unwrap().unwrap();
            assert_eq!(before.phase, MaterializationPhase::Applying); assert_eq!(before.applied, u32::from(ordinal - 1));
            let fresh_gate = Gate::default();
            if reverse && !external {
                fresh_gate.arm(Some(Fault { event: Event::Mutation, ordinal, after: true }));
            }
            let mut fresh = JournaledMaterializer::new(fresh_store, Files { root: root.clone(), gate: fresh_gate.clone() });
            let direction = if reverse { MaterializationRecovery::RollBack } else { MaterializationRecovery::Complete };
            let mut result = fresh.recover(plan().operation_id, direction).await;
            if reverse && !external {
                assert!(matches!(result, Err(MaterializationError::Backend(_))));
                assert!(fresh_gate.consumed(), "restore mutation failure must actually fire");
                // In either case, crash after the final physical restore but
                // before the journal can record that last restoration.
                assert_eq!(disk(&root), original(), "last physical restore precedes its progress CAS");
                drop(fresh);
                let again = LocalCoreStateStore::new(&journals);
                let interrupted = again.load(plan().operation_id).await.unwrap().unwrap();
                assert_eq!(interrupted.phase, MaterializationPhase::RollingBack);
                assert_eq!(interrupted.restored, u32::from(ordinal - 1));
                fresh = JournaledMaterializer::new(again, Files { root: root.clone(), gate: Gate::default() });
                result = fresh.recover(plan().operation_id, MaterializationRecovery::RollBack).await;
            }
            if external {
                assert!(matches!(result, Err(MaterializationError::ExternalMutation)));
                assert_eq!(disk(&root), expected, "external postimage cannot be overwritten");
            } else {
                let final_journal = result.unwrap().unwrap();
                if reverse {
                    assert_eq!(final_journal.phase, MaterializationPhase::RolledBack); expected = original();
                    assert!(matches!(fresh.apply(plan()).await, Err(MaterializationError::PlanConflict)));
                } else {
                    assert_eq!(final_journal.phase, MaterializationPhase::Applied); expected.remove("remove");
                    let before_retry = disk(&root); let retry = fresh.apply(plan()).await.unwrap();
                    assert_eq!(retry, final_journal); assert_eq!(disk(&root), before_retry);
                }
                assert_eq!(disk(&root), expected);
            }
            assert_eq!(std::fs::read(root.join("outside")).unwrap(), OUTSIDE);
            drop(fresh);
            let reopened = LocalCoreStateStore::new(&journals).load(plan().operation_id).await.unwrap().unwrap();
            assert_eq!(reopened.plan, plan(), "original operation identity survives fresh journal handles");
        } } }
        eprintln!("physical oracle: 2 mutation boundaries x 2 recovery directions x 2 external-write cases = 8 cases, including 2 after-restore crashes; direct std::fs bytes/listing; no power-loss/kernel/FFI claim");
    }
}
