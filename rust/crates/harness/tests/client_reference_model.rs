//! Independent finite refinement oracle for the public production Client API.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "refinement assertions and persisted counterexamples"
)]
#[path = "support/client/mod.rs"]
mod support;
use acyclic_harness::client::*;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use support::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test_configure!(run_in_browser);

const NAMESPACE: u128 = 71;
const ALPHABET: [Action; 12] = [
    Action::Admit,
    Action::Uncertain,
    Action::CompleteExact,
    Action::CompleteOther,
    Action::Reject,
    Action::Successor,
    Action::Gap,
    Action::ChangedBytesSameIdentity,
    Action::OtherRecord,
    Action::DiscardRoot,
    Action::Expire,
    Action::BeginOriginalOperation,
];

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Action {
    Admit,
    Uncertain,
    CompleteExact,
    CompleteOther,
    Reject,
    Successor,
    Gap,
    ChangedBytesSameIdentity,
    OtherRecord,
    DiscardRoot,
    Expire,
    BeginOriginalOperation,
}

struct GoldenNumbers;
impl GoldenNumbers {
    fn increment(original: u64, delta: u64) -> u64 {
        original.checked_add(delta).unwrap()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct GoldenBranch {
    id: BranchId,
    adapter: u128,
    key: u64,
    operation: Option<u64>,
    basis: Basis,
    assumption: u64,
    dependencies: Vec<Dependency>,
    predicted: u64,
    expires: u64,
    prediction: PredictionOutcome,
    outcome: OperationOutcome,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct GoldenView {
    value: u64,
    basis: Basis,
    branch: Option<BranchId>,
    adapter: Option<u128>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Publication {
    authoritative: bool,
    hypotheses: Vec<BranchId>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct GoldenState {
    namespace: u128,
    adapter: u128,
    configuration: (usize, usize, usize, usize, usize, u64, usize),
    sequence: u64,
    now: u64,
    facts: Vec<(u64, Basis, u64)>,
    branches: Vec<GoldenBranch>,
    publications: Vec<Publication>,
    retained_snapshots: Vec<GoldenView>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Reply {
    Observe(Publication),
    Removed(Vec<BranchId>),
    Begun(BranchId),
}
fn id(sequence: u64) -> BranchId {
    BranchId { namespace: NAMESPACE, sequence }
}
fn terminal(outcome: OperationOutcome) -> bool {
    matches!(outcome, OperationOutcome::Completed | OperationOutcome::Rejected | OperationOutcome::Cancelled)
}
fn eligible(prediction: PredictionOutcome) -> bool {
    matches!(prediction, PredictionOutcome::Pending | PredictionOutcome::Confirmed)
}
impl GoldenState {
    fn initial() -> Self {
        let bounds = limits();
        let facts: Vec<_> = [(1, 10), (2, 20)].into_iter()
            .map(|(key, value)| (key, evidence(key, 0, value, None).basis, value)).collect();
        let branches = [(1, 1, 1, Some(9), vec![]), (2, 2, 2, Some(10), vec![Dependency::prediction(id(1))]),
            (3, 1, 3, Some(11), vec![])].into_iter().map(|(sequence, key, delta, operation, dependencies)| {
                let (_, basis, original) = facts.iter().find(|(k, _, _)| *k == key).unwrap();
                GoldenBranch { id: id(sequence), adapter: 1, key, operation, basis: basis.clone(),
                    assumption: *original, dependencies, predicted: GoldenNumbers::increment(*original, delta),
                    expires: 50, prediction: PredictionOutcome::Pending, outcome: OperationOutcome::Unknown }
            }).collect();
        let retained_snapshots = facts.iter().map(|(_, basis, value)| GoldenView {
            value: *value, basis: basis.clone(), branch: None, adapter: None,
        }).collect();
        Self { namespace: NAMESPACE, adapter: 1,
            configuration: (bounds.records, bounds.branches, bounds.edges, bounds.bytes, bounds.work, bounds.retention, bounds.visible),
            sequence: 3, now: 0, facts, branches, publications: vec![], retained_snapshots }
    }
    fn fingerprint(&self) -> String {
        blake3::hash(&serde_json::to_vec(self).unwrap()).to_hex().to_string()
    }
    fn view(&self, key: u64, overlays: &[BranchId]) -> Result<GoldenView, Error> {
        let (_, basis, value) = self.facts.iter().find(|(k, _, _)| *k == key).ok_or(Error::Missing)?;
        let mut selected = Vec::new();
        for branch in overlays {
            let claim = self.branches.iter().find(|b| b.id == *branch).ok_or(Error::Missing)?;
            if claim.key == key && claim.prediction == PredictionOutcome::Pending { selected.push(claim); }
        }
        match selected.as_slice() {
            [] => Ok(GoldenView { value: *value, basis: basis.clone(), branch: None, adapter: None }),
            [claim] => Ok(GoldenView { value: claim.predicted, basis: claim.basis.clone(), branch: Some(claim.id), adapter: Some(claim.adapter) }),
            _ => Err(Error::Ambiguous),
        }
    }
    fn descendants(&self, roots: &BTreeSet<BranchId>) -> BTreeSet<BranchId> {
        let mut closure = roots.clone();
        loop {
            let previous = closure.len();
            for branch in &self.branches {
                if branch.dependencies.iter().any(|edge| closure.contains(&edge.branch)) { closure.insert(branch.id); }
            }
            if closure.len() == previous { return closure; }
        }
    }
    fn apply(&mut self, action: Action) -> Result<Reply, Error> {
        match action {
            Action::DiscardRoot | Action::Expire => {
                let roots: BTreeSet<_> = if matches!(action, Action::Expire) {
                    self.now = 50;
                    self.branches.iter().filter(|b| b.expires <= self.now).map(|b| b.id).collect()
                } else {
                    if !self.branches.iter().any(|b| b.id == id(1)) { return Err(Error::Missing); }
                    BTreeSet::from([id(1)])
                };
                let affected = self.descendants(&roots);
                for branch in &mut self.branches {
                    if affected.contains(&branch.id) { branch.prediction = PredictionOutcome::Invalidated; }
                }
                self.branches.retain(|b| !roots.contains(&b.id));
                Ok(Reply::Removed(affected.into_iter().collect()))
            }
            Action::BeginOriginalOperation => {
                if self.now >= 50 { return Err(Error::Time); }
                if self.branches.iter().any(|b| b.operation == Some(9)) { return Err(Error::Conflict); }
                let (_, basis, value) = &self.facts[0];
                self.sequence += 1;
                let branch = GoldenBranch { id: id(self.sequence), adapter: 1, key: 1, operation: Some(9), basis: basis.clone(),
                    assumption: *value, dependencies: vec![], predicted: GoldenNumbers::increment(*value, 1), expires: 50,
                    prediction: PredictionOutcome::Pending, outcome: OperationOutcome::Unknown };
                let allocated = branch.id;
                self.branches.push(branch);
                Ok(Reply::Begun(allocated))
            }
            _ => self.observe(&input(self, action).unwrap()),
        }
    }
    fn observe(&mut self, event: &Evidence) -> Result<Reply, Error> {
        let old = self.facts.iter().find(|(key, _, _)| *key == event.key).unwrap();
        if event.publish && (event.basis.content != event.value || event.basis.authority != old.1.authority ||
            event.basis.generation != old.1.generation || event.basis.revision < old.1.revision ||
            event.basis.revision.saturating_sub(old.1.revision) > 1 ||
            (event.basis.revision == old.1.revision && event.basis != old.1)) { return Err(Error::Conflict); }
        let authoritative = event.publish && event.basis != old.1;
        let prior = self.branches.clone();
        let correlation = event.operation.and_then(|(operation, outcome)|
            prior.iter().find(|b| b.operation == Some(operation)).map(|b| (b.id, outcome)));
        let mut resolution = None;
        if let Some((branch_id, incoming)) = correlation {
            let claim = prior.iter().find(|b| b.id == branch_id).unwrap();
            if claim.key != event.key || (terminal(claim.outcome) && claim.outcome != incoming) { return Err(Error::Conflict); }
            let rank = |outcome| match outcome { OperationOutcome::Unknown => 0, OperationOutcome::Admitted => 1,
                OperationOutcome::Indeterminate => 2, _ => 3 };
            let outcome = if rank(incoming) < rank(claim.outcome) { claim.outcome } else { incoming };
            let prediction = if claim.prediction == PredictionOutcome::Invalidated || terminal(claim.outcome) { claim.prediction }
                else { match outcome {
                    OperationOutcome::Completed => if claim.predicted == if event.publish { event.value } else { old.2 } {
                        PredictionOutcome::Confirmed
                    } else { PredictionOutcome::Replaced },
                    OperationOutcome::Rejected | OperationOutcome::Cancelled => PredictionOutcome::Removed,
                    _ => claim.prediction,
                } };
            resolution = Some((branch_id, outcome, prediction));
        }
        let roots: BTreeSet<_> = prior.iter().filter(|claim| {
            let newly_confirmed = resolution.is_some_and(|(branch_id, _, p)| branch_id == claim.id && p == PredictionOutcome::Confirmed && !terminal(claim.outcome));
            let own_basis_lost = authoritative && claim.key == event.key && eligible(claim.prediction) && !newly_confirmed;
            let denied = resolution.is_some_and(|(branch_id, _, p)| branch_id == claim.id &&
                matches!(p, PredictionOutcome::Removed | PredictionOutcome::Replaced) && claim.prediction != p);
            own_basis_lost || denied
        }).map(|b| b.id).collect();
        let invalid = self.descendants(&roots);
        let mut published = BTreeSet::new();
        for branch in &mut self.branches {
            if invalid.contains(&branch.id) && branch.prediction != PredictionOutcome::Invalidated {
                branch.prediction = PredictionOutcome::Invalidated;
                published.insert(branch.id);
            }
            if let Some((branch_id, outcome, mut prediction)) = resolution && branch_id == branch.id {
                if invalid.contains(&branch_id) && eligible(prediction) { prediction = PredictionOutcome::Invalidated; }
                if branch.prediction != prediction || branch.outcome != outcome { published.insert(branch.id); }
                branch.prediction = prediction;
                branch.outcome = outcome;
            }
        }
        if authoritative {
            let fact = self.facts.iter_mut().find(|(key, _, _)| *key == event.key).unwrap();
            *fact = (event.key, event.basis.clone(), event.value);
        }
        let publication = Publication { authoritative, hypotheses: published.into_iter().collect() };
        self.publications.push(publication.clone());
        Ok(Reply::Observe(publication))
    }
}
fn input(state: &GoldenState, action: Action) -> Option<Evidence> {
    Some(match action {
        Action::Admit => status(1, 9, OperationOutcome::Admitted),
        Action::Uncertain => status(1, 9, OperationOutcome::Indeterminate),
        Action::CompleteExact => evidence(1, 1, GoldenNumbers::increment(10, 1), Some((9, OperationOutcome::Completed))),
        Action::CompleteOther => evidence(1, 1, GoldenNumbers::increment(10, 2), Some((9, OperationOutcome::Completed))),
        Action::Reject => status(1, 9, OperationOutcome::Rejected),
        Action::Successor => evidence(1, 2, GoldenNumbers::increment(10, 4), None),
        Action::Gap => evidence(1, 4, GoldenNumbers::increment(10, 7), None),
        Action::ChangedBytesSameIdentity => {
            let (_, basis, value) = &state.facts[0];
            Evidence { key: 1, basis: basis.clone(), value: GoldenNumbers::increment(*value, 1), operation: None, publish: true }
        }
        Action::OtherRecord => evidence(2, 1, GoldenNumbers::increment(20, 2), None),
        _ => return None,
    })
}
fn actual_view(view: View<Basis, u64>) -> GoldenView {
    match view.provenance {
        Provenance::Authoritative(basis) => GoldenView { value: *view.value, basis: (*basis).clone(), branch: None, adapter: None },
        Provenance::Hypothesis { id, basis, adapter } => GoldenView { value: *view.value, basis: (*basis).clone(), branch: Some(id), adapter: Some(adapter) },
    }
}
fn actual_branch(branch: &ClientHypothesis<Numbers>) -> GoldenBranch {
    GoldenBranch { id: branch.id, adapter: branch.adapter, key: branch.key, operation: branch.operation,
        basis: (*branch.basis).clone(), assumption: branch.assumption, dependencies: branch.dependencies.clone(),
        predicted: *branch.predicted, expires: branch.expires, prediction: branch.prediction, outcome: branch.outcome }
}
fn initialized() -> Client<Numbers> {
    let mut client = Client::new(Numbers::default(), NAMESPACE, 0, limits()).unwrap();
    for (key, value) in [(1, 10), (2, 20)] { client.observe(key, &evidence(key, 0, value, None)).unwrap(); }
    for (key, original, delta, operation, dependencies) in [(1, 10, 1, Some(9), vec![]),
        (2, 20, 2, Some(10), vec![id(1)]), (1, 10, 3, Some(11), vec![])] {
        client.begin(request(key, 0, original, GoldenNumbers::increment(original, delta), operation, dependencies)).unwrap();
    }
    client
}
#[derive(Debug, Serialize)]
struct Counterexample {
    actions: Vec<Action>,
    step: usize,
    state_hash: String,
    expected: GoldenState,
    mismatch: String,
}
fn replay(actions: &[Action]) -> Result<GoldenState, Counterexample> {
    let mut client = initialized();
    let mut golden = GoldenState::initial();
    let retained = [client.view(&1, &[]).unwrap(), client.view(&2, &[]).unwrap()];
    let mut actual_publications = Vec::new();
    for (step, action) in actions.iter().copied().enumerate() {
        let before = golden.clone();
        let actual = match action {
            Action::DiscardRoot => client.discard(id(1)).map(Reply::Removed),
            Action::Expire => client.advance(50).map(Reply::Removed),
            Action::BeginOriginalOperation => {
                let (_, basis, value) = &before.facts[0];
                client.begin(request(1, basis.revision, *value, GoldenNumbers::increment(*value, 1), Some(9), vec![])).map(Reply::Begun)
            }
            _ => { let event = input(&before, action).unwrap(); client.observe(event.key, &event).map(|changes| {
                let mut hypotheses = changes.hypotheses;
                hypotheses.sort();
                let publication = Publication { authoritative: changes.authoritative, hypotheses };
                actual_publications.push(publication.clone());
                Reply::Observe(publication)
            }) }
        };
        let expected = golden.apply(action);
        let mut mismatch = None;
        if actual != expected { mismatch = Some(format!("reply expected {expected:?}; actual {actual:?}")); }
        if client.sequence() != golden.sequence { mismatch = Some(format!("sequence expected {}; actual {}", golden.sequence, client.sequence())); }
        for sequence in 1..=golden.sequence.max(3) + 1 {
            let expected = golden.branches.iter().find(|b| b.id == id(sequence)).cloned();
            let actual = client.hypothesis(id(sequence)).map(actual_branch);
            if actual != expected { mismatch = Some(format!("branch {sequence}: expected {expected:?}; actual {actual:?}")); }
        }
        let live: Vec<_> = golden.branches.iter().map(|b| b.id).collect();
        let mut selections = vec![vec![], live.clone(), vec![id(golden.sequence + 1)]];
        for branch in &live { selections.push(vec![*branch]); }
        for left in &live { for right in &live { selections.push(vec![*left, *right]); } }
        for key in [1, 2] { for selection in &selections {
            let expected = golden.view(key, selection);
            let actual = client.view(&key, selection).map(actual_view);
            if actual != expected { mismatch = Some(format!("view key {key}, overlays {selection:?}: expected {expected:?}; actual {actual:?}")); }
        } }
        for (index, snapshot) in retained.iter().enumerate() {
            let expected = &golden.retained_snapshots[index];
            if *snapshot.value != expected.value || snapshot.provenance != Provenance::Authoritative(Arc::new(expected.basis.clone())) {
                mismatch = Some(format!("retained snapshot {index} changed"));
            }
        }
        if actual_publications != golden.publications { mismatch = Some(format!("publication history expected {:?}; actual {actual_publications:?}", golden.publications)); }
        if let Some(mismatch) = mismatch { return Err(Counterexample { actions: actions[..=step].to_vec(), step, state_hash: golden.fingerprint(), expected: golden, mismatch }); }
    }
    Ok(golden)
}
fn checked(actions: &[Action]) -> GoldenState {
    match replay(actions) {
        Ok(state) => state,
        Err(mut failure) => {
            let mut index = 0;
            while index < failure.actions.len() {
                let mut candidate = failure.actions.clone();
                candidate.remove(index);
                if let Err(smaller) = replay(&candidate) { failure = smaller; index = 0; } else { index += 1; }
            }
            let encoded = serde_json::to_string_pretty(&failure).unwrap();
            #[cfg(not(target_arch = "wasm32"))]
            {
                let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../target");
                std::fs::create_dir_all(&directory).unwrap();
                std::fs::write(directory.join("client-reference-counterexample.json"), &encoded).unwrap();
            }
            panic!("client reference counterexample (replay with replay_json_counterexample):\n{encoded}");
        }
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn bounded_complete_client_refinement() {
    let mut layer = vec![vec![]];
    let mut fingerprints = BTreeSet::new();
    let mut prefixes = 0;
    for _ in 0..3 {
        let mut next = Vec::new();
        for prefix in &layer { for action in ALPHABET {
            let mut trace = prefix.clone(); trace.push(action);
            fingerprints.insert(checked(&trace).fingerprint());
            prefixes += 1;
            next.push(trace);
        } }
        layer = next;
    }
    assert_eq!(prefixes, 1_884);
    assert_eq!(layer.len(), 1_728);
    assert!(fingerprints.len() > 12);
    println!("client-reference coverage: leaves=1728 prefixes=1884 distinct_full_state_hashes={} state_pruning=false", fingerprints.len());
}
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn fixed_seed_long_client_refinement() {
    for seed in [1_u64, 7, 17, 31, 71, 101, 257, 65_537] {
        let mut random = seed;
        let trace: Vec<_> = (0..48).map(|_| {
            random ^= random << 13; random ^= random >> 7; random ^= random << 17;
            ALPHABET[(random % ALPHABET.len() as u64) as usize]
        }).collect();
        checked(&trace);
    }
}
#[cfg(not(target_arch = "wasm32"))]
#[test]
#[ignore = "explicit replay utility; set HARNESS_CLIENT_COUNTEREXAMPLE to the persisted JSON path"]
fn replay_json_counterexample() {
    let path = std::env::var("HARNESS_CLIENT_COUNTEREXAMPLE").expect("set HARNESS_CLIENT_COUNTEREXAMPLE");
    let data: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let actions: Vec<Action> = serde_json::from_value(data["actions"].clone()).unwrap();
    checked(&actions);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn checkpoint_original_identity_and_content_are_not_operation_evidence() {
    let original = initialized();
    let encoded = serde_json::to_vec(original.hypothesis(id(1)).unwrap()).unwrap();
    let mut recovered = Client::new(Numbers::default(), NAMESPACE, 3, limits()).unwrap();
    recovered.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let branch: ClientHypothesis<Numbers> = serde_json::from_slice(&encoded).unwrap();
    recovered.restore(branch.clone()).unwrap();
    assert_eq!(actual_branch(recovered.hypothesis(id(1)).unwrap()), GoldenState::initial().branches[0]);
    assert_eq!(recovered.restore(branch), Err(Error::Conflict));
    assert_eq!(recovered.begin(request(1, 0, 10, 12, Some(9), vec![])), Err(Error::Conflict));
    recovered.observe(1, &status(1, 9, OperationOutcome::Indeterminate)).unwrap();
    assert_eq!(recovered.hypothesis(id(1)).unwrap().outcome, OperationOutcome::Indeterminate);
    recovered.observe(1, &evidence(1, 1, GoldenNumbers::increment(10, 1), Some((9, OperationOutcome::Completed)))).unwrap();
    let claim = recovered.hypothesis(id(1)).unwrap();
    assert_eq!((claim.id, claim.operation, claim.prediction, claim.outcome), (id(1), Some(9), PredictionOutcome::Confirmed, OperationOutcome::Completed));
    assert_eq!(actual_view(recovered.view(&1, &[id(1)]).unwrap()), GoldenView { value: 11,
        basis: evidence(1, 1, 11, None).basis, branch: None, adapter: None });
    let mut altered = evidence(1, 1, 11, None); altered.value = 12;
    assert_eq!(recovered.observe(1, &altered), Err(Error::Conflict));
    assert_eq!(*recovered.view(&1, &[]).unwrap().value, 11);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn oracle_negative_controls_change_consumer_visible_state() {
    let expected = checked(&[Action::CompleteExact]);
    let mut wrong = expected.clone(); wrong.facts[0].2 += 1;
    assert_ne!(wrong.view(1, &[]), expected.view(1, &[]));
    let mut wrong = expected.clone(); wrong.branches[0].id = id(99);
    assert_ne!(wrong.branches, expected.branches);
    let mut wrong = expected.clone(); wrong.branches[0].outcome = OperationOutcome::Unknown;
    assert_ne!(wrong.branches, expected.branches);
    let mut wrong = expected.clone(); wrong.publications.clear();
    assert_ne!(wrong.publications, expected.publications);
    assert_ne!(wrong.fingerprint(), expected.fingerprint());
}
