//! Identical native/browser production transition and bounded trace exploration.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions stop on a violated contract"
)]
#[path = "support/client/mod.rs"]
mod support;
use acyclic_harness::client::*;
use std::sync::Arc;
use support::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test_configure!(run_in_browser);

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn confirmed_only_edges_require_correspondence_and_never_revive() {
    let mut client = Client::new(Numbers::default(), 71, 0, limits()).unwrap();
    for key in 1..=3 {
        client.observe(key, &evidence(key, 0, 10, None)).unwrap();
    }
    let root = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    let confirmed_edge = Dependency {
        branch: root,
        requirement: DependencyRequirement::Confirmed,
    };
    let mut child_request = request(2, 0, 10, 12, None, vec![]);
    child_request.dependencies.push(confirmed_edge);
    assert_eq!(client.begin(child_request), Err(Error::Conflict));
    client
        .observe(1, &status(1, 9, OperationOutcome::Indeterminate))
        .unwrap();
    // Reordered admission does not clear uncertainty or establish correspondence.
    client
        .observe(1, &status(1, 9, OperationOutcome::Admitted))
        .unwrap();
    assert_eq!(
        client.hypothesis(root).unwrap().outcome,
        OperationOutcome::Indeterminate
    );
    let mut child_request = request(2, 0, 10, 12, None, vec![]);
    child_request.dependencies.push(confirmed_edge);
    assert_eq!(client.begin(child_request), Err(Error::Conflict));
    client
        .observe(
            1,
            &evidence(1, 1, 11, Some((9, OperationOutcome::Completed))),
        )
        .unwrap();
    let mut child_request = request(2, 0, 10, 12, None, vec![]);
    child_request.dependencies.push(confirmed_edge);
    let child = client.begin(child_request).unwrap();
    let grandchild = client
        .begin(request(3, 0, 10, 13, None, vec![child]))
        .unwrap();
    let duplicate = client
        .observe(
            1,
            &evidence(1, 1, 11, Some((9, OperationOutcome::Completed))),
        )
        .unwrap();
    assert!(!duplicate.authoritative && duplicate.hypotheses.is_empty());
    // Later canonical state invalidates confirmed source correspondence too.
    client
        .observe(
            1,
            &evidence(1, 2, 20, Some((9, OperationOutcome::Completed))),
        )
        .unwrap();
    for id in [root, child, grandchild] {
        assert_eq!(
            client.hypothesis(id).unwrap().prediction,
            PredictionOutcome::Invalidated
        );
    }
    assert_eq!(
        client.hypothesis(root).unwrap().outcome,
        OperationOutcome::Completed
    );
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn resource_and_work_follow_active_demand_not_lifetime() {
    let mut client = Client::new(Numbers::default(), 71, 0, limits()).unwrap();
    let mut resident = 0;
    for revision in 0..10_000 {
        let changes = client
            .observe(1, &evidence(1, revision, revision, None))
            .unwrap();
        assert_eq!(changes.work, 1);
        assert!(changes.authoritative && changes.hypotheses.is_empty());
        if revision == 0 {
            resident = client.residency().0;
        }
        assert_eq!(client.residency(), (resident, 1, 0, 0));
    }
    let root = client
        .begin(request(1, 9999, 9999, 10_000, None, vec![]))
        .unwrap();
    for key in 2..=16 {
        client.observe(key, &evidence(key, 0, 10, None)).unwrap();
        client.begin(request(key, 0, 10, 11, None, vec![])).unwrap();
    }
    let changes = client.observe(1, &evidence(1, 10_000, 77, None)).unwrap();
    assert_eq!(changes.work, 3); // adapter + one keyed root + one closure node
    assert_eq!(changes.hypotheses, vec![root]);
    client.advance(50).unwrap();
    for key in 1..=16 {
        client.release(&key).unwrap();
    }
    assert_eq!(client.residency(), (0, 0, 0, 0));
    let baseline_sequence = client.sequence();
    for _ in 0..1000 {
        client.observe(1, &evidence(1, 0, 10, None)).unwrap();
        let id = client
            .begin(request(1, 0, 10, 11, None, vec![]))
            .unwrap_err();
        assert_eq!(id, Error::Time); // expired absolute expiry, no hidden timer/reset
        client.release(&1).unwrap();
    }
    assert_eq!(client.sequence(), baseline_sequence);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn missing_wrong_authority_generation_operation_and_finite_bounds() {
    let mut bounds = limits();
    bounds.records = 1;
    let mut client = Client::new(Numbers::default(), 71, 0, bounds).unwrap();
    assert_eq!(client.view(&1, &[]).err(), Some(Error::Missing));
    assert_eq!(
        client.begin(request(1, 0, 10, 11, None, vec![])),
        Err(Error::Missing)
    );
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let id = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    let before = client.residency();
    assert_eq!(
        client.observe(2, &evidence(2, 0, 10, None)),
        Err(Error::Budget)
    );
    for field in 0..3 {
        let mut wrong = evidence(1, 1, 11, None);
        match field {
            0 => wrong.basis.authority = 2,
            1 => wrong.basis.generation = 2,
            _ => wrong.basis.content = 12,
        }
        assert_eq!(client.observe(1, &wrong), Err(Error::Conflict));
        assert_eq!(client.residency(), before);
    }
    assert_eq!(
        client.observe(2, &status(2, 9, OperationOutcome::Completed)),
        Err(Error::Conflict)
    );
    assert_eq!(client.view(&1, &vec![id; 9]).err(), Some(Error::Budget));
    let mut expired = request(1, 0, 10, 11, None, vec![]);
    expired.expires = 101;
    assert_eq!(client.begin(expired), Err(Error::Time));
    let mut edge_bounds = limits();
    edge_bounds.edges = 1;
    let mut edges = Client::new(Numbers::default(), 72, 0, edge_bounds).unwrap();
    edges.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let a = edges.begin(request(1, 0, 10, 11, None, vec![])).unwrap();
    let b = edges.begin(request(1, 0, 10, 12, None, vec![])).unwrap();
    assert_eq!(
        edges.begin(request(1, 0, 10, 13, None, vec![a, b])),
        Err(Error::Budget)
    );
    let mut exhausted = Client::new(Numbers::default(), 73, u64::MAX, limits()).unwrap();
    exhausted.observe(1, &evidence(1, 0, 10, None)).unwrap();
    assert_eq!(
        exhausted.begin(request(1, 0, 10, 11, None, vec![])),
        Err(Error::Budget)
    );
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn pure_begin_explicit_view_uncertainty_and_stable_snapshots() {
    let domain = Numbers::default();
    let visits = domain.visits.clone();
    let mut client = Client::new(domain, 71, 0, limits()).unwrap();
    assert_eq!(visits.get(), 0);
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let original = client.view(&1, &[]).unwrap();
    let id = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    assert_eq!(*client.view(&1, &[id]).unwrap().value, 11);
    assert_eq!(*client.view(&1, &[]).unwrap().value, 10);
    for outcome in [OperationOutcome::Admitted, OperationOutcome::Indeterminate] {
        client.observe(1, &status(1, 9, outcome)).unwrap();
        assert_eq!(
            client.hypothesis(id).unwrap().prediction,
            PredictionOutcome::Pending
        );
    }
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    assert!(Arc::ptr_eq(
        &original.value,
        &client.view(&1, &[]).unwrap().value
    ));
    let repeated = client.view(&1, &[]).unwrap();
    match (&original.provenance, &repeated.provenance) {
        (Provenance::Authoritative(a), Provenance::Authoritative(b)) => assert!(Arc::ptr_eq(a, b)),
        _ => panic!("canonical views require authoritative provenance"),
    }
    client
        .observe(
            1,
            &evidence(1, 1, 11, Some((9, OperationOutcome::Completed))),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(id).unwrap().prediction,
        PredictionOutcome::Confirmed
    );
    assert_eq!(*original.value, 10);
    assert!(matches!(
        client.view(&1, &[id]).unwrap().provenance,
        Provenance::Authoritative(_)
    ));
    assert!(
        client
            .observe(
                1,
                &evidence(1, 1, 11, Some((9, OperationOutcome::Completed)))
            )
            .unwrap()
            .hypotheses
            .is_empty()
    );
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn unsupported_correspondence_is_explicit_and_observation_can_resume_after_discard() {
    let mut client = Client::new(Numbers::default(), 71, 0, limits()).unwrap();
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let root = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    let before = client.residency();
    let actual = evidence(1, 1, u64::MAX, Some((9, OperationOutcome::Completed)));
    assert_eq!(client.observe(1, &actual), Err(Error::Unsupported));
    assert_eq!(client.residency(), before);
    assert_eq!(*client.view(&1, &[]).unwrap().value, 10);
    assert_eq!(
        client.hypothesis(root).unwrap().outcome,
        OperationOutcome::Unknown
    );
    client.discard(root).unwrap();
    client.observe(1, &actual).unwrap();
    assert_eq!(*client.view(&1, &[]).unwrap().value, u64::MAX);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn different_results_rejection_and_selective_transitive_invalidation() {
    let mut client = Client::new(Numbers::default(), 71, 0, limits()).unwrap();
    for key in 1..=4 {
        client.observe(key, &evidence(key, 0, 10, None)).unwrap();
    }
    let root = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    let child = client
        .begin(request(2, 0, 10, 12, Some(10), vec![root]))
        .unwrap();
    let grandchild = client
        .begin(request(3, 0, 10, 13, None, vec![child]))
        .unwrap();
    let independent = client.begin(request(4, 0, 10, 14, None, vec![])).unwrap();
    let changes = client
        .observe(
            1,
            &evidence(1, 1, 99, Some((9, OperationOutcome::Completed))),
        )
        .unwrap();
    assert_eq!(changes.hypotheses.len(), 3);
    assert_eq!(
        client.hypothesis(root).unwrap().prediction,
        PredictionOutcome::Replaced
    );
    for id in [child, grandchild] {
        assert_eq!(
            client.hypothesis(id).unwrap().prediction,
            PredictionOutcome::Invalidated
        );
    }
    assert_eq!(
        client.hypothesis(independent).unwrap().prediction,
        PredictionOutcome::Pending
    );
    // Late completion cannot make an invalid dependent available again.
    client
        .observe(
            2,
            &evidence(2, 1, 12, Some((10, OperationOutcome::Completed))),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(child).unwrap().prediction,
        PredictionOutcome::Invalidated
    );
    client.observe(4, &evidence(4, 1, 70, None)).unwrap();
    client
        .observe(1, &status(1, 9, OperationOutcome::Rejected))
        .unwrap_err();
    assert_eq!(*client.view(&4, &[]).unwrap().value, 70);
    assert_eq!(*client.view(&1, &[]).unwrap().value, 99);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn competing_claims_admission_partial_updates_and_cancel_are_honest() {
    let mut client = Client::new(Numbers::default(), 71, 0, limits()).unwrap();
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    client.observe(2, &evidence(2, 0, 20, None)).unwrap();
    let a = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    let b = client
        .begin(request(1, 0, 10, 12, Some(10), vec![]))
        .unwrap();
    assert_eq!(client.view(&1, &[a, b]).err(), Some(Error::Ambiguous));
    client.observe(2, &evidence(2, 1, 21, None)).unwrap();
    client
        .observe(1, &status(1, 9, OperationOutcome::Rejected))
        .unwrap();
    assert_eq!(*client.view(&2, &[]).unwrap().value, 21);
    assert_eq!(
        client.hypothesis(a).unwrap().prediction,
        PredictionOutcome::Removed
    );
    assert_eq!(
        client.hypothesis(b).unwrap().prediction,
        PredictionOutcome::Pending
    );
    client
        .observe(
            1,
            &evidence(1, 1, 15, Some((10, OperationOutcome::Admitted))),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(b).unwrap().prediction,
        PredictionOutcome::Invalidated
    );
    assert_eq!(
        client.hypothesis(b).unwrap().outcome,
        OperationOutcome::Admitted
    );
    client
        .observe(1, &status(1, 10, OperationOutcome::Cancelled))
        .unwrap();
    assert_eq!(
        client.hypothesis(b).unwrap().outcome,
        OperationOutcome::Cancelled
    );
    assert_eq!(*client.view(&1, &[a, b]).unwrap().value, 15);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn stale_bases_cycles_duplicates_export_disposal_and_adapter_pins() {
    let domain = Numbers::default();
    let adapter = domain.adapter.clone();
    let mut client = Client::new(domain, 71, 10, limits()).unwrap();
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let a = client
        .begin(request(1, 0, 10, 11, Some(9), vec![]))
        .unwrap();
    assert_eq!(a.sequence, 11);
    assert_eq!(
        client.begin(request(1, 0, 10, 11, Some(9), vec![])),
        Err(Error::Conflict)
    );
    let absent = BranchId {
        namespace: 71,
        sequence: 12,
    };
    assert_eq!(
        client.begin(request(1, 0, 10, 11, None, vec![absent])),
        Err(Error::Missing)
    );
    assert_eq!(
        client.begin(request(1, 0, 10, 11, None, vec![a, a])),
        Err(Error::Conflict)
    );
    assert_eq!(
        client.begin(request(1, 1, 10, 11, None, vec![])),
        Err(Error::StaleBasis)
    );
    let encoded = serde_json::to_vec(client.hypothesis(a).unwrap()).unwrap();
    let export: Hypothesis<u64, Basis, u64, u64, u64> = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(export.operation, Some(9));
    assert_eq!(export.adapter, 1);
    assert_eq!(export.prediction, PredictionOutcome::Pending);
    assert_eq!(client.release(&1), Err(Error::Conflict));
    let b = client.begin(request(1, 0, 10, 12, None, vec![a])).unwrap();
    client.discard(a).unwrap();
    assert_eq!(
        client.hypothesis(b).unwrap().prediction,
        PredictionOutcome::Invalidated
    );
    assert_eq!(
        client.hypothesis(b).unwrap().outcome,
        OperationOutcome::Unknown
    );
    client.discard(b).unwrap();
    assert_eq!(client.residency().2, 0);
    assert_eq!(client.residency().3, 0);
    let c = client.begin(request(1, 0, 10, 13, None, vec![])).unwrap();
    assert!(c.sequence > b.sequence);
    adapter.set(2);
    assert_eq!(
        client.begin(request(1, 0, 10, 13, None, vec![])),
        Err(Error::Conflict)
    );
    assert_eq!(
        client.observe(1, &evidence(1, 1, 13, None)),
        Err(Error::Conflict)
    );
    client.discard(c).unwrap();
    client.release(&1).unwrap();
    assert_eq!(client.residency(), (0, 0, 0, 0));
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn every_budget_failure_is_atomic_and_expiry_is_bounded() {
    let mut bounds = limits();
    bounds.branches = 2;
    bounds.edges = 1;
    bounds.work = 8;
    let mut client = Client::new(Numbers::default(), 71, 0, bounds).unwrap();
    client.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let canonical_bytes = client.residency().0;
    let a = client.begin(request(1, 0, 10, 11, None, vec![])).unwrap();
    let b = client.begin(request(1, 0, 10, 12, None, vec![a])).unwrap();
    let before = client.residency();
    assert_eq!(
        client.begin(request(1, 0, 10, 13, None, vec![])),
        Err(Error::Budget)
    );
    assert_eq!(client.residency(), before);
    client.advance(50).unwrap();
    assert!(client.hypothesis(a).is_none() && client.hypothesis(b).is_none());
    assert_eq!(client.residency().0, canonical_bytes);
    assert_eq!(client.advance(49), Err(Error::Time));
    let mut bytes = limits();
    bytes.bytes = 1;
    let mut tiny = Client::new(Numbers::default(), 71, 0, bytes).unwrap();
    assert_eq!(
        tiny.observe(1, &evidence(1, 0, 10, None)),
        Err(Error::Budget)
    );
    assert_eq!(tiny.residency(), (0, 0, 0, 0));
    let mut work = limits();
    work.work = 2;
    work.visible = 2;
    let mut bounded = Client::new(Numbers::default(), 71, 0, work).unwrap();
    bounded.observe(1, &evidence(1, 0, 10, None)).unwrap();
    let id = bounded.begin(request(1, 0, 10, 11, None, vec![])).unwrap();
    assert_eq!(
        bounded.observe(1, &evidence(1, 1, 12, None)),
        Err(Error::Budget)
    );
    assert_eq!(*bounded.view(&1, &[]).unwrap().value, 10);
    assert_eq!(
        bounded.hypothesis(id).unwrap().prediction,
        PredictionOutcome::Pending
    );
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn bounded_evidence_permutations_missing_duplicate_delayed_and_conflicting() {
    // All 4^4 traces through production transitions; three concurrent hypotheses.
    // Domain receipt validation is assumed; stale/gapped revisions explicitly fail.
    for encoded in 0..256usize {
        let mut client = Client::new(Numbers::default(), 71, 0, limits()).unwrap();
        client.observe(1, &evidence(1, 0, 10, None)).unwrap();
        client.observe(2, &evidence(2, 0, 20, None)).unwrap();
        let root = client
            .begin(request(1, 0, 10, 11, Some(9), vec![]))
            .unwrap();
        let child = client
            .begin(request(2, 0, 20, 21, None, vec![root]))
            .unwrap();
        let independent = client.begin(request(2, 0, 20, 22, None, vec![])).unwrap();
        let mut trace = encoded;
        let mut canonical = 10;
        for _ in 0..4 {
            let event = match trace % 4 {
                0 => status(1, 9, OperationOutcome::Indeterminate),
                1 => evidence(1, 1, 11, Some((9, OperationOutcome::Completed))),
                2 => evidence(1, 1, 12, Some((9, OperationOutcome::Completed))),
                _ => status(1, 9, OperationOutcome::Rejected),
            };
            trace /= 4;
            let before = client.residency();
            match client.observe(1, &event) {
                Ok(_) => {
                    if event.publish {
                        canonical = event.value;
                    }
                }
                Err(_) => {
                    assert_eq!(client.residency(), before);
                }
            }
            assert_eq!(*client.view(&1, &[]).unwrap().value, canonical);
            assert_eq!(
                client.hypothesis(independent).unwrap().prediction,
                PredictionOutcome::Pending
            );
            if matches!(
                client.hypothesis(root).unwrap().prediction,
                PredictionOutcome::Removed
                    | PredictionOutcome::Replaced
                    | PredictionOutcome::Invalidated
            ) {
                assert_eq!(
                    client.hypothesis(child).unwrap().prediction,
                    PredictionOutcome::Invalidated
                );
            }
        }
        let before = client.residency();
        assert_eq!(
            client.observe(1, &evidence(1, 4, 99, None)),
            Err(Error::Conflict)
        );
        assert_eq!(client.residency(), before);
        assert_eq!(*client.view(&2, &[]).unwrap().value, 20);
    }
}
