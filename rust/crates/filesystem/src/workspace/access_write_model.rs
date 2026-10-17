//! Models the production admission predicate and authority CAS together.
use super::checkout::authorization_changed;
use crate::{
    AppendOutcome, AsyncAuthorityStore, AuthorityId, AuthorityWork, CancellationToken, Digest,
    Epoch, GenerationId, GuardedAppend, Head, MemoryAuthorityStore, OperationId, ProposedCommit,
    PublicationPermit, WorkBudget,
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

pub(super) const AUTHORITY: AuthorityId = AuthorityId::from_bytes([1; 16]);

pub(super) fn ready<F: Future>(create: impl FnOnce() -> F) -> F::Output {
    match crate::heap_future::in_heap(create)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("the in-process model backend must not yield"),
    }
}

pub(super) fn head(store: &MemoryAuthorityStore) -> Head {
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    ready(|| store.head(AUTHORITY, &mut work)).unwrap()
}

pub(super) fn publish(store: &MemoryAuthorityStore, expected: Head, operation: u8) -> bool {
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    matches!(
        ready(|| store.compare_and_append(
            GuardedAppend {
                authority_id: AUTHORITY,
                epoch: expected.epoch,
                expected,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([operation; 16]),
                    fingerprint: Digest::from_bytes([operation; 32]),
                    payload: bytes::Bytes::from_static(b"generation"),
                },
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>
        ))
        .unwrap(),
        AppendOutcome::Committed(_)
    )
}

fn model(check_base: bool) {
    loom::model(move || {
        // The real backend mutex uses crate::sync. The bounded model removes
        // filesystem codecs, not the admission check or either CAS lock phase.
        let store = loom::sync::Arc::new(MemoryAuthorityStore::default());
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        ready(|| store.create_authority(AUTHORITY, Epoch::GENESIS, &mut work, None::<&mut ()>))
            .unwrap();
        let authorized = head(&store);
        let revocation = loom::sync::Arc::clone(&store);
        let revoker = loom::thread::spawn(move || publish(&revocation, authorized, 2));
        let observed = head(&store);
        // Only identity equality reaches the production admission predicate.
        // Commit digests name the two distinct generations in this model.
        let stale = authorization_changed(
            GenerationId::new(authorized.digest),
            GenerationId::new(observed.digest),
        );
        // Negative control reproduces the old Exact constructor's missing fence.
        let published = (!check_base || !stale) && publish(&store, observed, 3);
        let revoked = revoker
            .join()
            .expect("revocation completes without deadlock");
        assert!(published || revoked, "neither publication made progress");
        assert_eq!(
            head(&store).sequence.get(),
            u64::from(published) + u64::from(revoked),
            "authority publication was torn or lost"
        );
        assert!(
            !(published && revoked),
            "write crossed its authorization generation"
        );
    });
}

#[test]
fn access_write_model_fences_revocation_before_and_after_transaction_admission() {
    model(true);
}

#[test]
#[should_panic(expected = "write crossed its authorization generation")]
fn access_write_model_detects_an_exact_root_bound_to_a_newer_head() {
    model(false);
}
