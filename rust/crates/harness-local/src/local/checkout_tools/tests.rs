//! The task's git publication permit is checked atomically after preparation.
//!
//! The permit gate is the production `acyclic_fs::PermitGate`; this case drives only its
//! synchronisation state with plain loom threads, not the checkout-tools runtime future.

use acyclic_fs::{AuthorityId, Epoch, Head, OperationId, PermitGate, PublicationPermit};
use loom::sync::{Arc, Mutex};

fn operation() -> OperationId {
    OperationId::from_bytes([1; 16])
}

fn permit() -> (PublicationPermit, AuthorityId, Head, u64) {
    let head = Head::genesis(Epoch::GENESIS);
    (
        PublicationPermit::Reservation {
            operation_id: [1; 16],
            gate_tail: 7,
            expected: head,
        },
        AuthorityId::from_bytes([2; 16]),
        head,
        7,
    )
}

#[test]
fn task_git_permit_is_checked_atomically_after_preparation() {
    loom::model(|| {
        let gate = Arc::new(Mutex::new(PermitGate::new(Some(operation()))));
        let (permit, authority, expected, tail) = permit();
        let revoker = {
            let gate = Arc::clone(&gate);
            loom::thread::spawn(move || gate.lock().unwrap().revoke())
        };
        {
            let gate = gate.lock().unwrap();
            assert_eq!(
                gate.permits(permit, authority, expected, tail),
                gate.active().is_some()
            );
        }
        revoker.join().unwrap();
        assert!(
            !gate
                .lock()
                .unwrap()
                .permits(permit, authority, expected, tail)
        );
    });
}
