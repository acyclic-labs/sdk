//! The permit gate: a reservation is admitted only while it is the active operation.

use super::PermitGate;
use crate::{AuthorityId, Epoch, Head, OperationId, PublicationPermit};
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
fn reservation_is_admitted_only_while_active() {
    loom::model(|| {
        let gate = Arc::new(Mutex::new(PermitGate::new(None)));
        let owner = {
            let gate = Arc::clone(&gate);
            loom::thread::spawn(move || gate.lock().unwrap().activate(operation()))
        };
        let (permit, authority, expected, tail) = permit();
        {
            let gate = gate.lock().unwrap();
            assert_eq!(
                gate.permits(permit, authority, expected, tail),
                gate.active().is_some()
            );
        }
        owner.join().unwrap();
        assert!(
            gate.lock()
                .unwrap()
                .permits(permit, authority, expected, tail)
        );
    });
}

#[test]
fn revoked_reservation_is_refused() {
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
