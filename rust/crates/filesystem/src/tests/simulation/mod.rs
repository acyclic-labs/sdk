use super::*;
use crate::foundation::Digest;
use crate::storage::{AuthorityResult, FenceOutcome, ObjectKind};
use crate::{
    AuthorityFailure, AuthorityWork, CancellationToken, ObjectResult, WorkBudget, WorkError,
};
use acyclic_stream::OwnedBytes;
use bytes::Bytes;
const B: WorkBudget = WorkBudget::UNBOUNDED;

macro_rules! work {
    ($store:expr, $method:ident($($arg:expr),*), $budget:expr, $cancel:expr) => {
        async {
            let mut work = AuthorityWork::new($budget, $cancel);
            let result = work!(@call $store, $method($($arg),*), work);
            work.receipt(result)
        }
    };
    (@call $store:expr, create_authority($id:expr, $epoch:expr), $work:ident) => {
        $store.create_authority($id, $epoch, &mut $work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity))
    };
    (@call $store:expr, compare_and_append($id:expr, $epoch:expr, $head:expr, $commit:expr), $work:ident) => {
        $store.compare_and_append(crate::GuardedAppend {
            authority_id: $id, epoch: $epoch, expected: $head, commit: $commit, permit: crate::PublicationPermit::Unrestricted,
        }, &mut $work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity))
    };
    (@call $store:expr, replay($($arg:expr),*), $work:ident) => {
        $store.replay($($arg,)* &mut $work).await.map(|(records, _)| records)
    };
    (@call $store:expr, read_many($($arg:expr),*), $work:ident) => {
        $store.read_many($($arg,)* &mut $work).await.map(|(values, _)| values)
    };
    (@call $store:expr, $method:ident($($arg:expr),*), $work:ident) => {
        $store.$method($($arg,)* &mut $work).await
    };
}
fn object(bytes: &[u8]) -> (ObjectId, OwnedBytes) {
    let bytes = OwnedBytes::copy_from_slice(bytes);
    (
        ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        },
        bytes,
    )
}

fn proposal(operation: u8, fingerprint: u8) -> ProposedCommit {
    ProposedCommit {
        publication: None,
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload: Bytes::from(vec![operation, fingerprint]),
    }
}

fn assert_authority_before_work<T>(
    result: AuthorityResult<T>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Err(failure) = result else {
        return Err("simulated authority operation unexpectedly succeeded".into());
    };
    assert!(if cancelled {
        matches!(failure.error, AuthorityStoreError::Cancelled)
    } else {
        matches!(failure.error, AuthorityStoreError::RejectedStatic(_))
    });
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

async fn exercise_authority_before_work(
    fault: Option<SimulationFault>,
) -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    if let Some(fault) = fault {
        for operation in [
            SimulationOperation::AuthorityCreate,
            SimulationOperation::AuthorityHead,
            SimulationOperation::AuthorityAppend,
            SimulationOperation::AuthorityReplay,
            SimulationOperation::AuthorityFence,
            SimulationOperation::AuthorityFindOperation,
        ] {
            simulation.schedule(ScheduledSimulationFault {
                operation,
                occurrence: 1,
                fault,
            })?;
        }
    }
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    if fault.is_none() {
        cancellation.cancel();
    }
    let authority_id = AuthorityId::from_bytes([41; 16]);
    let operation_id = OperationId::from_bytes([42; 16]);
    let head = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([43; 32]),
    };
    let cancelled = fault.is_none();
    assert_authority_before_work(
        (work!(
            authority,
            create_authority(authority_id, Epoch::GENESIS),
            B,
            &cancellation
        ))
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (work!(authority, head(authority_id), B, &cancellation)).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (work!(
            authority,
            compare_and_append(authority_id, Epoch::GENESIS, head, proposal(44, 45)),
            B,
            &cancellation
        ))
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (work!(
            authority,
            replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 64,
                }
            ),
            B,
            &cancellation
        ))
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (work!(authority, fence(authority_id, head), B, &cancellation)).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (work!(
            authority,
            find_operation(authority_id, operation_id),
            B,
            &cancellation
        ))
        .await,
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

fn assert_object_before_work<T>(
    result: ObjectResult<T>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Err(failure) = result else {
        return Err("simulated object operation unexpectedly succeeded".into());
    };
    assert!(if cancelled {
        matches!(failure.error, ObjectStoreError::Cancelled)
    } else {
        matches!(
            failure.error,
            ObjectStoreError::Rejected(_)
                | ObjectStoreError::Simulation("scheduled rejection" | "scheduled partition")
        )
    });
    if cancelled {
        assert_eq!(*failure.work, WorkCounters::default());
    }
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    assert_eq!(failure.work.durability_operations, 0);
    Ok(())
}

async fn exercise_object_before_work(
    fault: Option<SimulationFault>,
) -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    if let Some(fault) = fault {
        for operation in [
            SimulationOperation::ObjectPut,
            SimulationOperation::ObjectRead,
            SimulationOperation::ObjectReadMany,
            SimulationOperation::ObjectContains,
        ] {
            simulation.schedule(ScheduledSimulationFault {
                operation,
                occurrence: 1,
                fault,
            })?;
        }
    }
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    if fault.is_none() {
        cancellation.cancel();
    }
    let (object_id, bytes) = object(b"fault-matrix");
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 64,
    }];
    let cancelled = fault.is_none();
    assert_object_before_work(
        (work!(objects, put(object_id, bytes), B, &cancellation)).await,
        cancelled,
    )?;
    assert_object_before_work(
        (work!(objects, read(object_id, 64), B, &cancellation)).await,
        cancelled,
    )?;
    assert_object_before_work(
        (work!(objects, read_many(&requests), B, &cancellation)).await,
        cancelled,
    )?;
    assert_object_before_work(
        (work!(objects, contains(object_id), B, &cancellation)).await,
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

mod authority;
mod faults;
mod native_authority;
mod objects;
