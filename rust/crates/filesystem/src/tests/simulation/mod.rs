use super::*;
#[cfg(not(target_arch = "wasm32"))]
use crate::facade::test_authority::OpaqueAuthority;

use crate::foundation::Digest;

use crate::storage::{FenceOutcome, ObjectKind};

use crate::{AuthorityFailure, AuthorityResult, WorkError};

use acyclic_stream::OwnedBytes;

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
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload: bytes::Bytes::from(vec![operation, fingerprint]),
    }
}

fn assert_authority_before_work<T>(
    result: Result<T, AuthorityStoreError>,
    work: &crate::AuthorityWork<'_>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Err(error) = result else {
        return Err("simulated authority operation unexpectedly succeeded".into());
    };
    assert!(if cancelled {
        matches!(error, AuthorityStoreError::Cancelled)
    } else {
        matches!(error, AuthorityStoreError::RejectedStatic(_))
    });
    assert_eq!(work.spent(), WorkCounters::default());
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
    let cancelled = fault.is_none();
    if cancelled {
        cancellation.cancel();
    }
    let id = AuthorityId::from_bytes([41; 16]);
    let operation = OperationId::from_bytes([42; 16]);
    let head = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([43; 32]),
    };
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert_authority_before_work(
        authority
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        &work,
        cancelled,
    )?;
    assert_authority_before_work(authority.head(id, &mut work).await, &work, cancelled)?;
    assert_authority_before_work(
        authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: proposal(44, 45),
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity)),
        &work,
        cancelled,
    )?;
    assert_authority_before_work(
        authority
            .replay(
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 64,
                },
                &mut work,
            )
            .await,
        &work,
        cancelled,
    )?;
    assert_authority_before_work(authority.fence(id, head, &mut work).await, &work, cancelled)?;
    assert_authority_before_work(
        authority.find_operation(id, operation, &mut work).await,
        &work,
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

fn assert_object_before_work<T>(
    result: Result<T, ObjectStoreError>,
    work: &crate::AuthorityWork<'_>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Err(error) = result else {
        return Err("simulated object operation unexpectedly succeeded".into());
    };
    assert!(if cancelled {
        matches!(error, ObjectStoreError::Cancelled)
    } else {
        matches!(error, ObjectStoreError::Rejected(_))
    });
    assert_eq!(work.spent(), WorkCounters::default());
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
    let cancelled = fault.is_none();
    if cancelled {
        cancellation.cancel();
    }
    let (id, bytes) = object(b"fault-matrix");
    let requests = [ObjectReadRequest {
        object_id: id,
        maximum_bytes: 64,
    }];
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert_object_before_work(objects.put(id, bytes, &mut work).await, &work, cancelled)?;
    assert_object_before_work(objects.read(id, 64, &mut work).await, &work, cancelled)?;
    assert_object_before_work(
        objects.read_many(&requests, &mut work).await,
        &work,
        cancelled,
    )?;
    assert_object_before_work(objects.contains(id, &mut work).await, &work, cancelled)?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_authority_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_024,
        authority_records_appended: 1_024,
        authority_bytes_read: 1_048_576,
        authority_bytes_written: 1_048_576,
        backend_read_operations: 1_024,
        backend_write_operations: 1_024,
        durability_operations: 1_024,
        bytes_hashed: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        items_examined: 1_024,
        items_returned: 1_024,
        allocation_operations: 1_024,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn original_proposal(
    operation: u8,
    fingerprint: u8,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<ProposedCommit, Box<dyn std::error::Error>> {
    let mut allocation = original.claim(2, 1, crate::WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(2)?;
    original.charge(WorkCounters {
        bytes_copied: 2,
        ..WorkCounters::UNCHARGED
    })?;
    bytes.extend_from_slice(&[operation, fingerprint]);
    cancellation.check()?;
    allocation.reconcile_capacity(bytes.capacity() as u64)?;
    let hash = WorkCounters {
        bytes_hashed: bytes.len() as u64 + crate::storage::OBJECT_DIGEST_ENVELOPE_BYTES,
        ..WorkCounters::UNCHARGED
    };
    original.admit(hash)?;
    let _digest = crate::storage::object_digest(ObjectKind::BlobChunk, &bytes);
    original.record_admitted(hash)?;
    let control_bytes =
        crate::kernel::allocation::bytes_owner_control_bytes::<acyclic_stream::OwnedBytes>()?;
    let control = original.claim(
        control_bytes,
        u64::from(control_bytes != 0),
        WorkCounters::UNCHARGED,
    )?;
    let payload = acyclic_stream::OwnedBytes::from_vec_with_guard(
        bytes,
        (allocation, control),
        original.runtime_meter(),
    )
    .map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
            crate::NativeTreeMaterializationError::InvalidLayout
        }
    })?;
    let payload = bytes::Bytes::from_owner(payload);
    Ok(ProposedCommit {
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn original_append(
    id: AuthorityId,
    expected: Head,
    commit: ProposedCommit,
) -> crate::GuardedAppend {
    crate::GuardedAppend {
        authority_id: id,
        epoch: expected.epoch,
        expected,
        commit,
        permit: crate::PublicationPermit::Unrestricted,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
enum SimulationOwnerFailure {
    Authority(AuthorityStoreError),
    Stopped(u64),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<AuthorityStoreError> for SimulationOwnerFailure {
    fn from(error: AuthorityStoreError) -> Self {
        Self::Authority(error)
    }
}

mod authority;
mod faults;
mod native_authority;
mod objects;
