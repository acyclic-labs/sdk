use super::*;

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
        (async {
            let __authority_args = (
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .create_authority(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity));
            __authority_work.receipt(__authority_result)
        })
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async {
            let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation);
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
            let __authority_result = authority
                .head(__authority_args.0, &mut __authority_work)
                .await;
            __authority_work.receipt(__authority_result)
        })
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async {
            let __authority_args = (
                authority_id,
                Epoch::GENESIS,
                head,
                proposal(44, 45),
                WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.4, __authority_args.5);
            let __authority_result = authority
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: __authority_args.0,
                        epoch: __authority_args.1,
                        expected: __authority_args.2,
                        commit: __authority_args.3,
                        permit: crate::PublicationPermit::Unrestricted,
                    },
                    &mut __authority_work,
                    None::<&mut ()>,
                )
                .await
                .map_err(|error| error.into_error(std::convert::identity))
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async {
            let __authority_args = (
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 64,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.3, __authority_args.4);
            let __authority_result = authority
                .replay(
                    __authority_args.0,
                    __authority_args.1,
                    __authority_args.2,
                    &mut __authority_work,
                )
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async {
            let __authority_args = (authority_id, head, WorkBudget::UNBOUNDED, &cancellation);
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .fence(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                )
                .await;
            __authority_work.receipt(__authority_result)
        })
        .await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async {
            let __authority_args = (
                authority_id,
                operation_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
            let __authority_result = authority
                .find_operation(
                    __authority_args.0,
                    __authority_args.1,
                    &mut __authority_work,
                )
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })
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
        matches!(failure.error, ObjectStoreError::Rejected(_))
    });
    assert_eq!(*failure.work, WorkCounters::default());
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
        (objects.put(object_id, bytes, WorkBudget::UNBOUNDED, &cancellation)).await,
        cancelled,
    )?;
    assert_object_before_work(
        (objects.read(object_id, 64, WorkBudget::UNBOUNDED, &cancellation)).await,
        cancelled,
    )?;
    assert_object_before_work(
        (objects.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation)).await,
        cancelled,
    )?;
    assert_object_before_work(
        (objects.contains(object_id, WorkBudget::UNBOUNDED, &cancellation)).await,
        cancelled,
    )?;
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
struct OpaqueOriginalAuthority {
    inner: crate::MemoryAuthorityStore,
    ordinary_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::AsyncAuthorityStore for OpaqueOriginalAuthority {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let value = crate::AsyncAuthorityStore::create_authority(
            &self.inner,
            authority,
            epoch,
            work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        if request.permit != crate::PublicationPermit::Unrestricted {
            return Err(crate::AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::compare_and_append(&self.inner, request, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
    }
    async fn head(
        &self,
        authority: crate::AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::Head, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::head(&self.inner, authority, work).await
    }
    async fn replay(
        &self,
        authority: crate::AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::replay(&self.inner, authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: crate::AuthorityId,
        expected: crate::Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::fence(&self.inner, authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        (Option<crate::DurableCommit>, crate::AuthorityAllocation),
        crate::AuthorityStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::AsyncAuthorityStore::find_operation(&self.inner, authority, operation, work).await
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
