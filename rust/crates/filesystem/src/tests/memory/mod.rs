use super::*;
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::foundation::Digest;
use crate::storage::FenceOutcome;
use acyclic_stream::OwnedBytes;

#[cfg(not(target_arch = "wasm32"))]
fn native_authority_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_records_appended: 32,
        authority_bytes_read: 131_072,
        authority_bytes_written: 131_072,
        backend_read_operations: 32,
        backend_write_operations: 32,
        bytes_hashed: 131_072,
        bytes_copied: 131_072,
        items_examined: 128,
        allocation_operations: 32,
        peak_allocation_bytes: 131_072,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_authority_fixture()
-> Result<(MemoryAuthorityStore, AuthorityId, DurableCommit), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0xe1; 16]);
    let budget = native_authority_budget();
    let CreateAuthorityOutcome::Created(head) = ({
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work = crate::AuthorityWork::new(budget, authority_cancellation);
        let __authority_result = store
            .create_authority(id, Epoch::GENESIS, &mut __authority_work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?
    .value
    else {
        return Err("native fixture authority already exists".into());
    };
    let AppendOutcome::Committed(commit) = ({
        let authority_input_3 = ProposedCommit {
            operation_id: OperationId::from_bytes([0xe2; 16]),
            fingerprint: Digest::from_bytes([0xe3; 32]),
            payload: bytes::Bytes::from(vec![0x5a; 8_192]),
        };
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __authority_work = crate::AuthorityWork::new(budget, authority_cancellation);
        let __authority_result = store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: authority_input_3,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })?
    .value
    else {
        return Err("native fixture did not append".into());
    };
    Ok((store, id, commit))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_object_budget() -> WorkBudget {
    WorkBudget {
        object_probes: 128,
        object_bytes_read: 131_072,
        object_bytes_written: 131_072,
        bytes_hashed: 131_072,
        bytes_copied: 131_072,
        backend_read_operations: 32,
        backend_write_operations: 32,
        allocation_operations: 32,
        peak_allocation_bytes: 131_072,
        items_examined: 128,
        items_returned: 128,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_object_put_denies_table_allocation_before_backend_write()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::materializer::NativeMaterializationWork;
    let cancellation = crate::CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 1, // Only the original meter's own allocation.
        ..native_object_budget()
    };
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let store = MemoryObjectStore::default();
    let object = crate::storage::HashedObject::new(
        crate::ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"body"),
    );
    let result = {
        let authority_input_0 = object.clone();

        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result =
            AsyncObjectStore::put_hashed(&store, authority_input_0, &mut __object_work).await;
        __object_result
    };
    assert!(matches!(
        result,
        Err(ObjectStoreError::Native(
            crate::materializer::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(work.receipt().bytes_hashed, 0);
    assert_eq!(work.receipt().object_bytes_written, 0);
    assert!(
        !{
            let authority_input_0 = object.object_id();
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.contains(authority_input_0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    assert!(
        store
            .objects
            .read()
            .map_err(|_| "object table lock poisoned")?
            .objects
            .is_empty()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_object_intrinsic_payload_lives_until_last_resident_alias()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncObjectStore;
    use crate::materializer::NativeMaterializationWork;
    use std::sync::Arc;
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_object_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes();
    let mut allocation = work.claim(4, 1, crate::WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(4)?;
    allocation.resize(bytes.capacity() as u64)?;
    bytes.extend_from_slice(b"body");
    let object = crate::storage::HashedObject::from_native_vec(
        crate::ObjectKind::BlobChunk,
        &mut bytes,
        &mut allocation,
        &work,
        &cancellation,
    )?;
    assert!(bytes.is_empty());
    assert_eq!(allocation.bytes(), 0);
    assert_eq!(
        work.receipt().bytes_hashed,
        4 + OBJECT_DIGEST_ENVELOPE_BYTES
    );
    let id = object.object_id();
    let store = Arc::new(MemoryObjectStore::default());
    (async {
        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result =
            AsyncObjectStore::put_hashed(&store, object, &mut __object_work).await;
        __object_result
    })
    .await?;
    assert_eq!(
        work.receipt().bytes_hashed,
        4 + OBJECT_DIGEST_ENVELOPE_BYTES
    );
    assert_eq!(work.receipt().backend_write_operations, 1);
    assert_eq!(work.receipt().object_bytes_written, 4);
    let body_live = work.live_allocation_bytes();
    let view = {
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = AsyncObjectStore::read(&*store, id, 4, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }?
    .value;
    let reader = NativeMaterializationWork::new(native_object_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let reader_before = reader.receipt();
    let native_view = {
        let mut __object_work = crate::AuthorityWork::native(&reader, &cancellation);
        let __object_result = AsyncObjectStore::read(&*store, id, 4, &mut __object_work).await;
        __object_result
    }?;
    assert_eq!(
        reader.receipt().allocation_operations,
        reader_before.allocation_operations
    );
    assert_eq!(work.live_allocation_bytes(), body_live);
    assert_eq!(native_view.bytes.as_ptr(), view.bytes.as_ptr());
    let alias_before = work.receipt();
    let alias = native_view.bytes.clone();
    assert_eq!(work.receipt(), alias_before);
    assert_eq!(alias.as_ptr(), view.bytes.as_ptr());
    {
        let authority_input_1 = OwnedBytes::from_static(b"body");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = // A duplicate ordinary input leaves the original resident carrier intact.
    AsyncObjectStore::put(&*store, id, authority_input_1, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(work.live_allocation_bytes(), body_live);
    drop(native_view);
    // Table growth moves the carrier without replacing its payload controls.
    let second = crate::storage::HashedObject::new(
        crate::ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"second"),
    );
    {
        let mut __object_work = crate::AuthorityWork::native(&work, &cancellation);
        let __object_result =
            AsyncObjectStore::put_hashed(&*store, second, &mut __object_work).await;
        __object_result
    }?;
    let resident = {
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = AsyncObjectStore::read(&*store, id, 4, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }?
    .value;
    assert_eq!(resident.bytes.as_ptr(), alias.as_ptr());
    drop(resident);
    // Removing the map's alias cannot release custody retained by either read.
    let mut state = store
        .objects
        .write()
        .map_err(|_| "object table lock poisoned")?;
    let index = state
        .objects
        .binary_search_by_key(&id, |(id, _)| *id)
        .map_err(|_| "resident object is missing")?;
    drop(state.objects.remove(index));
    drop(state);
    assert!(work.live_allocation_bytes() > baseline);
    drop(store);
    assert!(work.live_allocation_bytes() < body_live);
    assert!(work.live_allocation_bytes() > baseline);
    assert_eq!(view.bytes.as_ref(), b"body");
    assert_eq!(alias.as_ptr(), view.bytes.as_ptr());
    drop(view);
    assert!(work.live_allocation_bytes() > baseline);
    drop(alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_mutation_request(
    authority: AuthorityId,
    expected: Head,
) -> crate::storage::GuardedAppend {
    crate::storage::GuardedAppend {
        authority_id: authority,
        epoch: expected.epoch,
        expected,
        commit: ProposedCommit {
            operation_id: OperationId::from_bytes([0x91; 16]),
            fingerprint: Digest::from_bytes([0x92; 32]),
            payload: bytes::Bytes::from_static(b"canonical configuration bytes"),
        },
        permit: crate::storage::PublicationPermit::Unrestricted,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
enum OriginalMutationOwnerFailure {
    Authority(AuthorityStoreError),
    Owner(u8),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<AuthorityStoreError> for OriginalMutationOwnerFailure {
    fn from(error: AuthorityStoreError) -> Self {
        Self::Authority(error)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for OriginalMutationOwnerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authority(error) => std::fmt::Display::fmt(error, formatter),
            Self::Owner(code) => write!(formatter, "owner rejection {code}"),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::error::Error for OriginalMutationOwnerFailure {}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct OpaqueImmediateAuthority(MemoryAuthorityStore);

#[cfg(not(target_arch = "wasm32"))]
impl crate::AsyncAuthorityStore for OpaqueImmediateAuthority {
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
        let value = crate::AsyncAuthorityStore::create_authority(
            &self.0,
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
    ) -> Result<crate::AppendOutcome, crate::AuthorityError<G::Error>>
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
        crate::AsyncAuthorityStore::compare_and_append(&self.0, request, work, None::<&mut ()>)
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
        crate::AsyncAuthorityStore::head(&self.0, authority, work).await
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
        crate::AsyncAuthorityStore::replay(&self.0, authority, after, limit, work).await
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
        crate::AsyncAuthorityStore::fence(&self.0, authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        Option<crate::DurableCommit>,
        crate::AuthorityStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        crate::AsyncAuthorityStore::find_operation(&self.0, authority, operation, work).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn opaque_immediate_native_mutations_reject_before_io_and_preserve_ordinary_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(OpaqueImmediateAuthority::default());
    let id = AuthorityId::from_bytes([0x99; 16]);
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(native_authority_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let prefix = work.receipt();
    let request = native_mutation_request(id, Head::genesis(Epoch::GENESIS));
    assert!(matches!(
        (async {
            let authority_input_1 = request.commit.clone();

            let mut __authority_work = crate::AuthorityWork::native(&work, &cancellation);
            let __authority_result = AsyncAuthorityStore::create_first_record(
                &store,
                id,
                authority_input_1,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
            __authority_result
        })
        .await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        (async {
            let authority_input_0 = request.clone();

            let mut __authority_work = crate::AuthorityWork::native(&work, &cancellation);
            let __authority_result = AsyncAuthorityStore::compare_and_append(
                &store,
                authority_input_0,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
            __authority_result
        })
        .await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = called.clone();
    let mut guard = move |_: &NativeMaterializationWork| {
        callback_called.store(true, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(matches!(
        (async {
            let authority_input_0 = request.clone();
            let authority_input_3 = &mut guard;

            let mut __authority_work = crate::AuthorityWork::native(&work, &cancellation);
            let __authority_result = AsyncAuthorityStore::compare_and_append(
                &store,
                authority_input_0,
                &mut __authority_work,
                Some(&mut crate::NativeAuthorityGuard(
                    (authority_input_3).clone(),
                )),
            )
            .await
            .map_err(|error| error.into_error(std::convert::Into::into));
            __authority_result
        })
        .await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(work.receipt(), prefix);
    assert!(matches!(
        ({
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __authority_result =
                AsyncAuthorityStore::head(store.as_ref(), id, &mut __authority_work).await;
            __authority_work.receipt(__authority_result)
        }),
        Err(AuthorityFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    let ordinary = (async {
        let authority_input_1 = request.commit;

        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = AsyncAuthorityStore::create_first_record(
            &store,
            id,
            authority_input_1,
            &mut __authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(ordinary.value);
    assert_eq!(ordinary.work.backend_write_operations, 2);
    Ok(())
}

mod authority;
mod native_publication;
mod objects;
