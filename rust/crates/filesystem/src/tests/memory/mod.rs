use super::*;
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
#[cfg(not(target_arch = "wasm32"))]
use crate::facade::test_authority::OpaqueAuthority;
use crate::foundation::Digest;
use crate::storage::FenceOutcome;
use acyclic_stream::OwnedBytes;

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_AUTHORITY_BUDGET: WorkBudget = WorkBudget {
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
    ..WorkCounters::UNCHARGED
};

#[cfg(not(target_arch = "wasm32"))]
async fn native_authority_fixture()
-> Result<(MemoryAuthorityStore, AuthorityId, DurableCommit), Box<dyn std::error::Error>> {
    let store = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0xe1; 16]);
    let budget = NATIVE_AUTHORITY_BUDGET;
    let CreateAuthorityOutcome::Created(head) = ({
        let authority_cancellation = &crate::CancellationToken::new();

        store
            .create_authority(
                id,
                Epoch::GENESIS,
                &mut crate::AuthorityWork::new(budget, authority_cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
    })?
    else {
        return Err("native fixture authority already exists".into());
    };
    let AppendOutcome::Committed(commit) = ({
        let authority_cancellation = &crate::CancellationToken::new();

        store
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id: id,
                    epoch: Epoch::GENESIS,
                    expected: head,
                    commit: ProposedCommit {
                        operation_id: OperationId::from_bytes([0xe2; 16]),
                        fingerprint: Digest::from_bytes([0xe3; 32]),
                        payload: bytes::Bytes::from(vec![0x5a; 8_192]),
                    },
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut crate::AuthorityWork::new(budget, authority_cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, _allocation)| value)
    })?
    else {
        return Err("native fixture did not append".into());
    };
    Ok((store, id, commit))
}

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_OBJECT_BUDGET: WorkBudget = WorkBudget {
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
    ..WorkCounters::UNCHARGED
};

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_object_put_denies_table_allocation_before_backend_write()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::materializer::NativeMaterializationWork;
    let cancellation = crate::CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 1, // Only the original meter's own allocation.
        ..NATIVE_OBJECT_BUDGET
    };
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let store = MemoryObjectStore::default();
    let object = crate::storage::HashedObject::new(
        crate::ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"body"),
    );
    let result = {
        AsyncObjectStore::put_hashed(
            &store,
            object.clone(),
            &mut crate::AuthorityWork::native(&work, &cancellation),
        )
        .await
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
    assert!(!{
        let authority_cancellation = &crate::CancellationToken::new();

        store
            .contains(
                object.object_id(),
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
    }?);
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
    let work = NativeMaterializationWork::new(NATIVE_OBJECT_BUDGET, &cancellation)
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
        AsyncObjectStore::put_hashed(
            &store,
            object,
            &mut crate::AuthorityWork::native(&work, &cancellation),
        )
        .await
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

        AsyncObjectStore::read(
            &*store,
            id,
            4,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
        )
        .await
    }?;
    let reader = NativeMaterializationWork::new(NATIVE_OBJECT_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let reader_before = reader.receipt();
    let native_view = {
        AsyncObjectStore::read(
            &*store,
            id,
            4,
            &mut crate::AuthorityWork::native(&reader, &cancellation),
        )
        .await
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
        let authority_cancellation = &crate::CancellationToken::new();

        AsyncObjectStore::put(
            &*store,
            id,
            OwnedBytes::from_static(b"body"),
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
        )
        .await
    }?;
    assert_eq!(work.live_allocation_bytes(), body_live);
    drop(native_view);
    // Table growth moves the carrier without replacing its payload controls.
    let second = crate::storage::HashedObject::new(
        crate::ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"second"),
    );
    {
        AsyncObjectStore::put_hashed(
            &*store,
            second,
            &mut crate::AuthorityWork::native(&work, &cancellation),
        )
        .await
    }?;
    let resident = {
        let authority_cancellation = &crate::CancellationToken::new();

        AsyncObjectStore::read(
            &*store,
            id,
            4,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
        )
        .await
    }?;
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
#[derive(Debug, thiserror::Error)]
enum OriginalMutationOwnerFailure {
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
    #[error("owner rejection {0}")]
    Owner(u8),
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn opaque_immediate_native_mutations_reject_before_io_and_preserve_ordinary_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::async_storage::AsyncAuthorityStore;
    use crate::materializer::NativeMaterializationWork;
    let store = std::sync::Arc::new(OpaqueAuthority::default());
    let id = AuthorityId::from_bytes([0x99; 16]);
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let prefix = work.receipt();
    let request = native_mutation_request(id, Head::genesis(Epoch::GENESIS));
    assert!(matches!(
        (async {
            AsyncAuthorityStore::create_first_record(
                &store,
                id,
                request.commit.clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
        })
        .await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert!(matches!(
        (async {
            AsyncAuthorityStore::compare_and_append(
                &store,
                request.clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation),
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .map(|(value, allocation)| (value, allocation.expect("native authority allocation")))
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
            AsyncAuthorityStore::compare_and_append(
                &store,
                request.clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation),
                Some(&mut crate::NativeAuthorityGuard((&mut guard).clone())),
            )
            .await
            .map_err(|error| error.into_error(std::convert::Into::into))
            .map(|(value, allocation)| (value, allocation.expect("native authority allocation")))
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
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = AsyncAuthorityStore::create_first_record(
            &store,
            id,
            request.commit,
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

const B: WorkBudget = WorkBudget::UNBOUNDED;
