//! Provider-independent page I/O, using the original admitted read frame.
use super::*;
use crate::heap_future::{AdmittedFrame, FrameObservation, OwnedFuture, TaskFuture};

type ReadFrame<'a, 'observer, T> =
    Result<OwnedFuture<TaskFuture<'a, Result<T, FsError>>, FrameObservation<'observer>>, FsError>;

/// The tree kernels borrow cache policy and admitted reads, never a provider's
/// concrete future. Erasure reuses the existing box and its exact custody.
pub(crate) trait Source: acyclic_stream::ProviderPlatform {
    fn supports_decoded_cache(&self) -> bool;
    fn decoded_cache_lookup(
        &self,
        key: DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError>;
    fn read_one<'a, 'borrow, 'accounting, 'observer>(
        &'a self,
        page: ObjectId,
        maximum_bytes: u64,
        frame: &'a mut AdmittedFrame<'borrow, KernelAccounting<'accounting>>,
        cancellation: &'a CancellationToken,
        observer: Option<&'observer crate::kernel::ProofDataCustody>,
    ) -> ReadFrame<'a, 'observer, (ObjectRead, u64)>
    where
        'observer: 'a;
    fn read_many<'a, 'borrow, 'accounting, 'observer>(
        &'a self,
        requests: &'a [ObjectReadRequest],
        frame: &'a mut AdmittedFrame<'borrow, KernelAccounting<'accounting>>,
        cancellation: &'a CancellationToken,
        observer: Option<&'observer crate::kernel::ProofDataCustody>,
    ) -> ReadFrame<'a, 'observer, ReadOwned<Vec<ObjectRead>>>
    where
        'observer: 'a;
}

impl<S: AsyncObjectStore> Source for S {
    fn supports_decoded_cache(&self) -> bool {
        AsyncObjectStore::supports_decoded_cache(self)
    }

    fn decoded_cache_lookup(
        &self,
        key: DecodedCacheKey,
    ) -> Result<crate::async_storage::DecodedCacheLookup<'_>, ObjectStoreError> {
        AsyncObjectStore::decoded_cache_lookup(self, key)
    }

    fn read_one<'a, 'borrow, 'accounting, 'observer>(
        &'a self,
        page: ObjectId,
        maximum_bytes: u64,
        frame: &'a mut AdmittedFrame<'borrow, KernelAccounting<'accounting>>,
        cancellation: &'a CancellationToken,
        observer: Option<&'observer crate::kernel::ProofDataCustody>,
    ) -> ReadFrame<'a, 'observer, (ObjectRead, u64)>
    where
        'observer: 'a,
    {
        read_in_heap(frame, observer, |accounting| {
            crate::kernel::file_read::read_object_observed(
                self,
                page,
                maximum_bytes,
                accounting,
                cancellation,
                observer,
            )
        })
    }

    fn read_many<'a, 'borrow, 'accounting, 'observer>(
        &'a self,
        requests: &'a [ObjectReadRequest],
        frame: &'a mut AdmittedFrame<'borrow, KernelAccounting<'accounting>>,
        cancellation: &'a CancellationToken,
        observer: Option<&'observer crate::kernel::ProofDataCustody>,
    ) -> ReadFrame<'a, 'observer, ReadOwned<Vec<ObjectRead>>>
    where
        'observer: 'a,
    {
        read_in_heap(frame, observer, |accounting| {
            crate::kernel::file_read::read_objects_with_accounting(
                self,
                requests,
                accounting,
                cancellation,
            )
        })
    }
}

fn read_in_heap<'a, 'borrow, 'accounting, 'observer, F>(
    frame: &'a mut crate::heap_future::AdmittedFrame<'borrow, KernelAccounting<'accounting>>,
    owner: Option<&'observer crate::kernel::ProofDataCustody>,
    create: impl FnOnce(&'a mut KernelAccounting<'accounting>) -> F,
) -> Result<
    crate::heap_future::OwnedFuture<
        crate::heap_future::TaskFuture<'a, F::Output>,
        crate::heap_future::FrameObservation<'observer>,
    >,
    FsError,
>
where
    F: Future + acyclic_stream::ProviderTask + 'a,
{
    #[cfg(not(target_arch = "wasm32"))]
    let owner = if frame.accounting().native_work().is_some() {
        None
    } else {
        owner
    };
    let mut observation = crate::heap_future::FrameObservation { owner, bytes: 0 };
    let (future, _) = crate::heap_future::in_heap_admitted(
        frame,
        |frame, bytes| {
            frame.admit(bytes)?;
            observation.admitted(bytes)
        },
        |frame| create(frame.accounting()),
    )?;
    // Erase the original admitted allocation so both single and batched reads
    // stop propagating provider state into every persistent-tree caller.
    Ok(future.with_allocation(observation).erase_task())
}
