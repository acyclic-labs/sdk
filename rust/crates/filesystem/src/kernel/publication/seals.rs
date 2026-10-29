use super::*;
use std::sync::Arc;

pub(super) struct SealError<E> {
    error: Mutex<Option<E>>,
    _allocation: crate::NativeAllocation,
}

impl<E> SealError<E> {
    pub(super) fn take(&self) -> Option<E> {
        self.error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

pub(super) struct NativeSeal<E> {
    pub(super) seal: combiner::SharedSeal,
    pub(super) error: Arc<SealError<E>>,
}

pub(super) fn shared<E, F, Fut>(
    original: &crate::NativeMaterializationWork,
    factory: F,
) -> Result<NativeSeal<E>, E>
where
    E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    let lifetime = crate::kernel::allocation::retained::AuthorityLifetime::admit(
        &mut KernelAccounting::native(original, original.cancellation()),
    )
    .map_err(E::from)?;
    let invocation = original.clone();
    let original = original
        .authority_publication_view(&lifetime)
        .map_err(crate::FsError::from)
        .map_err(E::from)?;
    let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<SealError<E>>())
        .map_err(|_| E::from(WorkError::Overflow.into()))? as u64;
    let allocation = original
        .claim(bytes, 1, WorkCounters::UNCHARGED)
        .map_err(crate::FsError::from)
        .map_err(E::from)?;
    let error = Arc::new(SealError {
        error: Mutex::new(None),
        _allocation: allocation,
    });
    let slot = Arc::clone(&error);
    let funding = original.clone();
    let owner = api::native_publication_owner(invocation.clone(), factory);
    let callback = move || {
        let mut owner = owner.clone();
        let slot = Arc::clone(&slot);
        let owner_work = invocation.clone();
        let boxed = crate::heap_future::native_future(&original, || async move {
            // Cancellation withdraws this seal even if reply delivery has not
            // yet polled and marked the member withdrawn in the group.
            if owner_work.cancellation().is_cancelled() {
                return Ok(());
            }
            match owner(&owner_work).await {
                Ok(()) => Ok(()),
                Err(_) if owner_work.cancellation().is_cancelled() => Ok(()),
                Err(error) => {
                    *slot
                        .error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                    Err(PublicationError::GroupAborted(GroupAbort::SealFailed).into())
                }
            }
        })
        .map(crate::heap_future::OwnedFuture::erase_send)
        .map_err(crate::FsError::from);
        match boxed {
            Ok(future) => futures::future::Either::Right(future),
            Err(error) => futures::future::Either::Left(std::future::ready(Err(error))),
        }
    };
    let allocation = funding
        .claim(
            std::mem::size_of_val(&callback) as u64,
            1,
            WorkCounters::UNCHARGED,
        )
        .map_err(crate::FsError::from)
        .map_err(E::from)?;
    let allocation = KernelOwnedAllocation::NativeOriginal(allocation);
    Ok(NativeSeal {
        seal: combiner::SharedSeal {
            callback: Box::new(callback),
            _allocation: allocation,
        },
        error,
    })
}
