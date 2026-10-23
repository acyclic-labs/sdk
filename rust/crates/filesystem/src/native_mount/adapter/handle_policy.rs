use super::*;

pub(super) trait HandlePolicy: Send + Sync {
    fn clock(&self) -> &super::super::clock::Clock;
    fn check(
        &self,
        permissions: &crate::access::PinnedPath,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError>;
}

struct CurrentPolicy<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    view: Arc<crate::access::AccessView>,
    cancellation: CancellationToken,
    runtime: CallbackRuntime,
    _allocation: crate::NativeAllocation,
}

impl<A, O> CurrentPolicy<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn check_with_meter(
        &self,
        permissions: &crate::access::PinnedPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let current = self
            .view
            .current()
            .map_err(|_| MountSourceError::AccessDenied)?;
        if !std::ptr::eq(current.as_ref(), permissions.policy_view()) {
            return Err(MountSourceError::AccessDenied);
        }
        drop(current);
        invoke(&self.runtime, work, || async {
            let revision =
                access::observe_current_policy(&self.checkout, &self.cancellation, work).await?;
            if !self
                .view
                .confirms_disclosure(
                    permissions.policy_view(),
                    Some(revision),
                    access::access_time(&self.runtime.clock)?,
                )
                .map_err(|_| MountSourceError::AccessDenied)?
            {
                return Err(MountSourceError::AccessDenied);
            }
            NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))
        })
    }
}

impl<A, O> HandlePolicy for CurrentPolicy<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn clock(&self) -> &super::super::clock::Clock {
        &self.runtime.clock
    }

    fn check(
        &self,
        permissions: &crate::access::PinnedPath,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError> {
        match work {
            Some(work) => self.check_with_meter(permissions, work),
            None => boundary(&self.cancellation, |work| {
                self.check_with_meter(permissions, work)
            }),
        }
    }
}

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(in crate::native_mount) fn authorize_file(
        &self,
        file: (Arc<dyn MountOpenFile>, u64),
        permissions: crate::access::PinnedPath,
        file_id: FileId,
        created: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let view = self
            .access_view
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?;
        let allocation = work.claim(
            (size_of::<CurrentPolicy<A, O>>() + 2 * size_of::<usize>()) as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let policy = Arc::new(CurrentPolicy {
            checkout: self.checkout.clone(),
            view: view.clone(),
            cancellation: self.cancellation.clone(),
            runtime: self.runtime.clone(),
            _allocation: allocation,
        });
        authorized_file::authorize_file(file, permissions, policy, file_id, created, work)
    }
}
