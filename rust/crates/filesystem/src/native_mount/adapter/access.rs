use super::*;
use crate::access::{AccessView, Action, PinnedPath};

pub(super) async fn observe_current_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &SharedCheckout<A, O>,
    cancellation: &CancellationToken,
    work: &crate::NativeMaterializationWork,
) -> Result<(crate::GenerationId, crate::kernel::MetadataField<ObjectId>), MountSourceError> {
    crate::access::observe_policy(work.cancellation(), cancellation, || async {
        let observed = checkout
            .volume
            .current_access_policy_revision_accounted(
                &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                work.cancellation(),
            )
            .await
            .map_err(MountSourceError::from)?;
        #[cfg(test)]
        if super::tests::return_fence::skip_retry() && observed.is_none() {
            return Err(MountSourceError::AccessDenied);
        }
        // Retry only the accounted policy observation, never the data.
        Ok(observed)
    })
    .await
    .map_err(|error| match error {
        crate::access::RetryError::CallerCancelled => MountSourceError::Cancelled,
        crate::access::RetryError::MountCancelled => MountSourceError::Stale,
        crate::access::RetryError::Observation(error) => error,
    })
}

pub(in crate::native_mount) fn access_time(
    clock: &super::super::clock::Clock,
) -> Result<u64, MountSourceError> {
    #[cfg(test)]
    if let Some(now) = super::tests::return_fence::override_unix_seconds() {
        return Ok(now);
    }
    clock
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .map_err(|_| MountSourceError::AccessDenied)
}

pub(in crate::native_mount) fn require(
    path: &PinnedPath,
    action: Action,
    now: u64,
) -> Result<(), MountSourceError> {
    if path.decide(action, now).allowed {
        return Ok(());
    }
    let mode = path.mount_mode(now);
    if matches!(
        action,
        Action::Create | Action::Write | Action::Append | Action::Delete | Action::Rename
    ) && mode.read_only()
        && (mode.visible() || mode.allows(Action::Read))
    {
        Err(MountSourceError::ReadOnly)
    } else {
        Err(MountSourceError::AccessDenied)
    }
}

pub(in crate::native_mount) fn require_open(
    path: &PinnedPath,
    access: Option<MountOpenAccess>,
    clock: &super::super::clock::Clock,
) -> Result<(), MountSourceError> {
    let now = access_time(clock)?;
    if let Some(access) = access {
        require(path, Action::List, now)?;
        if matches!(access, MountOpenAccess::Read | MountOpenAccess::ReadWrite) {
            require(path, Action::Read, now)?;
        }
        if matches!(access, MountOpenAccess::Write | MountOpenAccess::ReadWrite)
            && !path.decide(Action::Append, now).allowed
        {
            require(path, Action::Write, now)?;
        }
        Ok(())
    } else if [Action::List, Action::Read, Action::Write, Action::Append]
        .into_iter()
        .any(|action| path.decide(action, now).allowed)
    {
        Ok(())
    } else {
        Err(MountSourceError::AccessDenied)
    }
}

impl<A, O> CheckoutMountSource<A, O> {
    pub(in crate::native_mount) fn mutation_deadline(
        &self,
    ) -> Result<Option<u64>, MountSourceError> {
        Ok(self
            .access_view
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?
            .current()
            .map_err(|_| MountSourceError::AccessDenied)?
            .valid_until())
    }

    /// Bind the authenticated policy/capability view used by every callback.
    pub fn with_access_view(mut self, view: Arc<AccessView>) -> Result<Self, NativeMountError> {
        let current = view.current().map_err(|_| MountSourceError::AccessDenied)?;
        if current.config() != self.checkout.config
            || uuid::Uuid::try_parse(current.volume())
                .map(|id| id.into_bytes())
                .ok()
                != Some(self.checkout.volume_id.into_bytes())
        {
            return Err(MountSourceError::AccessDenied.into());
        }
        drop(current);
        self.access_view = Some(view);
        Ok(self)
    }

    /// Reload an authenticated policy view after its persisted fence changes.
    pub fn with_access_refresh(
        mut self,
        refresh: Option<Arc<dyn super::super::NativeAccessOwner>>,
    ) -> Self {
        self.access_refresh = refresh;
        self
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    /// Return/admission boundary for native disclosures.
    pub(in crate::native_mount) async fn disclose<T>(
        &self,
        view: &crate::access::PathTrie,
        work: &crate::NativeMaterializationWork,
        read: impl Future<Output = Result<T, MountSourceError>>,
    ) -> Result<T, MountSourceError> {
        let result = read.await;
        #[cfg(test)]
        if super::tests::return_fence::omitted() {
            return result;
        }
        let revision = observe_current_policy(&self.checkout, &self.cancellation, work).await?;
        let valid = self
            .access_view
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?
            .confirms_disclosure(view, Some(revision), access_time(&self.runtime.clock)?)
            .map_err(|_| MountSourceError::AccessDenied)?;
        if !valid {
            return Err(MountSourceError::AccessDenied);
        }
        NativeDemandCarrier::Native(work).check(work.cancellation(), Some(&self.cancellation))?;
        result
    }

    pub(super) async fn require_snapshot(
        &self,
        permissions: &crate::access::PathTrie,
        checkout: &mut crate::Checkout<A, O>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let generation = checkout.generation_id();
        if let Some(selected) = permissions.data_generation() {
            return if selected == generation {
                Ok(())
            } else {
                Err(MountSourceError::AccessDenied)
            };
        }
        let revision = checkout
            .access_policy_revision(work, work.cancellation())
            .await?;
        if !permissions.matches_policy_revision(generation, revision) {
            return Err(MountSourceError::AccessDenied);
        }
        Ok(())
    }

    pub(in crate::native_mount) async fn access_snapshot(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<crate::access::PathTrie>, MountSourceError> {
        let published = self
            .access_view
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?;
        let mut view = published
            .current()
            .map_err(|_| MountSourceError::AccessDenied)?;
        let mut head = self
            .checkout
            .volume
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::read_only_pinned(),
                work,
                work.cancellation(),
            )
            .await?;
        let mut revision = head
            .access_policy_revision(work, work.cancellation())
            .await?;
        let expired = if let Some(expiry) = view.valid_until() {
            access_time(&self.runtime.clock)? >= expiry
        } else {
            false
        };
        if !view.matches_policy_revision(head.generation_id(), revision) || expired {
            let refresh = self
                .access_refresh
                .as_ref()
                .ok_or(MountSourceError::AccessDenied)?;
            // Authentication and compilation run outside every checkout/view lock.
            let future = refresh.check(work).map_err(publication_error)?;
            publication_result(future.await)?;
            view = published
                .current()
                .map_err(|_| MountSourceError::AccessDenied)?;
            head = self
                .checkout
                .volume
                .checkout(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::read_only_pinned(),
                    work,
                    work.cancellation(),
                )
                .await?;
            revision = head
                .access_policy_revision(work, work.cancellation())
                .await?;
        }
        if !view.matches_policy_revision(head.generation_id(), revision) {
            return Err(MountSourceError::AccessDenied);
        }
        Ok(view)
    }

    pub(in crate::native_mount) async fn pin_access(
        &self,
        path: &NamespacePath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<PinnedPath, MountSourceError> {
        self.access_snapshot(work)
            .await?
            .pin(path)
            .map_err(|_| MountSourceError::AccessDenied)
    }

    pub(super) async fn require_access(
        &self,
        path: &NamespacePath,
        action: Action,
        work: &crate::NativeMaterializationWork,
    ) -> Result<PinnedPath, MountSourceError> {
        let pin = self.pin_access(path, work).await?;
        require(&pin, action, access_time(&self.runtime.clock)?)?;
        Ok(pin)
    }
}

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(in crate::native_mount) fn disclose_result<T>(
        &self,
        view: &crate::access::PathTrie,
        result: Result<T, MountSourceError>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<T, MountSourceError> {
        invoke(&self.runtime, work, || {
            self.disclose(view, work, std::future::ready(result))
        })
    }

    pub(in crate::native_mount) fn access_snapshot_with_meter(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<crate::access::PathTrie>, MountSourceError> {
        invoke(&self.runtime, work, || self.access_snapshot(work))
    }

    pub(super) fn require_paths(
        &self,
        requests: &[(Action, &MountPath)],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let published = self
                .access_view
                .as_ref()
                .ok_or(MountSourceError::AccessDenied)?;
            // An authenticated refresh owner must observe policy publication
            // even when the previous revision denied the request. Bindings
            // without an owner can only change through explicit publication.
            let view = if self.access_refresh.is_some() {
                self.access_snapshot(work).await?
            } else {
                published
                    .current()
                    .map_err(|_| MountSourceError::AccessDenied)?
            };
            let now = access_time(&self.runtime.clock)?;
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            for (action, path) in requests {
                inputs.items(1)?;
                let path = self.namespace_inputs(path, &mut inputs)?;
                let permissions = view
                    .pin(&path)
                    .map_err(|_| MountSourceError::AccessDenied)?;
                require(&permissions, *action, now)?;
            }
            // Explicitly published denials need no host probe. Potentially
            // allowing requests still fence against persisted revocation.
            let current = if self.access_refresh.is_some() {
                Arc::clone(&view)
            } else {
                self.access_snapshot(work).await?
            };
            if !Arc::ptr_eq(&current, &view) {
                let now = access_time(&self.runtime.clock)?;
                for (action, path) in requests {
                    let path = self.namespace_inputs(path, &mut inputs)?;
                    let permissions = current
                        .pin(&path)
                        .map_err(|_| MountSourceError::AccessDenied)?;
                    require(&permissions, *action, now)?;
                }
            }
            self.disclose(&current, work, std::future::ready(Ok(())))
                .await
        })
    }

    pub(in crate::native_mount) fn permissions_for_path(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<PinnedPath, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            self.pin_access(&path, work).await
        })
    }
}

#[cfg(any(windows, test))]
impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(in crate::native_mount) fn projection_lease(
        &self,
        writable: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Box<dyn Send + Sync>, NativeMountError> {
        if !self.checkout.volume.fs.has_process_policy_owner() {
            return Err(NativeMountError::Lifecycle(
                "ProjFS requires a process-owned policy authority",
            ));
        }
        let lease = crate::access::projection::enter(
            self.checkout.volume.fs.operation_claims(),
            self.checkout.volume.id,
        )
        .map_err(MountSourceError::from)?;
        let view = self.access_snapshot_with_meter(work)?;
        crate::native_mount::actions::require_projfs_mode(
            &view,
            writable,
            access_time(&self.runtime.clock)?,
        )
        .map_err(|error| match error {
            super::super::MountLifecycleError::NativePolicy(reason) => {
                NativeMountError::Lifecycle(reason)
            }
            _ => NativeMountError::Lifecycle("ProjFS policy admission failed"),
        })?;
        let custody = work
            .claim(
                std::mem::size_of::<(crate::access::projection::Lease, crate::NativeAllocation)>()
                    as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        Ok(Box::new((lease, custody)))
    }
}

pub(super) fn visible_name(
    parent: crate::access::PreparedPath<'_>,
    name: &LogicalName,
    now: u64,
) -> Result<bool, MountSourceError> {
    Ok(parent
        .child_name(name)
        .map_err(|_| MountSourceError::AccessDenied)?
        .mount_mode(now)
        .visible())
}

pub(in crate::native_mount) fn require_list<'a>(
    view: &'a crate::access::PathTrie,
    path: &crate::kernel::NamespacePath,
    clock: &super::super::clock::Clock,
) -> Result<(crate::access::PreparedPath<'a>, u64), MountSourceError> {
    let access = view
        .prepare_namespace(path)
        .map_err(|_| MountSourceError::AccessDenied)?;
    let now = access_time(clock)?;
    if !access.decide(Action::List, now).allowed {
        return Err(MountSourceError::AccessDenied);
    }
    Ok((access, now))
}
