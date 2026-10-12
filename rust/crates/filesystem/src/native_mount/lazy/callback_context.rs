use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn capture_host_with_native_work(
        &self, source_root: &Path, paths: &[MountPath], expected_root_identity: NativeRootIdentity,
        scope: crate::native_mount::adapter::HostCaptureScope,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        original.admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        self.authored.cancellation().check().map_err(|_| MountSourceError::Stale)?;
        if paths.is_empty() { return Ok(()); }
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(original);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                for path in paths {
                    carrier.with_work(|work| mount_items(work, 1))?;
                    self.promote_parents_with_carrier(path, owner, carrier).await?;
                    match self.promote_with_carrier(path, owner, carrier).await {
                        Ok(()) | Err(MountSourceError::NotFound) => {}
                        Err(error) => return Err(error),
                    }
                }
                self.authored.capture_host(
                    source_root, paths, expected_root_identity, scope, None, original,
                ).await
            })
        })
    }

pub(in crate::native_mount::lazy) async fn source_read_lease_with_carrier(
        &self, owner: crate::native_mount::view_gate::CallbackOwner, expected: Option<u64>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<SourceViewLease, MountSourceError> {
        let cancellation = carrier.cancellation();
        self.source_view.read_for_callback_with_carrier(
            owner, expected, carrier, &cancellation, Some(self.authored.cancellation()),
        ).await
    }

pub(in crate::native_mount::lazy) async fn source_mutation_lease_with_carrier(
        &self, expected: Option<u64>, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<crate::native_mount::view_gate::ViewMutationPhase, MountSourceError> {
        let cancellation = carrier.cancellation();
        self.source_view.write_stable_with_carrier(
            expected, carrier, &cancellation, Some(self.authored.cancellation()),
        ).await.map(crate::native_mount::view_gate::ViewWriteLease::into_mutation_phase)
    }

pub(in crate::native_mount::lazy) fn flush_with_carrier(
        &self, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let cancellation = carrier.cancellation();
                carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                if !self.authored.shared_checkout().publishes_at_native_boundary_with_carrier(
                    carrier, &cancellation, Some(self.authored.cancellation()),
                ).await? { return Ok(()); }
                self.sync_async_with_permit_carrier_guarded(
                    crate::PublicationPermit::Unrestricted, carrier.reborrow(), &cancellation,
                    &mut self.authored.sdk_publication_policy(),
                ).await
            })
        })
    }

pub(in crate::native_mount::lazy) fn fence_changes_with_carrier(
        &self, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        if self.authored.cancellation().is_cancelled() { return Err(MountSourceError::Stale); }
        let Some(watch) = &self.source_watch else { return Ok(()); };
        let cancellation = carrier.cancellation();
        let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
        let result = crate::demand::fence_watch(&**watch, budget, &cancellation);
        carrier.with_work(|work| mount_accept(work, result, mount_demand_error))?;
        if self.authored.cancellation().is_cancelled() { return Err(MountSourceError::Stale); }
        Ok(())
    }

pub(in crate::native_mount::lazy) fn acquire_binding_lease_with_carrier(
        &self, expected: Option<u64>, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let mut lease = self.source_read_lease_with_carrier(owner, expected, carrier).await?;
                let bytes = std::mem::size_of::<SourceViewLease>() as u64;
                let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::default() };
                carrier.with_work(|work| {
                    work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
                    work.allocate(bytes).map_err(mount_demand_error)?;
                    if let Err(error) = lease.attach_owned_payload(bytes) {
                        work.release(bytes).map_err(mount_demand_error)?;
                        return Err(error);
                    }
                    Ok(())
                })?;
                let lease = Box::new(lease) as Box<dyn Send>;
                carrier.with_work(|work| work.record(copied)).map_err(mount_demand_error)?;
                Ok(lease)
            })
        })
    }

pub(in crate::native_mount::lazy) fn native_attributes<T>(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(Option<Arc<CheckoutDetachedFile<A, O>>>, bool) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        let owner = SourceViewGate::callback_owner();
        let (lease, detached, projected) = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
                let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
                let resolution = self.resolve(path, &text, owner, carrier).await?;
                let detached = self.content_detached_with_carrier(&resolution, &text, carrier).await?;
                let projected = match resolution {
                    Resolution::Absent => return Err(MountSourceError::NotFound),
                    Resolution::Authored(_) => false,
                    Resolution::Unauthored(LazyLookup::Source(_), _, _) => true,
                    Resolution::Unauthored(_, _, _) if detached.is_some() => false,
                    Resolution::Unauthored(_, _, _) => return Err(MountSourceError::Unsupported(
                        "named attributes of an unpromoted shadow are not projected".to_owned())),
                };
                Ok((lease, detached, projected))
            })
        })?;
        let result = operation(detached, projected);
        drop(lease);
        result
    }

pub(in crate::native_mount::lazy) fn native_call<T>(&self, operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, MountSourceError>)
        -> Result<T, MountSourceError> {
        if let Some(work) = self.authored.native_original_work()? { return operation(&work); }
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation())
            .map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        operation(&work)
    }

pub(in crate::native_mount::lazy) fn native_mutation<T>(
        &self, path: &MountPath, source: Option<&MountPath>, promote: bool, rebound: bool,
        work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(Option<Arc<CheckoutDetachedFile<A, O>>>) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(work);
        let owner = SourceViewGate::callback_owner();
        let (lease, text, detached) = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
                let detached = if promote {
                    let resolution = self.resolve(path, &text, owner, carrier).await?;
                    self.content_detached_with_carrier(&resolution, &text, carrier).await?
                } else { None };
                if let Some(source) = source { self.promote_with_carrier(source, owner, carrier).await?; }
                if promote {
                    if detached.is_none() { self.promote_with_carrier(path, owner, carrier).await?; }
                } else { self.promote_parents_with_carrier(path, owner, carrier).await?; }
                Ok((lease, text, detached))
            })
        })?;
        let original = work.clone();
        let delta = if rebound {
            carrier.with_work(|work| {
                mount_items(work, 1)?;
                let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                let removed = removals.paths.contains_key(text.value.as_str());
                Ok::<_, MountSourceError>(crate::WorkCounters {
                    items_examined: 1 + u64::from(removed), ..crate::WorkCounters::UNCHARGED
                })
            })?
        } else { crate::WorkCounters::UNCHARGED };
        let mut completion = original.reserve_work(delta).map_err(MountSourceError::NativeWork)?;
        if rebound { self.cursors.retire(&mut carrier)?; }
        let result = operation(detached);
        let result = match result {
            Ok(value) if rebound => {
                let restored = self.record_rebound(&text);
                let recorded = completion.record_completed(delta).map_err(MountSourceError::NativeWork);
                restored.and(recorded).map(|()| value)
            }
            result => result,
        };
        let bytes = text.legacy_owned_bytes();
        drop(text);
        drop(lease);
        let released = carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error);
        match result { Ok(value) => released.map(|()| value), Err(error) => Err(error) }
    }
}
