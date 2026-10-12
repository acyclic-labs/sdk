use super::*;

pub(in crate::native_mount::lazy) async fn stage_mount_promotion_with_carrier<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    path: &str,
    mounted: &MountPath,
    expected_source: FileId,
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let mut work = MountSyncWork::new(authored, carrier.reborrow())?;
    let cancellation = work.meter.cancellation();
    for _ in 0..MAXIMUM_PROMOTION_RETRIES {
        work.meter.with_work(|meter| mount_items(meter, 1))?;
        authored.cancellation().check().map_err(|_| MountSourceError::Stale)?;
        let (mut candidate, epoch, stamp) = authored.shared_checkout().prepare_native_phase_with_carrier(
            &mut work.meter, &cancellation, Some(authored.cancellation()),
        ).await?;
        let accounting = lazy_context(&mut work.meter, &cancellation)?;
        let result = lazy.stage_into_checkout(
            &mut candidate.checkout, None, path, expected_source, MAXIMUM_PROMOTION_BYTES, &accounting,
        ).await;
        let changed = lazy_result(&mut work.meter, &accounting, result)?;
        if work.meter.original_native_work().is_none() {
            work.meter.with_work(|work| work.retain_checkout_control(
                &candidate.checkout.private_control_custody(),
            )).map_err(mount_demand_error)?;
        }
        if !changed { return Ok(()); }
        let mut scratch = DirectoryCarrierScratch::new(&mut work.meter);
        let installed = promotion_effect_with_carrier(
            lazy, authored, &candidate, path, mounted, scratch.carrier,
        ).await?;
        scratch.track(installed.legacy_owned_bytes())?;
        let installed_root = authored.shared_checkout().finish_phase(
            candidate, epoch, stamp, scratch.carrier, &cancellation, Some(authored.cancellation()),
            |carrier| crate::native_mount::adapter::record_view_change(&authored.shared_checkout().ledger,
                &ViewChange::Effect(&installed), carrier,
            ), None, |publication, _| publication,
        ).await?;
        if installed_root.is_some() {
            return authored.shared_checkout().publish_mutation_with_carrier(
                scratch.carrier, &cancellation, Some(authored.cancellation()),
                authored.callback_guard(),
            ).await;
        }
    }
    Err(MountSourceError::Stale)
}

pub(in crate::native_mount::lazy) async fn promotion_effect_with_carrier<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    candidate: &CheckoutCandidate<A, O>,
    path: &str,
    mounted: &MountPath,
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<ViewEffect>, MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let mut scratch = DirectoryCarrierScratch::new(carrier);
    let levels = mounted.components().len().checked_add(1)
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let node_slots = levels.checked_mul(2)
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let mut installed = ViewEffect::default();
    installed.nodes = scratch.vec::<FileId>(node_slots)?;
    installed.listings = scratch.vec::<crate::kernel::NamespacePath>(levels)?;
    let mut prefix = mount_components_with_carrier(mounted.components(), None, scratch.carrier)?;
    scratch.track(prefix.legacy_owned_bytes())?;
    let mut text = path;
    loop {
        scratch.carrier.with_work(|work| mount_items(work, 1))?;
        let after = authored.lookup_in_with_carrier(&candidate.checkout, &prefix, scratch.carrier).await?;
        let base = authored.lookup_in_with_carrier(&candidate.base, &prefix, scratch.carrier).await?;
        if base.is_none() && after.is_some() && !prefix.components().is_empty() {
            let parent = mount_components_with_carrier(
                &prefix.components()[..prefix.components().len() - 1], None, scratch.carrier,
            )?;
            scratch.track(parent.legacy_owned_bytes())?;
            let mut namespace = authored.namespace_path(&parent, scratch.carrier)?;
            if let Some(allocation) = &mut namespace.1 {
                scratch.absorb_native(allocation)?;
            } else {
                scratch.track(namespace.0.retained_bytes())?;
            }
            installed.listings.push(namespace.0);
            let bytes = parent.legacy_owned_bytes();
            drop(parent);
            scratch.release(bytes)?;
        }
        let before = match base {
            Some(lookup) => Some(lookup),
            None => match inspect(lazy, text, InspectBasis::default(), scratch.carrier).await {
                Ok((lookup, _, allocation)) => {
                    let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                    let owned = lookup.legacy_owned_bytes();
                    scratch.track(owned)?;
                    scratch.carrier.with_work(|work| work.record_adoption(owned, &None)).map_err(mount_demand_error)?;
                    let result = file_id(lazy, text, &lookup.value, scratch.carrier).await;
                    let projected = result.map(|file_id| mount_lookup(&lookup.value, file_id));
                    drop(lookup);
                    scratch.release(owned)?;
                    Some(projected?)
                }
                Err(MountSourceError::NotFound) => None,
                Err(error) => return Err(error),
            }
        };
        if before != after {
            let copies = ((u64::from(before.is_some()) + u64::from(after.is_some()))
                * std::mem::size_of::<FileId>() as u64) as u64;
            let copy = if let Some(original) = scratch.carrier.original_native_work() {
                Some(original.reserve_work(crate::WorkCounters { bytes_copied: copies, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?)
            } else { None };
            installed.nodes.extend([before, after].into_iter().flatten().map(|lookup| lookup.node.file_id));
            if let Some(copy) = copy { { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: copies, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork)?; }
        }
        let Some(component) = prefix.value.components.pop() else { break; };
        let bytes = component.capacity() as u64;
        drop(component);
        if let Some(allocation) = &mut prefix.allocation {
            allocation.release_deferred(bytes);
        } else {
            prefix.legacy_bytes -= bytes;
            scratch.release(bytes)?;
        }
        text = match text.rsplit_once('/') { Some(("", _)) | None => "/", Some((parent, _)) => parent };
    }
    let prefix_bytes = prefix.legacy_owned_bytes();
    drop(prefix);
    scratch.release(prefix_bytes)?;
    if let Some(original) = scratch.carrier.original_native_work() {
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
    }
    let bytes = scratch.bytes;
    scratch.keep(bytes)?;
    Ok(MountOwnedBuffer { value: installed, legacy_bytes: bytes, allocation: scratch.native_backing.take() })
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) async fn promote_with_carrier(
        &self, mounted: &MountPath, owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let path = mount_path_text_with_carrier(&self.root, mounted, scratch.carrier)?;
        scratch.track(path.legacy_owned_bytes())?;
        let resolution = self.resolve(
            mounted, &path, owner, scratch.carrier,
        ).await?;
        let (lookup, _, allocation) = match resolution {
            Resolution::Authored(_) => return Ok(()),
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Unauthored(lookup, source, allocation) => (lookup, source, allocation),
        };
        let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
        let owned = lookup.legacy_owned_bytes();
        scratch.track(owned)?;
        scratch.carrier.with_work(|work| work.record_adoption(owned, &None)).map_err(mount_demand_error)?;
        let expected_source = file_id(&self.lazy,
            &path, &lookup.value, scratch.carrier,
        ).await?;
        drop(lookup);
        scratch.release(owned)?;
        stage_mount_promotion_with_carrier(
            &self.lazy, &self.authored, &path, mounted, expected_source, scratch.carrier,
        ).await
    }

pub(in crate::native_mount::lazy) async fn promote_parents_with_carrier(
        &self, path: &MountPath, owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        for length in 1..path.components().len() {
            let mut scratch = DirectoryCarrierScratch::new(carrier);
            let parent = mount_components_with_carrier(
                &path.components()[..length], None, scratch.carrier,
            )?;
            scratch.track(parent.legacy_owned_bytes())?;
            self.promote_with_carrier(&parent, owner, scratch.carrier).await?;
        }
        Ok(())
    }

pub(in crate::native_mount::lazy) async fn promote(&self, mounted: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        in_heap(move || async move {
            let mut work = crate::demand::DemandWork::new(
                crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
            );
            self.promote_with_carrier(
                mounted, SourceViewGate::callback_owner(),
                &mut crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut work),
            ).await
        })
        .await
    }

pub(in crate::native_mount::lazy) async fn promote_subtree_with_carrier(
        &self, path: &MountPath, owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        match carrier {
            crate::native_mount::adapter::NativeDemandCarrier::Native(original) =>
                self.promote_subtree_with_original_work(path, owner, original).await,
            crate::native_mount::adapter::NativeDemandCarrier::Shared(original) => {
                let original = crate::demand::OriginalDemandWork::from_meter(Arc::clone(original));
                let original = crate::NativeMaterializationWork::from_original_demand_work(&original)
                    .map_err(mount_demand_error)?;
                self.promote_subtree_with_original_work(path, owner, &original).await
            }
            crate::native_mount::adapter::NativeDemandCarrier::Borrowed(work) =>
                crate::native_mount::adapter::BorrowedNativeDemandMeter::new(work)?
                    .run_async(|original| async move {
                        let original = crate::demand::OriginalDemandWork::from_meter(original);
                        let original = crate::NativeMaterializationWork::from_original_demand_work(&original)
                            .map_err(mount_demand_error)?;
                        self.promote_subtree_with_original_work(path, owner, &original).await
                    }).await,
        }
    }

pub(in crate::native_mount::lazy) async fn promote_subtree_with_original_work(
        &self, path: &MountPath, owner: crate::native_mount::view_gate::CallbackOwner,
        native: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let original = native.clone();
        let cancellation = original.cancellation();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let mut carrier = crate::native_mount::adapter::NativeDemandCarrier::Native(native);
        let mut scratch = DirectoryCarrierScratch::new(&mut carrier);
        let root = mount_components_with_carrier(path.components(), None, scratch.carrier)?;
        let mut directories = Vec::new();
        promotion_push(&mut directories, root, &mut scratch)?;
        while let Some(directory) = directories.pop() {
            let text = mount_path_text_with_carrier(&self.root, &directory, scratch.carrier)?;
            let (removed, opaque, removal_epoch) =
                self.removal_snapshot_with_carrier(&text, scratch.carrier)?;
            let source_lease = self.source_read_lease_with_carrier(owner, None, scratch.carrier).await?;
            let stamp = ViewStamp::current();
            let mut unresolved = Vec::new();
            let mut cursor = None;
            loop {
                original.charge(crate::WorkCounters { items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
                let observation = self.authored.shared_checkout().observe(
                    owner, scratch.carrier, cancellation, Some(self.authored.cancellation()),
                ).await?;
                observation.ensure_publication_resolved()?;
                let state = self.lazy.state(&LazyTreapWork::native_with_work(&original, cancellation).map_err(native_lazy_error)?).await
                    .map_err(native_lazy_error)?;
                let mut page = self.lazy.list_directory_in_state_with_native_work(
                    state.as_state(), &text, cursor.take(), DIRECTORY_PROMOTION_PAGE,
                    &original, cancellation, Some(&mut observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded observer preserves a validated checkout").value),
                    Some((&removed, opaque)), None,
                ).await.map_err(native_lazy_error)?;
                for entry in &page.entries {
                    original.charge(crate::WorkCounters { items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
                    if entry.authored { continue; }
                    let name = mount_name_with_carrier(&entry.name, scratch.carrier)?;
                    let child = mount_components_with_carrier(
                        directory.components(), Some(&name), scratch.carrier,
                    )?;
                    promotion_push(&mut unresolved,
                        (child, entry.kind == SourceNodeKind::Directory), &mut scratch)?;
                }
                cursor = page.next.take();
                if cursor.is_none() { break; }
            }
            if !self.authored.unchanged_since_with_carrier(&directory, None, stamp, scratch.carrier)? {
                return Err(MountSourceError::Stale);
            }
            let latest = self.removal_snapshot_with_carrier(&text, scratch.carrier)?;
            let unchanged = latest.2 == removal_epoch;
            drop(latest);
            if !unchanged { return Err(MountSourceError::Stale); }
            drop(source_lease);
            self.promote_with_carrier(&directory, owner, scratch.carrier).await?;
            for (child, is_directory) in unresolved.drain(..) {
                if is_directory {
                    promotion_push(&mut directories, child, &mut scratch)?;
                } else {
                    self.promote_with_carrier(&child, owner, scratch.carrier).await?;
                }
            }
            let unresolved_bytes = unresolved.capacity().checked_mul(
                std::mem::size_of::<(MountOwnedBuffer<MountPath>, bool)>(),
            ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            drop(unresolved);
            scratch.release_backing(unresolved_bytes)?;
        }
        let directory_bytes = directories.capacity().checked_mul(
            std::mem::size_of::<MountOwnedBuffer<MountPath>>(),
        ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        drop(directories);
        scratch.release_backing(directory_bytes)?;
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn promote_locked(&self, path: &MountPath) -> Result<(), MountSourceError> {
        if self.authored.lookup(path)?.is_some() {
            return Ok(());
        }
        if self.is_removed(&self.path(path)?, None)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| self.promote(path))
    }
}
