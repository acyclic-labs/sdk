use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) fn directory_page(
        &self,
        path: &MountPath,
        cursor: Option<&crate::native_mount::MountDirectoryCursor>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let view = self.authored.access_snapshot_with_meter(work)?;
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let mut inputs = adapter::InputBuffers::new(&mut carrier)?;
        let namespace = self.authored.namespace_inputs(path, &mut inputs)?;
        let (access, _now) = adapter::access::require_list(&view, &namespace, &self.runtime.clock)?;
        let result = (|| {
            let (text, _allocation) = self.path(path, work)?;
            let owner = ViewGate::callback_owner()?;
            let mut carrier = adapter::NativeDemandCarrier::Native(work);
            with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let lease = self
                        .source_view
                        .read_for_callback_with_carrier(
                            owner,
                            None,
                            carrier,
                            work.cancellation(),
                            Some(self.authored.cancellation()),
                        )
                        .await?;
                    let (removed, opaque, epoch) = self.removal_snapshot(&text)?;
                    if opaque
                        && self
                            .authored
                            .lookup_async(path, owner, carrier)
                            .await?
                            .is_none()
                    {
                        return Err(MountSourceError::NotFound);
                    }
                    let generation = (lease.generation, epoch);
                    let (stamp, cursor) = match cursor
                        .map(|cursor| cursor.take_lazy(&self.source_view, generation))
                        .transpose()?
                    {
                        Some((stamp, cursor)) => (stamp, Some(cursor)),
                        None => (ViewStamp::current(), None),
                    };
                    self.check_directory_cut(path, stamp, None, work)?;
                    let mut accounting =
                        crate::kernel::KernelAccounting::native(work, work.cancellation());
                    let observation = self
                        .authored
                        .shared_checkout()
                        .observe(
                            owner,
                            carrier,
                            work.cancellation(),
                            Some(self.authored.cancellation()),
                        )
                        .await?;
                    observation.ensure_publication_resolved()?;
                    let mut checkout = observation
                        .observer(&mut accounting, work.cancellation())
                        .map_err(mount_engine_error)?;
                    let (mut page, state, _page_allocation) = self
                        .runtime
                        .prepare_future(work, || {
                            self.lazy.list_directory_in_checkout(
                                &mut checkout,
                                &text,
                                cursor,
                                maximum_entries,
                                Some((&removed, opaque)),
                                &mut accounting,
                            )
                        })?
                        .await
                        .map_err(lazy_error)?;
                    let (entries, output_allocation) = self
                        .runtime
                        .prepare_future(work, || {
                            self.directory_entries(
                                path,
                                &page,
                                &mut checkout,
                                &state,
                                carrier,
                                access,
                            )
                        })?
                        .await?;
                    self.check_directory_cut(path, stamp, Some((&text, epoch)), work)?;
                    let next_cursor =
                        self.next_directory_cursor(&mut page, generation, stamp, work)?;
                    Ok(MountDirectoryPage::new(
                        entries,
                        next_cursor,
                        Some(output_allocation),
                    ))
                })
            })
        })();
        self.authored.disclose_result(&view, result, work)
    }

    fn check_directory_cut(
        &self,
        path: &MountPath,
        stamp: ViewStamp,
        removal: Option<(&str, u64)>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        if !self
            .authored
            .unchanged_since_with_meter(path, None, stamp, work)?
        {
            return Err(MountSourceError::Stale);
        }
        if let Some((text, epoch)) = removal
            && self.removal_snapshot(text)?.2 != epoch
        {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }

    fn next_directory_cursor(
        &self,
        page: &mut crate::LazyDirectoryPage,
        generation: (u64, u64),
        stamp: ViewStamp,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::native_mount::MountDirectoryCursor>, MountSourceError> {
        let next_cursor = page
            .next
            .take()
            .map(|cursor| {
                use crate::native_mount::directory_cursor::{
                    DirectoryPosition, LazyDirectoryPosition,
                };
                let allocation = work
                    .claim(
                        std::mem::size_of::<LazyDirectoryPosition>() as u64,
                        1,
                        crate::WorkCounters::UNCHARGED,
                    )
                    .map_err(MountSourceError::NativeWork)?;
                let credit = self.authored.reserve_capture_poll()?;
                let producer = self.authored.capture_producer(credit)?;
                Ok::<_, MountSourceError>(crate::native_mount::MountDirectoryCursor {
                    position: DirectoryPosition::Lazy(Box::new(LazyDirectoryPosition {
                        source: Arc::clone(&self.source_view),
                        generation,
                        stamp,
                        cursor: Mutex::new(Some(cursor)),
                        _producer: producer,
                    })),
                    _allocation: crate::kernel::KernelOwnedAllocation::NativeOriginal(allocation),
                })
            })
            .transpose()?;
        Ok(next_cursor)
    }

    async fn directory_entries(
        &self,
        path: &MountPath,
        page: &crate::LazyDirectoryPage,
        checkout: &mut crate::Checkout<A, O>,
        state: &crate::LazyWorkspaceState,
        carrier: &mut adapter::NativeDemandCarrier<'_>,
        access: crate::access::PreparedPath<'_>,
    ) -> Result<(Vec<MountDirectoryEntry>, crate::NativeAllocation), MountSourceError> {
        let owner = ViewGate::callback_owner()?;
        let now = adapter::access::access_time(&self.runtime.clock)?;
        let work = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeUnsupported)?;
        let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
        let mut output_carrier = adapter::NativeDemandCarrier::Native(work);
        let mut output = adapter::InputBuffers::new(&mut output_carrier)?;
        let mut entries = output.vec(page.entries.len())?;
        let mut output_allocation = output.retain().ok_or(MountSourceError::NativeUnsupported)?;
        for entry in &page.entries {
            work.charge(crate::materializer::NativeResourceCredit::Items.delta(1))
                .map_err(MountSourceError::NativeWork)?;
            if !access
                .child_name(&entry.name)
                .map_err(|_| MountSourceError::AccessDenied)?
                .decide(crate::access::Action::List, now)
                .allowed
            {
                continue;
            }
            let (name, allocation) = adapter::native_mount_name(&entry.name, carrier)?;
            if let Some(mut allocation) = allocation {
                output_allocation
                    .absorb(&mut allocation)
                    .map_err(MountSourceError::NativeWork)?;
            }
            let (child, _allocation) = copy_path(path.components(), Some(&name), work)?;
            let (child_text, _text_allocation) = self.path(&child, work)?;
            let (lookup, pin) = if let Some(node) = entry.source {
                let (lookup, _, _allocation) = self
                    .lazy
                    .inspect_unauthored(
                        checkout,
                        &child_text,
                        None,
                        crate::lazy_workspace::InspectBasis {
                            state: Some(state),
                            listed: Some(node),
                            unobserved: entry.unobserved,
                            ..crate::lazy_workspace::InspectBasis::default()
                        },
                        &mut accounting,
                    )
                    .await
                    .map_err(lazy_error)?;
                let Some(lookup) = lookup else {
                    continue;
                };
                self.project_unauthored(&child_text, lookup, work).await?
            } else {
                match self.resolve(&child, &child_text, owner, work).await? {
                    Resolution::Absent => continue,
                    Resolution::Authored(lookup) => {
                        (self.project_authored(lookup, work).await?, None)
                    }
                    Resolution::Unauthored(lookup, _allocation) => {
                        self.project_unauthored(&child_text, lookup, work).await?
                    }
                }
            };
            entries.push(MountDirectoryEntry {
                name,
                node: lookup.node,
                metadata: lookup.metadata,
                pin,
            });
        }
        Ok((entries, output_allocation))
    }
}
