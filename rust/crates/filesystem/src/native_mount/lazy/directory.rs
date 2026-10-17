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
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let view = self.authored.access_snapshot_with_meter(work)?;
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let mut inputs = adapter::InputBuffers::new(&mut carrier)?;
        let namespace = self.authored.namespace_inputs(path, &mut inputs)?;
        let access = view
            .prepare_namespace(&namespace)
            .map_err(|_| MountSourceError::AccessDenied)?;
        let now = adapter::access::access_time()?;
        if !access.decide(crate::access::Action::List, now).allowed {
            return Err(MountSourceError::AccessDenied);
        }
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
                let (stamp, cursor) = match self.cursors.take(generation, cursor)? {
                    Some((stamp, cursor)) => (stamp, Some(cursor)),
                    None => (ViewStamp::current(), None),
                };
                if !self
                    .authored
                    .unchanged_since_with_meter(path, None, stamp, work)?
                {
                    return Err(MountSourceError::Stale);
                }
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
                    .lazy
                    .list_directory_in_checkout(
                        &mut checkout,
                        &text,
                        cursor,
                        maximum_entries,
                        Some((&removed, opaque)),
                        &mut accounting,
                    )
                    .await
                    .map_err(lazy_error)?;
                let (entries, mut output_allocation) = self
                    .directory_entries(path, &page, &mut checkout, &state, carrier, access)
                    .await?;
                if !self
                    .authored
                    .unchanged_since_with_meter(path, None, stamp, work)?
                    || self.removal_snapshot(&text)?.2 != epoch
                {
                    return Err(MountSourceError::Stale);
                }
                let mut output_carrier = adapter::NativeDemandCarrier::Native(work);
                let next_cursor = if let Some(cursor) = page.next.take() {
                    let token = self.cursors.remember(generation, (stamp, cursor))?;
                    let mut continuation = adapter::InputBuffers::new(&mut output_carrier)?;
                    let mut bytes = continuation.vec(8)?;
                    let mut allocation = continuation
                        .retain()
                        .ok_or(MountSourceError::NativeUnsupported)?;
                    output_allocation
                        .absorb(&mut allocation)
                        .map_err(MountSourceError::NativeWork)?;
                    bytes.extend_from_slice(&token);
                    Some(bytes)
                } else {
                    None
                };
                Ok(MountDirectoryPage::new(
                    entries,
                    next_cursor,
                    Some(output_allocation),
                ))
            })
        })
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
        let now = adapter::access::access_time()?;
        let work = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeUnsupported)?;
        let mut accounting = crate::kernel::KernelAccounting::native(&work, work.cancellation());
        let mut output_carrier = adapter::NativeDemandCarrier::Native(&work);
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
            let (child, _allocation) = copy_path(path.components(), Some(&name), &work)?;
            let (child_text, _text_allocation) = self.path(&child, &work)?;
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
                self.project_unauthored(&child_text, lookup, &work).await?
            } else {
                match self.resolve(&child, &child_text, owner, &work).await? {
                    Resolution::Absent => continue,
                    Resolution::Authored(lookup) => {
                        (self.project_authored(lookup, &work).await?, None)
                    }
                    Resolution::Unauthored(lookup, _allocation) => {
                        self.project_unauthored(&child_text, lookup, &work).await?
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
