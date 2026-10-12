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
        use adapter::CaptureInputScratch;
        let (text, _allocation) = self.path(path, Some(work))?;
        let owner = SourceViewGate::callback_owner();
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
                    .observer(work, work.cancellation())
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
                let mut output = adapter::NativeCaptureScratchAllocation::new(work)?;
                let mut entries = output.vec(page.entries.len())?;
                for entry in &page.entries {
                    output.items(1)?;
                    let (name, allocation) = adapter::native_mount_name(&entry.name, carrier)?;
                    if let Some(allocation) = allocation {
                        output.absorb(allocation);
                    }
                    let (child, _allocation) = copy_path(path.components(), Some(&name), work)?;
                    let (child_text, _text_allocation) = self.path(&child, Some(work))?;
                    let (lookup, pin) = if let Some(node) = entry.source {
                        let (lookup, _, _allocation) = self
                            .lazy
                            .inspect_unauthored(
                                &mut checkout,
                                &child_text,
                                None,
                                crate::lazy_workspace::InspectBasis {
                                    state: Some(&state),
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
                        let source = match &lookup {
                            LazyLookup::Source { source, .. } => Some(*source),
                            _ => None,
                        };
                        self.project_unauthored(&child_text, lookup, source, Some(work))
                            .await?
                    } else {
                        match self.resolve(&child, &child_text, owner, Some(work)).await? {
                            Resolution::Absent => continue,
                            Resolution::Authored(lookup) => {
                                (self.project_authored(lookup, Some(work)).await?, None)
                            }
                            Resolution::Unauthored(lookup, source, _allocation) => {
                                self.project_unauthored(&child_text, lookup, source, Some(work))
                                    .await?
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
                if !self
                    .authored
                    .unchanged_since_with_meter(path, None, stamp, work)?
                    || self.removal_snapshot(&text)?.2 != epoch
                {
                    return Err(MountSourceError::Stale);
                }
                let next_cursor = if let Some(cursor) = page.next.take() {
                    let token = self.cursors.remember(generation, (stamp, cursor))?;
                    let mut bytes = output.vec(8)?;
                    bytes.extend_from_slice(&token);
                    Some(bytes)
                } else {
                    None
                };
                Ok(MountDirectoryPage::new(
                    entries,
                    next_cursor,
                    Some(output.retain()),
                ))
            })
        })
    }
}
