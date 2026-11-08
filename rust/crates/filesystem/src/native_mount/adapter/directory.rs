use super::*;

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(super) fn directory_page(
        &self,
        path: &MountPath,
        cursor: Option<&crate::native_mount::MountDirectoryCursor>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let view = self.access_snapshot(work).await?;
            let (access, now) = access::require_list(&view, &path, &self.runtime.clock)?;
            self.disclose(&view, work, async {
                let cursor = cursor.map(crate::native_mount::MountDirectoryCursor::authored).transpose()?;
                let observation = self
                    .checkout
                    .observe(
                        ViewGate::callback_owner()?,
                        inputs.carrier,
                        cancellation,
                        Some(&self.cancellation),
                    )
                    .await?;
                let mut checkout = observation.observer(
                    &mut crate::kernel::allocation::KernelAccounting::native(work, cancellation),
                    cancellation,
                )?;
                self.require_snapshot(&view, &mut checkout, work).await?;
                let folded = if self.folds_names() {
                    self.require_unique_folded_path(&mut checkout, &path, true, work, cancellation)
                        .await?
                } else {
                    None
                };
                let (page, source_allocation) = checkout
                    .list_directory_records(
                        &path,
                        cursor
                            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                        maximum_entries,
                        work,
                        cancellation,
                    )
                    .await?;
                let mut entries = inputs.vec(page.entries.len())?;
                for entry in &page.entries {
                    inputs.items(1)?;
                    if !access::visible_name(access, &entry.name, now)? {
                        continue;
                    }
                    if let Some(keys) = &folded
                        && crate::facade::ambiguous_folded_name(
                            &keys.value,
                            &entry.name,
                            &mut crate::kernel::KernelAccounting::native(work, cancellation),
                        )?
                    {
                        continue;
                    }
                    let name = inputs.mount_name(&entry.name)?;
                    entries.push(MountDirectoryEntry {
                        name,
                        node: mount_node(entry.record),
                        metadata: entry.metadata,
                        pin: None,
                    });
                }
                // Continue after the last examined source entry even when the page
                // contains no visible entries. Filtering must not truncate traversal.
                let next_cursor = if page.has_more {
                    page.entries
                        .last()
                        .map(|entry| {
                            let (name, allocation) = entry.name.clone_with_accounting(
                                &mut crate::kernel::KernelAccounting::native(work, cancellation), cancellation,
                            )?;
                            Ok::<_, MountSourceError>(crate::native_mount::MountDirectoryCursor {
                                position: crate::native_mount::directory_cursor::DirectoryPosition::Authored(name),
                                _allocation: allocation,
                            })
                        })
                        .transpose()?
                } else {
                    None
                };
                drop(page);
                drop(source_allocation);
                drop(checkout);
                let bytes = path.retained_bytes();
                drop(path);
                inputs.release(bytes)?;
                inputs
                    .carrier
                    .check(cancellation, Some(&self.cancellation))?;
                Ok(MountDirectoryPage::new(
                    entries,
                    next_cursor,
                    inputs.retain(),
                ))
            })
            .await
        })
    }
}
