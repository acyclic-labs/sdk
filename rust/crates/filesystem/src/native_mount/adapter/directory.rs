use super::*;

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(super) fn directory_page(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let cursor = cursor
                .map(|bytes| native_name(bytes, self.profile, self.limits, &mut inputs))
                .transpose()?;
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
                        .as_ref()
                        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    maximum_entries,
                    work,
                    cancellation,
                )
                .await?;
            let mut entries = inputs.vec(page.entries.len())?;
            for entry in &page.entries {
                inputs.items(1)?;
                if let Some(keys) = &folded
                    && crate::facade::ambiguous_folded_name(
                        &keys.value,
                        &entry.name,
                        &mut crate::kernel::KernelAccounting::native(work, cancellation),
                    )?
                {
                    continue;
                }
                let (name, mut allocation) = native_mount_name(&entry.name, inputs.carrier)?;
                inputs
                    .allocation
                    .as_mut()
                    .ok_or(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?
                    .absorb(allocation.as_mut().ok_or(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?)
                    .map_err(MountSourceError::NativeWork)?;
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
                        let (name, allocation) = native_mount_name(&entry.name, inputs.carrier)?;
                        if let Some(mut allocation) = allocation {
                            inputs
                                .allocation
                                .as_mut()
                                .ok_or(MountSourceError::NativeWork(
                                    crate::NativeTreeMaterializationError::InvalidLayout,
                                ))?
                                .absorb(&mut allocation)
                                .map_err(MountSourceError::NativeWork)?;
                        }
                        Ok::<_, MountSourceError>(name)
                    })
                    .transpose()?
            } else {
                None
            };
            drop(page);
            drop(source_allocation);
            drop(checkout);
            let bytes = path.retained_bytes()
                + cursor
                    .as_ref()
                    .map_or(0, |name| name.retained_bytes() as u64);
            drop(path);
            drop(cursor);
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
    }
}
