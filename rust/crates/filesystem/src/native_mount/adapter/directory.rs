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
                    ViewGate::callback_owner(),
                    inputs.carrier,
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let mut checkout = inputs.observer(&observation, cancellation)?;
            let (page, source_allocation) = checkout
                .list_directory_records(
                    &path,
                    cursor.as_ref().map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    maximum_entries,
                    work,
                    cancellation,
                )
                .await?;
            let mut entries = inputs.vec(page.entries.len())?;
            for entry in &page.entries {
                inputs.items(1)?;
                let (name, mut allocation) = native_mount_name(&entry.name, inputs.carrier)?;
                inputs
                    .allocation
                    .as_mut()
                    .expect("native directory output")
                    .absorb(allocation.as_mut().expect("native name"))
                    .map_err(MountSourceError::NativeWork)?;
                entries.push(MountDirectoryEntry {
                    name,
                    node: mount_node(entry.record),
                    metadata: entry.metadata,
                    pin: None,
                });
            }
            let next_cursor = if page.has_more {
                entries
                    .last()
                    .map(|entry| inputs.copy_bytes(&entry.name))
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
