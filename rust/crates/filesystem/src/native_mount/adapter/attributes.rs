impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let (mut candidate, epoch, stamp) = self
                .checkout
                .candidate(None, &mut carrier, cancellation, Some(&self.cancellation))
                .await?;
            let lookup = self
                .lookup_in(&candidate.checkout, path, &mut carrier)
                .await?
                .ok_or(MountSourceError::NotFound)?;
            candidate
                .checkout
                .set_attributes_by_id(
                    lookup.node.file_id,
                    metadata,
                    logical_bytes,
                    work,
                    cancellation,
                )
                .await
                .map_err(fs_error)?;
            self.checkout
                .install_change(
                    candidate,
                    epoch,
                    stamp,
                    ViewChange::Node(lookup.node.file_id),
                    work,
                    &self.cancellation,
                    self.native_callback_guard.clone(),
                )
                .await?;
            Ok(())
        })
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let name = inputs.attribute_name(name, self.profile, self.limits)?;
            let observation = self
                .checkout
                .observe(
                    owner,
                    inputs.carrier,
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let mut checkout = observation.observer(work, cancellation).map_err(fs_error)?;
            let value = checkout
                .read_named_attribute(&path, &name, work, cancellation)
                .await
                .map_err(fs_error)?;
            inputs
                .carrier
                .check(cancellation, Some(&self.cancellation))?;
            Ok(value)
        })
    }

    fn attribute_page(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let cursor = cursor
                .map(|bytes| inputs.attribute_name(bytes, self.profile, self.limits))
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
            let mut checkout = observation.observer(work, cancellation).map_err(fs_error)?;
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(work, cancellation);
            let listing = checkout
                .list_named_attributes_with_accounting(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(fs_error)?;
            let page = inputs.attribute_page(&listing.0)?;
            drop(listing);
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
            Ok((page, inputs.retain().expect("native attribute page")))
        })
    }
}
