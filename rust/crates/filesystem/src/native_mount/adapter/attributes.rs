fn require_attribute_root(
    requested: FileMetadata,
    current: FileMetadata,
) -> Result<(), MountSourceError> {
    // Native setattr edits scalar metadata. An attribute-tree root is an SDK
    // object identity, not a native attribute; replacing it would bypass the
    // named-attribute owner and could erase the volume policy.
    if requested.named_attributes != current.named_attributes {
        return Err(MountSourceError::AccessDenied);
    }
    Ok(())
}

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
            let observation = self
                .checkout
                .observe(
                    ViewGate::callback_owner()?,
                    &mut carrier,
                    cancellation,
                    Some(&self.cancellation),
                )
                .await?;
            let file = self
                .lookup_in(
                    &observation,
                    path,
                    crate::access::Action::Write,
                    &mut carrier,
                )
                .await?
                .ok_or(MountSourceError::NotFound)?
                .node
                .file_id;
            drop(observation);
            let mut candidate = self
                .checkout
                .file_candidate(file, &mut carrier, cancellation, Some(&self.cancellation))
                .await?;
            let lookup = self
                .lookup_in(
                    &candidate.checkout,
                    path,
                    crate::access::Action::Write,
                    &mut carrier,
                )
                .await?
                .ok_or(MountSourceError::NotFound)?;
            if lookup.node.file_id != file {
                return Err(MountSourceError::Stale);
            }
            candidate.deadline = self.mutation_deadline()?;
            require_attribute_root(metadata, lookup.metadata)?;
            candidate
                .checkout
                .set_attributes(
                    lookup.node.file_id,
                    metadata,
                    logical_bytes,
                    work,
                    cancellation,
                )
                .await?;
            self.checkout
                .install_change(
                    candidate,
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
        let owner = ViewGate::callback_owner()?;
        invoke(&self.runtime, work, || async {
            let cancellation = work.cancellation();
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let permissions = self
                .require_access(&path, crate::access::Action::Read, work)
                .await?;
            self.disclose(permissions.policy_view(), work, async {
                let name = inputs.attribute_name(name, self.config.profile, self.config.limits)?;
                let observation = self
                    .checkout
                    .observe(
                        owner,
                        inputs.carrier,
                        cancellation,
                        Some(&self.cancellation),
                    )
                    .await?;
                let mut checkout = observation.observer(
                    &mut crate::kernel::allocation::KernelAccounting::native(work, cancellation),
                    cancellation,
                )?;
                self.require_snapshot(permissions.policy_view(), &mut checkout, work)
                    .await?;
                let value = checkout
                    .read_named_attribute(&path, &name, work, cancellation)
                    .await?;
                inputs
                    .carrier
                    .check(cancellation, Some(&self.cancellation))?;
                Ok(value)
            })
            .await
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
            let permissions = self
                .require_access(&path, crate::access::Action::List, work)
                .await?;
            self.disclose(permissions.policy_view(), work, async {
                let cursor = cursor
                    .map(|bytes| {
                        inputs.attribute_name(bytes, self.config.profile, self.config.limits)
                    })
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
                self.require_snapshot(permissions.policy_view(), &mut checkout, work)
                    .await?;
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
                    .await?;
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
                Ok((
                    page,
                    inputs.retain().ok_or(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))?,
                ))
            })
            .await
        })
    }
}
