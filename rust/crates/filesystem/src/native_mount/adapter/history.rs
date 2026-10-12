impl<A, O> CheckoutMountSource<A, O> {
    fn query_history(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
        query: impl FnOnce(
            &NamespacePath,
            &mut crate::demand::DemandWork,
        ) -> Result<bool, MountSourceError>,
    ) -> Result<bool, MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        carrier.check(work.cancellation(), Some(&self.cancellation))?;
        let mut inputs = InputBuffers::new(&mut carrier)?;
        let path = self.namespace_inputs(path, &mut inputs)?;
        let result = self.checkout.view_gate.is_stable()
            && inputs.carrier.with_work(|leaf| query(&path, leaf))?;
        inputs
            .carrier
            .check(work.cancellation(), Some(&self.cancellation))?;
        Ok(result)
    }

    fn fold_path(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountPath, u64)>, MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        carrier.check(work.cancellation(), Some(&self.cancellation))?;
        if !self.folds_names() {
            return Ok(None);
        }
        let result = carrier.with_work(|leaf| {
            let mut carrier = NativeDemandCarrier::Borrowed(leaf);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let mut components = inputs.vec(path.components().len())?;
            for component in path.components() {
                let name = match native_name(component, self.profile, self.limits, &mut inputs) {
                    Ok(name) => name,
                    Err(
                        MountSourceError::Unsupported(_)
                        | MountSourceError::Invalid(_)
                        | MountSourceError::NamespacePath(_),
                    ) => {
                        components.push(inputs.copy_bytes(component)?);
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let text =
                    std::str::from_utf8(name.as_bytes()).map_err(|_| MountSourceError::Stale)?;
                let folded = inputs
                    .carrier
                    .with_work(|leaf| crate::demand::lowercase_unicode_with_work(text, leaf))
                    .map_err(super::lazy::mount_demand_error)?;
                inputs.bytes = inputs
                    .bytes
                    .checked_add(folded.capacity() as u64)
                    .ok_or(MountSourceError::Work(WorkError::Overflow))?;
                let folded = LogicalName::new(
                    NameEncoding::Utf8,
                    folded,
                    self.limits.maximum_component_bytes,
                )
                .map_err(|error| {
                    MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Name(error))
                })?;
                let (native, _) = native_mount_name(&folded, inputs.carrier)?;
                inputs.bytes = inputs
                    .bytes
                    .checked_add(native.capacity() as u64)
                    .ok_or(MountSourceError::Work(WorkError::Overflow))?;
                let bytes = name.retained_bytes() as u64 + folded.retained_bytes() as u64;
                drop(name);
                drop(folded);
                inputs.release(bytes)?;
                components.push(native);
            }
            inputs
                .carrier
                .check(work.cancellation(), Some(&self.cancellation))?;
            let bytes = inputs.bytes;
            inputs.retain();
            Ok(Some((MountPath { components }, bytes)))
        });
        result
    }
}
