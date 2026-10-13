use super::*;

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(super) fn created_handle(
        &self,
        created: &MountLookup,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        if created.node.kind != MountNodeKind::Regular {
            return Err(MountSourceError::Invalid(
                "only regular files can own native handles".into(),
            ));
        }
        self.attached_file(created.node.file_id, &mut NativeDemandCarrier::Native(work))
    }

    pub(super) fn seek_extent(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let observation = self
                .checkout
                .observe(
                    ViewGate::callback_owner(),
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut checkout = observation
                .observer(work, work.cancellation())
                .map_err(fs_error)?;
            let file_id = checkout
                .lookup_no_follow(&path, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?
                .file_id;
            let result = checkout
                .seek_file_extent_by_id(
                    file_id,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    work,
                    work.cancellation(),
                )
                .await?;
            inputs
                .carrier
                .check(work.cancellation(), Some(&self.cancellation))?;
            Ok(result)
        })
    }

    pub(super) fn open_handle(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let observation = self
                .checkout
                .observe(
                    ViewGate::callback_owner(),
                    &mut carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let lookup = self
                .lookup_in(&observation, path, &mut carrier)
                .await?
                .ok_or(MountSourceError::NotFound)?;
            if lookup.node.kind != MountNodeKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own native handles".into(),
                ));
            }
            Ok((
                self.attached_file(lookup.node.file_id, &mut carrier)?,
                lookup,
            ))
        })
    }

    pub(super) fn attached_file(
        &self,
        file_id: FileId,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let cancellation = carrier.cancellation();
        carrier.check(&cancellation, Some(&self.cancellation))?;
        let bytes = std::mem::size_of::<CheckoutAttachedFile<A, O>>() as u64;
        let allocation = if let Some(work) = carrier.original_native_work() {
            Some(
                work.claim(bytes, 1, crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork)?,
            )
        } else {
            carrier
                .with_work(|work| work.claim_allocation(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED))
                .map_err(super::super::lazy::mount_demand_error)?;
            None
        };
        let retained = if allocation.is_some() { 0 } else { bytes };
        let attached: Arc<dyn MountOpenFile> = Arc::new(CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
            _native_allocation: allocation,
            native_callback_guard: self.native_callback_guard.clone(),
        });
        if let Err(error) = carrier.check(&cancellation, Some(&self.cancellation)) {
            drop(attached);
            if retained != 0 {
                carrier
                    .with_work(|work| work.release(retained))
                    .map_err(super::super::lazy::mount_demand_error)?;
            }
            return Err(error);
        }
        Ok((attached, retained))
    }

    pub(super) fn detached_handle(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let observation = self
                .checkout
                .observe(
                    ViewGate::callback_owner(),
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut checkout = observation
                .observer(work, work.cancellation())
                .map_err(fs_error)?;
            let lookup = checkout
                .lookup_no_follow_with_metadata(&path, work, work.cancellation())
                .await?
                .ok_or(MountSourceError::NotFound)?;
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own detached handles".into(),
                ));
            }
            let allocation = work
                .claim(
                    std::mem::size_of::<CheckoutDetachedFile<A, O>>() as u64,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )
                .map_err(MountSourceError::NativeWork)?;
            let file = checkout.detached_from_record(lookup.record, work, work.cancellation())?;
            inputs
                .carrier
                .check(work.cancellation(), Some(&self.cancellation))?;
            let file: Arc<dyn MountOpenFile> = Arc::new(CheckoutDetachedFile {
                state: tokio::sync::Mutex::new(DetachedMountState {
                    file,
                    metadata: lookup.metadata,
                    mutation_epoch: 0,
                }),
                ledger: Arc::clone(&self.checkout.ledger),
                runtime: self.runtime.clone(),
                cancellation: self.cancellation.clone(),
                profile: self.profile,
                limits: self.limits,
                _native_allocation: Some(allocation),
            });
            Ok((file, 0))
        })
    }
}
