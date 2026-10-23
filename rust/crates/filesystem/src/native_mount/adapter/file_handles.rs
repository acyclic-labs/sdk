use super::*;

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(in crate::native_mount) fn created_handle(
        &self,
        path: &MountPath,
        created: &MountLookup,
        work: &crate::NativeMaterializationWork,
        wrap: impl FnOnce(
            (Arc<dyn MountOpenFile>, u64),
        ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        if created.node.kind != MountNodeKind::Regular {
            return Err(MountSourceError::Invalid(
                "only regular files can own native handles".into(),
            ));
        }
        let permissions = self.permissions_for_path(path, work)?;
        access::require(
            &permissions,
            crate::access::Action::Create,
            access::access_time(&self.runtime.clock)?,
        )?;
        let file =
            wrap(self.attached_file(created.node.file_id, &mut NativeDemandCarrier::Native(work))?);
        let file = self.disclose_result(permissions.policy_view(), file, work)?;
        self.authorize_file(file, permissions, created.node.file_id, true, work)
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
            let permissions = self
                .require_access(&path, crate::access::Action::Read, work)
                .await?;
            self.disclose(permissions.policy_view(), work, async {
                let observation = self
                    .checkout
                    .observe(
                        ViewGate::callback_owner()?,
                        inputs.carrier,
                        work.cancellation(),
                        Some(&self.cancellation),
                    )
                    .await?;
                let mut checkout = observation.observer(
                    &mut crate::kernel::allocation::KernelAccounting::native(
                        work,
                        work.cancellation(),
                    ),
                    work.cancellation(),
                )?;
                self.require_snapshot(permissions.policy_view(), &mut checkout, work)
                    .await?;
                let file_id = checkout
                    .lookup_no_follow(&path, work, work.cancellation())
                    .await?
                    .record
                    .ok_or(MountSourceError::NotFound)?
                    .file_id;
                let result = checkout
                    .seek_file_extent(
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
            .await
        })
    }

    pub(super) fn open_handle(
        &self,
        path: &MountPath,
        access: Option<MountOpenAccess>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(crate::native_mount::MountOpenClaim, MountLookup), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let namespace = self.namespace_inputs(path, &mut inputs)?;
            let permissions = self.pin_access(&namespace, work).await?;
            access::require_open(&permissions, access, &self.runtime.clock)?;

            let lookup = self
                .disclose(permissions.policy_view(), work, async {
                    let observation = self
                        .checkout
                        .observe(
                            ViewGate::callback_owner()?,
                            inputs.carrier,
                            work.cancellation(),
                            Some(&self.cancellation),
                        )
                        .await?;
                    let mut observer = observation.observer(
                        &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        work.cancellation(),
                    )?;
                    self.require_snapshot(permissions.policy_view(), &mut observer, work)
                        .await?;
                    let lookup = self
                        .read_lookup(&mut observer, &namespace, &mut inputs, work.cancellation())
                        .await?
                        .ok_or(MountSourceError::NotFound)?;
                    if lookup.node.kind != MountNodeKind::Regular {
                        return Err(MountSourceError::Invalid(
                            "only regular files can own native handles".into(),
                        ));
                    }
                    Ok(lookup)
                })
                .await?;
            let raw = self.attached_file(lookup.node.file_id, inputs.carrier)?;
            let (file, bytes) =
                self.authorize_file(raw, permissions, lookup.node.file_id, false, work)?;
            Ok((crate::native_mount::MountOpenClaim { file, bytes }, lookup))
        })
    }

    pub(in crate::native_mount) fn attached_file(
        &self,
        file_id: FileId,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let cancellation = carrier.cancellation();
        carrier.check(&cancellation, Some(&self.cancellation))?;
        let bytes = std::mem::size_of::<CheckoutAttachedFile<A, O>>() as u64;
        let work = carrier
            .original_native_work()
            .ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
        let allocation = work
            .claim(bytes, 1, crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        let attached: Arc<dyn MountOpenFile> = Arc::new(CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id,
            deadline: self.mutation_deadline()?,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.config.profile,
            limits: self.config.limits,
            _native_allocation: allocation,
            native_callback_guard: self.native_callback_guard.clone(),
        });
        if let Err(error) = carrier.check(&cancellation, Some(&self.cancellation)) {
            drop(attached);
            return Err(error);
        }
        Ok((attached, 0))
    }

    pub(in crate::native_mount) fn detached_handle(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
        wrap: impl FnOnce(
            (Arc<dyn MountOpenFile>, u64),
        ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let permissions = self.pin_access(&path, work).await?;
            access::require_open(&permissions, None, &self.runtime.clock)?;
            let (file, file_id) = self
                .disclose(permissions.policy_view(), work, async {
                    let observation = self
                        .checkout
                        .observe(
                            ViewGate::callback_owner()?,
                            inputs.carrier,
                            work.cancellation(),
                            Some(&self.cancellation),
                        )
                        .await?;
                    let mut checkout = observation.observer(
                        &mut crate::kernel::allocation::KernelAccounting::native(
                            work,
                            work.cancellation(),
                        ),
                        work.cancellation(),
                    )?;
                    self.require_snapshot(permissions.policy_view(), &mut checkout, work)
                        .await?;
                    let lookup = checkout
                        .lookup_no_follow_with_metadata(&path, work, work.cancellation())
                        .await?
                        .ok_or(MountSourceError::NotFound)?;
                    if lookup.record.kind != FileKind::Regular {
                        return Err(MountSourceError::Invalid(
                            "only regular files can own detached handles".into(),
                        ));
                    }
                    let file: Arc<dyn MountOpenFile> = self.detached_from_checkout(
                        &checkout,
                        lookup.record,
                        lookup.metadata,
                        work,
                    )?;
                    Ok((file, lookup.record.file_id))
                })
                .await?;
            self.authorize_file(wrap((file, 0))?, permissions, file_id, false, work)
        })
    }
}
