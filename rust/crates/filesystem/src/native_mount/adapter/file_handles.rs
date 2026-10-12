use super::*;

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(super) fn open_file(
        &self,
        path: &MountPath,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let lookup = self.lookup(path)?.ok_or(MountSourceError::NotFound)?;
        if lookup.node.kind != MountNodeKind::Regular {
            return Err(MountSourceError::Invalid(
                "only regular files can own native file handles".to_owned(),
            ));
        }
        self.open_created(path, &lookup)
    }

    pub(super) fn open_created(
        &self,
        _path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let attached = CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id: created.node.file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        };
        Ok(Arc::new(attached))
    }

    pub(super) fn detach_file(
        &self,
        path: &MountPath,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        let (file, metadata) = self.runtime.wait(|| async {
            let observation = self
                .checkout
                .observe(
                    owner,
                    &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                        boundary_budget(),
                        &self.cancellation,
                    )),
                    &self.cancellation,
                    None,
                )
                .await?;
            observation.ensure_publication_resolved()?;
            let mut checkout = observation
                .observer(boundary_budget(), &self.cancellation)
                .map_err(facade_error)?
                .value;
            let lookup = checkout
                .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .ok_or(MountSourceError::NotFound)?;
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own detached native handles".to_owned(),
                ));
            }
            let file = checkout
                .detach_regular_file(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            Ok((file, lookup.metadata))
        })?;
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState {
                file,
                metadata,
                mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }
}
