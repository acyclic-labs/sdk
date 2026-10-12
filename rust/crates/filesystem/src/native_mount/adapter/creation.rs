impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    async fn create(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
        build: impl FnOnce(NamespacePath) -> AuthoredMutation,
    ) -> Result<MountLookup, MountSourceError> {
        let cancellation = work.cancellation();
        let mut carrier = NativeDemandCarrier::Native(work);
        carrier.check(cancellation, Some(&self.cancellation))?;
        let (path, _path_allocation) = self.namespace_path(path, &mut carrier)?;
        let (mut candidate, epoch, stamp) = self
            .checkout
            .candidate(None, &mut carrier, cancellation, Some(&self.cancellation))
            .await?;
        let mut inputs = InputBuffers::new(&mut carrier)?;
        let mut changes = inputs.vec(1)?;
        let owned = inputs.clone_namespace(path.components(), self.limits)?;
        inputs.charge(crate::WorkCounters {
            items_examined: 1,
            bytes_copied: std::mem::size_of::<AuthoredMutation>() as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        changes.push(build(owned));
        let allocation = inputs.retain().expect("native creation input");
        let result = candidate
            .checkout
            .apply_borrowed_authored_transaction_with_native_work(
                &changes,
                &allocation,
                work,
                cancellation,
            )
            .await
            .map_err(fs_error)?;
        drop(result);
        drop(changes);
        drop(allocation);
        let mut inspector = candidate
            .checkout
            .inspector(work, cancellation)
            .map_err(MountSourceError::Filesystem)?;
        let created = inspector
            .lookup_no_follow_with_metadata(&path, work, cancellation)
            .await
            .map_err(MountSourceError::Filesystem)?
            .ok_or(MountSourceError::Stale)?;
        drop(inspector);
        let publication = self
            .checkout
            .install_candidate(
                candidate,
                epoch,
                stamp,
                &mut carrier,
                cancellation,
                Some(&self.cancellation),
                |carrier| {
                    self.with_capture_observer_dispatch(|| {
                        record_view_change(
                            &self.checkout.ledger,
                            &ViewChange::Bound(&path),
                            carrier,
                        )
                    })
                },
                |checkout, candidate, prepared| {
                    let gate = self
                        .capture_gate
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    if gate.cancelled {
                        return Err(MountSourceError::Stale);
                    }
                    cancellation
                        .commit_if_both_live(&self.cancellation, || {
                            checkout.adopt_prepared_candidate(
                                &candidate.base,
                                candidate.checkout,
                                prepared,
                            )
                        })
                        .map_err(|_| MountSourceError::Cancelled)
                },
                None,
                |publication, _| publication,
            )
            .await?
            .ok_or(MountSourceError::Stale)?;
        if publication == MountPublication::PerMutation {
            self.checkout
                .seal_original_with_carrier_guarded(
                    crate::PublicationPermit::Unrestricted,
                    false,
                    &mut carrier,
                    cancellation,
                    Some(&self.cancellation),
                    &mut source_publication_guard(
                        self.native_callback_guard.clone(),
                        Some(self.cancellation.clone()),
                    ),
                )
                .await?;
        }
        Ok(MountLookup {
            node: mount_node(created.record),
            metadata: created.metadata,
        })
    }
}
