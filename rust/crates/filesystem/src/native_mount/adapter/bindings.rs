impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn remove(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let path = self.namespace_inputs(path, &mut inputs)?;
            let (candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut inspector = candidate.checkout.inspector(work, work.cancellation())?;
            let node = inspector
                .lookup_no_follow(&path, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?
                .file_id;
            drop(inspector);
            let owned = inputs.clone_namespace(path.components(), self.limits)?;
            self.mutate(
                candidate,
                epoch,
                stamp,
                AuthoredMutation::Remove {
                    path: owned,
                    expected_file_id: expected,
                },
                ViewChange::Entry(&path, node),
                work,
            )
            .await
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let source = self.namespace_inputs(source, &mut inputs)?;
            let destination = self.namespace_inputs(destination, &mut inputs)?;
            let (candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut inspector = candidate.checkout.inspector(work, work.cancellation())?;
            let moved = inspector
                .lookup_no_follow(&source, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?
                .file_id;
            let replaced = inspector
                .lookup_no_follow(&destination, work, work.cancellation())
                .await?
                .record
                .map(|record| record.file_id);
            drop(inspector);
            let change = AuthoredMutation::Rename {
                source: inputs.clone_namespace(source.components(), self.limits)?,
                destination: inputs.clone_namespace(destination.components(), self.limits)?,
                replace,
            };
            self.mutate(
                candidate,
                epoch,
                stamp,
                change,
                ViewChange::Moved {
                    from: &source,
                    to: &destination,
                    moved,
                    replaced,
                },
                work,
            )
            .await
        })
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let source = self.namespace_inputs(source, &mut inputs)?;
            let destination = self.namespace_inputs(destination, &mut inputs)?;
            let (candidate, epoch, stamp) = self
                .checkout
                .candidate(
                    None,
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut inspector = candidate.checkout.inspector(work, work.cancellation())?;
            let node = inspector
                .lookup_no_follow(&source, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?
                .file_id;
            drop(inspector);
            let change = AuthoredMutation::HardLink {
                source: inputs.clone_namespace(source.components(), self.limits)?,
                destination: inputs.clone_namespace(destination.components(), self.limits)?,
            };
            self.mutate(
                candidate,
                epoch,
                stamp,
                change,
                ViewChange::Entry(&destination, node),
                work,
            )
            .await
        })
    }
}
