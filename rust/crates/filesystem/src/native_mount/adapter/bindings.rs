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
            self.require_access(
                &path,
                crate::native_mount::actions::Mutation::Delete.actions().0,
                work,
            )
            .await?;
            let candidate = self
                .checkout
                .candidate(
                    None,
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut inspector = candidate.checkout.inspector(
                &mut crate::kernel::allocation::KernelAccounting::native(work, work.cancellation()),
                work.cancellation(),
            )?;
            let node = inspector
                .lookup_no_follow(&path, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?
                .file_id;
            drop(inspector);
            let owned = inputs.clone_namespace(path.components(), self.config.limits)?;
            self.mutate(
                candidate,
                AuthoredMutation::Remove {
                    path: owned,
                    expected_file_id: expected,
                },
                ViewChange::Entry(&path, node),
                None,
                work,
            )
            .await
            .map(drop)
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
            let view = self.access_snapshot(work).await?;
            let now = access::access_time(&self.runtime.clock)?;
            let actions = crate::native_mount::actions::Mutation::Rename.actions();
            access::require(
                &view
                    .pin(&source)
                    .map_err(|_| MountSourceError::AccessDenied)?,
                actions.0,
                now,
            )?;
            access::require(
                &view
                    .pin(&destination)
                    .map_err(|_| MountSourceError::AccessDenied)?,
                actions.1,
                now,
            )?;
            let candidate = self
                .checkout
                .candidate(
                    None,
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut inspector = candidate.checkout.inspector(
                &mut crate::kernel::allocation::KernelAccounting::native(work, work.cancellation()),
                work.cancellation(),
            )?;
            let record = inspector
                .lookup_no_follow(&source, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?;
            if !view.permits_relocation(
                &source,
                &destination,
                record.kind == FileKind::Directory,
                now,
            ) {
                return Err(MountSourceError::AccessDenied);
            }
            let moved = record.file_id;
            let replaced = inspector
                .lookup_no_follow(&destination, work, work.cancellation())
                .await?
                .record
                .map(|record| record.file_id);
            drop(inspector);
            let change = AuthoredMutation::Rename {
                source: inputs.clone_namespace(source.components(), self.config.limits)?,
                destination: inputs
                    .clone_namespace(destination.components(), self.config.limits)?,
                replace,
            };
            self.mutate(
                candidate,
                change,
                ViewChange::Moved {
                    from: &source,
                    to: &destination,
                    moved,
                    replaced,
                },
                None,
                work,
            )
            .await
            .map(drop)
        })
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
        expected: Option<FileId>,
        facts: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountMutationResult, MountSourceError> {
        invoke(&self.runtime, work, || async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let mut inputs = InputBuffers::new(&mut carrier)?;
            let source = self.namespace_inputs(source, &mut inputs)?;
            let destination = self.namespace_inputs(destination, &mut inputs)?;
            let view = self.access_snapshot(work).await?;
            let now = access::access_time(&self.runtime.clock)?;
            let source_authority = view
                .pin(&source)
                .map_err(|_| MountSourceError::AccessDenied)?;
            if facts {
                // Refuse missing metadata authority before installing a name
                // whose native reply requires those facts.
                access::require(&source_authority, crate::access::Action::List, now)?;
            }
            let actions = crate::native_mount::actions::Mutation::HardLink.actions();
            access::require(&source_authority, actions.0, now)?;
            access::require(
                &view
                    .pin(&destination)
                    .map_err(|_| MountSourceError::AccessDenied)?,
                actions.1,
                now,
            )?;
            if !view.permits_relocation(&source, &destination, false, now) {
                return Err(MountSourceError::AccessDenied);
            }
            let candidate = self
                .checkout
                .candidate(
                    None,
                    inputs.carrier,
                    work.cancellation(),
                    Some(&self.cancellation),
                )
                .await?;
            let mut inspector = candidate.checkout.inspector(
                &mut crate::kernel::allocation::KernelAccounting::native(work, work.cancellation()),
                work.cancellation(),
            )?;
            let node = inspector
                .lookup_no_follow(&source, work, work.cancellation())
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?
                .file_id;
            drop(inspector);
            if expected.is_some_and(|expected| expected != node) {
                return Err(MountSourceError::Stale);
            }
            let change = AuthoredMutation::HardLink {
                source: inputs.clone_namespace(source.components(), self.config.limits)?,
                destination: inputs
                    .clone_namespace(destination.components(), self.config.limits)?,
            };
            let mut result = self
                .mutate(
                    candidate,
                    change,
                    ViewChange::Entry(&destination, node),
                    facts.then_some(node),
                    work,
                )
                .await?;
            if result.facts.is_some() {
                match self
                    .require_access(&source, crate::access::Action::List, work)
                    .await
                {
                    Ok(_) => {}
                    Err(MountSourceError::AccessDenied) => result.facts = None,
                    Err(error) => return Err(error),
                }
            }
            Ok(result)
        })
    }
}
