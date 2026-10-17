use super::*;
use crate::access::Action;

/// A selected root whose identity and current disclosure rights were checked
/// under the publication lease. No unchecked root enters a mount constructor.
pub(in crate::native_mount::customer) struct AdmittedRoot<'a> {
    selection: AdmittedSelection<'a>,
    root: (
        crate::kernel::NamespacePath,
        crate::kernel::KernelOwnedAllocation,
    ),
    text: (String, crate::kernel::KernelOwnedAllocation),
}

impl<'a> AdmittedRoot<'a> {
    pub(in crate::native_mount::customer) async fn admit<A, O>(
        options: &'a MountOptions,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        work: &NativeMaterializationWork,
    ) -> Result<Self, MountLifecycleError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let selection =
            AdmittedSelection::bind(options, workspace, generation, work, false).await?;
        let mut accounting = KernelAccounting::native(work, work.cancellation());
        let root = crate::workspace::customer_path_with_accounting(
            &options.subdirectory,
            selection.config,
            &mut accounting,
            work.cancellation(),
        )?;
        let text = root
            .0
            .unicode_text_with_accounting(&mut accounting, work.cancellation())
            .map_err(WorkspaceError::from)?;
        let admitted = Self {
            selection,
            root,
            text,
        };
        admitted.require_list()?;
        Ok(admitted)
    }

    fn require_list(&self) -> Result<(), MountLifecycleError> {
        if !self
            .selection
            .view
            .prepare_namespace(&self.root.0)
            .map_err(|_| MountSourceError::AccessDenied)?
            .decide(
                Action::List,
                super::super::super::adapter::access::access_time()?,
            )
            .allowed
        {
            return Err(MountSourceError::AccessDenied.into());
        }
        Ok(())
    }

    pub(in crate::native_mount::customer) async fn checkout_source<A, O>(
        self,
        mut checkout: crate::Checkout<A, O>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<CheckoutMountSource<A, O>>, MountLifecycleError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.selection.require_checkout(&checkout)?;
        let selected = checkout
            .lookup_no_follow(&self.root.0, work, work.cancellation())
            .await;
        self.require_list()?;
        let selected = selected
            .map_err(WorkspaceError::from)?
            .record
            .ok_or(WorkspaceError::NotFound)?;
        if selected.kind != FileKind::Directory {
            return Err(WorkspaceError::NotDirectory.into());
        }
        self.selection.checkout_source(checkout, self.root, work)
    }

    pub(in crate::native_mount::customer) async fn lazy_source<A, O, D, S>(
        mut self,
        lazy: &LazyWorkspace<A, O, D, S>,
        work: &NativeMaterializationWork,
    ) -> Result<
        (
            Arc<CheckoutMountSource<A, O>>,
            VolumeId,
            (String, crate::kernel::KernelOwnedAllocation),
        ),
        MountLifecycleError,
    >
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        require_volume(
            &self.selection.view,
            lazy.workspace().id().volume_id(),
            lazy.workspace().volume_config(),
        )?;
        let mut accounting = KernelAccounting::native(work, work.cancellation());
        let root_text = &self.text;
        let selected = lazy.lookup(&root_text.0, &mut accounting).await;
        self.require_list()?;
        let selected = selected?;
        let selected_custody = selected.1;
        let selected = selected.0;
        if selected.stat().kind != SourceNodeKind::Directory {
            return Err(WorkspaceError::NotDirectory.into());
        }
        if let LazyLookup::Source { node, .. } = &selected {
            let expected_source = lazy.source_file_id(&node.file_identity, &mut accounting)?;
            work.charge(crate::WorkCounters {
                bytes_hashed: (b"acyclic-fs-lazy-mount-root-v1\0".len()
                    + 16
                    + root_text.0.len()
                    + node.version.0.len()) as u64,
                ..crate::WorkCounters::default()
            })?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-lazy-mount-root-v1\0");
            hasher.update(&lazy.workspace().id().into_bytes());
            hasher.update(root_text.0.as_bytes());
            hasher.update(&node.version.0);
            let mut key = [0; 16];
            key.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            lazy.promote_exact(
                &root_text.0,
                expected_source,
                0,
                IdempotencyKey::from_bytes(key),
                &mut accounting,
            )
            .await?;
        }
        drop(selected);
        drop(selected_custody);

        let options = self.selection.options;
        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let checkout = lazy
            .workspace()
            .checkout(GenerationSelector::Head, mode, work, work.cancellation())
            .await?;
        // Promotion changes data, never policy. Bind the imported generation to
        // the same authenticated owner while retaining the publication lease.
        if checkout.generation_id() != self.selection.generation {
            let (binding, view) = fresh(
                options,
                lazy.workspace(),
                checkout.generation_id(),
                work,
                false,
            )
            .await?;
            self.selection.binding = binding;
            self.selection.view = view;
            self.selection.generation = checkout.generation_id();
        }
        self.require_list()?;
        let source = self.selection.checkout_source(checkout, self.root, work)?;
        Ok((source, lazy.workspace().id().volume_id(), self.text))
    }
}

#[cfg(test)]
mod tests;
