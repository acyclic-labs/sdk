use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) fn source_observed(&self) -> bool {
        self.source_watch
            .as_ref()
            .and_then(|owner| owner.watch.as_ref())
            .is_some_and(|watch| watch.is_exact())
    }

    pub(super) fn path(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(String, crate::NativeAllocation), MountSourceError> {
        let cancellation = work.cancellation();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (namespace, _allocation) = self.authored.namespace_path(path, &mut carrier)?;
        let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
        let (text, allocation) = namespace
            .unicode_text_with_accounting(&mut accounting, cancellation)
            .map_err(mount_engine_error)?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
            allocation
        else {
            return Err(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ));
        };
        Ok((text, allocation))
    }

    pub(super) fn removal_snapshot(
        &self,
        directory: &str,
    ) -> Result<(Vec<String>, bool, u64), MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let opaque = Self::removed_by(&removals.paths, directory);
        let prefix = if directory == "/" {
            "/".to_owned()
        } else {
            format!("{directory}/")
        };
        let children = removals
            .paths
            .range(prefix.clone()..)
            .take_while(|(path, _)| path.starts_with(&prefix))
            .filter(|(path, _)| {
                path.strip_prefix(&prefix)
                    .is_some_and(|suffix| !suffix.contains('/'))
            })
            .map(|(path, _)| path.clone())
            .collect();
        Ok((children, opaque, removals.epoch))
    }

    pub(super) fn removed_by(
        paths: &BTreeMap<String, (FileId, crate::NativeAllocation)>,
        path: &str,
    ) -> bool {
        let mut current = path;
        loop {
            if paths.contains_key(current) {
                return true;
            }
            if current == "/" {
                return false;
            }
            current =
                current.rsplit_once('/').map_or(
                    "/",
                    |(parent, _)| if parent.is_empty() { "/" } else { parent },
                );
        }
    }

    pub(super) fn is_removed(&self, path: &str) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(Self::removed_by(&removals.paths, path))
    }

    pub(super) fn removed_identity(
        &self,
        path: &str,
        file_id: FileId,
    ) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(removals
            .paths
            .get(path)
            .is_some_and(|(removed, _)| *removed == file_id))
    }

    pub(super) fn record_removed(
        &self,
        path: &MountPath,
        file_id: FileId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let (text, allocation) = self.path(path, work)?;
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals
            .paths
            .insert(text, (file_id, allocation))
            .is_none_or(|(id, _)| id != file_id)
        {
            removals.epoch = removals.epoch.wrapping_add(1);
            self.cursors.clear();
        }
        drop(removals);
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        let (namespace, _allocation) = self.authored.namespace_path(path, &mut carrier)?;
        adapter::record_view_change(
            &self.authored.shared_checkout().ledger,
            &ViewChange::Entry(&namespace, file_id),
            &mut carrier,
        )
    }

    pub(super) fn record_removed_identity(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
        file: Arc<CheckoutDetachedFile<A, O>>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .entry(record.file_id)
            .or_insert(DetachedIdentity {
                file,
                ready: true,
                published_epoch: 0,
                pending_epoch: None,
            });
        self.removals
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .records
            .insert(record.file_id, (record, metadata));
        self.authored.record_projection_change(
            &ViewChange::Node(record.file_id),
            &mut adapter::NativeDemandCarrier::Native(work),
            || Ok(()),
        )?;
        Ok(())
    }

    pub(super) fn record_rebound(&self, path: &str) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.rebound(path) {
            self.cursors.clear();
        }
        Ok(())
    }

    pub(super) async fn resolve(
        &self,
        path: &MountPath,
        text: &str,
        owner: crate::native_mount::view_gate::CallbackOwner,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Resolution, MountSourceError> {
        let cancellation = work.cancellation();
        let mut carrier = adapter::NativeDemandCarrier::Native(work);
        if let Some(lookup) = self
            .authored
            .lookup_async(path, owner, &mut carrier)
            .await?
        {
            return Ok(if self.removed_identity(text, lookup.node.file_id)? {
                Resolution::Absent
            } else {
                Resolution::Authored(lookup)
            });
        }
        if self.is_removed(text)? {
            return Ok(Resolution::Absent);
        }
        let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
        let observation = self
            .authored
            .shared_checkout()
            .observe(
                owner,
                &mut carrier,
                cancellation,
                Some(self.authored.cancellation()),
            )
            .await?;
        observation.ensure_publication_resolved()?;
        let mut checkout = observation
            .observer(work, cancellation)
            .map_err(mount_engine_error)?;
        let (lookup, _, allocation) = self
            .lazy
            .inspect_unauthored(
                &mut checkout,
                text,
                None,
                crate::lazy_workspace::InspectBasis {
                    authored_absent: true,
                    ..Default::default()
                },
                &mut accounting,
            )
            .await
            .map_err(lazy_error)?;
        let Some(lookup) = lookup else {
            return Ok(Resolution::Absent);
        };
        let allocation = match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                Some(allocation)
            }
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => None,
        };
        Ok(Resolution::Unauthored(lookup, allocation))
    }

    pub(super) async fn project_authored(
        &self,
        lookup: MountLookup,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError> {
        let Some(file) = self.detached_by_id(lookup.node.file_id)? else {
            return Ok(lookup);
        };
        let mut current = file.lookup_with_meter(work)?;
        current.node.link_count = lookup.node.link_count;
        Ok(current)
    }

    pub(super) async fn project_unauthored(
        &self,
        path: &str,
        lookup: LazyLookup,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(MountLookup, Option<MountContentPin>), MountSourceError> {
        let pin = match &lookup {
            LazyLookup::Source { node, source } if node.kind == SourceNodeKind::RegularFile => {
                Some(source_content_pin(*source, node))
            }
            _ => None,
        };
        let file_id = file_id(&self.lazy, path, &lookup, work).await?;
        if let LazyLookup::Source { node, .. } = &lookup
            && linked(node)
        {
            self.linked
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .insert(file_id);
        }
        let projected = mount_lookup(lookup, file_id);
        if self.detached_by_id(file_id)?.is_some() {
            return Ok((self.project_authored(projected, work).await?, None));
        }
        Ok((projected, pin))
    }

    pub(super) async fn resolve_opened(
        &self,
        path: &MountPath,
        text: &str,
        owner: crate::native_mount::view_gate::CallbackOwner,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Resolution, Option<Box<dyn DemandFile>>), MountSourceError> {
        for _ in 0..2 {
            let resolution = self.resolve(path, text, owner, work).await?;
            let Resolution::Unauthored(LazyLookup::Source { node, source }, _) = resolution else {
                return Ok((resolution, None));
            };
            if node.kind != SourceNodeKind::RegularFile {
                return Ok((resolution, None));
            }
            let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
            let opened = self
                .lazy
                .open_source_file(text, source, node, &mut accounting)
                .await;
            match opened {
                Ok(file) => return Ok((resolution, Some(file))),
                Err(LazyWorkspaceError::Demand(
                    DemandError::StaleVersion | DemandError::Absent,
                )) => {}
                Err(error) => return Err(lazy_error(error)),
            }
        }
        Err(MountSourceError::Stale)
    }
}
