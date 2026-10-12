use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Lists one page of `path`, or fails stale when the view changed
    pub(super) fn directory_page(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            let lease = self.source_view.read_for_callback(owner, None).await?;
            let (removed_children, opaque, removal_epoch) = self.removal_snapshot(&text)?;
            if opaque && self.authored.lookup_async(path, owner).await?.is_none() {
                return Err(MountSourceError::NotFound);
            }
            let generation = (lease.generation, removal_epoch);
            // A continuation stays exact while its directory's listing does.
            let (stamp, cursor) = match self.take_cursor(generation, cursor)? {
                Some((stamp, cursor)) => (stamp, Some(cursor)),
                None => (ViewStamp::current(), None),
            };
            if !self.authored.unchanged_since(path, None, stamp) {
                return Err(MountSourceError::Stale);
            }
            // Each listed source entry is resolved exactly as a lookup
            // resolves it, so a later lookup or read reuses that deferral.
            let deferrals = self.source_view.is_stable();
            let (page, state) = {
                let observation = self.authored.shared_checkout().observe(owner).await?;
                observation.ensure_publication_resolved()?;
                self.lazy
                    .list_directory_in_checkout(
                        &mut observation.observer(),
                        &text,
                        cursor,
                        maximum_entries,
                        Some((&removed_children, opaque)),
                    )
                    .await
                    .map_err(|error| match error {
                        // The directory changed while it was listed.
                        LazyWorkspaceError::Demand(DemandError::StaleVersion) => {
                            MountSourceError::Stale
                        }
                        error => lazy_error(error),
                    })?
            };
            let mut entries = Vec::with_capacity(page.entries.len());
            for entry in page.entries {
                let child = logical_child_path(&text, &entry.name).ok_or_else(|| {
                    MountSourceError::Unsupported(
                        "lazy mount cannot address a non-Unicode name".to_owned(),
                    )
                })?;
                let name = super::adapter::native_mount_name(&entry.name)?;
                let mounted_child = path.child(name.clone());
                let (lookup, pin) = if let Some(node) = entry.source {
                    let (lookup, source) = self
                        .lazy
                        .inspect_listed(&state, &child, node, entry.unobserved)
                        .await
                        .map_err(lazy_error)?;
                    if deferrals && let (LazyLookup::Source(node), Some(source)) = (&lookup, source)
                    {
                        self.resolutions.remember(
                            &mounted_child,
                            Deferral {
                                stamp,
                                source,
                                node: Some(*node),
                            },
                        );
                    }
                    self.project_unauthored(&child, lookup, source).await?
                } else if let Some(authored) =
                    self.authored.lookup_async(&mounted_child, owner).await?
                {
                    if self.removed_identity(&child, authored.node.file_id)? {
                        continue;
                    }
                    (self.project_authored(authored).await?, None)
                } else {
                    let (lookup, source) = self
                        .lazy
                        .inspect_unauthored(&child, Some(&state))
                        .await
                        .map_err(lazy_error)?;
                    self.project_unauthored(&child, lookup, source).await?
                };
                entries.push(MountDirectoryEntry {
                    name,
                    node: lookup.node,
                    metadata: lookup.metadata,
                    pin,
                });
            }
            if !self.authored.unchanged_since(path, None, stamp)
                || self.removal_snapshot(&text)?.2 != generation.1
            {
                return Err(MountSourceError::Stale);
            }
            Ok(MountDirectoryPage {
                entries,
                next_cursor: page
                    .next
                    .map(|cursor| self.remember_cursor(generation, (stamp, cursor)))
                    .transpose()?,
            })
        })
    }

pub(super) fn remember_cursor(
        &self,
        generation: (u64, u64),
        cursor: (ViewStamp, LazyDirectoryCursor),
    ) -> Result<Vec<u8>, MountSourceError> {
        self.cursors.remember(generation, cursor)
    }

pub(super) fn take_cursor(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
    ) -> Result<Option<(ViewStamp, LazyDirectoryCursor)>, MountSourceError> {
        self.cursors.take(generation, cursor)
    }
}
