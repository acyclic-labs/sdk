use super::*;

pub(super) async fn stage_mount_promotion<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    path: &str,
    mounted: &MountPath,
    expected_source: FileId,
) -> Result<(), MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    in_heap(move || async move {
        for _ in 0..MAXIMUM_PROMOTION_RETRIES {
            let mut candidate = authored.shared_checkout().candidate().await?;
            let changed = lazy
                .stage_exact_into_checkout(
                    &mut candidate.checkout,
                    path,
                    expected_source,
                    MAXIMUM_PROMOTION_BYTES,
                    authored.cancellation(),
                )
                .await
                .map_err(lazy_error)?;
            if !changed {
                return Ok(());
            }
            let installed = promotion_effect(lazy, authored, &candidate, path, mounted).await?;
            let mut checkout = authored.shared_checkout().lock().await;
            checkout.ensure_publication_resolved()?;
            if checkout.install_candidate(candidate, &installed) {
                return checkout.publish_mutation(authored.cancellation()).await;
            }
        }
        Err(MountSourceError::Stale)
    })
    .await
}

/// The observable effect of promoting `mounted` and the ancestors staged
pub(super) async fn promotion_effect<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    candidate: &CheckoutCandidate<A, O>,
    path: &str,
    mounted: &MountPath,
) -> Result<ViewEffect, MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    in_heap(move || async move {
        let mut installed = ViewEffect::default();
        let mut prefix = mounted.clone();
        let mut text = path.to_owned();
        loop {
            let after = authored.lookup_in(&candidate.checkout, &prefix).await?;
            let base = authored.lookup_in(candidate.base(), &prefix).await?;
            let parent = prefix.parent();
            if let (None, Some(_), Some(parent)) = (base, after, &parent) {
                installed.listings.push(authored.namespace_path(parent)?);
            }
            let before = match base {
                Some(lookup) => Some(lookup),
                None => match lazy.inspect_unauthored(&text, None).await {
                    Ok((lookup, _)) => {
                        let file_id = lazy
                            .stable_file_id_for_lookup(&text, &lookup)
                            .await
                            .map_err(lazy_error)?;
                        Some(mount_lookup(lookup, file_id))
                    }
                    Err(LazyWorkspaceError::NotFound) => None,
                    Err(error) => return Err(lazy_error(error)),
                },
            };
            if before != after {
                installed.nodes.extend(
                    [before, after]
                        .into_iter()
                        .flatten()
                        .map(|lookup| lookup.node.file_id),
                );
            }
            let Some(parent) = parent else {
                return Ok(installed);
            };
            prefix = parent;
            text = match text.rsplit_once('/') {
                Some(("", _)) | None => "/".to_owned(),
                Some((parent, _)) => parent.to_owned(),
            };
        }
    })
    .await
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(super) async fn promote(&self, mounted: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        in_heap(move || async move {
            let path = self.path(mounted)?;
            let lookup = self.lazy.lookup(&path).await.map_err(lazy_error)?;
            let expected_source = match lookup {
                LazyLookup::Source(node) => self.lazy.source_file_id(&node),
                LazyLookup::Authored { stat, .. } => stat.file_id,
                LazyLookup::Shadow { record, .. } => record.file_id,
            };
            stage_mount_promotion(&self.lazy, &self.authored, &path, mounted, expected_source).await
        })
        .await
    }

pub(super) fn promote_parents_locked(&self, path: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let components = path.components();
        let mut parent = MountPath::root();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            parent = parent.child((*component).to_vec());
            if self.holds_authored_directory(&parent) {
                continue;
            }
            // Sampled first: a rebinding after it retires the confirmation.
            let stamp = self.ledger_stamp();
            if self.authored.lookup(&parent)?.is_none() {
                self.wait(|| async { self.promote(&parent).await })?;
            }
            if let Some(stamp) = stamp {
                let mut held = self
                    .authored_directories
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if held.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
                    held.clear();
                }
                held.insert(parent.clone(), stamp);
            }
        }
        Ok(())
    }

/// Promotes the source directory at `path` and everything beneath it
    pub(super) fn promote_subtree_locked(&self, path: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let mut directories = vec![path.clone()];
        while let Some(directory) = directories.pop() {
            // Listed whole first: promoting what it holds changes the
            // listing, which no cursor outlives.
            let text = self.path(&directory)?;
            let mut unresolved = Vec::new();
            let mut cursor = None;
            loop {
                let page = self.wait(|| async {
                    self.lazy
                        .list_directory(&text, cursor.clone(), DIRECTORY_PROMOTION_PAGE)
                        .await
                        .map_err(lazy_error)
                })?;
                for entry in page.entries {
                    if !entry.authored {
                        let child =
                            directory.child(super::adapter::native_mount_name(&entry.name)?);
                        unresolved.push((child, entry.kind == SourceNodeKind::Directory));
                    }
                }
                match page.next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            self.promote_locked(&directory)?;
            for (child, is_directory) in unresolved {
                if is_directory {
                    directories.push(child);
                } else {
                    self.promote_locked(&child)?;
                }
            }
        }
        Ok(())
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(super) fn promote_locked(&self, path: &MountPath) -> Result<(), MountSourceError> {
        if self.authored.lookup(path)?.is_some() {
            return Ok(());
        }
        if self.is_removed(&self.path(path)?)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| self.promote(path))
    }
}
