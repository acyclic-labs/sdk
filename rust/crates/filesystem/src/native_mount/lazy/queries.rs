use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Whether facts about `file_id` may be reused on the source's reports.
    pub(super) fn reported(&self, file_id: FileId) -> bool {
        !self
            .linked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&file_id)
    }

/// Whether the source still reports every change made to it.
    pub(super) fn source_observed(&self) -> bool {
        self.source_watch
            .as_ref()
            .is_some_and(|watch| watch.is_exact())
    }

/// A stamp at or after every change the view has recorded, while it is
    pub(super) fn ledger_stamp(&self) -> Option<ViewStamp> {
        self.source_view
            .is_stable()
            .then(|| self.authored.view_stamp())
            .flatten()
    }

pub(super) fn detached_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        Ok(self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .get(&file_id)
            .filter(|entry| entry.ready)
            .map(|entry| Arc::clone(&entry.file)))
    }

/// A detached identity's live state under the link count of the name
    pub(super) async fn detached_identity(
        &self,
        named: MountLookup,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        let Some(file) = self.detached_by_id(named.node.file_id)? else {
            return Ok(None);
        };
        let mut current = file.lookup_async().await?;
        current.node.link_count = named.node.link_count;
        Ok(Some(current))
    }

pub(super) fn has_open_source_handle(&self, file_id: FileId) -> Result<bool, MountSourceError> {
        let mut open = self
            .open_sources
            .lock()
            .map_err(|_| MountSourceError::Stale)?;
        let Some(handles) = open.get_mut(&file_id) else {
            return Ok(false);
        };
        handles.retain(|handle| handle.strong_count() != 0);
        let present = !handles.is_empty();
        if !present {
            open.remove(&file_id);
        }
        Ok(present)
    }

pub(super) fn detached_for_path(
        &self,
        path: &MountPath,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .is_empty()
        {
            return Ok(None);
        }
        if let Some(authored) = self.authored.lookup(path)? {
            return self.detached_by_id(authored.node.file_id);
        }
        let text = self.path(path)?;
        if self.is_removed(&text)? {
            return Err(MountSourceError::NotFound);
        }
        let file_id = self.wait(|| async {
            let lookup = self.lazy.lookup(&text).await.map_err(lazy_error)?;
            self.lazy
                .stable_file_id_for_lookup(&text, &lookup)
                .await
                .map_err(lazy_error)
        })?;
        self.detached_by_id(file_id)
    }

pub(super) fn path(&self, path: &MountPath) -> Result<String, MountSourceError> {
        let mut value = String::new();
        for component in path.components() {
            value.push('/');
            let text = if cfg!(target_os = "windows") {
                let units = crate::kernel::types::utf16le_units(component)
                    .ok_or_else(|| {
                        MountSourceError::Invalid(
                            "native UTF-16 name has an odd byte length".to_owned(),
                        )
                    })?
                    .collect::<Vec<_>>();
                String::from_utf16(&units).map_err(|_| {
                    MountSourceError::Unsupported(
                        "lazy portable mount cannot represent an unpaired UTF-16 name".to_owned(),
                    )
                })?
            } else {
                std::str::from_utf8(component)
                    .map_err(|_| {
                        MountSourceError::Unsupported(
                            "lazy portable mount cannot represent a non-UTF-8 name".to_owned(),
                        )
                    })?
                    .to_owned()
            };
            value.push_str(&text);
        }
        if value.is_empty() {
            value.push('/');
        }
        if self.root == "/" {
            Ok(value)
        } else if value == "/" {
            Ok(self.root.clone())
        } else {
            Ok(format!("{}{value}", self.root))
        }
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

pub(super) fn removed_by(paths: &BTreeMap<String, FileId>, path: &str) -> bool {
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

pub(super) fn removed_identity(&self, path: &str, file_id: FileId) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(removals
            .paths
            .get(path)
            .is_some_and(|removed| *removed == file_id))
    }

/// Resolves `path` through the view: the authored checkout first, then
    pub(super) async fn resolve(
        &self,
        path: &MountPath,
        text: &str,
        owner: std::thread::ThreadId,
        proof: SourceProof,
    ) -> Result<Resolution, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        in_heap(move || async move {
            if let Some(deferral) = self.resolutions.get(path) {
                let file_id = deferral.node.map(|node| self.lazy.source_file_id(&node));
                if deferral.source == self.lazy.source_reference()
                    && self.unchanged_since(path, file_id, deferral.stamp)
                {
                    // A watched source reported any change since; an open at
                    // the exact version proves a remembered node itself.
                    if self.source_observed()
                        || proof == SourceProof::Open && deferral.node.is_some()
                    {
                        return Ok(deferral.resolution());
                    }
                    match self.lazy.source_lookup(deferral.source, text).await {
                        Ok(node) if node == deferral.node => return Ok(deferral.resolution()),
                        // The source changed, or its view was invalidated.
                        Ok(_) | Err(LazyWorkspaceError::StaleSource) => {}
                        Err(error) => return Err(lazy_error(error)),
                    }
                }
            }
            // Sampled first: a change to anything read below records a later
            // position, so a remembered answer can never validate over it.
            let stamp = self.ledger_stamp();
            let source = self.lazy.source_reference();
            if let Some(lookup) = self.authored.lookup_async(path, owner).await? {
                if self.removed_identity(text, lookup.node.file_id)? {
                    return Ok(Resolution::Absent);
                }
                return Ok(Resolution::Authored(lookup));
            }
            if self.is_removed(text)? {
                return Ok(Resolution::Absent);
            }
            let parent = path.parent();
            if parent
                .as_ref()
                .is_some_and(|parent| self.source_lacks(parent))
            {
                return Ok(Resolution::Absent);
            }
            let resolved = match self.lazy.inspect_unauthored(text, None).await {
                Ok(resolved) => resolved,
                Err(LazyWorkspaceError::NotFound) => {
                    if let Some(stamp) = stamp {
                        let node = None;
                        self.resolutions.remember(
                            path,
                            Deferral {
                                stamp,
                                source,
                                node,
                            },
                        );
                        if let Some(parent) = parent {
                            self.observe_source_absence(&parent, stamp, source).await?;
                        }
                    }
                    return Ok(Resolution::Absent);
                }
                Err(error) => return Err(lazy_error(error)),
            };
            if let (Some(stamp), (LazyLookup::Source(node), Some(source))) = (stamp, &resolved) {
                let (source, node) = (*source, Some(*node));
                self.resolutions.remember(
                    path,
                    Deferral {
                        stamp,
                        source,
                        node,
                    },
                );
            }
            Ok(Resolution::Unauthored(resolved.0, resolved.1))
        })
        .await
    }

/// Whether the checkout holds `directory`, as found after a stamp no
    pub(super) fn holds_authored_directory(&self, directory: &MountPath) -> bool {
        self.authored_directories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(directory)
            .copied()
            .is_some_and(|stamp| {
                self.source_view.is_stable()
                    && self.authored.binding_unchanged_since(directory, stamp)
            })
    }

/// Whether the source lacks `directory`, as a watched source read after
    pub(super) fn source_lacks(&self, directory: &MountPath) -> bool {
        self.source_observed()
            && self
                .source_absent
                .get(directory)
                .is_some_and(|(stamp, source)| {
                    source == self.lazy.source_reference()
                        && self.source_view.is_stable()
                        && self.authored.binding_unchanged_since(directory, stamp)
                })
    }

/// Remembers that the source lacks `directory`, which a name beneath it
    pub(super) async fn observe_source_absence(
        &self,
        directory: &MountPath,
        stamp: ViewStamp,
        source: SourceReference,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if directory.components().is_empty()
            || !self.source_observed()
            || self.source_absent.get(directory).is_some()
        {
            return Ok(());
        }
        let text = self.path(directory)?;
        match self.lazy.source_lookup(source, &text).await {
            Ok(None) => self.source_absent.remember(directory, stamp, source),
            Ok(Some(_)) | Err(LazyWorkspaceError::StaleSource) => {}
            Err(error) => return Err(lazy_error(error)),
        }
        Ok(())
    }

/// What a lookup reports for an authored `lookup`: a detached
    pub(super) async fn project_authored(&self, lookup: MountLookup) -> Result<MountLookup, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        Ok(self.detached_identity(lookup).await?.unwrap_or(lookup))
    }

/// What a lookup of `path` reports for an unauthored `lookup` resolved
    pub(super) async fn project_unauthored(
        &self,
        path: &str,
        lookup: LazyLookup,
        source: Option<SourceReference>,
    ) -> Result<(MountLookup, Option<MountContentPin>), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if let LazyLookup::Source(node) = &lookup
            && linked(node)
        {
            self.linked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(self.lazy.source_file_id(node));
        }
        let pin = match (&lookup, source) {
            (LazyLookup::Source(node), Some(source))
                if node.kind == SourceNodeKind::RegularFile =>
            {
                Some(source_content_pin(source, node))
            }
            _ => None,
        };
        let file_id = self
            .lazy
            .stable_file_id_for_lookup(path, &lookup)
            .await
            .map_err(lazy_error)?;
        let projected = mount_lookup(lookup, file_id);
        if let Some(current) = self.detached_identity(projected).await? {
            return Ok((current, None));
        }
        Ok((projected, pin))
    }

/// Resolves `path` like [`Self::resolve`] and, where the source answers
    pub(super) async fn resolve_opened(
        &self,
        path: &MountPath,
        text: &str,
        owner: std::thread::ThreadId,
        proof: SourceProof,
    ) -> Result<(Resolution, Option<Box<dyn DemandFile>>), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        for _ in 0..2 {
            let resolution = self.resolve(path, text, owner, proof).await?;
            let Resolution::Unauthored(LazyLookup::Source(node), Some(source)) = resolution else {
                return Ok((resolution, None));
            };
            if node.kind != SourceNodeKind::RegularFile {
                return Ok((resolution, None));
            }
            match self.lazy.open_source_file(text, source, node).await {
                Ok(file) => return Ok((resolution, Some(file))),
                Err(LazyWorkspaceError::Demand(
                    DemandError::StaleVersion | DemandError::Absent,
                )) => {
                    self.resolutions.forget(path);
                }
                Err(error) => return Err(lazy_error(error)),
            }
        }
        Err(MountSourceError::Stale)
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Records that another handle on the workspace changed each of
    pub(crate) fn changed_outside(&self, paths: &[String]) -> Result<(), MountSourceError> {
        let mut effect = ViewEffect::default();
        for path in paths {
            effect
                .subtrees
                .push(self.lazy.namespace_path(path).map_err(lazy_error)?);
        }
        if !effect.is_empty() {
            self.cursors.clear();
            self.authored
                .record_projection_change(&ViewChange::Effect(&effect));
        }
        Ok(())
    }

/// Whether `path` is answered from its source rather than the authored
    pub(super) fn is_source_projection(&self, path: &MountPath) -> Result<bool, MountSourceError> {
        let text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            if self.authored.lookup_async(path, owner).await?.is_some() {
                return Ok(false);
            }
            if self.is_removed(&text)? {
                return Err(MountSourceError::NotFound);
            }
            match self.lazy.inspect_unauthored(&text, None).await {
                Ok((LazyLookup::Source(_), _)) => Ok(true),
                Ok(_) => Err(MountSourceError::Unsupported(
                    "named attributes of an unpromoted shadow are not projected".to_owned(),
                )),
                Err(error) => Err(lazy_error(error)),
            }
        })
    }
}
