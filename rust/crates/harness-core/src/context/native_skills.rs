//! Host-owned lazy skill sources. Manifests carry opaque retained identities,
//! never authority to open a host path supplied by a task.
use crate::context::native_discovery::LazyDiscovery;
use crate::context::{
    ContextRoot, DiscoveryPage, DiscoverySource, DiscoveryWork, PROJECT_SKILL_ROOTS,
    PinnedContextPath, SkillCatalog, SkillMetadata, SkillReader, discover_skills_from,
};
use crate::conversation::VolumeRef;
use crate::{Error, Result};
use acyclic_fs::{
    demand::{
        DemandError, DemandSource, SourceNode, SourceNodeKind, SourceReference,
        native::NativeDemandSource,
    },
    kernel::NamespacePath,
};
use acyclic_stream::BoxProviderFuture;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

type View = acyclic_fs::LazyWorkspace<
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
    NativeDemandSource,
    acyclic_fs::LocalCoreStateStore,
>;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Source {
    volume: VolumeRef,
    root: PathBuf,
    root_identity: Vec<u8>,
    reference: SourceReference,
    path: String,
    node: SourceNode,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum RetainedSource {
    Native(Box<Source>),
    Shadow(Box<ShadowSource>),
    Batch(std::collections::HashMap<String, RetainedSource>),
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ShadowSource {
    volume: VolumeRef,
    cut: acyclic_fs::LazySnapshotRef,
    file_id: acyclic_fs::FileId,
}

mod retained;
use retained::RetainedBatch;

type UserCatalog = Option<SkillCatalog>;

type Catalog = Option<(acyclic_fs::LazySnapshotId, SkillCatalog)>;

#[derive(Clone)]
struct SkillWorkspace {
    filesystem: acyclic_fs::LocalFs,
    store: acyclic_fs::LocalCoreStateStore,
    view: Arc<View>,
}

#[doc(hidden)]
pub struct NativeSkills {
    directory: PathBuf,
    journal: VolumeRef,
    workspace: Option<SkillWorkspace>,
    pub(crate) materializer: super::skill_materializer::SkillMaterializer,
    user_roots: Vec<PathBuf>,
    cache: crate::sync::owned::Arc<crate::sync::owned::Mutex<Catalog>>,
    users: crate::sync::owned::Arc<crate::sync::owned::Mutex<UserCatalog>>,
    user_sources: crate::sync::owned::Arc<crate::sync::owned::Mutex<UserCatalog>>,
    #[cfg(test)]
    user_pages: Arc<std::sync::atomic::AtomicUsize>,
    records: retained::RecordCache,
}

impl NativeSkills {
    /// Binds the existing host sources to another executing owner's journal.
    ///
    /// This does not grant access or copy owner-bound metadata: retained pins
    /// embed their journal owner, so those caches start empty. Configured user
    /// source discovery stays shared and its pins are rebound before use.
    /// Call `catalog` with that owner's bound view to retain its exact lazy cut.
    /// Retained source storage and decoded records remain shared without I/O.
    pub fn for_volume(&self, journal: VolumeRef) -> Arc<Self> {
        Arc::new(Self {
            directory: self.directory.clone(),
            journal,
            workspace: self.workspace.clone(),
            materializer: super::skill_materializer::SkillMaterializer::new(),
            user_roots: self.user_roots.clone(),
            cache: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None)),
            users: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None)),
            user_sources: self.user_sources.clone(),
            #[cfg(test)]
            user_pages: Arc::clone(&self.user_pages),
            records: self.records.clone(),
        })
    }

    pub(crate) fn retain_sources_from(&mut self, previous: &Self) {
        self.directory = previous.directory.clone();
        self.materializer = previous.materializer.clone();
    }

    pub(crate) fn volume(&self) -> &VolumeRef {
        &self.journal
    }
    pub(crate) fn materialized(
        &self,
        generation: &crate::resources::GenerationRef,
    ) -> Result<Option<crate::resources::GenerationRef>> {
        let path = self.directory.join(format!(
            "{}.body.json",
            hex::encode(generation.as_resource().key())
        ));
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(storage),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    pub(crate) fn retain_materialized(
        &self,
        source: &crate::resources::GenerationRef,
        generation: &crate::resources::GenerationRef,
    ) -> Result<()> {
        let path = self.directory.join(format!(
            "{}.body.json",
            hex::encode(source.as_resource().key())
        ));
        let temporary = self
            .directory
            .join(crate::identity::fresh_uuid().to_string());
        std::fs::write(&temporary, serde_json::to_vec(generation).map_err(storage)?)
            .map_err(storage)?;
        if let Err(error) = std::fs::rename(&temporary, &path) {
            std::fs::remove_file(&temporary).map_err(storage)?;
            if !path.exists() {
                return Err(storage(error));
            }
        }
        Ok(())
    }

    pub(crate) fn matches(
        &self,
        volume: &VolumeRef,
        generation: &crate::resources::GenerationRef,
        path: &str,
    ) -> bool {
        volume == &self.journal
            && path
                == format!(
                    "inputs/skills/{}/SKILL.md",
                    hex::encode(generation.as_resource().key())
                )
    }
    pub fn new(
        directory: PathBuf,
        journal: VolumeRef,
        workspace: Option<(&View, acyclic_fs::LocalFs, acyclic_fs::LocalCoreStateStore)>,
        user_roots: Vec<PathBuf>,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&directory).map_err(storage)?;
        let workspace = workspace
            .map(|(view, filesystem, store)| {
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = native_work(&cancellation)?;
                let view =
                    view.share(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work: &work,
                        cancellation: &cancellation,
                    })?;
                Ok::<_, Error>(SkillWorkspace {
                    filesystem,
                    store,
                    view,
                })
            })
            .transpose()?;
        Ok(Arc::new(Self {
            directory,
            journal,
            workspace,
            materializer: super::skill_materializer::SkillMaterializer::new(),
            user_roots,
            cache: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None)),
            users: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None)),
            user_sources: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(None)),
            #[cfg(test)]
            user_pages: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            records: retained::RecordCache::default(),
        }))
    }

    fn retain(
        &self,
        batch: &mut RetainedBatch,
        source: &NativeDemandSource,
        root: &Path,
        path: &str,
        node: SourceNode,
    ) -> Result<PinnedContextPath> {
        batch.retain(
            &self.journal,
            RetainedSource::Native(Box::new(Source {
                volume: self.journal.clone(),
                root: root.into(),
                root_identity: source.root_identity().to_bytes().to_vec(),
                reference: source.reference(),
                path: path.into(),
                node,
            })),
        )
    }

    /// Only host-retained source records, in this owner's private journal, resolve here.
    #[inline(never)]
    pub(crate) fn read<'a>(
        &'a self,
        volume: &'a VolumeRef,
        generation: &'a crate::resources::GenerationRef,
        path: &'a str,
        maximum: u64,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<acyclic_stream::OwnedBytes>>>
    {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.read_retained(volume, generation, path, maximum, io)
        })
    }

    async fn read_retained(
        &self,
        volume: &VolumeRef,
        generation: &crate::resources::GenerationRef,
        path: &str,
        maximum: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        if !self.matches(volume, generation, path) {
            return Ok(None);
        }
        let digest = hex::encode(generation.as_resource().key());
        if path != format!("inputs/skills/{digest}/SKILL.md") {
            return Ok(None);
        }
        let Some(retained) = self.records.read(self.directory.clone(), digest).await? else {
            return Ok(None);
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let owned;
        let work = if let Some(io) = io {
            io.original_native_work().ok_or_else(|| {
                Error::Unsupported("native skill reads require original native work".into())
            })?
        } else {
            owned = native_work(&cancellation)?;
            &owned
        };
        match retained.as_ref() {
            RetainedSource::Batch(_) => Err(Error::Invalid("nested retained skill batch".into())),
            RetainedSource::Shadow(record) => self.read_shadow(volume, record, maximum, work).await,
            RetainedSource::Native(record) => {
                Self::read_native(volume, record, maximum, work).await
            }
        }
    }

    async fn read_shadow(
        &self,
        volume: &VolumeRef,
        record: &ShadowSource,
        maximum: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        if &record.volume != volume {
            return Err(Error::Unauthorized(
                "skill source belongs to another journal".into(),
            ));
        }
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Conflict("skill checkout is absent".into()))?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        };
        let related;
        let view = if record.cut.workspace_id == workspace.view.workspace().id() {
            workspace.view.as_ref()
        } else {
            let selected = acyclic_fs::WorkspaceResolver::resolve(
                &(
                    workspace.filesystem.clone(),
                    acyclic_fs::WorkspaceGraph::new(workspace.store.clone()),
                ),
                record.cut.workspace_id,
            )
            .await
            .map_err(storage)?;
            related = workspace
                .view
                .open_related(selected, &mut accounting)
                .await?;
            &related
        };
        Ok(Some(
            view.read_file_at_snapshot(record.cut, record.file_id, maximum, &mut accounting)
                .await?
                .bytes,
        ))
    }

    async fn read_native(
        volume: &VolumeRef,
        record: &Source,
        maximum: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        if record.volume != *volume {
            return Err(Error::Unauthorized(
                "skill source belongs to another journal".into(),
            ));
        }
        let length = record
            .node
            .logical_bytes
            .ok_or_else(|| Error::Invalid("skill length is absent".into()))?;
        if length > maximum {
            return Err(Error::Invalid("skill body exceeds byte bound".into()));
        }
        let cancellation = work.cancellation();
        let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
        let source = NativeDemandSource::open(
            std::borrow::Cow::Borrowed(&record.root),
            config.profile,
            config.limits,
            Some(record.reference),
            None,
            work,
            cancellation,
        )
        .await
        .map_err(storage)?;
        if source.root_identity().to_bytes().as_slice() != record.root_identity {
            return Err(Error::Conflict("skill source root changed".into()));
        }
        let mut accounting =
            acyclic_fs::kernel::KernelAccounting::NativeOriginal { work, cancellation };
        let (path, _allocation) = NamespacePath::parse_in_profile(
            &record.path,
            config.profile,
            config.limits,
            &mut accounting,
            cancellation,
        )?;
        let chunk = source
            .read_range(
                record.reference,
                &path,
                record.node.version,
                0,
                length,
                work,
                cancellation,
            )
            .await
            .map_err(storage)?;
        Ok(Some(chunk.bytes))
    }

    #[inline(never)]
    pub fn catalog<'a>(
        &'a self,
        root: &'a Path,
        view: &'a View,
        bounds: &'a mut DiscoveryWork,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<SkillMetadata>>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            bounds.limits.validate()?;
            let before = *bounds;
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = native_work(&cancellation)?;
            let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            };
            let cut = view.snapshot(&mut accounting).await?;
            let mut cached = crate::sync::owned::lock_owned(self.cache.clone()).await;
            if let Some((id, catalog)) = &*cached
                && id == &cut.id
                && let Some(skills) = catalog.reuse(bounds)?
            {
                return Ok(skills);
            }
            let mut skills = self.user_catalog(bounds)?.await?;
            let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
            let native = NativeDemandSource::open(
                std::borrow::Cow::Borrowed(root),
                config.profile,
                config.limits,
                Some(cut.source),
                None,
                &work,
                &cancellation,
            )
            .await
            .map_err(storage)?;
            let mut records = RetainedBatch::default();
            let mut scan = ProjectCatalog {
                sources: self,
                root,
                directory: LazyDiscovery {
                    view,
                    cut,
                    work: &work,
                },
                native,
                records: &mut records,
            };
            for directory in PROJECT_SKILL_ROOTS {
                skills.extend(
                    discover_skills_from(
                        &mut scan,
                        directory,
                        "SKILL.md",
                        bounds,
                        Some(&cut),
                        true,
                    )
                    .await?,
                );
            }
            let catalog = SkillCatalog::capture(skills.clone(), *bounds, before)?;
            drop(scan);
            records.publish(self.directory.clone()).await?;
            *cached = Some((cut.id, catalog));
            Ok(skills)
        })
    }

    #[inline(never)]
    pub fn user_catalog<'a>(
        &'a self,
        bounds: &'a mut DiscoveryWork,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<SkillMetadata>>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            bounds.limits.validate()?;
            let before = *bounds;
            let mut cached = crate::sync::owned::lock_owned(self.users.clone()).await;
            if let Some(catalog) = &*cached
                && let Some(skills) = catalog.reuse(bounds)?
            {
                return Ok(skills);
            }
            let mut skills = self.source_user_catalog(bounds)?.await?;
            let mut records = RetainedBatch::default();
            for skill in &mut skills {
                if skill.source.root.volume == self.journal {
                    continue;
                }
                let retained = self
                    .records
                    .read(
                        self.directory.clone(),
                        hex::encode(skill.source.generation.as_resource().key()),
                    )
                    .await?
                    .ok_or_else(|| {
                        Error::NotFound("retained user skill source is absent".into())
                    })?;
                let RetainedSource::Native(source) = retained.as_ref() else {
                    return Err(Error::Invalid(
                        "configured user skill source is not native".into(),
                    ));
                };
                if source.volume != skill.source.root.volume {
                    return Err(Error::Conflict("retained user skill owner changed".into()));
                }
                let mut source = source.as_ref().clone();
                source.volume = self.journal.clone();
                skill.source =
                    records.retain(&self.journal, RetainedSource::Native(Box::new(source)))?;
            }
            let catalog = SkillCatalog::capture(skills.clone(), *bounds, before)?;
            records.publish(self.directory.clone()).await?;
            *cached = Some(catalog);
            Ok(skills)
        })
    }

    // This cache belongs to the configured source set, not an executing owner.
    // Its pins remain internal: user_catalog re-owns them before returning.
    #[inline(never)]
    fn source_user_catalog<'a>(
        &'a self,
        bounds: &'a mut DiscoveryWork,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<SkillMetadata>>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            bounds.limits.validate()?;
            let before = *bounds;
            let mut cached = crate::sync::owned::lock_owned(self.user_sources.clone()).await;
            if let Some(catalog) = &*cached
                && let Some(skills) = catalog.reuse(bounds)?
            {
                return Ok(skills);
            }
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = native_work(&cancellation)?;
            let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
            let mut skills = Vec::new();
            let mut roots = std::collections::HashSet::new();
            let mut records = RetainedBatch::default();
            for root in &self.user_roots {
                let source = match NativeDemandSource::open(
                    std::borrow::Cow::Borrowed(root),
                    config.profile,
                    config.limits,
                    None,
                    None,
                    &work,
                    &cancellation,
                )
                .await
                {
                    Ok(source) => source,
                    Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        continue;
                    }
                    Err(error) => return Err(storage(error)),
                };
                // Identity comes from the handle already opened by native I/O,
                // so aliases collapse before any directory entries are scanned.
                if !roots.insert(source.root_identity()) {
                    continue;
                }
                let mut scan = UserCatalogSource {
                    sources: self,
                    source: &source,
                    root,
                    work: &work,
                    records: &mut records,
                };
                skills.extend(
                    discover_skills_from(&mut scan, "", "SKILL.md", bounds, None, false).await?,
                );
            }
            let catalog = SkillCatalog::capture(skills.clone(), *bounds, before)?;
            records.publish(self.directory.clone()).await?;
            *cached = Some(catalog);
            Ok(skills)
        })
    }
}

struct ProjectCatalog<'a> {
    sources: &'a NativeSkills,
    root: &'a Path,
    directory: LazyDiscovery<'a>,
    native: NativeDemandSource,
    records: &'a mut RetainedBatch,
}
impl DiscoverySource for ProjectCatalog<'_> {
    type Revision = acyclic_fs::LazySnapshotRef;
    type Cursor = acyclic_fs::LazyDirectoryCursor;
    fn page<'a>(
        &'a mut self,
        directory: &'a str,
        revision: Option<&'a Self::Revision>,
        cursor: Option<Self::Cursor>,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<DiscoveryPage<Self::Revision, Self::Cursor>>> {
        self.directory.page(directory, revision, cursor, maximum)
    }
}
impl SkillReader for ProjectCatalog<'_> {
    fn skill_prefix<'a>(
        &'a mut self,
        relative: &'a str,
        cut: &'a Self::Revision,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<(Vec<u8>, PinnedContextPath)>> {
        Box::pin(async move {
            let path = format!("/{relative}");
            let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: self.directory.work,
                cancellation: self.directory.work.cancellation(),
            };
            let (lookup, _, allocation) = self
                .directory
                .view
                .lookup_at_snapshot(*cut, &path, &mut accounting)
                .await?;
            let lookup =
                lookup.ok_or_else(|| Error::NotFound("discovered skill disappeared".into()))?;
            let stat = lookup.stat();
            if stat.kind != SourceNodeKind::RegularFile {
                self.directory
                    .view
                    .retire_lookup((lookup, allocation), &mut accounting)?;
                return Err(Error::Invalid("skill entry is not a file".into()));
            }
            let length = stat
                .logical_bytes
                .ok_or_else(|| Error::Invalid("skill length is absent".into()))?;
            let pinned = match &lookup {
                acyclic_fs::LazyLookup::Source { node, .. } => {
                    self.sources
                        .retain(self.records, &self.native, self.root, &path, *node)?
                }
                acyclic_fs::LazyLookup::Authored { record, .. }
                | acyclic_fs::LazyLookup::Shadow { record, .. } => self.records.retain(
                    &self.sources.journal,
                    RetainedSource::Shadow(Box::new(ShadowSource {
                        volume: self.sources.journal.clone(),
                        cut: *cut,
                        file_id: record.file_id,
                    })),
                )?,
            };
            self.directory
                .view
                .retire_lookup((lookup, allocation), &mut accounting)?;
            let (prefix, _) = self
                .directory
                .view
                .read_at_snapshot(
                    *cut,
                    &path,
                    Some(acyclic_fs::ByteRange {
                        offset: 0,
                        length: length.min(u64::from(maximum)),
                    }),
                    u64::from(maximum),
                    &mut accounting,
                )
                .await?;
            Ok((prefix.bytes.to_vec(), pinned))
        })
    }
}

struct UserCatalogSource<'a> {
    sources: &'a NativeSkills,
    source: &'a NativeDemandSource,
    root: &'a Path,
    work: &'a acyclic_fs::NativeMaterializationWork,
    records: &'a mut RetainedBatch,
}
impl DiscoverySource for UserCatalogSource<'_> {
    type Revision = SourceReference;
    type Cursor = acyclic_fs::demand::SourceCursor;
    fn page<'a>(
        &'a mut self,
        directory: &'a str,
        revision: Option<&'a SourceReference>,
        cursor: Option<Self::Cursor>,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<DiscoveryPage<SourceReference, Self::Cursor>>> {
        Box::pin(async move {
            let reference = revision.copied().unwrap_or_else(|| self.source.reference());
            let cancellation = self.work.cancellation();
            let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
            let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: self.work,
                cancellation,
            };
            let (path, _allocation) = NamespacePath::parse_in_profile(
                &format!("/{directory}"),
                config.profile,
                config.limits,
                &mut accounting,
                cancellation,
            )?;
            #[cfg(test)]
            self.sources
                .user_pages
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut page = self
                .source
                .list_page(reference, &path, cursor, maximum, self.work, cancellation)
                .await
                .map_err(storage)?;
            let mut entries = Vec::with_capacity(page.entries.len());
            let scanned = u32::try_from(page.entries.len())
                .map_err(|_| Error::Invalid("entry count overflow".into()))?;
            for entry in &page.entries {
                let Some(name) = super::native_discovery::portable_name(&entry.name, self.work)?
                else {
                    continue;
                };
                entries.push(crate::context::DiscoveryEntry {
                    name,
                    kind: entry.node.kind.into(),
                });
            }
            if let Some(publication) = page.publication.as_mut() {
                publication.accept(&mut accounting).map_err(storage)?;
            }
            Ok(DiscoveryPage {
                revision: reference,
                scanned,
                entries,
                next: page.next,
            })
        })
    }
}
impl SkillReader for UserCatalogSource<'_> {
    fn skill_prefix<'a>(
        &'a mut self,
        path: &'a str,
        reference: &'a SourceReference,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<(Vec<u8>, PinnedContextPath)>> {
        Box::pin(async move {
            let cancellation = self.work.cancellation();
            let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
            let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: self.work,
                cancellation,
            };
            let path = format!("/{path}");
            let (parsed, _allocation) = NamespacePath::parse_in_profile(
                &path,
                config.profile,
                config.limits,
                &mut accounting,
                cancellation,
            )?;
            let node = self
                .source
                .lookup(*reference, &parsed, self.work, cancellation)
                .await
                .map_err(storage)?
                .ok_or_else(|| Error::NotFound("discovered skill disappeared".into()))?;
            if node.kind != SourceNodeKind::RegularFile {
                return Err(Error::Invalid("skill entry is not a file".into()));
            }
            let length = node
                .logical_bytes
                .ok_or_else(|| Error::Invalid("skill length is absent".into()))?;
            let prefix = self
                .source
                .read_range(
                    *reference,
                    &parsed,
                    node.version,
                    0,
                    length.min(u64::from(maximum)),
                    self.work,
                    cancellation,
                )
                .await
                .map_err(storage)?;
            let pinned = self
                .sources
                .retain(self.records, self.source, self.root, &path, node)?;
            Ok((prefix.bytes.to_vec(), pinned))
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod binding_tests {
    use super::*;
    use crate::{
        Capabilities,
        conversation::{ContentAccess, ContentGrant, VolumeClass, VolumeOperation, VolumeOwner},
        core::{AggregateKind, Authority, AuthorityRoot},
        filesystem::{FilesystemHost, PreparationIo},
        resources::ProviderRef,
    };

    #[tokio::test]
    async fn child_skill_binding_reads_own_sources_and_rejects_parent_private_sources() -> Result<()>
    {
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
        let home = tempfile::tempdir().map_err(storage)?;
        let skill = home.path().join("skills/example");
        std::fs::create_dir_all(&skill).map_err(storage)?;
        let body = b"---\nname: example\ndescription: Example skill\n---\nretained body";
        std::fs::write(skill.join("SKILL.md"), body).map_err(storage)?;
        let provider =
            ProviderRef::new("binding".into(), "filesystem".into(), "1".into(), &mut None)?;
        let volume = |id| {
            VolumeRef::new(
                provider.clone(),
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([id; 16])),
                &mut None,
            )
        };
        let parent = volume(1)?;
        let child = volume(2)?;
        let sources = NativeSkills::new(
            home.path().join("retained"),
            parent.clone(),
            None,
            vec![home.path().join("skills")],
        )?;
        let bound = sources.for_volume(child.clone());
        let mut parent_work = DiscoveryWork::default();
        let parent_skills = sources.user_catalog(&mut parent_work)?.await?;
        let pages = sources.user_pages.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            pages > 0,
            "the first catalog must exercise native discovery"
        );
        let mut child_work = DiscoveryWork::default();
        let child_skills = bound.user_catalog(&mut child_work)?.await?;
        assert_eq!(
            sources.user_pages.load(std::sync::atomic::Ordering::SeqCst),
            pages,
            "the first child catalog must reuse configured source discovery",
        );
        assert_eq!(child_work.entries, parent_work.entries);
        assert_eq!(child_work.directories, parent_work.directories);
        sources.user_catalog(&mut DiscoveryWork::default())?.await?;
        assert_eq!(
            sources.user_pages.load(std::sync::atomic::Ordering::SeqCst),
            pages,
            "returning to the parent must also reuse discovery",
        );
        let parent_source = &parent_skills[0].source;
        let child_source = &child_skills[0].source;
        let mut host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        host.create_volume(&parent).await?;
        host.create_volume(&child).await?;
        host = host
            .with_native_skills(sources)?
            .with_native_skills(bound)?;
        let issuer = AuthorityRoot::new(
            "binding",
            [21; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "binding".into(),
            },
        );
        let scope = issuer.bootstrap_scope(
            "child",
            &Capabilities::new([child.capability(VolumeOperation::Read, &mut None)?]),
            Some(crate::AgentId::from_bytes([2; 16])),
            &mut None,
        )?;
        let grant = ContentGrant::verify_directory_read(
            &issuer.verifier(&mut None)?,
            &scope,
            &child,
            "inputs/skills",
            &mut None,
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = native_work(&cancellation)?;
        let io = PreparationIo::from_native_work(&work)?;
        let access = ContentAccess {
            ceiling: None,
            io: Some(&io),
        };
        // Negative controls use the actual child grant: neither the original
        // parent reference nor relabeling its outer volume can read the body.
        assert!(matches!(
            host.read_private_path(
                &parent,
                &grant,
                &parent_source.path,
                Some(&parent_source.generation),
                4096,
                ContentAccess::default(),
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            host.read_private_path(
                &child,
                &grant,
                &parent_source.path,
                Some(&parent_source.generation),
                4096,
                access,
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        let (_, bytes) = host
            .read_private_path(
                &child,
                &grant,
                &child_source.path,
                Some(&child_source.generation),
                4096,
                access,
            )
            .await?;
        assert_eq!(bytes.as_ref(), body);
        Ok(())
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn native_work(
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}
