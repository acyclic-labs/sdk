//! Host-owned lazy skill sources. Manifests carry opaque retained identities,
//! never authority to open a host path supplied by a task.
use crate::context::ContextDiscoveryLimits;
use crate::context::{ContextRoot, PinnedContextPath, SkillMetadata, parse_skill_metadata};
use crate::conversation::VolumeRef;
use crate::{Error, Result};
use acyclic_fs::{
    demand::{
        DemandError, DemandSource, SourceNode, SourceNodeKind, SourceReference,
        native::NativeDemandSource,
    },
    kernel::NamespacePath,
};
use std::{
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
};

type View = acyclic_fs::LazyWorkspace<
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
    NativeDemandSource,
    acyclic_fs::LocalCoreStateStore,
>;

#[derive(serde::Serialize, serde::Deserialize)]
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
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ShadowSource {
    volume: VolumeRef,
    cut: acyclic_fs::LazySnapshotRef,
    file_id: acyclic_fs::FileId,
}

type UserCatalog = Option<(Vec<SkillMetadata>, Bounds)>;

type Catalog = Option<(acyclic_fs::LazySnapshotId, Vec<SkillMetadata>)>;

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
}

impl NativeSkills {
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
        let temporary = self.directory.join(uuid::Uuid::new_v4().to_string());
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
        }))
    }

    fn retain(
        &self,
        source: &NativeDemandSource,
        root: &Path,
        path: &str,
        node: SourceNode,
    ) -> Result<PinnedContextPath> {
        let record = Source {
            volume: self.journal.clone(),
            root: root.into(),
            root_identity: source.root_identity().to_bytes().to_vec(),
            reference: source.reference(),
            path: path.into(),
            node,
        };
        self.retain_record(&RetainedSource::Native(Box::new(record)))
    }

    fn retain_record(&self, record: &RetainedSource) -> Result<PinnedContextPath> {
        let bytes = serde_json::to_vec(record).map_err(storage)?;
        let digest = blake3::hash(&bytes);
        let destination = self.directory.join(format!("{digest}.json"));
        if !destination.exists() {
            let temporary = self.directory.join(uuid::Uuid::new_v4().to_string());
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(storage)?;
            file.write_all(&bytes).map_err(storage)?;
            file.sync_all().map_err(storage)?;
            drop(file);
            if let Err(error) = std::fs::rename(&temporary, &destination) {
                std::fs::remove_file(&temporary).map_err(storage)?;
                if !destination.exists() {
                    return Err(storage(error));
                }
            }
        }
        Ok(PinnedContextPath {
            root: ContextRoot {
                volume: self.journal.clone(),
                directory: "inputs/skills".into(),
            },
            generation: crate::resources::GenerationRef::new(
                self.journal.provider(),
                digest.as_bytes(),
                None,
                None,
            )?
            .0,
            path: format!("inputs/skills/{digest}/SKILL.md"),
        })
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
        let bytes = match std::fs::read(self.directory.join(format!("{digest}.json"))) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        if blake3::hash(&bytes).to_hex().as_str() != digest {
            return Err(Error::Conflict("retained skill source changed".into()));
        }
        let retained: RetainedSource = serde_json::from_slice(&bytes).map_err(storage)?;
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
        match retained {
            RetainedSource::Shadow(record) => {
                self.read_shadow(volume, &record, maximum, work).await
            }
            RetainedSource::Native(record) => {
                Self::read_native(volume, &record, maximum, work).await
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

    pub async fn catalog(&self, root: &Path, view: &View) -> Result<Vec<SkillMetadata>> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = native_work(&cancellation)?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let cut = view.snapshot(&mut accounting).await?;
        let mut cached = crate::sync::owned::lock_owned(self.cache.clone()).await;
        if let Some((id, skills)) = &*cached
            && id == &cut.id
        {
            return Ok(skills.clone());
        }
        let mut skills = self.user_catalog().await?;
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
        let scan = ProjectCatalog {
            sources: self,
            root,
            view,
            cut,
            native,
        };
        let mut bounds = crate::sync::owned::lock_owned(self.users.clone())
            .await
            .as_ref()
            .map(|(_, bounds)| *bounds)
            .unwrap_or_default();
        for directory in [".agents/skills", ".claude/skills"] {
            bounds.directory()?;
            let mut cursor = None;
            loop {
                let listed = view
                    .list_directory_at_snapshot(
                        cut,
                        &format!("/{directory}"),
                        cursor,
                        bounds.page()?,
                        &mut accounting,
                    )
                    .await;
                let (page, _, _allocation) = match listed {
                    Ok(page) => page,
                    Err(
                        acyclic_fs::LazyWorkspaceError::NotFound
                        | acyclic_fs::LazyWorkspaceError::Filesystem(acyclic_fs::FsError::NotFound),
                    ) => break,
                    Err(error) => return Err(error.into()),
                };
                bounds.entries(page.entries.len())?;
                for entry in &page.entries {
                    if entry.kind != SourceNodeKind::Directory {
                        continue;
                    }
                    bounds.directory()?;
                    let (name, _allocation) = entry
                        .name
                        .unicode_text(&work, &cancellation)
                        .map_err(storage)?;
                    if let Some(skill) = scan
                        .skill(directory, &name, &bounds, &mut accounting)
                        .await?
                    {
                        skills.push(skill);
                    }
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
        }
        validate(&skills)?;
        *cached = Some((cut.id, skills.clone()));
        Ok(skills)
    }

    pub async fn user_catalog(&self) -> Result<Vec<SkillMetadata>> {
        let mut cached = crate::sync::owned::lock_owned(self.users.clone()).await;
        if let Some((skills, _)) = &*cached {
            return Ok(skills.clone());
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = native_work(&cancellation)?;
        let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let mut bounds = Bounds::default();
        let mut skills = Vec::new();
        for root in &self.user_roots {
            bounds.directory()?;
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
            let reference = source.reference();
            let (path, _allocation) = NamespacePath::parse_in_profile(
                "/",
                config.profile,
                config.limits,
                &mut accounting,
                &cancellation,
            )?;
            let mut cursor = None;
            loop {
                let mut page = source
                    .list_page(
                        reference,
                        &path,
                        cursor,
                        bounds.page()?,
                        &work,
                        &cancellation,
                    )
                    .await
                    .map_err(storage)?;
                bounds.entries(page.entries.len())?;
                for entry in &page.entries {
                    if entry.node.kind != SourceNodeKind::Directory {
                        continue;
                    }
                    bounds.directory()?;
                    let (name, _allocation) = entry
                        .name
                        .unicode_text(&work, &cancellation)
                        .map_err(storage)?;
                    if let Some(skill) = self
                        .user_skill(&source, root, &name, &work, bounds.limits.header_bytes)
                        .await?
                    {
                        skills.push(skill);
                    }
                }
                if let Some(publication) = page.publication.as_mut() {
                    publication.accept(&mut accounting).map_err(storage)?;
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
        }
        validate(&skills)?;
        *cached = Some((skills.clone(), bounds));
        Ok(skills)
    }

    async fn user_skill(
        &self,
        source: &NativeDemandSource,
        root: &Path,
        name: &str,
        work: &acyclic_fs::NativeMaterializationWork,
        header_bytes: u32,
    ) -> Result<Option<SkillMetadata>> {
        let reference = source.reference();
        let cancellation = work.cancellation();
        let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
        let mut accounting =
            acyclic_fs::kernel::KernelAccounting::NativeOriginal { work, cancellation };
        let path = format!("/{name}/SKILL.md");
        let (parsed, _allocation) = NamespacePath::parse_in_profile(
            &path,
            config.profile,
            config.limits,
            &mut accounting,
            cancellation,
        )?;
        let Some(node) = source
            .lookup(reference, &parsed, work, cancellation)
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        if node.kind != SourceNodeKind::RegularFile {
            return Err(Error::Invalid("skill entry is not a file".into()));
        }
        let length = node
            .logical_bytes
            .ok_or_else(|| Error::Invalid("skill length is absent".into()))?;
        let prefix = source
            .read_range(
                reference,
                &parsed,
                node.version,
                0,
                length.min(u64::from(header_bytes)),
                work,
                cancellation,
            )
            .await
            .map_err(storage)?;
        Ok(Some(parse_skill_metadata(
            prefix.bytes.as_ref(),
            self.retain(source, root, &path, node)?,
        )?))
    }

    pub async fn user_discovery(
        &self,
    ) -> Result<(Vec<SkillMetadata>, crate::context::DiscoveryWork)> {
        let skills = self.user_catalog().await?;
        let cached = crate::sync::owned::lock_owned(self.users.clone()).await;
        let bounds = cached
            .as_ref()
            .map(|(_, bounds)| *bounds)
            .unwrap_or_default();
        Ok((
            skills,
            crate::context::DiscoveryWork {
                entries: bounds.entries,
                directories: bounds.directories,
                limits: bounds.limits,
            },
        ))
    }
}

struct ProjectCatalog<'a> {
    sources: &'a NativeSkills,
    root: &'a Path,
    view: &'a View,
    cut: acyclic_fs::LazySnapshotRef,
    native: NativeDemandSource,
}
impl ProjectCatalog<'_> {
    async fn skill(
        &self,
        directory: &str,
        name: &str,
        bounds: &Bounds,
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<Option<SkillMetadata>> {
        let relative = format!("{directory}/{name}/SKILL.md");
        let path = format!("/{relative}");
        let (lookup, _, allocation) = self
            .view
            .lookup_at_snapshot(self.cut, &path, accounting)
            .await?;
        let Some(lookup) = lookup else {
            drop(allocation);
            return Ok(None);
        };
        let stat = lookup.stat();
        if stat.kind != SourceNodeKind::RegularFile {
            return Err(Error::Invalid("skill entry is not a file".into()));
        }
        let length = stat
            .logical_bytes
            .ok_or_else(|| Error::Invalid("skill length is absent".into()))?;
        let pinned = match &lookup {
            acyclic_fs::LazyLookup::Source { node, .. } => {
                self.sources.retain(&self.native, self.root, &path, *node)?
            }
            acyclic_fs::LazyLookup::Authored { record, .. }
            | acyclic_fs::LazyLookup::Shadow { record, .. } => {
                // Both records belong to this immutable cut, including a
                // selected child. Load through the journal with native work.
                self.sources
                    .retain_record(&RetainedSource::Shadow(Box::new(ShadowSource {
                        volume: self.sources.journal.clone(),
                        cut: self.cut,
                        file_id: record.file_id,
                    })))?
            }
        };
        self.view.retire_lookup((lookup, allocation), accounting)?;
        let (prefix, _) = self
            .view
            .read_at_snapshot(
                self.cut,
                &path,
                Some(acyclic_fs::ByteRange {
                    offset: 0,
                    length: length.min(u64::from(bounds.limits.header_bytes)),
                }),
                u64::from(bounds.limits.header_bytes),
                accounting,
            )
            .await?;
        Ok(Some(parse_skill_metadata(prefix.bytes.as_ref(), pinned)?))
    }
}

fn validate(skills: &[SkillMetadata]) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    for skill in skills {
        if !names.insert(&skill.name) {
            return Err(Error::Conflict("skill name is declared twice".into()));
        }
    }
    Ok(())
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

#[derive(Clone, Copy, Default)]
struct Bounds {
    entries: u32,
    directories: u32,
    limits: ContextDiscoveryLimits,
}
impl Bounds {
    fn directory(&mut self) -> Result<()> {
        self.directories += 1;
        if self.directories > self.limits.directories {
            return Err(Error::Invalid("skill directory bound exceeded".into()));
        }
        Ok(())
    }
    fn entries(&mut self, count: usize) -> Result<()> {
        self.entries += u32::try_from(count).map_err(storage)?;
        Ok(())
    }
    fn page(&self) -> Result<u32> {
        self.limits
            .entries
            .checked_sub(self.entries)
            .filter(|n| *n > 0)
            .map(|n| n.min(128))
            .ok_or_else(|| Error::Invalid("skill entry bound exceeded".into()))
    }
}
