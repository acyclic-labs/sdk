//! One bounded traversal for immutable volumes and host-owned lazy sources.
use super::*;
use crate::conversation::PrivateDirectoryEntry;
use acyclic_stream::BoxProviderFuture;

/// Storage preserves unsupported node kinds so discovery cannot treat links as files.
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryKind {
    File,
    Directory,
    Other,
}

#[doc(hidden)]
pub struct DiscoveryEntry {
    pub name: String,
    pub kind: DiscoveryKind,
}

impl From<PrivateDirectoryEntry> for DiscoveryEntry {
    fn from(entry: PrivateDirectoryEntry) -> Self {
        Self {
            name: entry.name,
            kind: match entry.kind {
                PrivateDirectoryEntryKind::File => DiscoveryKind::File,
                PrivateDirectoryEntryKind::Directory => DiscoveryKind::Directory,
            },
        }
    }
}

/// A source page preserves its source's opaque cursor and revision.
#[doc(hidden)]
pub struct DiscoveryPage<R, C> {
    pub revision: R,
    /// All source entries visited, including names outside the portable namespace.
    pub scanned: u32,
    pub entries: Vec<DiscoveryEntry>,
    pub next: Option<C>,
}

/// Storage adapters supply advancing pages against the same immutable revision.
/// Opaque cursors must progress, including across empty source/authored phases;
/// ordering, revision validation and optional quotas belong to discovery.
#[doc(hidden)]
pub trait DiscoverySource {
    type Revision: Clone + Eq;
    type Cursor;
    fn page<'a>(
        &'a mut self,
        directory: &'a str,
        revision: Option<&'a Self::Revision>,
        cursor: Option<Self::Cursor>,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<DiscoveryPage<Self::Revision, Self::Cursor>>>;
}

#[doc(hidden)]
pub trait InstructionReader: DiscoverySource {
    fn instruction<'a>(
        &'a mut self,
        path: &'a str,
        revision: &'a Self::Revision,
        maximum: u64,
    ) -> BoxProviderFuture<'a, Result<crate::model::ProjectInstruction>>;
}

#[doc(hidden)]
pub trait SkillReader: DiscoverySource {
    /// Preserves the provider's byte owner while metadata borrows its prefix.
    fn skill_prefix<'a>(
        &'a mut self,
        path: &'a str,
        revision: &'a Self::Revision,
        maximum: u64,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (acyclic_stream::OwnedBytes, PinnedContextPath),
        >,
    >;
}

impl DiscoveryWork {
    /// Reusing a catalog still consumes its discovery work in the caller's one budget.
    pub fn include(&mut self, used: Self) -> Result<()> {
        self.limits.validate()?;
        let entries = self.entries.checked_add(used.entries);
        let directories = self.directories.checked_add(used.directories);
        match (entries, directories) {
            (Some(entries), Some(directories))
                if self.limits.entries.is_none_or(|maximum| entries <= maximum)
                    && self
                        .limits
                        .directories
                        .is_none_or(|maximum| directories <= maximum) =>
            {
                self.entries = entries;
                self.directories = directories;
                Ok(())
            }
            _ => Err(Error::Invalid("discovery work bound exceeded".into())),
        }
    }
    /// Work attributable to a cached component, independent of earlier components.
    pub fn since(&self, before: Self) -> Result<Self> {
        Ok(Self {
            entries: self
                .entries
                .checked_sub(before.entries)
                .ok_or_else(|| Error::Invalid("discovery work moved backwards".into()))?,
            directories: self
                .directories
                .checked_sub(before.directories)
                .ok_or_else(|| Error::Invalid("discovery work moved backwards".into()))?,
            limits: self.limits,
        })
    }
    #[doc(hidden)]
    pub async fn list_source<S: DiscoverySource>(
        &mut self,
        source: &mut S,
        directory: &str,
        revision: Option<&S::Revision>,
    ) -> Result<(S::Revision, Vec<DiscoveryEntry>)> {
        self.limits.validate()?;
        self.directories = self
            .directories
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("discovery work overflow".into()))?;
        if self
            .limits
            .directories
            .is_some_and(|maximum| self.directories > maximum)
        {
            return Err(Error::Invalid("discovery directory bound exceeded".into()));
        }
        let mut pinned = revision.cloned();
        let mut entries: Vec<DiscoveryEntry> = Vec::new();
        let mut cursor = None;
        loop {
            let maximum = match self.limits.entries {
                Some(maximum) => maximum
                    .checked_sub(self.entries)
                    .filter(|remaining| *remaining > 0)
                    .ok_or_else(|| Error::Invalid("discovery entry bound exceeded".into()))?
                    .min(128) as u32,
                None => 128,
            };
            let page = source
                .page(directory, pinned.as_ref(), cursor, maximum)
                .await?;
            if pinned.as_ref().is_some_and(|r| r != &page.revision)
                || page.scanned > maximum
                || page.entries.len() > page.scanned as usize
            {
                return Err(Error::Storage(
                    "discovery page violated pinned range".into(),
                ));
            }
            for entry in &page.entries {
                validate_content_path(&entry.name, &mut None)?;
                if entry.name.contains('/') {
                    return Err(Error::Storage(
                        "discovery page entry is not one name".into(),
                    ));
                }
            }
            pinned = Some(page.revision);
            self.entries = self
                .entries
                .checked_add(u64::from(page.scanned))
                .ok_or_else(|| Error::Invalid("discovery work overflow".into()))?;
            entries.extend(page.entries);
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        entries.sort_unstable_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        if entries
            .iter()
            .zip(entries.iter().skip(1))
            .any(|(left, right)| left.name == right.name)
        {
            return Err(Error::Storage("discovery directory repeats a name".into()));
        }
        Ok((
            pinned.ok_or_else(|| Error::Storage("missing discovery revision".into()))?,
            entries,
        ))
    }
}

/// A cached catalog cannot yield metadata without charging the shared discovery budget.
#[doc(hidden)]
pub struct SkillCatalog {
    skills: Vec<SkillMetadata>,
    usage: DiscoveryWork,
}
impl SkillCatalog {
    pub fn capture(
        skills: Vec<SkillMetadata>,
        mut work: DiscoveryWork,
        before: DiscoveryWork,
    ) -> Result<Self> {
        work.include(DiscoveryWork::default())?;
        validate_skill_catalog(&skills, &mut None)?;
        Ok(Self {
            skills,
            usage: work.since(before)?,
        })
    }
    pub fn reuse(&self, work: &mut DiscoveryWork) -> Result<Option<Vec<SkillMetadata>>> {
        work.limits.validate()?;
        if self.usage.limits.header_bytes != work.limits.header_bytes {
            return Ok(None);
        }
        work.include(self.usage)?;
        Ok(Some(self.skills.clone()))
    }
}

/// The only root-to-active-directory instruction walk, shared by all hosts.
#[doc(hidden)]
pub async fn capture_instructions<S: InstructionReader>(
    source: &mut S,
    directory: &str,
    active_directory: &str,
    names: &[String],
    work: &mut DiscoveryWork,
    revision: Option<&S::Revision>,
) -> Result<Vec<crate::model::ProjectInstruction>> {
    work.limits.validate()?;
    for path in [directory, active_directory] {
        if !path.is_empty() {
            validate_content_path(path, &mut None)?;
        }
    }
    for name in names {
        validate_content_path(name, &mut None)?;
        if name.contains('/') {
            return Err(Error::Invalid(
                "discovery entry must be one filename".into(),
            ));
        }
    }
    if work
        .limits
        .entries
        .is_some_and(|maximum| names.len() as u64 > maximum)
    {
        return Err(Error::Invalid(
            "discovery declarations exceed work bounds".into(),
        ));
    }
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let mut directory = directory.to_owned();
    let mut segments = active_directory.split('/').filter(|s| !s.is_empty());
    let mut pinned = revision.cloned();
    let mut result = Vec::new();
    loop {
        let (r, entries) = work
            .list_source(source, &directory, pinned.as_ref())
            .await?;
        for name in names {
            if let Some(entry) = entries.iter().find(|e| &e.name == name) {
                if entry.kind != DiscoveryKind::File {
                    return Err(Error::Invalid("instruction entry is not a file".into()));
                }
                result.push(
                    source
                        .instruction(
                            &child(&directory, name),
                            &r,
                            // The owner reader's numeric file representation is
                            // the only default whole-file bound. A workflow can
                            // attenuate it explicitly with instruction_bytes.
                            work.limits
                                .instruction_bytes
                                .unwrap_or(crate::conversation::MAX_EXACT_JS_INTEGER),
                        )
                        .await?,
                );
            }
        }
        pinned = Some(r);
        match segments.next() {
            Some(segment) => directory = child(&directory, segment),
            None => break,
        }
    }
    Ok(result)
}

/// The only skill-directory walk; bodies remain lazy and metadata parsing is shared.
#[doc(hidden)]
pub async fn discover_skills_from<S: SkillReader>(
    source: &mut S,
    directory: &str,
    name: &str,
    work: &mut DiscoveryWork,
    revision: Option<&S::Revision>,
    optional_root: bool,
) -> Result<Vec<SkillMetadata>> {
    validate_content_path(name, &mut None)?;
    if name.contains('/') {
        return Err(Error::Invalid(
            "discovery entry must be one filename".into(),
        ));
    }
    let (revision, entries) = match work.list_source(source, directory, revision).await {
        Ok(listed) => listed,
        Err(Error::NotFound(_)) if optional_root => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut result = Vec::new();
    for entry in entries {
        if entry.kind != DiscoveryKind::Directory {
            continue;
        }
        let directory = child(directory, &entry.name);
        let (_, files) = work
            .list_source(source, &directory, Some(&revision))
            .await?;
        let Some(entry) = files.iter().find(|e| e.name == name) else {
            continue;
        };
        if entry.kind != DiscoveryKind::File {
            return Err(Error::Invalid("skill entry is not a file".into()));
        }
        result.push(
            read_skill_metadata(source, &child(&directory, name), &revision, work.limits).await?,
        );
    }
    Ok(result)
}

async fn read_skill_metadata<S: SkillReader>(
    source: &mut S,
    path: &str,
    revision: &S::Revision,
    limits: ContextDiscoveryLimits,
) -> Result<SkillMetadata> {
    read_skill_metadata_parsed(source, path, revision, limits).await?
}

// Preserve the distinction between a failed authorized read and an invalid
// header. Only the latter can be checkpointed as a rejected replacement.
async fn read_skill_metadata_parsed<S: SkillReader>(
    source: &mut S,
    path: &str,
    revision: &S::Revision,
    limits: ContextDiscoveryLimits,
) -> Result<Result<SkillMetadata>> {
    limits.validate()?;
    let mut maximum = limits.header_bytes.unwrap_or(16_384);
    let mut selected = None;
    let (prefix, pinned) = loop {
        let (prefix, pinned) = source.skill_prefix(path, revision, maximum)?.await?;
        if prefix.len() as u64 > maximum {
            return Err(Error::Storage("prefix reader exceeded bound".into()));
        }
        if selected
            .as_ref()
            .is_some_and(|previous| previous != &pinned)
        {
            return Err(Error::Storage(
                "prefix reader changed its pinned source".into(),
            ));
        }
        if frontmatter_end(&prefix).is_ok()
            || limits.header_bytes.is_some()
            || (prefix.len() as u64) < maximum
            || !(prefix.starts_with(b"---\n") || prefix.starts_with(b"---\r\n"))
        {
            break (prefix, pinned);
        }
        selected = Some(pinned);
        maximum = maximum
            .checked_mul(2)
            .ok_or_else(|| Error::Invalid("frontmatter length overflows".into()))?;
    };
    Ok(parse_skill_metadata(&prefix, pinned))
}

impl ContextRoot {
    pub(super) fn is_skill_path(&self, path: &str, name: &str) -> bool {
        let relative = if self.directory.is_empty() {
            path
        } else if let Some(relative) = path
            .strip_prefix(&self.directory)
            .and_then(|tail| tail.strip_prefix('/'))
        {
            relative
        } else {
            return false;
        };
        relative
            .split_once('/')
            .is_some_and(|(directory, entry)| !directory.is_empty() && entry == name)
    }
    /// Reads only the metadata of an immediate skill entry changed at `generation`.
    /// Unrelated paths perform no provider calls. Removal is represented by the
    /// publication's removal entry, rather than a read against a newer head.
    pub async fn skill_metadata_at(
        &self,
        reader: &dyn ContentResidencyVerifier,
        path: &str,
        generation: &GenerationRef,
        name: &str,
        limits: ContextDiscoveryLimits,
    ) -> Result<Option<SkillMetadata>> {
        self.skill_metadata_parsed_at(reader, path, generation, name, limits, None)
            .await?
            .transpose()
    }

    pub(super) async fn skill_metadata_parsed_at(
        &self,
        reader: &dyn ContentResidencyVerifier,
        path: &str,
        generation: &GenerationRef,
        name: &str,
        limits: ContextDiscoveryLimits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Result<SkillMetadata>>> {
        crate::executor::with_history_io(io, |work| {
            self.validate(work)?;
            validate_content_path(name, work)
        })?;
        if name.contains('/') {
            return Err(Error::Invalid("skill entry must be one filename".into()));
        }
        if !self.is_skill_path(path, name) {
            return Ok(None);
        }
        let metadata = read_skill_metadata_parsed(
            &mut VolumeDiscovery {
                reader,
                root: self,
                io,
            },
            path,
            generation,
            limits,
        )
        .await?;
        Ok(Some(metadata))
    }
}

/// Permission-checked immutable-volume adapter, also used by browser and TS hosts.
#[doc(hidden)]
pub struct VolumeDiscovery<'a> {
    pub reader: &'a dyn ContentResidencyVerifier,
    pub root: &'a ContextRoot,
    /// Original owner of prefix reads and retained pinned identities.
    pub io: Option<&'a crate::executor::PreparationIo>,
}
impl DiscoverySource for VolumeDiscovery<'_> {
    type Revision = GenerationRef;
    type Cursor = String;
    fn page<'a>(
        &'a mut self,
        directory: &'a str,
        revision: Option<&'a GenerationRef>,
        cursor: Option<String>,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<DiscoveryPage<GenerationRef, String>>> {
        Box::pin(async move {
            self.root.validate(&mut None)?;
            let page = self
                .reader
                .list_private_directory(
                    &self.root.volume,
                    &self.root.directory,
                    directory,
                    revision,
                    cursor.as_deref(),
                    maximum,
                    None,
                    None,
                )?
                .await?;
            page.validate()?;
            if page.entries.first().is_some_and(|e| {
                cursor
                    .as_ref()
                    .is_some_and(|c| e.name.as_bytes() <= c.as_bytes())
            }) {
                return Err(Error::Storage(
                    "discovery page violated pinned range".into(),
                ));
            }
            if page.generation.as_resource().provider() != self.root.volume.provider() {
                return Err(Error::Storage(
                    "discovery page returned another provider".into(),
                ));
            }
            let next = if page.has_more {
                page.entries.last().map(|e| e.name.clone())
            } else {
                None
            };
            if page.has_more && next.is_none() {
                return Err(Error::Storage("empty discovery continuation".into()));
            }
            Ok(DiscoveryPage {
                revision: page.generation,
                scanned: u32::try_from(page.entries.len())
                    .map_err(|_| Error::Invalid("entry count overflow".into()))?,
                entries: page.entries.into_iter().map(Into::into).collect(),
                next,
            })
        })
    }
}
impl InstructionReader for VolumeDiscovery<'_> {
    fn instruction<'a>(
        &'a mut self,
        path: &'a str,
        revision: &'a GenerationRef,
        maximum: u64,
    ) -> BoxProviderFuture<'a, Result<crate::model::ProjectInstruction>> {
        Box::pin(async move {
            let source = PinnedContextPath {
                root: self.root.clone(),
                generation: revision.clone(),
                path: path.into(),
            };
            let (file, bytes) = source.read(self.reader, maximum).await?;
            ContextDiscoveryLimits {
                instruction_bytes: Some(maximum),
                ..Default::default()
            }
            .validate_instruction(&bytes)?;
            Ok(file.into())
        })
    }
}
impl SkillReader for VolumeDiscovery<'_> {
    fn skill_prefix<'a>(
        &'a mut self,
        path: &'a str,
        revision: &'a GenerationRef,
        maximum: u64,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (acyclic_stream::OwnedBytes, PinnedContextPath),
        >,
    > {
        #[cfg(feature = "filesystem")]
        let content_io = self.io;
        #[cfg(not(feature = "filesystem"))]
        let content_io = None;
        crate::conversation::BoundedContentFuture::new(content_io, || async move {
            let pinned = crate::executor::with_history_io(self.io, |work| {
                PinnedContextPath::capture(self.root, revision, path, work)
            })?;
            let prefix = self
                .reader
                .read_private_prefix(
                    &self.root.volume,
                    &self.root.directory,
                    path,
                    revision,
                    maximum,
                    crate::conversation::ContentAccess {
                        ceiling: None,
                        io: content_io,
                    },
                )?
                .await?;
            Ok((prefix, pinned))
        })
    }
}

/// Conventional project roots, shared by all storage and host adapters.
pub const PROJECT_SKILL_ROOTS: [&str; 2] = [".agents/skills", ".claude/skills"];

#[cfg(test)]
mod tests;
