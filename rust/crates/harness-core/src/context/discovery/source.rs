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
    pub entries: Vec<DiscoveryEntry>,
    pub next: Option<C>,
}

/// Storage adapters supply pages; ordering, pinning and quotas belong to discovery.
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
    fn skill_prefix<'a>(
        &'a mut self,
        path: &'a str,
        revision: &'a Self::Revision,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<(Vec<u8>, PinnedContextPath)>>;
}

impl DiscoveryWork {
    /// Reusing a catalog still consumes its discovery work in the caller's one budget.
    pub fn include(&mut self, used: Self) -> Result<()> {
        self.limits.validate()?;
        let entries = self.entries.checked_add(used.entries);
        let directories = self.directories.checked_add(used.directories);
        match (entries, directories) {
            (Some(entries), Some(directories))
                if entries <= self.limits.entries && directories <= self.limits.directories =>
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
        if self.directories > self.limits.directories {
            return Err(Error::Invalid("discovery directory bound exceeded".into()));
        }
        let mut pinned = revision.cloned();
        let mut entries: Vec<DiscoveryEntry> = Vec::new();
        let mut cursor = None;
        let mut pages = 0u32;
        loop {
            let remaining = self
                .limits
                .entries
                .checked_sub(self.entries)
                .filter(|n| *n > 0)
                .ok_or_else(|| Error::Invalid("discovery entry bound exceeded".into()))?;
            pages = pages
                .checked_add(1)
                .filter(|p| *p <= self.limits.entries)
                .ok_or_else(|| Error::Invalid("discovery page bound exceeded".into()))?;
            let page = source
                .page(directory, pinned.as_ref(), cursor, remaining.min(128))
                .await?;
            if pinned.as_ref().is_some_and(|r| r != &page.revision)
                || page.entries.len() > remaining.min(128) as usize
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
            self.entries += u32::try_from(page.entries.len())
                .map_err(|_| Error::Invalid("entry count overflow".into()))?;
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
    if names.len() > work.limits.entries as usize {
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
                        .instruction(&child(&directory, name), &r, work.limits.instruction_bytes)
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
        let (prefix, pinned) = source
            .skill_prefix(
                &child(&directory, name),
                &revision,
                work.limits.header_bytes,
            )
            .await?;
        if prefix.len() > work.limits.header_bytes as usize {
            return Err(Error::Storage("prefix reader exceeded bound".into()));
        }
        result.push(parse_skill_metadata(&prefix, pinned)?);
    }
    Ok(result)
}

/// Permission-checked immutable-volume adapter, also used by browser and TS hosts.
#[doc(hidden)]
pub struct VolumeDiscovery<'a> {
    pub reader: &'a dyn ContentResidencyVerifier,
    pub root: &'a ContextRoot,
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
                instruction_bytes: maximum,
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
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<(Vec<u8>, PinnedContextPath)>> {
        Box::pin(async move {
            let prefix = self
                .reader
                .read_private_prefix(
                    &self.root.volume,
                    &self.root.directory,
                    path,
                    revision,
                    u64::from(maximum),
                    crate::conversation::ContentAccess::default(),
                )?
                .await?;
            Ok((
                prefix.to_vec(),
                PinnedContextPath {
                    root: self.root.clone(),
                    generation: revision.clone(),
                    path: path.into(),
                },
            ))
        })
    }
}

/// Conventional project roots, shared by all storage and host adapters.
pub const PROJECT_SKILL_ROOTS: [&str; 2] = [".agents/skills", ".claude/skills"];

#[cfg(test)]
mod tests;
