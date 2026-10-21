use crate::{Error, Result, conversation::VolumeRef};
use acyclic_fs::access::{
    Action, Capability, Context, MountMode, PathTrie, Policy, Principal, Resource,
};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

/// Host-selected published view. `source` remains unopened during composition.
pub struct VolumeView<T> {
    /// Stable lineage name, also used by `fs --volume`.
    pub name: String,
    /// Host-authenticated agent reference owning this fork.
    pub agent: String,
    /// Provider-owned volume identity.
    pub volume: VolumeRef,
    /// Root-first ancestry of the owning task.
    pub lineage: Vec<String>,
    /// Selected published generation sequence.
    pub generation: u64,
    /// Authoritative current published head sequence.
    pub head: u64,
    /// Name semantics and limits of this volume.
    pub config: acyclic_fs::VolumeConfig,
    /// Policy loaded by the host from this volume.
    pub policy: Arc<Policy>,
    /// Lazy provider binding, retained without opening or copying its files.
    pub source: T,
}

/// One independently captured source in a task's composed namespace.
pub struct Mount<T> {
    /// Published volume selected by the task binding.
    pub view: VolumeView<T>,
    /// Maximum access anywhere in this mount; paths may be more restrictive.
    pub mode: MountMode,
    access: PathTrie,
    #[doc(hidden)]
    pub native_root: Option<PathBuf>,
}

impl<T> Mount<T> {
    /// Uses the same compiled decision for tools and native provider callbacks.
    pub fn check(&self, action: Action, path: &str, now: u64) -> Result<()> {
        if self.access.decide(action, path, now).allowed {
            return Ok(());
        }
        Err(Error::Unauthorized(
            "filesystem policy or signed capability denied access".into(),
        ))
    }
}

/// A task's primary view plus lazy named mounts, including the single-volume case.
pub struct MountTable<T> {
    /// Absolute native process root.
    pub root: PathBuf,
    entries: BTreeMap<String, Option<Mount<T>>>,
}

impl<T> MountTable<T> {
    /// Resolves either a task-relative path or an exact volume's relative path.
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    #[doc(hidden)]
    pub fn file(&self, volume: Option<&VolumeRef>, path: &str) -> Result<(&Mount<T>, String)> {
        let selected = if let Some(volume) = volume {
            let (prefix, _) = self
                .mounts()
                .find(|(_, mount)| &mount.view.volume == volume)
                .ok_or_else(|| {
                    Error::Unauthorized("volume is not mounted in this task view".into())
                })?;
            self.resolve(&Path::new(prefix).join(path))?
        } else {
            self.resolve(Path::new(path))?
        };
        if volume.is_some_and(|volume| volume != &selected.0.view.volume) {
            return Err(Error::Unauthorized(
                "file path names another mounted volume".into(),
            ));
        }
        Ok(selected)
    }
    /// Resolves a qualifier through the visible namespace, never by opening a
    /// model-supplied volume identity.
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    #[doc(hidden)]
    pub fn named(&self, name: Option<&str>) -> Result<(&str, &Mount<T>)> {
        self.mounts()
            .find(|(path, mount)| match name {
                None => path.is_empty(),
                Some(name) => {
                    (path.is_empty() && mount.view.name == name)
                        || path.strip_prefix(".volumes/") == Some(name)
                }
            })
            .ok_or_else(|| Error::NotFound("volume is not mounted in this task view".into()))
    }

    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    #[doc(hidden)]
    pub fn mounts_mut(&mut self) -> impl Iterator<Item = (&str, &mut Mount<T>)> {
        self.entries
            .iter_mut()
            .filter_map(|(path, mount)| mount.as_mut().map(|mount| (path.as_str(), mount)))
    }

    /// Visible mounts in canonical path order; the primary path is empty.
    pub fn mounts(&self) -> impl Iterator<Item = (&str, &Mount<T>)> {
        self.entries
            .iter()
            .filter_map(|(path, mount)| mount.as_ref().map(|mount| (path.as_str(), mount)))
    }

    /// Resolves a relative path without permitting traversal outside the view.
    pub fn resolve(&self, path: &Path) -> Result<(&Mount<T>, String)> {
        let mut relative = String::new();
        for component in path.components() {
            match component {
                Component::CurDir => {}
                Component::Normal(name) => {
                    let name = name
                        .to_str()
                        .ok_or_else(|| Error::Invalid("workspace path is not UTF-8".into()))?;
                    if !relative.is_empty() {
                        relative.push('/');
                    }
                    relative.push_str(name);
                }
                _ => return Err(Error::Invalid("path escapes the task view".into())),
            }
        }
        let mut prefix = relative.as_str();
        loop {
            if let Some(entry) = self.entries.get(prefix) {
                let mount = entry.as_ref().ok_or_else(|| {
                    Error::NotFound("volume is not mounted in this task view".into())
                })?;
                let selected = relative
                    .get(prefix.len()..)
                    .unwrap_or_default()
                    .trim_start_matches('/');
                return Ok((mount, selected.into()));
            }
            if prefix.is_empty() {
                return Err(Error::Unauthorized("task has no primary view".into()));
            }
            prefix = prefix.rsplit_once('/').map_or("", |(parent, _)| parent);
        }
    }
}

/// Composes only policy- and capability-visible volumes. The caller supplies
/// its authenticated principal, verified capability, and authoritative context.
/// Other agents' sources pin their latest published generation, excluding their
/// in-flight writes. No source is opened while enumerating visible mounts.
pub fn compose<T>(
    root: PathBuf,
    principal: &Principal,
    capability: &Capability,
    context: Context,
    primary: VolumeView<T>,
    volumes: impl IntoIterator<Item = VolumeView<T>>,
) -> Result<MountTable<T>> {
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(Error::Invalid(
            "task view root must be absolute without parent traversal".into(),
        ));
    }
    let primary_name = primary.name.clone();
    let mut mounts = BTreeMap::new();
    for (index, view) in std::iter::once(primary).chain(volumes).enumerate() {
        let access = PathTrie::compile(
            &view.policy,
            principal,
            Resource {
                lineage: &view.lineage,
                volume: view.volume.id(),
                generation: view.generation,
                head: view.head,
                path: "",
                config: view.config,
            },
            context,
            capability,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let mode = access.subtree_mode("", context.now);
        let path = if index == 0 {
            String::new()
        } else {
            component(&view.name)?;
            if view.agent == principal.id {
                format!(".volumes/{}", view.name)
            } else {
                component(&view.agent)?;
                if view.name == primary_name {
                    format!(".volumes/agents/{}", view.agent)
                } else {
                    format!(".volumes/agents/{}/.volumes/{}", view.agent, view.name)
                }
            }
        };
        let entry = match mounts.entry(path) {
            Entry::Vacant(entry) => entry,
            Entry::Occupied(_) => {
                return Err(Error::Conflict(
                    "task view has duplicate mount paths".into(),
                ));
            }
        };
        if !Action::ALL.into_iter().any(|action| mode.allows(action)) {
            if index == 0 {
                return Err(Error::Unauthorized(
                    "primary workspace is not granted".into(),
                ));
            }
            entry.insert(None);
            continue;
        }
        if view.agent != principal.id && view.generation != view.head {
            return Err(Error::Conflict(
                "foreign mount must select its latest published generation".into(),
            ));
        }
        entry.insert(Some(Mount {
            view,
            mode,
            access,
            native_root: None,
        }));
    }
    Ok(MountTable {
        root,
        entries: mounts,
    })
}

fn component(name: &str) -> Result<()> {
    if name.is_empty()
        || matches!(name, "." | ".." | "agents")
        || name.contains(['/', '\\', '\0', ':'])
        || name.chars().any(char::is_control)
    {
        return Err(Error::Invalid(
            "invalid or reserved volume mount name".into(),
        ));
    }
    Ok(())
}
