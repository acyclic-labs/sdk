use crate::{Error, Result, conversation::VolumeRef};
use acyclic_fs::access::{
    Action, Capability, Context, MountMode, PathTrie, Policy, Principal, Resource,
};
#[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
use std::borrow::Cow;
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
        self.check_with_work(action, path, now, None)
    }

    /// Uses the original invocation ledger for cancellation and typed refusal.
    pub fn check_with_work(
        &self,
        action: Action,
        path: &str,
        now: u64,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
        }
        if self.access.decide(action, path, now).allowed {
            return Ok(());
        }
        Err(path_error(
            crate::wire::ErrorCode::Unauthorized,
            "filesystem policy or signed capability denied access",
            work,
        ))
    }
}

/// A task's primary view plus lazy named mounts, including the single-volume case.
pub struct MountTable<T> {
    /// Absolute native process root.
    pub root: PathBuf,
    entries: BTreeMap<Arc<str>, Option<usize>>,
    // Immutable indices into canonical path order. Paths share their allocation
    // with the namespace; neither identity lookup nor iteration copies a path.
    mounted: Vec<(Arc<str>, Mount<T>)>,
    volumes: std::collections::HashMap<[u8; 32], usize>,
}

impl<T> MountTable<T> {
    /// Resolves an admitted volume without scanning the mounted namespace.
    #[doc(hidden)]
    pub fn by_volume(
        &self,
        volume: &VolumeRef,
        io: Option<&crate::conversation::ContentIo>,
    ) -> Result<&Mount<T>> {
        let digest = crate::executor::with_history_io(io, |work| {
            volume.identity_digest(
                crate::store::default_projection_read_limits().maximum_bytes,
                work,
            )
        })?;
        let mount = self
            .volumes
            .get(&digest)
            .and_then(|&index| self.mounted.get(index))
            .map(|(_, mount)| mount)
            .filter(|mount| &mount.view.volume == volume);
        mount.ok_or_else(|| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "volume is not mounted in this task view",
            )
        })
    }

    /// Resolves either a task-relative path or an exact volume's relative path.
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    #[doc(hidden)]
    pub fn file<'a>(
        &'a self,
        volume: Option<&VolumeRef>,
        path: &'a str,
    ) -> Result<(&'a Mount<T>, Cow<'a, str>)> {
        self.file_with_work(volume, path, None)
    }

    /// Borrowed canonical content-path routing on the original tool ledger.
    /// Explicit-volume native routing is refused before allocating a path.
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    #[doc(hidden)]
    pub fn file_with_work<'a>(
        &'a self,
        volume: Option<&VolumeRef>,
        path: &'a str,
        work: crate::NativeWork<'_>,
    ) -> Result<(&'a Mount<T>, Cow<'a, str>)> {
        if volume.is_none() {
            #[cfg(not(target_arch = "wasm32"))]
            let io = work
                .map(crate::filesystem::PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(target_arch = "wasm32")]
            let io: Option<&crate::filesystem::PreparationIo> = None;
            #[cfg(not(target_arch = "wasm32"))]
            let io = io.as_ref();
            crate::executor::with_history_io(io, |history| {
                crate::conversation::validate_content_path(path, history)
            })?;
            if Path::new(path)
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            {
                return Err(path_error(
                    crate::wire::ErrorCode::Invalid,
                    "path escapes the task view",
                    work,
                ));
            }
            return self
                .select(path, work)
                .map(|(mount, path)| (mount, Cow::Borrowed(path)));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<&crate::conversation::ContentIo> = None;
        #[cfg(not(target_arch = "wasm32"))]
        let io = io.as_ref();
        let mount = self.by_volume(volume.expect("explicit volume checked above"), io)?;
        crate::executor::with_history_io(io, |history| {
            crate::conversation::validate_content_path(path, history)
        })?;
        if Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(path_error(
                crate::wire::ErrorCode::Invalid,
                "path escapes the task view",
                work,
            ));
        }
        if path.starts_with(".volumes/") {
            return Err(path_error(
                crate::wire::ErrorCode::Unauthorized,
                "explicit-volume path crosses the mounted namespace",
                work,
            ));
        }
        Ok((mount, Cow::Borrowed(path)))
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

    /// Mutable mounts in canonical path order, independent of the source backend.
    #[doc(hidden)]
    pub fn mounts_mut(&mut self) -> impl Iterator<Item = (&str, &mut Mount<T>)> {
        self.mounted
            .iter_mut()
            .map(|(path, mount)| (&**path, mount))
    }

    /// Visible mounts in canonical path order; the primary path is empty.
    pub fn mounts(&self) -> impl Iterator<Item = (&str, &Mount<T>)> {
        self.mounted
            .iter()
            .map(|(path, mount)| (path.as_ref(), mount))
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
        self.select(&relative, None)
            .map(|(mount, path)| (mount, path.to_owned()))
    }

    fn select<'a>(
        &self,
        relative: &'a str,
        work: crate::NativeWork<'_>,
    ) -> Result<(&Mount<T>, &'a str)> {
        let mut prefix = relative;
        loop {
            if let Some(entry) = self.entries.get(prefix) {
                let index = entry.ok_or_else(|| {
                    path_error(
                        crate::wire::ErrorCode::NotFound,
                        "volume is not mounted in this task view",
                        work,
                    )
                })?;
                let mount = &self.mounted[index].1;
                let selected = relative
                    .get(prefix.len()..)
                    .unwrap_or_default()
                    .trim_start_matches('/');
                return Ok((mount, selected));
            }
            if prefix.is_empty() {
                return Err(path_error(
                    crate::wire::ErrorCode::Unauthorized,
                    "task has no primary view",
                    work,
                ));
            }
            prefix = prefix.rsplit_once('/').map_or("", |(parent, _)| parent);
        }
    }
}

fn path_error(
    code: crate::wire::ErrorCode,
    reason: &'static str,
    work: crate::NativeWork<'_>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Unauthorized(reason.into()),
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
    let mut mounted = Vec::new();
    let mut entries = BTreeMap::new();
    let mut volumes_by_identity = std::collections::HashMap::new();
    for (path, mount) in mounts {
        let path: Arc<str> = path.into();
        let index = match mount {
            Some(mount) => {
                let index = mounted.len();
                let digest = mount.view.volume.identity_digest(
                    crate::store::default_projection_read_limits().maximum_bytes,
                    &mut None,
                )?;
                // Canonical order chooses the same first visible alias for a
                // repeated volume identity, without an invocation-time scan.
                volumes_by_identity.entry(digest).or_insert(index);
                mounted.push((Arc::clone(&path), mount));
                Some(index)
            }
            None => None,
        };
        entries.insert(path, index);
    }
    Ok(MountTable {
        root,
        entries,
        mounted,
        volumes: volumes_by_identity,
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
