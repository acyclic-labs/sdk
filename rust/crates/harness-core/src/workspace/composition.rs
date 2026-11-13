use crate::{Error, Result, conversation::VolumeRef};
use acyclic_fs::access::{
    Action, Capability, Context, MountMode, PathTrie, Policy, Principal, Resource,
};
#[cfg(not(target_arch = "wasm32"))]
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
    /// Canonical source-relative directory exposed as this binding's root.
    /// Empty selects the whole volume; this is never a native path.
    pub root: String,
    /// Root-first ancestry of the owning task.
    pub lineage: Vec<String>,
    /// Selected published generation sequence.
    pub generation: u64,
    /// Authoritative current published head sequence.
    pub head: u64,
    /// Name semantics and limits of this volume.
    pub config: acyclic_fs::VolumeConfig,
    /// Host-selected policy, including this binding's access restrictions.
    pub policy: Arc<Policy>,
    /// Lazy provider binding, retained without opening or copying its files.
    pub source: T,
}

/// Native-path authority carried by one composed volume binding.
#[doc(hidden)]
pub enum NativeRoot {
    /// A physical directory in the composed task root.
    Composed,
    /// A logical volume available through filesystem tools, with no OS path.
    Logical,
    /// An independently projected native volume.
    Path(PathBuf),
}

impl NativeRoot {
    pub fn override_path(&self) -> Option<&PathBuf> {
        match self {
            Self::Path(path) => Some(path),
            Self::Composed | Self::Logical => None,
        }
    }
}

/// One independently captured source in a task's composed namespace.
pub struct Mount<T> {
    /// Published volume selected by the task binding.
    pub view: VolumeView<T>,
    /// Maximum access anywhere in this mount; paths may be more restrictive.
    pub mode: MountMode,
    access: PathTrie,
    /// Host-projected OS root. An absent named root remains logical-only.
    #[doc(hidden)]
    pub native_root: NativeRoot,
}

impl<T> Mount<T> {
    /// Checks a source-relative path, including this binding's source root.
    pub fn check(&self, action: Action, path: &str, now: u64) -> Result<()> {
        self.check_with_work(action, path, now, None)
    }

    /// Uses the original invocation ledger for cancellation and typed refusal.
    pub fn check_with_work(
        &self,
        action: Action,
        path: &str,
        now: u64,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.remaining()?;
        }
        self.relative_path_with_work(path, work)?;
        if self.access.decide(action, path, now).allowed {
            return Ok(());
        }
        Err(path_error(
            crate::wire::ErrorCode::Unauthorized,
            "filesystem policy or signed capability denied access",
            work,
        ))
    }

    /// Maps an immutable source path back into its projected namespace.
    pub fn relative_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        crate::tool::ReadPathKind::Directory.validate(path, &mut None)?;
        self.relative_path_with_work(path, None)
    }

    fn relative_path_with_work<'a>(
        &self,
        path: &'a str,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<&'a str> {
        if self.view.root.is_empty() {
            return Ok(path);
        }
        if path == self.view.root {
            return Ok("");
        }
        path.strip_prefix(self.view.root.as_str())
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| {
                path_error(
                    crate::wire::ErrorCode::Unauthorized,
                    "source path is outside the workspace root",
                    work,
                )
            })
    }

    /// Converts a namespace-relative path exactly once, using the invocation's
    /// original allocation ledger. Native roots are already projected and do
    /// not use this source path for their OS cwd.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn source_path<'a>(
        &'a self,
        path: &'a str,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<Cow<'a, str>> {
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        let io = io.as_ref();
        crate::executor::with_history_io(io, |history| {
            crate::tool::ReadPathKind::Directory.validate(path, history)
        })?;
        self.source_path_validated(path, io)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn source_path_validated<'a>(
        &'a self,
        path: &'a str,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Cow<'a, str>> {
        if self.view.root.is_empty() {
            return Ok(Cow::Borrowed(path));
        }
        if path.is_empty() {
            return Ok(Cow::Borrowed(&self.view.root));
        }
        let mut source = crate::executor::stock_string_copy(&self.view.root, io)?;
        for suffix in ["/", path] {
            crate::executor::stock_string_push(
                &mut source,
                suffix,
                u64::MAX,
                "workspace source path size overflow",
                io,
            )?;
        }
        Ok(Cow::Owned(source))
    }

    /// Checks paths inside an already projected native directory without
    /// mistaking them for source-volume coordinates.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn check_relative(&self, action: Action, path: &str, now: u64) -> Result<()> {
        if self.view.root.is_empty() {
            return self.check(action, path, now);
        }
        self.check(action, &self.source_path(path, None)?, now)
    }
}

/// A task's primary view plus lazy named mounts, including the single-volume case.
pub struct MountTable<T> {
    /// Absolute native process root.
    pub root: PathBuf,
    /// Host-owned scratch outside authored project files, when locally bound.
    #[doc(hidden)]
    pub scratch_root: Option<PathBuf>,
    /// Selected host-owned read roots for this native binding. New host entries
    /// require a new binding; absent paths add no authority. Execution grants
    /// identity-pin available roots under their retained authority ceilings.
    #[doc(hidden)]
    #[cfg(not(target_arch = "wasm32"))]
    pub native_read_roots: Vec<(PathBuf, acyclic_native_runtime::execution::ExecutionGrant)>,
    entries: BTreeMap<Arc<str>, Option<usize>>,
    // Immutable indices into canonical path order. Paths share their allocation
    // with the namespace; neither identity lookup nor iteration copies a path.
    mounted: Vec<(Arc<str>, Mount<T>)>,
    volumes: std::collections::HashMap<[u8; 32], Vec<usize>>,
}

impl<T> MountTable<T> {
    /// OS root of an exposed mount; the primary defaults to this task's root.
    /// Named mounts require an explicit native projection.
    pub fn native_root_for<'a>(&'a self, mount: &'a Mount<T>) -> Option<&'a Path> {
        match &mount.native_root {
            NativeRoot::Path(path) => Some(path),
            NativeRoot::Logical => None,
            NativeRoot::Composed => self.mounted.first().and_then(|(_, primary)| {
                std::ptr::eq(primary, mount).then_some(self.root.as_path())
            }),
        }
    }

    /// The native subset of this composed namespace, retaining its access modes.
    pub fn native_mounts(&self) -> impl Iterator<Item = (&str, &Path, &Mount<T>)> {
        self.mounts().filter_map(|(relative, mount)| {
            self.native_root_for(mount)
                .map(|root| (relative, root, mount))
        })
    }

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
            .and_then(|indices| indices.first())
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

    /// An immutable reference names a source volume and path, not a namespace
    /// alias. Select an admitted binding granting the requested operation;
    /// named addresses continue to use only their explicitly selected binding.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn by_volume_path(
        &self,
        volume: &VolumeRef,
        path: &str,
        action: Action,
        now: u64,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<&Mount<T>> {
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        let digest = crate::executor::with_history_io(io.as_ref(), |history| {
            volume.identity_digest(
                crate::store::default_projection_read_limits().maximum_bytes,
                history,
            )
        })?;
        let mut refused = None;
        for &index in self.volumes.get(&digest).into_iter().flatten() {
            let mount = &self.mounted[index].1;
            if &mount.view.volume != volume {
                continue;
            }
            match mount.check_with_work(action, path, now, work) {
                Ok(()) => return Ok(mount),
                Err(error @ Error::Unauthorized(_))
                | Err(
                    error @ Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        ..
                    },
                ) => {
                    refused.get_or_insert(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(refused.unwrap_or_else(|| {
            path_error(
                crate::wire::ErrorCode::Unauthorized,
                "volume path is not mounted in this task view",
                work,
            )
        }))
    }

    /// Resolves a file for the operation that will consume its source path.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn file_for<'a>(
        &'a self,
        action: Action,
        volume: Option<&VolumeRef>,
        path: &'a str,
        now: u64,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(&'a Mount<T>, Cow<'a, str>)> {
        self.path_for(
            volume,
            path,
            crate::tool::ReadPathKind::File,
            action,
            now,
            work,
        )
    }

    /// Resolves and authorizes once on the original ledger. Explicit source
    /// references may use any granting alias; namespace qualifiers cannot.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn path_for<'a>(
        &'a self,
        volume: Option<&VolumeRef>,
        path: &'a str,
        kind: crate::tool::ReadPathKind,
        action: Action,
        now: u64,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(&'a Mount<T>, Cow<'a, str>)> {
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        let io = io.as_ref();
        let (mount, path) = if let Some(volume) = volume {
            validate_routed_path(path, kind, io, work)?;
            let mount = self.by_volume_path(volume, path, action, now, work)?;
            return Ok((mount, Cow::Borrowed(path)));
        } else {
            let (mount, path) = self.select(path, work)?;
            validate_routed_path(path, kind, io, work)?;
            (mount, mount.source_path_validated(path, io)?)
        };
        mount.check_with_work(action, &path, now, work)?;
        Ok((mount, path))
    }

    /// Resolves a qualifier through the visible namespace, never by opening a
    /// model-supplied volume identity.
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn named(&self, name: Option<&str>) -> Result<(&str, &Mount<T>)> {
        self.mounts()
            .find(|(path, mount)| match name {
                None => path.is_empty(),
                Some(name) => {
                    (path.is_empty() && mount.view.name == name)
                        || path.strip_prefix("volume:///") == Some(name)
                        || path.strip_prefix("volume://") == Some(name)
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
        if let Some(address) = path.to_str().filter(|path| path.starts_with("volume://")) {
            let (mount, path) = self.select(address, None)?;
            let path = normalize_path(Path::new(path))?;
            return Ok((mount, path));
        }
        let relative = normalize_path(path)?;
        self.select(&relative, None)
            .map(|(mount, path)| (mount, path.to_owned()))
    }

    fn select<'a>(
        &self,
        address: &'a str,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(&Mount<T>, &'a str)> {
        let (key, path) = split_address(address)
            .map_err(|reason| path_error(crate::wire::ErrorCode::Invalid, reason, work))?;
        let index = self
            .entries
            .get(key)
            .and_then(|entry| *entry)
            .ok_or_else(|| {
                path_error(
                    crate::wire::ErrorCode::NotFound,
                    "volume is not mounted in this task view",
                    work,
                )
            })?;
        Ok((&self.mounted[index].1, path))
    }
}

/// Separates explicit volume selection from the volume's ordinary file path.
pub(crate) fn split_address(address: &str) -> std::result::Result<(&str, &str), &'static str> {
    let Some(qualified) = address.strip_prefix("volume://") else {
        return Ok(("", address));
    };
    let (_, after_owner) = qualified
        .split_once('/')
        .ok_or("volume address needs an owner and name")?;
    let name_length = after_owner.find('/').unwrap_or(after_owner.len());
    if name_length == 0 {
        return Err("volume address needs a name");
    }
    let length = address.len() - after_owner.len() + name_length;
    Ok((
        &address[..length],
        address[length..].strip_prefix('/').unwrap_or(""),
    ))
}

fn normalize_path(path: &Path) -> Result<String> {
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
    Ok(relative)
}

#[cfg(not(target_arch = "wasm32"))]
fn validate_routed_path(
    path: &str,
    kind: crate::tool::ReadPathKind,
    io: Option<&crate::filesystem::PreparationIo>,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<()> {
    crate::executor::with_history_io(io, |history| kind.validate(path, history))?;
    if Path::new(path)
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(path_error(
            crate::wire::ErrorCode::Invalid,
            "path escapes the task view",
            work,
        ));
    }
    Ok(())
}

fn path_error(
    code: crate::wire::ErrorCode,
    reason: &'static str,
    work: acyclic_fs::NativeWork<'_>,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    crate::Error::from_code(code, reason)
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
    component(&primary.name)?;
    let primary_address = format!("volume:///{}", primary.name);
    let mut mounts = BTreeMap::new();
    for (index, view) in std::iter::once(primary).chain(volumes).enumerate() {
        if !view.root.is_empty() {
            crate::conversation::validate_content_path(&view.root, &mut None)?;
        }
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
        let mode = access.subtree_mode(&view.root, context.now);
        let path = if index == 0 {
            String::new()
        } else {
            component(&view.name)?;
            if view.agent == principal.id {
                format!("volume:///{}", view.name)
            } else {
                component(&view.agent)?;
                format!("volume://{}/{}", view.agent, view.name)
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
        let native_root = if index == 0 && view.root.is_empty() {
            NativeRoot::Composed
        } else {
            NativeRoot::Logical
        };
        entry.insert(Some(Mount {
            view,
            mode,
            access,
            native_root,
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
                // Index only aliases of this source identity. Source-path
                // authorization never scans unrelated workspaces or agents.
                volumes_by_identity
                    .entry(digest)
                    .or_insert_with(Vec::new)
                    .push(index);
                mounted.push((Arc::clone(&path), mount));
                Some(index)
            }
            None => None,
        };
        entries.insert(path, index);
    }
    entries.insert(primary_address.into(), entries.get("").copied().flatten());
    Ok(MountTable {
        root,
        scratch_root: None,
        #[cfg(not(target_arch = "wasm32"))]
        native_read_roots: Vec::new(),
        entries,
        mounted,
        volumes: volumes_by_identity,
    })
}

fn component(name: &str) -> Result<()> {
    if name.is_empty()
        || matches!(name, "." | "..")
        || name.contains(['/', '\\', '\0', ':'])
        || name.chars().any(char::is_control)
    {
        return Err(Error::Invalid("invalid volume mount name".into()));
    }
    Ok(())
}
