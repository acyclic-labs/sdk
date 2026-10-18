pub(super) use super::owner_record::Owner;
use super::*;
use std::{fs, io::Read};

/// Root of every project's default harness state: the per-user local data directory. This is the only place the
/// harness consults that directory. Unit tests cannot reach it: a test that does not inject
/// `LocalHarnessConfig::state_directory` fails here instead of sharing the real per-user store with every other
/// test and lane on the machine.
#[cfg(not(test))]
pub(super) fn user_state_root() -> Result<PathBuf> {
    crate::native_environment::state(None)
        .map(|root| root.join("acyclic/harness"))
        .ok_or_else(|| Error::Unsupported("per-user local data directory is unavailable".into()))
}

/// Unit tests have no per-user store; see the production definition.
#[cfg(test)]
pub(super) fn user_state_root() -> Result<PathBuf> {
    Err(Error::Unsupported(UNIT_TESTS_INJECT_STATE.into()))
}

#[cfg(test)]
pub(super) const UNIT_TESTS_INJECT_STATE: &str =
    "unit tests must inject LocalHarnessConfig::state_directory; the per-user store is unreachable";

/// Per-user state is keyed by the checkout's path and its directory incarnation:
/// a checkout deleted and cloned again at the same path is a new workspace and
/// starts with new state instead of being refused the old one.
pub(super) fn state_directory_in(
    root: &Path,
    project: &LocalProject,
    checkout: Option<&Checkout>,
) -> Result<PathBuf> {
    // The scope separates a volume's state by its storage name and a
    // checkout's by its incarnation.
    let (canonical, kind, scope) = match (project, checkout) {
        (LocalProject::Workspace(_), Some(checkout)) => (
            checkout.path.clone(),
            b"checkout\0".as_slice(),
            checkout.incarnation.to_vec(),
        ),
        (LocalProject::Volume { directory, volume }, None) => (
            fs::canonicalize(directory).map_err(storage)?,
            b"volume\0".as_slice(),
            volume.storage_name(&mut None)?.as_bytes().to_vec(),
        ),
        _ => {
            return Err(Error::Invalid(
                "checkout capture does not match the project".into(),
            ));
        }
    };
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.state/v2\0");
    hash.update(kind);
    let path_bytes = canonical.as_os_str().as_encoded_bytes();
    hash.update(&(path_bytes.len() as u64).to_le_bytes());
    hash.update(path_bytes);
    hash.update(&scope);
    let directory = root.join(hash.finalize().to_hex().as_str());
    let planned = directory::planned_directory(&directory).ok_or_else(|| {
        Error::Invalid("local state directory has an unresolved path suffix".into())
    })?;
    if matches!(project, LocalProject::Workspace(_)) && planned.starts_with(&canonical) {
        return Err(Error::Invalid(
            "per-user harness state would be inside the checkout".into(),
        ));
    }
    Ok(directory)
}

mod directory;
pub(super) use directory::separated_directory;

/// One capture of a checkout root, from a single held root: its canonical
/// path, its object identity and its incarnation. State selection and owner
/// validation share it instead of each querying the root again.
#[derive(Clone, Debug)]
pub(super) struct Checkout {
    pub(super) path: PathBuf,
    pub(super) identity: [u8; 16],
    pub(super) incarnation: [u8; 16],
}

#[cfg(test)]
thread_local! {
    /// Root captures on this thread, for exact-count guards.
    pub(super) static CAPTURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Checkout {
    pub(super) fn capture(path: &Path) -> Result<Self> {
        #[cfg(test)]
        CAPTURES.with(|captures| captures.set(captures.get() + 1));
        #[cfg(feature = "native-execution")]
        {
            let path = fs::canonicalize(path).map_err(storage)?;
            let root = acyclic_fs::capture_root_incarnation(&path).map_err(storage)?;
            let identity = root.identity.to_bytes();
            Ok(Self {
                path,
                identity,
                incarnation: incarnation_of(identity, root.handle, root.birth_time)?,
            })
        }
        #[cfg(not(feature = "native-execution"))]
        {
            let _ = path;
            Err(Error::Unsupported(
                "checkout workspaces require native-execution".into(),
            ))
        }
    }
}

/// A checkout's incarnation: its object identity bound to an attribute a
/// directory created later at the recycled object id cannot repeat. The
/// kernel file handle carries the inode generation and needs no birth time;
/// the birth time covers hosts without handles. A host recording neither
/// cannot tell a re-created checkout from the old one, so it is refused.
pub(super) fn incarnation_of(
    object: [u8; 16],
    handle: Option<[u8; 32]>,
    born: Option<std::time::Duration>,
) -> Result<[u8; 16]> {
    if handle.is_none() && born.is_none() {
        return Err(Error::Unsupported(
            "checkout filesystem records neither file handles nor birth times".into(),
        ));
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.workspace-incarnation/v2\0");
    hash.update(&object);
    for (tag, attribute) in [
        (1, handle.map(|handle| handle.to_vec())),
        (2, born.map(|born| born.as_nanos().to_le_bytes().to_vec())),
    ] {
        match attribute {
            Some(bytes) => {
                hash.update(&[tag]);
                hash.update(&bytes);
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(identity)
}


impl Owner {
    pub(super) fn open(
        directory: &Path,
        selection: &LocalProject,
        checkout: Option<&Checkout>,
    ) -> Result<Self> {
        let workspace = match (selection, checkout) {
            (LocalProject::Workspace(_), Some(checkout)) => Some(checkout.path.clone()),
            (LocalProject::Volume { .. }, None) => None,
            _ => {
                return Err(Error::Invalid(
                    "checkout capture does not match the project".into(),
                ));
            }
        };
        let path = directory.join("owner.json");
        let workspace_identity = checkout.map(|checkout| checkout.identity);
        let workspace_incarnation = checkout.map(|checkout| checkout.incarnation);
        let owner = match Self::load(&path) {
            Ok(owner) => owner,
            Err(Error::NotFound(_)) => {
                let project = match selection {
                    LocalProject::Volume { volume, .. } => volume.clone(),
                    LocalProject::Workspace(_) => VolumeRef::new(
                        crate::resources::ProviderRef::new(
                            "local".into(),
                            "filesystem".into(),
                            "2".into(),
                            &mut None,
                        )?,
                        crate::identity::fresh_uuid().to_string(),
                        VolumeClass::Project,
                        VolumeOwner::Project(crate::identity::fresh_uuid().to_string()),
                        &mut None,
                    )?,
                };
                let mut key = [0; 32];
                crate::identity::fill_random(&mut key).map_err(storage)?;
                let owner = Self {
                    key,
                    agent: AgentId::new(),
                    task: TaskId::new(),
                    project,
                    workspace: workspace.clone(),
                    workspace_identity,
                    workspace_incarnation,
                };
                key.fill(0);
                if owner.publish(directory, &path)? {
                    owner
                } else {
                    Self::load(&path)?
                }
            }
            Err(error) => return Err(error),
        };
        owner.project.validate(&mut None)?;
        if owner.key == [0; 32]
            || owner.agent.into_bytes() == [0; 16]
            || owner.task.into_bytes() == [0; 16]
            || owner.project.class() != VolumeClass::Project
            || owner.project.provider().family() != "filesystem"
            || owner.workspace != workspace
            || owner.workspace_identity != workspace_identity
            || owner.workspace_incarnation != workspace_incarnation
            || matches!(selection, LocalProject::Volume { volume, .. } if volume != &owner.project)
        {
            return Err(Error::Unauthorized(
                "local state belongs to a different workspace or volume".into(),
            ));
        }
        Ok(owner)
    }

    fn load(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound("local owner".into())
            } else {
                storage(error)
            }
        })?;
        if !metadata.is_file() || metadata.len() > 65_536 {
            return Err(Error::Invalid("invalid local owner record".into()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.mode() & 0o077 != 0 || metadata.nlink() != 1 {
                return Err(Error::Unauthorized(
                    "local owner record has public permissions".into(),
                ));
            }
        }
        let mut bytes = Vec::new();
        fs::File::open(path)
            .map_err(storage)?
            .take(65_537)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        let owner =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()));
        bytes.fill(0);
        owner
    }

    fn publish(&self, directory: &Path, path: &Path) -> Result<bool> {
        let temporary = directory.join(format!(".owner-{}.next", crate::identity::fresh_uuid()));
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| {
            let mut file = options.open(&temporary)?;
            let mut bytes = serde_json::to_vec(self).map_err(std::io::Error::other)?;
            let written = acyclic_native_runtime::write_file_all(&mut file, &bytes);
            bytes.fill(0);
            written?;
            acyclic_native_runtime::flush_file_all(&file)?;
            drop(file);
            acyclic_native_runtime::durable_rename(
                &temporary,
                path,
                acyclic_native_runtime::RenameMode::NoReplace,
                None,
            )
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        match result {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }
}
