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
pub(super) fn state_directory_in(root: &Path, project: &LocalProject) -> Result<PathBuf> {
    let (path, volume) = match project {
        LocalProject::Workspace(path) => (path, None),
        LocalProject::Volume { directory, volume } => (directory, Some(volume)),
    };
    let canonical = fs::canonicalize(path).map_err(storage)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.state/v2\0");
    hash.update(if volume.is_some() {
        b"volume\0"
    } else {
        b"checkout\0"
    });
    let path_bytes = canonical.as_os_str().as_encoded_bytes();
    hash.update(&(path_bytes.len() as u64).to_le_bytes());
    hash.update(path_bytes);
    match volume {
        Some(volume) => {
            hash.update(volume.storage_name(&mut None)?.as_bytes());
        }
        None => {
            hash.update(&incarnation(&canonical)?);
        }
    }
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

/// The directory's device and object id. A filesystem may hand the same pair
/// to a directory created after this one is deleted.
pub(super) fn object_identity(workspace: &Path) -> Result<[u8; 16]> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    Ok(work.root_identity(workspace)?.to_bytes())
}

/// The checkout directory's incarnation: its object identity bound to its
/// creation time, which a directory created later at a recycled object id
/// cannot repeat. A filesystem without a birth time contributes the object
/// identity alone.
fn incarnation(workspace: &Path) -> Result<[u8; 16]> {
    Ok(incarnation_of(
        object_identity(workspace)?,
        birth_time(workspace),
    ))
}

/// The directory's creation time, where the filesystem records one.
pub(super) fn birth_time(workspace: &Path) -> Option<std::time::Duration> {
    fs::metadata(workspace)
        .and_then(|metadata| metadata.created())
        .ok()
        .and_then(|created| created.duration_since(std::time::UNIX_EPOCH).ok())
}

pub(super) fn incarnation_of(object: [u8; 16], born: Option<std::time::Duration>) -> [u8; 16] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.workspace-incarnation/v1\0");
    hash.update(&object);
    match born {
        Some(born) => {
            hash.update(&[1]);
            hash.update(&born.as_nanos().to_le_bytes());
        }
        None => {
            hash.update(&[0]);
        }
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    identity
}


impl Owner {
    pub(super) fn open(directory: &Path, selection: &LocalProject) -> Result<Self> {
        let workspace = match selection {
            LocalProject::Workspace(path) => Some(fs::canonicalize(path).map_err(storage)?),
            LocalProject::Volume { .. } => None,
        };
        let path = directory.join("owner.json");
        let workspace_identity = workspace
            .as_ref()
            .map(|workspace| object_identity(workspace))
            .transpose()?;
        let workspace_incarnation = workspace
            .as_ref()
            .map(|workspace| incarnation(workspace))
            .transpose()?;
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
