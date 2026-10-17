pub(super) use super::owner_record::Owner;
use super::*;
use std::{
    fs,
    io::{Read, Write},
};

/// Root of every project's default harness state: the per-user local data directory. This is the only place the
/// harness consults that directory. Unit tests cannot reach it: a test that does not inject
/// `LocalHarnessConfig::state_directory` fails here instead of sharing the real per-user store with every other
/// test and lane on the machine.
#[cfg(not(test))]
pub(super) fn user_state_root() -> Result<PathBuf> {
    dirs::data_local_dir()
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

/// Default state directory for `project` under `root`: one directory per canonical project path and volume.
pub(super) fn state_directory_in(root: &Path, project: &LocalProject) -> Result<PathBuf> {
    let (path, volume) = match project {
        LocalProject::Workspace(path) => (path, None),
        LocalProject::Volume { directory, volume } => (directory, Some(volume)),
    };
    let canonical = fs::canonicalize(path).map_err(storage)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.state/v1\0");
    hash.update(if volume.is_some() {
        b"volume\0"
    } else {
        b"checkout\0"
    });
    let path_bytes = canonical.as_os_str().as_encoded_bytes();
    hash.update(&(path_bytes.len() as u64).to_le_bytes());
    hash.update(path_bytes);
    if let Some(volume) = volume {
        hash.update(volume.storage_name(&mut None)?.as_bytes());
    }
    let directory = root.join(hash.finalize().to_hex().as_str());
    if matches!(project, LocalProject::Workspace(_)) && directory.starts_with(&canonical) {
        return Err(Error::Invalid(
            "per-user harness state would be inside the checkout".into(),
        ));
    }
    Ok(directory)
}

pub(super) fn directory(path: &Path) -> Result<PathBuf> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).map_err(storage)?;
    fs::canonicalize(path).map_err(storage)
}

impl Owner {
    pub(super) fn open(directory: &Path, selection: &LocalProject) -> Result<Self> {
        let workspace = match selection {
            LocalProject::Workspace(path) => Some(fs::canonicalize(path).map_err(storage)?),
            LocalProject::Volume { .. } => None,
        };
        if workspace.as_ref().is_some_and(|workspace| {
            workspace.starts_with(directory) || directory.starts_with(workspace)
        }) {
            return Err(Error::Invalid(
                "local state directory must not contain the checkout".into(),
            ));
        }
        let path = directory.join("owner.json");
        let workspace_identity = workspace
            .as_ref()
            .map(|workspace| {
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                Ok::<_, Error>(work.root_identity(workspace)?.to_bytes())
            })
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
                        uuid::Uuid::new_v4().to_string(),
                        VolumeClass::Project,
                        VolumeOwner::Project(uuid::Uuid::new_v4().to_string()),
                        &mut None,
                    )?,
                };
                let mut key = [0; 32];
                getrandom::fill(&mut key).map_err(storage)?;
                let owner = Self {
                    key,
                    agent: AgentId::new(),
                    task: TaskId::new(),
                    project,
                    workspace: workspace.clone(),
                    workspace_identity,
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
        let temporary = directory.join(format!(".owner-{}.next", uuid::Uuid::new_v4()));
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
            let written = file.write_all(&bytes);
            bytes.fill(0);
            written?;
            file.sync_all()?;
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
