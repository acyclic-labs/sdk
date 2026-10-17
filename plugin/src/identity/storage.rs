use super::*;

pub(super) struct Store {
    pub(super) path: PathBuf,
    _lock: File,
}

impl Store {
    pub(super) fn open(path: &Path) -> Result<Self, Error> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| {
                Error::new("configuration_error", "credentials need a parent directory")
            })?;
        fs::create_dir_all(parent)?;
        let path = fs::canonicalize(parent)?.join(
            path.file_name()
                .ok_or_else(|| Error::new("configuration_error", "credentials need a filename"))?,
        );
        reject_alias(&path)?;
        let path = match fs::canonicalize(&path) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => path,
            Err(error) => return Err(error.into()),
        };
        let name = path
            .file_name()
            .ok_or_else(|| Error::new("configuration_error", "credentials need a filename"))?
            .to_string_lossy();
        if [".lock", ".next"]
            .iter()
            .any(|suffix| name.to_ascii_lowercase().ends_with(suffix))
        {
            return Err(Error::new(
                "configuration_error",
                "credential filenames ending in .lock or .next are reserved for sidecars",
            ));
        }
        #[cfg(windows)]
        if name.ends_with([' ', '.']) {
            return Err(Error::new(
                "configuration_error",
                "credential filenames must not end in a space or dot",
            ));
        }
        let lock = private_file(&sidecar(&path, ".lock"), false)?;
        lock.try_lock_exclusive().map_err(|_| {
            Error::new(
                "credentials_busy",
                "another identity operation holds the credential lock; retry after it completes",
            )
        })?;
        Ok(Self { path, _lock: lock })
    }

    pub(super) fn credentials(&self, endpoint: &str) -> Result<Credentials, Error> {
        reject_alias(&self.path)?;
        let bytes = fs::read(&self.path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::new("login_required", "run acyclic-plugin login first")
            } else {
                error.into()
            }
        })?;
        let credentials: Credentials = serde_json::from_slice(&bytes)
            .map_err(|_| Error::new("credential_error", "invalid credentials file"))?;
        if credentials.endpoint != endpoint {
            return Err(Error::new(
                "endpoint_mismatch",
                "credentials belong to another API endpoint",
            ));
        }
        Ok(credentials)
    }

    pub(super) fn login_agent(&self, endpoint: &str) -> Result<Option<agent::Agent>, Error> {
        if !self.path.exists() {
            return Ok(None);
        }
        reject_alias(&self.path)?;
        let value: Value = serde_json::from_slice(&fs::read(&self.path)?).map_err(|_| {
            Error::new(
                "credential_error",
                "invalid credentials file; use a new --credentials path to preserve its keys",
            )
        })?;
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("credential_error", "invalid credentials file"))?;
        if object.get("agent").is_none_or(Value::is_null) {
            return Ok(None);
        }
        // An unreadable or foreign agent identity must never be overwritten by login.
        Ok(self.credentials(endpoint)?.agent)
    }

    pub(super) fn login_agent_for(
        &self,
        endpoint: &str,
        device: &DeviceCode,
    ) -> Result<Option<agent::Agent>, Error> {
        let agent = self.login_agent(endpoint)?;
        let identity = agent
            .as_ref()
            .map(|agent| {
                agent
                    .registration()
                    .map(|registration| (registration.org_id.clone(), registration.key_id.clone()))
            })
            .transpose();
        if identity.as_ref().ok() != Some(&device.agent_identity) {
            return Err(Error::new(
                "credentials_changed",
                "agent changed while device approval was pending",
            ));
        }
        Ok(agent)
    }

    pub(super) fn read(&self, endpoint: &str) -> Result<Session, Error> {
        self.credentials(endpoint)?
            .session
            .ok_or_else(|| Error::new("login_required", "run acyclic-plugin login first"))
    }

    pub(super) fn write(&self, endpoint: &str, session: Session) -> Result<Session, Error> {
        let agent = if self.path.exists() {
            self.credentials(endpoint)?.agent
        } else {
            None
        };
        let credentials = Credentials {
            endpoint: endpoint.to_owned(),
            session: Some(session),
            agent,
        };
        self.save(&credentials)?;
        credentials
            .session
            .ok_or_else(|| Error::new("credential_error", "missing saved session"))
    }

    pub(super) fn complete_login(
        &self,
        endpoint: &str,
        session: Session,
        device: &DeviceCode,
    ) -> Result<Session, Error> {
        let mut credentials = Credentials {
            endpoint: endpoint.to_owned(),
            session: None,
            agent: self.login_agent_for(endpoint, device)?,
        };
        if let Some((org_id, key_id)) = &device.upgrade {
            match &mut credentials.agent {
                Some(agent::Agent::Active { registration, .. })
                    if &registration.org_id == org_id && &registration.key_id == key_id =>
                {
                    registration.tier = device
                        .upgrade_tier
                        .clone()
                        .unwrap_or_else(|| "oauth".to_owned());
                }
                _ => {
                    return Err(Error::new(
                        "credentials_changed",
                        "agent changed while device approval was pending",
                    ));
                }
            }
        }
        credentials.session = Some(session);
        self.save(&credentials)?;
        credentials
            .session
            .ok_or_else(|| Error::new("credential_error", "missing saved session"))
    }

    pub(super) fn save(&self, credentials: &Credentials) -> Result<(), Error> {
        reject_alias(&self.path)?;
        let bytes = serde_json::to_vec(credentials)
            .map_err(|_| Error::new("credential_error", "cannot serialize credentials"))?;
        let temporary = sidecar(&self.path, ".next");
        let mut file = private_file(&temporary, true)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        crate::durable_rename(&temporary, &self.path, crate::RenameMode::Replace, None)?;
        Ok(())
    }

    pub(super) fn clear(&self) -> Result<(), Error> {
        reject_alias(&self.path)?;
        if self.path.exists() {
            let mut credentials: Credentials = serde_json::from_slice(&fs::read(&self.path)?)
                .map_err(|_| Error::new("credential_error", "invalid credentials file"))?;
            if credentials.agent.is_some() {
                credentials.session = None;
                return self.save(&credentials);
            }
        }
        match fs::remove_file(&self.path) {
            Ok(()) => crate::sync_parent(
                self.path
                    .parent()
                    .ok_or_else(|| Error::new("credential_error", "missing parent"))?,
                crate::Durability::Full,
            )
            .map_err(Into::into),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

pub(super) fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn private_file(path: &Path, truncate: bool) -> io::Result<File> {
    reject_alias(path)?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn reject_alias(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "credentials and sidecars must be regular files, not aliases",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "credential hard links are not supported",
            ));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        let file = fs::OpenOptions::new()
            .access_mode(0)
            .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        let links = acyclic_native_runtime::hard_link_count(&file)?;
        if links != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "credential hard links are not supported",
            ));
        }
    }
    Ok(())
}
