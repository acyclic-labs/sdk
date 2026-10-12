//! Resources for the service.
use super::*;

pub(crate) fn session_directory(data: &Path, session_id: &str) -> PathBuf {
    data.join("sessions")
        .join(blake3::hash(session_id.as_bytes()).to_hex().as_str())
}

#[derive(Clone)]
pub(crate) struct ServiceResources {
    pub(crate) data: PathBuf,
    pub(crate) fs: LocalFs,
    pub(crate) store: LocalCoreStateStore,
    pub(crate) shared_roots: SharedRootRegistry,
    pub(crate) binary_identity: String,
    pub(crate) instance_id: String,
    pub(crate) private_state_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
}

pub(crate) type ServiceConfiguredInfrastructure = (LocalFs, LocalCoreStateStore, String, String);

impl ServiceResources {
    // The trusted, locked CLI bootstrap configures daemon-lifetime backends
    // before finite native work. This is not a Root IO/allocation receipt.
    pub(crate) async fn configure(
        data: &Path,
    ) -> acyclic_harness::Result<ServiceConfiguredInfrastructure> {
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(|error| Error::Storage(display(error)))?;
        acyclic_fs::initialize_native_mount_services()
            .map_err(|error| Error::Storage(display(error)))?;
        let identity_data = data.to_path_buf();
        let binary_identity = tokio::task::spawn_blocking(move || service_identity(&identity_data));
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(display(error)))?;
        let binary_identity = binary_identity
            .await
            .map_err(|error| Error::Storage(display(error)))?
            .map_err(Error::Storage)?;
        let store = LocalCoreStateStore::open_owned(data.join("core-state"))
            .map_err(|error| Error::Storage(display(error)))?;
        Ok((fs, store, binary_identity, uuid::Uuid::new_v4().to_string()))
    }

    pub(crate) async fn open(
        data: PathBuf,
        configured: ServiceConfiguredInfrastructure,
        private_state_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
        original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Self, BTreeMap<String, SessionController>)> {
        original_bootstrap_work
            .create_directories_with_native_work(&private_state_root, Path::new("sessions"))?;
        let (fs, store, binary_identity, instance_id) = configured;
        let resources = Self {
            store,
            shared_roots: SharedRootRegistry::default(),
            binary_identity,
            instance_id,
            data,
            fs,
            private_state_root,
        };
        resources
            .shared_roots
            .collection
            .start(resources.fs.clone(), resources.store.clone());
        let mut entries = original_bootstrap_work.read_private_state_directory_with_native_work(
            &resources.private_state_root,
            Path::new("sessions"),
        )?;
        for entry in entries.0.iter_mut().filter(|entry| entry.is_directory) {
            let relative = original_bootstrap_work
                .path(Path::new("sessions"), Some(Path::new(&entry.name)))?;
            let mut modified: Option<std::time::SystemTime> = None;
            for slot in ADAPTER_STATE_SLOTS {
                let path = original_bootstrap_work.path(&relative.0, Some(Path::new(slot)))?;
                match original_bootstrap_work
                    .state_path_modified_with_native_work(&resources.private_state_root, &path.0)
                {
                    Ok(Some(value)) => {
                        modified = Some(modified.map_or(value, |old| old.max(value)))
                    }
                    Ok(None) | Err(acyclic_fs::NativeTreeMaterializationError::Io(_)) => {}
                    Err(cause) => {
                        return Err(Error::NativeWork {
                            cause,
                            custody: None,
                        });
                    }
                }
            }
            entry.modified = modified.unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        }
        original_bootstrap_work.sort_by_with_native_work(&mut entries.0, |left, right| {
            if left.is_directory != right.is_directory {
                return Ok(!left.is_directory);
            }
            if !left.is_directory {
                return Ok(false);
            }
            let order = left.modified.cmp(&right.modified);
            if order != std::cmp::Ordering::Equal {
                return Ok(order == std::cmp::Ordering::Less);
            }
            Ok(left.name < right.name)
        })?;
        let mut sessions = BTreeMap::new();
        let mut retained_sources = BTreeMap::<PathBuf, ([u8; 16], [u8; 16])>::new();
        for row in entries.0.iter().filter(|entry| entry.is_directory) {
            let relative =
                original_bootstrap_work.path(Path::new("sessions"), Some(Path::new(&row.name)))?;
            let (directory_custody, entry) = {
                let (path, custody) =
                    original_bootstrap_work.path(&resources.data, Some(&relative.0))?;
                (custody, path)
            };
            let loaded = crate::adapter_state::load_state_with_native_work(
                &resources.private_state_root,
                &relative.0,
                original_bootstrap_work,
            )?;
            let state = &loaded.0.0;
            if !state.active {
                continue;
            }
            if matches!(state.task_binding, PluginTaskBinding::Pi(_)) {
                let control = SessionController::open(entry, &resources, original_bootstrap_work)
                    .await?
                    .retain_original_input(directory_custody)?;
                let (session_id, key_custody) =
                    original_bootstrap_work.display(&state.root_session_id)?;
                let control = control.retain_original_input(key_custody)?;
                sessions.insert(session_id, control);
                continue;
            }
            let mut roots = Vec::with_capacity(state.roots.len());
            let mut stale = false;
            for root in state.roots.values() {
                let creation = root.created().map_err(Error::Invalid)?;
                let canonical = root
                    .path
                    .canonicalize()
                    .map_err(|error| Error::Storage(display(error)))?;
                if retained_sources.get(&canonical).is_some_and(
                    |(source_identity, native_identity)| {
                        *native_identity == creation.native_root_identity
                            && *source_identity != creation.source_identity
                    },
                ) {
                    stale = true;
                    break;
                }
                roots.push((
                    canonical,
                    (creation.source_identity, creation.native_root_identity),
                ));
            }
            if stale {
                fs::remove_dir_all(&entry).map_err(|error| Error::Storage(display(error)))?;
                continue;
            }
            let control = resources
                .open_session_directory(entry)
                .await
                .map_err(Error::Storage)?;
            if !control.state.root_session_id.is_empty() {
                retained_sources.extend(roots);
                let (session_id, key_custody) =
                    original_bootstrap_work.display(&control.state.root_session_id)?;
                let control = SessionController::from(control)
                    .retain_original_input(directory_custody)?
                    .retain_original_input(key_custody)?;
                sessions.insert(session_id, control);
            }
        }
        Ok((resources, sessions))
    }

    pub(crate) fn session_directory(&self, session_id: &str) -> PathBuf {
        session_directory(&self.data, session_id)
    }

    pub(crate) fn original_session_directory_matches(
        &self,
        relative_directory: &Path,
        session_id: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<bool> {
        let items_examined =
            (session_id.len() as u64)
                .checked_add(64)
                .ok_or(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "original session identity inspection count overflows",
                })?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined,
            ..Default::default()
        })?;
        let key = blake3::hash(session_id.as_bytes()).to_hex();
        Ok(relative_directory.parent() == Some(Path::new("sessions"))
            && relative_directory.file_name() == Some(std::ffi::OsStr::new(key.as_str())))
    }

    pub(crate) fn private_session_data<'a>(
        &self,
        directory: &'a Path,
    ) -> acyclic_harness::Result<&'a Path> {
        let relative = directory
            .strip_prefix(&self.data)
            .map_err(|_| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "private session directory is outside the configured service root",
            })?;
        let mut components = relative.components();
        let Some(std::path::Component::Normal(namespace)) = components.next() else {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "private session directory has no configured sessions namespace",
            });
        };
        let key = match components.next() {
            Some(std::path::Component::Normal(key)) => key.to_str(),
            _ => None,
        };
        if namespace != "sessions"
            || !key.is_some_and(|key| {
                key.len() == 64
                    && key
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            || components.next().is_some()
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "private session directory is not an original service session key",
            });
        }
        Ok(relative)
    }

    pub(crate) async fn open_session_directory(
        &self,
        directory: PathBuf,
    ) -> Result<ControlPlane, String> {
        ControlPlane::open_with(
            directory,
            self.data.clone(),
            self.fs.clone(),
            self.store.clone(),
            self.shared_roots.clone(),
        )
        .await
    }
}
