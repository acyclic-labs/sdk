use super::*;
use crate::NativeRootIdentity;

impl NativeMaterializationWork {
    /// Opens a working directory through its witnessed root.
    pub fn open_working_directory(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
    ) -> Result<std::fs::File, NativeTreeMaterializationError> {
        for component in relative.components() {
            self.charge(NativeResourceCredit::Items.delta(1))?;
            if !matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            ) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        let held = self.builtin_root(root, expected)?;
        let directory = held.open_dir_held(relative, &mut &*self)?;
        self.builtin_root(root, expected)?;
        Ok(directory.into_std_file())
    }
    fn builtin_root(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
    ) -> Result<crate::native_host::HostRoot, NativeTreeMaterializationError> {
        let held = crate::native_host::HostRoot::open(root, &mut &*self)?;
        if held.identity() != expected {
            return Err(NativeTreeMaterializationError::AllocationChanged {
                expected,
                actual: held.identity(),
            });
        }
        Ok(held)
    }
    fn builtin_request(
        &self,
        relative: &Path,
        maximum: usize,
    ) -> Result<(), NativeTreeMaterializationError> {
        if !self.remaining()?.has_finite_allowances() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        if maximum > Self::MAXIMUM_BUILTIN_BYTES {
            return Err(NativeTreeMaterializationError::PayloadTooLarge {
                maximum: Self::MAXIMUM_BUILTIN_BYTES as u64,
                actual: maximum as u64,
            });
        }
        let relative = relative
            .to_str()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        if !crate::path::is_canonical_relative_with_step(relative, || {
            self.charge(crate::WorkCounters {
                source_path_components: 1,
                ..crate::WorkCounters::UNCHARGED
            })
        })? {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(())
    }
    async fn builtin_file<E, F, Fut>(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
        options: crate::native_host::NativeOpenOptions,
        create_parents: bool,
        guard: &mut F,
    ) -> Result<(std::fs::File, NativeRootIdentity, NativeRootIdentity), E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        guard().await?;
        let held = self.builtin_root(root, expected).map_err(E::from)?;
        let mut parent = held;
        for component in relative
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .components()
        {
            let std::path::Component::Normal(name) = component else {
                return Err(E::from(NativeTreeMaterializationError::InvalidLayout));
            };
            guard().await?;
            let opened = self
                .with_original_leaf(|state| {
                    if create_parents {
                        parent.create_dir_all_held(Path::new(name), state)
                    } else {
                        parent.open_child(Path::new(name), state)
                    }
                })
                .map_err(native_materialization_work_error);
            let checked = guard().await;
            parent = opened.map_err(E::from)?;
            checked?;
            self.builtin_root(root, expected).map_err(E::from)?;
        }
        let parent_identity = parent.identity();
        guard().await?;
        let opened = crate::native_host::open_leaf(
            &parent.directory,
            Path::new(
                relative
                    .file_name()
                    .ok_or_else(|| E::from(NativeTreeMaterializationError::InvalidLayout))?,
            ),
            &options,
            &mut &*self,
        );
        let checked = guard().await;
        let file = opened.map_err(E::from)?;
        checked?;
        crate::native_host::lock_builtin_file(
            &file,
            !matches!(options, crate::native_host::NativeOpenOptions::Read),
            self,
        )
        .map_err(E::from)?;
        let (identity, _) =
            crate::native_host::file_identity_and_length(&file, &mut &*self).map_err(E::from)?;
        self.validate_builtin(root, expected, relative, parent_identity, identity)
            .map_err(E::from)?;
        Ok((file, parent_identity, identity))
    }
    fn validate_builtin(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
        parent_identity: NativeRootIdentity,
        identity: NativeRootIdentity,
    ) -> Result<(), NativeTreeMaterializationError> {
        let root = self.builtin_root(root, expected)?;
        let parent = root.open_dir_held(
            relative.parent().unwrap_or_else(|| Path::new("")),
            &mut &*self,
        )?;
        let actual = crate::native_host::directory_identity(&parent, &mut &*self)?;
        if actual != parent_identity {
            return Err(NativeTreeMaterializationError::AllocationChanged {
                expected: parent_identity,
                actual,
            });
        }
        let actual = crate::native_host::entry_identity(
            &parent,
            Path::new(
                relative
                    .file_name()
                    .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
            ),
            &mut &*self,
        )?;
        if actual != Some(identity) {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        Ok(())
    }
    /// Reads a complete bounded regular file from its original held allocation.
    pub async fn read_working_file<E, F, Fut>(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
        maximum: usize,
        mut guard: F,
    ) -> Result<acyclic_stream::OwnedBytes, E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        self.builtin_request(relative, maximum).map_err(E::from)?;
        let held = self
            .builtin_file(
                root,
                expected,
                relative,
                crate::native_host::NativeOpenOptions::Read,
                false,
                &mut guard,
            )
            .await?;
        self.read_held(root, expected, relative, &held, maximum, &mut guard)
            .await
    }
    async fn read_held<E, F, Fut>(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
        held: &(std::fs::File, NativeRootIdentity, NativeRootIdentity),
        maximum: usize,
        guard: &mut F,
    ) -> Result<acyclic_stream::OwnedBytes, E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        guard().await?;
        self.validate_builtin(root, expected, relative, held.1, held.2)
            .map_err(E::from)?;
        let before = crate::native_host::stat_file(&held.0, &mut &*self).map_err(E::from)?;
        if before.len() > maximum as u64 {
            return Err(E::from(NativeTreeMaterializationError::PayloadTooLarge {
                maximum: maximum as u64,
                actual: before.len(),
            }));
        }
        let buffer = crate::native_host::native_buffer::<u8>(before.len() as usize, self)
            .map_err(E::from)?;
        let mut bytes = buffer.data;
        let initialized = NativeResourceCredit::Copy.delta(bytes.capacity() as u64);
        self.admit(initialized).map_err(E::from)?;
        bytes.resize(bytes.capacity(), 0);
        self.spent(initialized).map_err(E::from)?;
        let mut bytes = acyclic_stream::PreparedOwnedBytes::new(
            bytes,
            buffer._allocation,
            self.runtime_meter(),
        )
        .map_err(|failure| {
            E::from(match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    self.runtime_error(error)
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    NativeTreeMaterializationError::InvalidLayout
                }
            })
        })?;
        let mut filled = 0;
        while filled < before.len() as usize {
            guard().await?;
            self.validate_builtin(root, expected, relative, held.1, held.2)
                .map_err(E::from)?;
            let attempted = crate::native_host::read_builtin_file(
                &held.0,
                &mut bytes.writable()[filled..before.len() as usize],
                self,
            );
            let checked = guard().await;
            let count = match attempted {
                Err(NativeTreeMaterializationError::Io(error))
                    if error.kind() == std::io::ErrorKind::Interrupted =>
                {
                    checked?;
                    continue;
                }
                result => result.map_err(E::from)?,
            };
            checked?;
            if count == 0 {
                return Err(E::from(NativeTreeMaterializationError::ExternalMutation));
            }
            filled += count;
        }
        self.validate_builtin(root, expected, relative, held.1, held.2)
            .map_err(E::from)?;
        let after = crate::native_host::stat_file(&held.0, &mut &*self).map_err(E::from)?;
        if !same_file_version(&before, &after).map_err(E::from)? {
            return Err(E::from(NativeTreeMaterializationError::ExternalMutation));
        }
        guard().await?;
        bytes.freeze(filled).map_err(|bytes| {
            drop(bytes);
            E::from(NativeTreeMaterializationError::InvalidLayout)
        })
    }
    /// Writes bounded UTF-8, retaining the witnessed file through preimage checking.
    pub async fn write_working_file<E, F, Fut>(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
        content: &str,
        create_parents: bool,
        expected_sha256: Option<[u8; 32]>,
        mut guard: F,
    ) -> Result<(), E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        use sha2::{Digest as _, Sha256};
        self.builtin_request(relative, content.len())
            .map_err(E::from)?;
        let options = if expected_sha256.is_some() {
            crate::native_host::NativeOpenOptions::ReadWrite
        } else {
            crate::native_host::NativeOpenOptions::ReadWriteCreate
        };
        let held = self
            .builtin_file(
                root,
                expected,
                relative,
                options,
                create_parents && expected_sha256.is_none(),
                &mut guard,
            )
            .await?;
        let stamp = if let Some(expected_hash) = expected_sha256 {
            let stamp = crate::native_host::stat_file(&held.0, &mut &*self).map_err(E::from)?;
            let bytes = self
                .read_held(
                    root,
                    expected,
                    relative,
                    &held,
                    Self::MAXIMUM_BUILTIN_BYTES,
                    &mut guard,
                )
                .await?;
            self.charge(crate::WorkCounters {
                bytes_hashed: bytes.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(E::from)?;
            if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected_hash {
                return Err(E::from(NativeTreeMaterializationError::PreimageConflict));
            }
            Some(stamp)
        } else {
            None
        };
        guard().await?;
        self.validate_builtin(root, expected, relative, held.1, held.2)
            .map_err(E::from)?;
        self.charge(NativeResourceCredit::ReadOperations.delta(1))
            .map_err(E::from)?;
        std::io::Seek::rewind(&mut &held.0)
            .map_err(NativeTreeMaterializationError::from)
            .map_err(E::from)?;
        let mut written = 0;
        loop {
            guard().await?;
            self.validate_builtin(root, expected, relative, held.1, held.2)
                .map_err(E::from)?;
            if written == 0 {
                if let Some(stamp) = &stamp {
                    let current =
                        crate::native_host::stat_file(&held.0, &mut &*self).map_err(E::from)?;
                    if !same_file_version(stamp, &current).map_err(E::from)? {
                        return Err(E::from(NativeTreeMaterializationError::PreimageConflict));
                    }
                }
            }
            if written == content.len() {
                break;
            }
            let attempted = crate::native_host::write_builtin_file(
                &held.0,
                &content.as_bytes()[written..],
                self,
            );
            let checked = guard().await;
            let count = match attempted {
                Err(NativeTreeMaterializationError::Io(error))
                    if error.kind() == std::io::ErrorKind::Interrupted =>
                {
                    checked?;
                    continue;
                }
                result => result.map_err(E::from)?,
            };
            checked?;
            if count == 0 {
                return Err(E::from(
                    std::io::Error::from(std::io::ErrorKind::WriteZero).into(),
                ));
            }
            written += count;
        }
        let truncated =
            crate::native_host::truncate_file(&held.0, content.len() as u64, &mut &*self);
        let checked = guard().await;
        truncated.map_err(E::from)?;
        checked?;
        self.validate_builtin(root, expected, relative, held.1, held.2)
            .map_err(E::from)
    }
    /// Creates or opens a service directory through its original root.
    pub fn create_owned_directory(
        &self,
        root: &Path,
        expected: NativeRootIdentity,
        relative: &Path,
        require_new: bool,
    ) -> Result<NativeRootIdentity, NativeTreeMaterializationError> {
        for component in relative.components() {
            self.charge(NativeResourceCredit::Items.delta(1))?;
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        let held = self.builtin_root(root, expected)?;
        if relative.as_os_str().is_empty() {
            return if require_new {
                Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into())
            } else {
                Ok(held.identity())
            };
        }
        let parent = self
            .with_original_leaf(|state| {
                held.create_dir_all_held(relative.parent().unwrap_or_else(|| Path::new("")), state)
            })
            .map_err(native_materialization_work_error)?;
        let leaf = Path::new(
            relative
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        );
        let directory = if require_new {
            self.with_original_leaf(|state| parent.create_dir(leaf, state))
                .map_err(native_materialization_work_error)?;
            crate::native_host::sync_directory_held(&parent.directory, &mut &*self)?;
            self.with_original_leaf(|state| parent.open_child(leaf, state))
                .map_err(native_materialization_work_error)?
        } else {
            self.with_original_leaf(|state| parent.create_dir_all_held(leaf, state))
                .map_err(native_materialization_work_error)?
        };
        Ok(directory.identity())
    }
}
