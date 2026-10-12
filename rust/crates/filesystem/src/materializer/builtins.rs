use super::*;
use crate::NativeRootIdentity;

impl NativeMaterializationWork {
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
                    { #[cfg(unix)] { parent.open_child(Path::new(name), state) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new(name), state, |path, work| parent.open_child(path, work)) } }
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
            &crate::native_host::NativeOpenOptions::Read,
            &mut &*self,
        );
        let checked = guard().await;
        let file = opened.map_err(E::from)?;
        checked?;
        crate::native_host::lock_builtin_file(&file, false, self)
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
        let parent = { #[cfg(unix)] { root.open_dir_held(relative.parent().unwrap_or_else(|| Path::new("")), &mut &*self) } #[cfg(windows)] { crate::native_host::with_wide_path(relative.parent().unwrap_or_else(|| Path::new("")), &mut &*self, |path, work| root.open_dir_held(path, work)) } }?;
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
        let (mut bytes, allocation) =
            crate::native_host::buffer::<u8, _>(before.len() as usize, &mut &*self)
                .map_err(E::from)?;
        let initialized = NativeResourceCredit::Copy.delta(bytes.capacity() as u64);
        self.admit(initialized).map_err(E::from)?;
        bytes.resize(bytes.capacity(), 0);
        self.spent(initialized).map_err(E::from)?;
        let mut bytes = acyclic_stream::PreparedOwnedBytes::new(
            bytes,
            allocation,
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
            let attempted = crate::native_host::builtin_io(acyclic_native_runtime::NativeOperationKind::Read, (&mut bytes.writable()[filled..before.len() as usize]).len(), self, || std::io::Read::read(&mut &*(&held.0), &mut bytes.writable()[filled..before.len() as usize]));
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
                { #[cfg(unix)] { held.create_dir_all_held(relative.parent().unwrap_or_else(|| Path::new("")), state) } #[cfg(windows)] { crate::native_host::with_wide_path(relative.parent().unwrap_or_else(|| Path::new("")), state, |path, work| held.create_dir_all_held(path, work)) } }
            })
            .map_err(native_materialization_work_error)?;
        let leaf = Path::new(
            relative
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        );
        let directory = if require_new {
            self.with_original_leaf(|state| { #[cfg(unix)] { parent.create_dir(leaf, state) } #[cfg(windows)] { crate::native_host::with_wide_path(leaf, state, |path, work| parent.create_dir(path, work)) } })
                .map_err(native_materialization_work_error)?;
            crate::native_host::sync_directory_held(&parent.directory, &mut &*self)?;
            self.with_original_leaf(|state| { #[cfg(unix)] { parent.open_child(leaf, state) } #[cfg(windows)] { crate::native_host::with_wide_path(leaf, state, |path, work| parent.open_child(path, work)) } })
                .map_err(native_materialization_work_error)?
        } else {
            self.with_original_leaf(|state| { #[cfg(unix)] { parent.create_dir_all_held(leaf, state) } #[cfg(windows)] { crate::native_host::with_wide_path(leaf, state, |path, work| parent.create_dir_all_held(path, work)) } })
                .map_err(native_materialization_work_error)?
        };
        Ok(directory.identity())
    }
}
