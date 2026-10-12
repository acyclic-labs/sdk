use super::*;
struct Installed {
    implementation: Arc<dyn NativeExtension>,
    dependencies: Vec<ExtensionDependency>,
    accepting: bool,
    active: usize,
}
#[derive(Default)]
struct RegistryState {
    current: BTreeMap<String, ExtensionIdentity>,
    versions: BTreeMap<ExtensionIdentity, Installed>,
    version_digests: BTreeMap<(String, u32), [u8; 32]>,
}
/// Exact-version registry for native implementations.
#[derive(Clone, Default)]
pub struct ExtensionRegistry(Arc<Mutex<RegistryState>>);
impl ExtensionRegistry {
    /// Installs an exact version for new work.
    pub fn install(&self, implementation: Arc<dyn NativeExtension>) -> Result<()> {
        let identity = implementation.identity();
        identity.validate()?;
        let mut dependencies = implementation.dependencies();
        let mut unique = BTreeSet::new();
        for dependency in &dependencies {
            dependency.validate()?;
            if dependency.name == identity.name && dependency.version == identity.version {
                return Err(Error::Conflict("extension depends on itself".into()));
            }
            if !unique.insert(dependency.clone()) {
                return Err(Error::Conflict("duplicate extension dependency".into()));
            }
        }
        dependencies.sort();
        let mut state = self.lock()?;
        if state.versions.contains_key(&identity) {
            return Err(Error::Conflict(
                "extension version is already installed".into(),
            ));
        }
        let key = (identity.name.clone(), identity.version);
        if state
            .version_digests
            .get(&key)
            .is_some_and(|digest| digest != &identity.digest)
        {
            return Err(Error::Conflict(
                "extension version is pinned to another digest".into(),
            ));
        }
        state.version_digests.insert(key, identity.digest);
        state
            .current
            .insert(identity.name.clone(), identity.clone());
        state.versions.insert(
            identity,
            Installed {
                implementation,
                dependencies,
                accepting: true,
                active: 0,
            },
        );
        Ok(())
    }

    /// Disables new admissions for a logical name.
    pub fn disable(&self, name: &str) -> Result<()> {
        validate_extension_name(name)?;
        let mut state = self.lock()?;
        state.current.remove(name);
        for (identity, entry) in &mut state.versions {
            if identity.name == name {
                entry.accepting = false;
            }
        }
        Ok(())
    }

    /// Pins an exact version, optionally requiring new-admission eligibility.
    pub fn pin_exact(
        &self,
        identity: &ExtensionIdentity,
        accepting_only: bool,
    ) -> Result<ExtensionLease> {
        identity.validate()?;
        let mut state = self.lock()?;
        let entry = state.versions.get_mut(identity).ok_or_else(|| {
            Error::NotFound(format!("extension {}@{}", identity.name, identity.version))
        })?;
        if accepting_only && !entry.accepting {
            return Err(Error::Conflict(format!(
                "extension {}@{} is disabled",
                identity.name, identity.version
            )));
        }
        entry.active = entry
            .active
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("extension active count exhausted".into()))?;
        Ok(ExtensionLease {
            registry: self.clone(),
            identity: identity.clone(),
        })
    }

    /// Removes an unselected version after its last admission drops.
    pub fn remove(&self, identity: &ExtensionIdentity) -> Result<()> {
        identity.validate()?;
        let mut state = self.lock()?;
        if state.current.get(&identity.name) == Some(identity) {
            return Err(Error::Conflict(
                "current extension must be disabled before removal".into(),
            ));
        }
        let entry = state.versions.get(identity).ok_or_else(|| {
            Error::NotFound(format!("extension {}@{}", identity.name, identity.version))
        })?;
        if entry.active != 0 {
            return Err(Error::Conflict(
                "extension still has retained admissions".into(),
            ));
        }
        let entry = state
            .versions
            .remove(identity)
            .ok_or_else(|| Error::Storage("extension disappeared during removal".into()))?;
        drop(state);
        entry.implementation.dispose();
        Ok(())
    }

    pub(super) fn exact(&self, name: &str, version: u32) -> Result<Option<ExtensionIdentity>> {
        Ok(self
            .lock()?
            .versions
            .keys()
            .find(|identity| identity.name == name && identity.version == version)
            .cloned())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RegistryState>> {
        self.0
            .lock()
            .map_err(|_| Error::Storage("extension registry lock poisoned".into()))
    }

    pub(super) fn resolve(
        &self,
        roots: &[ExtensionIdentity],
        accepting_only: bool,
    ) -> Result<Vec<ExtensionIdentity>> {
        let state = self.lock()?;
        let mut selected = BTreeMap::<String, ExtensionIdentity>::new();
        let mut pending = roots.to_vec();
        while let Some(identity) = pending.pop() {
            identity.validate()?;
            if let Some(previous) = selected.get(&identity.name) {
                if previous != &identity {
                    return Err(Error::Conflict(format!(
                        "extension {} has conflicting exact versions",
                        identity.name
                    )));
                }
                continue;
            }
            let entry = state.versions.get(&identity).ok_or_else(|| {
                Error::Unsupported(format!(
                    "missing extension {}@{}",
                    identity.name, identity.version
                ))
            })?;
            if accepting_only && !entry.accepting {
                return Err(Error::Conflict(format!(
                    "extension {}@{} is disabled",
                    identity.name, identity.version
                )));
            }
            selected.insert(identity.name.clone(), identity);
            for dependency in &entry.dependencies {
                let dependency_identity = state
                    .versions
                    .keys()
                    .find(|identity| {
                        identity.name == dependency.name && identity.version == dependency.version
                    })
                    .cloned()
                    .ok_or_else(|| {
                        Error::Unsupported(format!(
                            "missing extension dependency {}@{}",
                            dependency.name, dependency.version
                        ))
                    })?;
                if dependency_identity.version != dependency.version {
                    return Err(Error::Conflict(format!(
                        "extension dependency {} is not installed at the required version",
                        dependency.name
                    )));
                }
                pending.push(dependency_identity);
            }
        }
        let mut remaining = selected
            .values()
            .map(|value| (value.clone(), 0usize))
            .collect::<BTreeMap<_, _>>();
        let mut dependents: BTreeMap<ExtensionIdentity, Vec<ExtensionIdentity>> = BTreeMap::new();
        for identity in selected.values() {
            let entry = state
                .versions
                .get(identity)
                .ok_or_else(|| Error::Storage("extension disappeared during resolution".into()))?;
            for dependency in &entry.dependencies {
                let dep = selected
                    .get(&dependency.name)
                    .ok_or_else(|| {
                        Error::Storage("extension dependency disappeared during resolution".into())
                    })?
                    .clone();
                let count = remaining.get_mut(identity).ok_or_else(|| {
                    Error::Storage("selected identity missing during ordering".into())
                })?;
                *count += 1;
                dependents.entry(dep).or_default().push(identity.clone());
            }
        }
        let mut ready = remaining
            .iter()
            .filter_map(|(identity, count)| (*count == 0).then_some(identity.clone()))
            .collect::<BTreeSet<_>>();
        let mut order = Vec::with_capacity(remaining.len());
        while let Some(identity) = ready.pop_first() {
            order.push(identity.clone());
            for dependent in dependents.get(&identity).into_iter().flatten() {
                let count = remaining.get_mut(dependent).ok_or_else(|| {
                    Error::Storage("dependent identity missing during ordering".into())
                })?;
                *count -= 1;
                if *count == 0 {
                    ready.insert(dependent.clone());
                }
            }
        }
        if order.len() != remaining.len() {
            return Err(Error::Conflict("extension dependency cycle".into()));
        }
        Ok(order)
    }
}

/// Pins one native implementation until its admission drops.
pub struct ExtensionLease {
    registry: ExtensionRegistry,
    identity: ExtensionIdentity,
}

impl ExtensionLease {
    /// Returns the exact version and digest retained by this invocation.
    #[must_use]
    pub const fn identity(&self) -> &ExtensionIdentity {
        &self.identity
    }
}

impl Drop for ExtensionLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.registry.lock()
            && let Some(entry) = state.versions.get_mut(&self.identity)
        {
            entry.active = entry.active.saturating_sub(1);
        }
    }
}
