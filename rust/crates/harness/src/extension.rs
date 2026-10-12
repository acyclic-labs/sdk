//! Native, linked extension implementations and their admission leases.

use crate::{
    Error, Result,
    core::{ExtensionAdmission, ExtensionDependency, Reducer, validate_extension_name},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Exact executable identity for one installed extension version.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ExtensionIdentity {
    /// Namespaced extension name.
    pub name: String,
    /// Positive extension version.
    pub version: u32,
    /// Digest of the linked implementation and its executable contract.
    pub digest: [u8; 32],
}

impl ExtensionIdentity {
    /// Creates and validates an exact executable identity.
    pub fn new(name: impl Into<String>, version: u32, digest: [u8; 32]) -> Result<Self> {
        let identity = Self {
            name: name.into(),
            version,
            digest,
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Validates the identity without consulting an installation registry.
    pub fn validate(&self) -> Result<()> {
        validate_extension_name(&self.name)?;
        if self.version == 0 || self.digest == [0; 32] {
            return Err(Error::Invalid(
                "extension version or digest is empty".into(),
            ));
        }
        Ok(())
    }

}

/// One process-local native extension implementation.
pub trait NativeExtension: Send + Sync {
    /// Returns the immutable executable identity.
    fn identity(&self) -> ExtensionIdentity;

    /// Returns exact installed dependencies.
    fn dependencies(&self) -> Vec<ExtensionDependency> {
        Vec::new()
    }

    /// Called once after an implementation has been removed and no admission lease can still observe it.
    fn dispose(&self) {}
}

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

/// Thread-safe exact-version registry for native implementations.
#[derive(Clone, Default)]
pub struct ExtensionRegistry(Arc<Mutex<RegistryState>>);

impl ExtensionRegistry {
    /// Installs one exact executable version and accepts it for new work.
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
        // The current pointer controls fresh activation.
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

    /// Disables all new admissions for a logical extension name.
    pub fn disable(&self, name: &str) -> Result<()> {
        validate_extension_name(name)?;
        let mut state = self.lock()?;
        state.current.remove(name);
        for (identity, entry) in &mut state.versions {
            if identity.name == name { entry.accepting = false; }
        }
        Ok(())
    }

    /// Pins a specific installed version and digest.
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

    /// Prevents removal of a current or actively retained implementation.
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

    /// Reports whether an exact version remains resident (accepting or draining).
    pub fn contains(&self, identity: &ExtensionIdentity) -> Result<bool> {
        Ok(self.lock()?.versions.contains_key(identity))
    }

    /// Reports whether a logical name accepts new admissions.
    pub fn accepting(&self, name: &str) -> Result<bool> {
        Ok(self.lock()?.current.contains_key(name))
    }

    /// Returns the currently accepting exact implementation, if any.
    pub fn current(&self, name: &str) -> Result<Option<ExtensionIdentity>> {
        Ok(self.lock()?.current.get(name).cloned())
    }

    /// Returns the resident identity for an exact logical version, including a draining version retained by an older admitted composition.
    pub fn exact(&self, name: &str, version: u32) -> Result<Option<ExtensionIdentity>> {
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

    fn resolve(
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

/// A retained native implementation lease.
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

/// Linked extension composition attached to a harness scope.
pub struct ExtensionRuntime {
    registry: ExtensionRegistry,
    retained: Vec<ExtensionLease>,
    accepting: AtomicBool,
    admission_gate: Mutex<()>,
}

impl ExtensionRuntime {
    /// Activates exact roots and their dependency closure for a composition.
    pub fn activate(
        registry: ExtensionRegistry,
        roots: impl IntoIterator<Item = ExtensionIdentity>,
    ) -> Result<Arc<Self>> {
        let roots = roots.into_iter().collect::<Vec<_>>();
        let selected = registry.resolve(&roots, true)?;
        let retained = selected
            .iter()
            .map(|identity| registry.pin_exact(identity, true))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self {
            registry,
            retained,
            accepting: AtomicBool::new(true),
            admission_gate: Mutex::new(()),
        }))
    }

    /// Activates a committed agent selection and checks every native digest against the reducer's exact schema binding.
    pub fn from_reducer(registry: ExtensionRegistry, reducer: &Reducer) -> Result<Arc<Self>> {
        let admission = reducer
            .extension_admission()?
            .ok_or_else(|| Error::Unsupported("agent has no committed extensions".into()))?;
        admission.validate(&mut None)?;
        let mut roots = Vec::with_capacity(admission.selected().len());
        for dependency in admission.selected() {
            let identity = registry
                .exact(&dependency.name, dependency.version)?
                .ok_or_else(|| {
                    Error::Unsupported(format!("extension {} is not installed", dependency.name))
                })?;
            if reducer.schemas().implementation_digest(&dependency.name, dependency.version, &mut None)? != identity.digest {
                return Err(Error::Conflict(format!(
                    "extension {}@{} implementation digest differs from admission",
                    dependency.name, dependency.version
                )));
            }
            roots.push(identity);
        }
        let selected = registry.resolve(&roots, false)?;
        let retained = selected
            .iter()
            .map(|identity| registry.pin_exact(identity, false))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self {
            registry,
            retained,
            accepting: AtomicBool::new(true),
            admission_gate: Mutex::new(()),
        }))
    }

    /// Disables this composition and its logical extensions for new task admissions.
    pub fn disable(&self) -> Result<()> {
        let _gate = self
            .admission_gate
            .lock()
            .map_err(|_| Error::Storage("extension admission gate poisoned".into()))?;
        self.accepting.store(false, Ordering::Release);
        Ok(())
    }

    /// Whether new task admissions can retain this composition.
    #[must_use]
    pub fn accepts_new_admissions(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Iterates dependency-ordered exact versions retained by this composition.
    #[must_use]
    pub fn selected(&self) -> impl ExactSizeIterator<Item = &ExtensionIdentity> {
        self.retained.iter().map(ExtensionLease::identity)
    }

    /// Retains required versions, admitting disabled versions only for committed operations.
    pub fn retain(
        &self,
        requirements: impl IntoIterator<Item = String>,
        existing: bool,
    ) -> Result<Vec<ExtensionLease>> {
        let _gate = self
            .admission_gate
            .lock()
            .map_err(|_| Error::Storage("extension admission gate poisoned".into()))?;
        if !existing && !self.accepts_new_admissions() {
            return Err(Error::Conflict("extension admissions are disabled".into()));
        }
        let required = requirements
            .into_iter()
            .map(|requirement| parse_requirement(&requirement))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let mut leases = Vec::new();
        for dependency in required {
            let identity = self
                .selected()
                .find(|identity| {
                    identity.name == dependency.name && identity.version == dependency.version
                })
                .ok_or_else(|| {
                    Error::Unsupported(format!(
                        "extension {}@{} is not active",
                        dependency.name, dependency.version
                    ))
                })?;
            let lease = self.registry.pin_exact(identity, !existing)?;
            leases.push(lease);
        }
        Ok(leases)
    }

    /// Checks that a committed metadata selection agrees with linked code.
    pub(crate) fn validate_admission(
        &self, admission: Option<&ExtensionAdmission>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if let Some(admission) = admission {
            admission.validate(work)?;
            if admission.selected().len() != self.retained.len() {
                return Err(crate::runtime::scope_metadata_boundary(
                    work, crate::wire::ErrorCode::Conflict,
                    "extension admission differs from linked composition",
                ));
            }
            for selected in admission.selected() {
                let mut found = false;
                for linked in self.selected() {
                    if let Some(io) = work.as_deref_mut() {
                        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                    }
                    if selected.name == linked.name && selected.version == linked.version {
                        found = true; break;
                    }
                }
                if !found {
                    return Err(crate::runtime::scope_metadata_boundary(
                        work, crate::wire::ErrorCode::Conflict,
                        "extension admission differs from linked composition",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn parse_requirement(requirement: &str) -> Result<Option<ExtensionDependency>> {
    let Some(value) = requirement.strip_prefix("extension:") else {
        return Ok(None);
    };
    let (name, version) = value
        .rsplit_once('@')
        .ok_or_else(|| Error::Invalid(format!("invalid extension dependency {requirement}")))?;
    let version = version
        .parse::<u32>()
        .map_err(|_| Error::Invalid(format!("invalid extension dependency {requirement}")))?;
    let dependency = ExtensionDependency {
        name: name.to_owned(),
        version,
    };
    dependency.validate()?;
    Ok(Some(dependency))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct TestExtension {
        identity: ExtensionIdentity,
        disposed: Arc<AtomicUsize>,
    }

    impl NativeExtension for TestExtension {
        fn identity(&self) -> ExtensionIdentity {
            self.identity.clone()
        }

        fn dispose(&self) {
            self.disposed.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn extension(version: u32, disposed: Arc<AtomicUsize>) -> Result<Arc<TestExtension>> {
        let digest_byte = u8::try_from(version)
            .map_err(|_| Error::Invalid("test extension version is out of range".into()))?;
        Ok(Arc::new(TestExtension {
            identity: ExtensionIdentity::new("example.extension", version, [digest_byte; 32])?,
            disposed,
        }))
    }

    #[test]
    fn upgrade_keeps_prior_exact_version_until_explicit_removal() -> Result<()> {
        let registry = ExtensionRegistry::default();
        let disposed = Arc::new(AtomicUsize::new(0));
        let first = extension(1, Arc::clone(&disposed))?;
        let first_identity = first.identity();
        registry.install(first)?;
        let runtime = ExtensionRuntime::activate(registry.clone(), [first_identity.clone()])?;

        registry.install(extension(2, Arc::clone(&disposed))?)?;
        assert!(registry.contains(&first_identity)?);
        assert!(registry.accepting("example.extension")?);
        assert_eq!(disposed.load(Ordering::Acquire), 0);

        registry.disable("example.extension")?;
        assert!(matches!(
            registry.remove(&first_identity),
            Err(Error::Conflict(_))
        ));
        drop(runtime);
        registry.remove(&first_identity)?;
        assert_eq!(disposed.load(Ordering::Acquire), 1);
        Ok(())
    }

    #[test]
    fn runtime_disable_is_local_to_its_composition() -> Result<()> {
        let registry = ExtensionRegistry::default();
        let disposed = Arc::new(AtomicUsize::new(0));
        let implementation = extension(1, disposed)?;
        let identity = implementation.identity();
        registry.install(implementation)?;
        let runtime = ExtensionRuntime::activate(registry.clone(), [identity])?;
        runtime.disable()?;
        assert!(registry.accepting("example.extension")?);
        assert!(matches!(
            runtime.retain(["extension:example.extension@1".into()], false),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn registry_disable_blocks_new_activation_but_not_reconciliation() -> Result<()> {
        let registry = ExtensionRegistry::default();
        let disposed = Arc::new(AtomicUsize::new(0));
        let implementation = extension(1, disposed)?;
        let identity = implementation.identity();
        registry.install(implementation)?;
        let runtime = ExtensionRuntime::activate(registry.clone(), [identity.clone()])?;
        registry.disable("example.extension")?;
        assert!(matches!(
            ExtensionRuntime::activate(registry, [identity]),
            Err(Error::Conflict(_))
        ));
        let retained = runtime.retain(["extension:example.extension@1".into()], true)?;
        assert_eq!(retained.len(), 1);
        Ok(())
    }
}
