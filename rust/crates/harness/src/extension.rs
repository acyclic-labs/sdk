//! Native, linked extension implementations and their admission leases.
//!
//! The event-sourced extension records in [`crate::core`] deliberately contain
//! no executable code.  This module is the process-local companion: it links
//! a registered implementation to a composition, keeps versions alive while
//! work is admitted, and makes replacement/disposal explicit.

use crate::{
    Error, Result,
    core::{ExtensionAdmission, ExtensionDependency, Reducer, validate_extension_name},
    durable_tool::ResumableToolRegistry,
    runtime::{
        ArtifactBindings, Bindings, ContentBindings, DurableTaskHost, ExecutionProvider,
        InteractionResolver, InteractionRouter, TaskRegistry, TaskSpawner, TaskStateProvider,
        ToolPolicy,
    },
    tool::ToolRegistry,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
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

    /// Returns the event-sourced dependency identity for this implementation.
    #[must_use]
    pub fn dependency(&self) -> ExtensionDependency {
        ExtensionDependency {
            name: self.name.clone(),
            version: self.version,
        }
    }
}

/// One process-local native extension implementation.
pub trait NativeExtension: Send + Sync {
    /// Returns the immutable executable identity.
    fn identity(&self) -> ExtensionIdentity;

    /// Returns exact installed dependencies.  Dependencies are linked before
    /// this implementation and are retained under the same admission lease.
    fn dependencies(&self) -> Vec<ExtensionDependency> {
        Vec::new()
    }

    /// Adds native tasks/tools to a composition.  Implementations must only
    /// register exact, versioned definitions; the linker does not synthesize
    /// or replace registrations.
    fn link(&self, _linker: &mut ExtensionLinker<'_>) -> Result<()> {
        Ok(())
    }

    /// Called once after an implementation has been removed and no admission
    /// lease can still observe it.  Cleanup must not resurrect the extension.
    fn dispose(&self) {}
}

/// Mutable registration view provided while a native extension is linked.
pub struct ExtensionLinker<'a> {
    bindings: &'a mut Bindings,
}

impl ExtensionLinker<'_> {
    /// Returns the composition's task registry. Registration remains exact and
    /// rejects a duplicate identity rather than replacing it.
    pub fn tasks(&mut self) -> &mut TaskRegistry {
        &mut self.bindings.tasks
    }

    /// Returns the composition's live tool registry.
    pub fn tools(&mut self) -> &mut ToolRegistry {
        &mut self.bindings.tools
    }

    /// Returns the composition's resumable tool registry.
    pub fn resumable_tools(&mut self) -> &mut ResumableToolRegistry {
        &mut self.bindings.resumable_tools
    }

    /// Adds a durable host only when the composition has no host yet.
    pub fn durable_host(&mut self, host: Arc<dyn DurableTaskHost>) -> Result<()> {
        bind_once(&mut self.bindings.durable_host, host, "durable host")
    }

    /// Adds retained-state observation only when the composition has no state provider yet.
    pub fn state(&mut self, state: Arc<dyn TaskStateProvider>) -> Result<()> {
        bind_once(&mut self.bindings.state, state, "task state provider")
    }

    /// Adds durable admission only when the composition has no spawner yet.
    pub fn spawner(&mut self, spawner: Arc<dyn TaskSpawner>) -> Result<()> {
        bind_once(&mut self.bindings.spawner, spawner, "task spawner")
    }

    /// Adds qualified execution only when the composition has no route yet.
    pub fn execution(&mut self, execution: Arc<dyn ExecutionProvider>) -> Result<()> {
        bind_once(
            &mut self.bindings.execution,
            execution,
            "execution provider",
        )
    }

    /// Adds local interactions only when the composition has no router yet.
    pub fn interactions(&mut self, router: Arc<dyn InteractionRouter>) -> Result<()> {
        bind_once(
            &mut self.bindings.interactions,
            router,
            "interaction router",
        )
    }

    /// Adds owner-mediated interaction resolution only when absent.
    pub fn interaction_resolver(&mut self, resolver: Arc<dyn InteractionResolver>) -> Result<()> {
        bind_once(
            &mut self.bindings.interaction_resolver,
            resolver,
            "interaction resolver",
        )
    }

    /// Adds content access only when the composition has no content binding.
    pub fn content(&mut self, content: ContentBindings) -> Result<()> {
        bind_once(&mut self.bindings.content, content, "content binding")
    }

    /// Adds artifact access only when the composition has no artifact binding.
    pub fn artifacts(&mut self, artifacts: ArtifactBindings) -> Result<()> {
        bind_once(&mut self.bindings.artifacts, artifacts, "artifact binding")
    }

    /// Adds an invocation policy only when the composition has no policy yet.
    pub fn policy(&mut self, policy: Arc<dyn ToolPolicy>) -> Result<()> {
        bind_once(&mut self.bindings.policy, policy, "tool policy")
    }

    /// Adds parent-bound fork capture only when absent.
    pub fn fork_preparer(&mut self, preparer: Arc<dyn crate::fork::ForkPreparer>) -> Result<()> {
        bind_once(&mut self.bindings.fork_preparer, preparer, "fork preparer")
    }

    /// Adds a project workspace provider only when absent.
    pub fn workspaces(
        &mut self,
        workspaces: Arc<dyn crate::merge::ProjectWorkspaceProvider>,
    ) -> Result<()> {
        bind_once(
            &mut self.bindings.workspaces,
            workspaces,
            "workspace provider",
        )
    }

    /// Returns the current scope without allowing an extension to retarget it.
    #[must_use]
    pub fn scope(&self) -> &crate::runtime::RuntimeScope {
        &self.bindings.scope
    }
}

fn bind_once<T>(slot: &mut Option<T>, value: T, description: &str) -> Result<()> {
    if slot.is_some() {
        return Err(Error::Conflict(format!(
            "extension cannot replace existing {description}"
        )));
    }
    *slot = Some(value);
    Ok(())
}

struct Installed {
    implementation: Arc<dyn NativeExtension>,
    identity: ExtensionIdentity,
    dependencies: Vec<ExtensionDependency>,
    accepting: bool,
    active: usize,
}

struct RegistryState {
    current: BTreeMap<String, ExtensionIdentity>,
    versions: BTreeMap<ExtensionIdentity, Installed>,
    version_digests: BTreeMap<(String, u32), [u8; 32]>,
}

/// Thread-safe exact-version registry for native implementations.
#[derive(Clone)]
pub struct ExtensionRegistry(Arc<Mutex<RegistryState>>);

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(RegistryState {
            current: BTreeMap::new(),
            versions: BTreeMap::new(),
            version_digests: BTreeMap::new(),
        })))
    }
}

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
        // The current pointer controls fresh activation.  Keep the previous
        // exact version accepting in the registry until an owner explicitly
        // disables/removes it: another agent may still be committed to that
        // version and must retain it during a rolling upgrade or rollback.
        state
            .current
            .insert(identity.name.clone(), identity.clone());
        state.versions.insert(
            identity.clone(),
            Installed {
                implementation,
                identity,
                dependencies,
                accepting: true,
                active: 0,
            },
        );
        Ok(())
    }

    /// Natural registration spelling for composition builders.
    pub fn register(&self, implementation: Arc<dyn NativeExtension>) -> Result<()> {
        self.install(implementation)
    }

    /// Disables all new admissions for a logical extension name. Existing
    /// leases remain valid and are released normally.
    pub fn disable(&self, name: &str) -> Result<()> {
        validate_extension_name(name)?;
        let mut state = self.lock()?;
        state.current.remove(name);
        for entry in state
            .versions
            .values_mut()
            .filter(|entry| entry.identity.name == name)
        {
            entry.accepting = false;
        }
        Ok(())
    }

    /// Pins the current accepting implementation for a new invocation.
    pub fn pin(&self, name: &str) -> Result<ExtensionLease> {
        let mut state = self.lock()?;
        let identity = state.current.get(name).cloned().ok_or_else(|| {
            Error::Unsupported(format!("extension {name} is not accepting admissions"))
        })?;
        let entry = state
            .versions
            .get_mut(&identity)
            .ok_or_else(|| Error::Storage("current extension version is missing".into()))?;
        if !entry.accepting {
            return Err(Error::Unsupported(format!(
                "extension {name} is not accepting admissions"
            )));
        }
        entry.active = entry
            .active
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("extension active count exhausted".into()))?;
        Ok(ExtensionLease {
            registry: self.clone(),
            identity,
            implementation: Arc::clone(&entry.implementation),
        })
    }

    /// Pins a specific installed version and digest.  This is the only path
    /// used to retain a version selected by an already admitted operation.
    pub fn pin_exact(&self, identity: &ExtensionIdentity) -> Result<ExtensionLease> {
        self.pin_exact_checked(identity, false)
    }

    fn pin_exact_accepting(&self, identity: &ExtensionIdentity) -> Result<ExtensionLease> {
        self.pin_exact_checked(identity, true)
    }

    fn pin_exact_checked(
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
            implementation: Arc::clone(&entry.implementation),
        })
    }

    /// Prevents removal of a current or actively retained implementation.
    /// Disposal occurs synchronously after the final registry reference is
    /// removed, outside the registry lock.
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

    /// Returns the resident identity for an exact logical version, including
    /// a draining version retained by an older admitted composition.
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

/// A retained native implementation lease. Dropping the lease releases one
/// active admission; disabled/replaced versions remain removable registry
/// entries until an owner explicitly calls `remove`.
pub struct ExtensionLease {
    registry: ExtensionRegistry,
    identity: ExtensionIdentity,
    implementation: Arc<dyn NativeExtension>,
}

impl ExtensionLease {
    /// Returns the exact version and digest retained by this invocation.
    #[must_use]
    pub const fn identity(&self) -> &ExtensionIdentity {
        &self.identity
    }
}

impl Deref for ExtensionLease {
    type Target = dyn NativeExtension;
    fn deref(&self) -> &Self::Target {
        self.implementation.as_ref()
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

/// Retained implementation set held by one admitted task.
pub struct ExtensionLeases(Vec<ExtensionLease>);

impl ExtensionLeases {
    /// Creates an empty retained set for tasks without extension requirements.
    #[must_use]
    pub(crate) const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Returns all exact executable identities retained by the task.
    pub fn identities(&self) -> impl Iterator<Item = &ExtensionIdentity> {
        self.0.iter().map(ExtensionLease::identity)
    }
}

/// Linked extension composition attached to a harness scope.
pub struct ExtensionRuntime {
    registry: ExtensionRegistry,
    selected: Vec<ExtensionIdentity>,
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
        // Exact roots are an explicit activation choice. They may point at a
        // retained prior version during rollback even when a newer version is
        // the registry's current default, but a disabled version cannot be
        // activated for new work.
        let selected = registry.resolve(&roots, true)?;
        let retained = selected
            .iter()
            .map(|identity| registry.pin_exact_accepting(identity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self {
            registry,
            selected,
            retained,
            accepting: AtomicBool::new(true),
            admission_gate: Mutex::new(()),
        }))
    }

    /// Activates the exact implementations selected by an agent admission.
    pub fn from_admission(
        registry: ExtensionRegistry,
        admission: &ExtensionAdmission,
    ) -> Result<Arc<Self>> {
        Self::from_admission_with_digests(registry, admission, |_, _| Ok(None))
    }

    /// Activates a committed agent selection and checks every native digest
    /// against the reducer's exact schema binding.
    pub fn from_reducer(registry: ExtensionRegistry, reducer: &Reducer) -> Result<Arc<Self>> {
        let admission = reducer
            .extension_admission()?
            .ok_or_else(|| Error::Unsupported("agent has no committed extensions".into()))?;
        Self::from_admission_with_digests(registry, &admission, |name, version| {
            reducer
                .extension_implementation_digest(name, version)
                .map(Some)
        })
    }

    /// Activates an admission while independently checking each exact linked
    /// digest against the owner's registered implementation digest.
    pub fn from_admission_with_digests<F>(
        registry: ExtensionRegistry,
        admission: &ExtensionAdmission,
        expected_digest: F,
    ) -> Result<Arc<Self>>
    where
        F: Fn(&str, u32) -> Result<Option<[u8; 32]>>,
    {
        admission.validate()?;
        let mut roots = Vec::with_capacity(admission.selected().len());
        for dependency in admission.selected() {
            let identity = registry
                .exact(&dependency.name, dependency.version)?
                .ok_or_else(|| {
                    Error::Unsupported(format!("extension {} is not installed", dependency.name))
                })?;
            if let Some(expected) = expected_digest(&dependency.name, dependency.version)?
                && expected != identity.digest
            {
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
            .map(|identity| registry.pin_exact(identity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Self {
            registry,
            selected,
            retained,
            accepting: AtomicBool::new(true),
            admission_gate: Mutex::new(()),
        }))
    }

    /// Disables this composition and its logical extensions for new task
    /// admissions. Existing task leases and reconciliation remain valid.
    pub fn disable(&self) -> Result<()> {
        let _gate = self
            .admission_gate
            .lock()
            .map_err(|_| Error::Storage("extension admission gate poisoned".into()))?;
        // Admission state is composition-local. A different agent may be
        // running another exact version (or the same version) in parallel;
        // only an explicit registry disable may affect that agent.
        self.accepting.store(false, Ordering::Release);
        Ok(())
    }

    /// Whether new task admissions can retain this composition.
    #[must_use]
    pub fn accepts_new_admissions(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Returns all dependency-ordered exact implementations linked here.
    #[must_use]
    pub fn selected(&self) -> &[ExtensionIdentity] {
        &self.selected
    }

    /// Links each selected implementation into one native task/tool registry.
    /// This narrow form is retained for callers that already own both registries.
    pub fn link_into(&self, tasks: &mut TaskRegistry, tools: &mut ToolRegistry) -> Result<()> {
        let mut bindings = Bindings {
            tasks: std::mem::take(tasks),
            tools: std::mem::take(tools),
            ..Bindings::local()
        };
        {
            let mut linker = ExtensionLinker {
                bindings: &mut bindings,
            };
            for extension in &self.retained {
                extension.link(&mut linker)?;
            }
        }
        *tasks = std::mem::take(&mut bindings.tasks);
        *tools = std::mem::take(&mut bindings.tools);
        Ok(())
    }

    /// Links each selected implementation into the complete composition. All
    /// existing provider slots remain authoritative; extension bindings may
    /// only fill an absent slot through the explicit linker methods.
    pub fn link_into_bindings(&self, bindings: &mut Bindings) -> Result<()> {
        let mut linker = ExtensionLinker { bindings };
        for extension in &self.retained {
            extension.link(&mut linker)?;
        }
        Ok(())
    }

    /// Retains exact extension versions required by a task admission.
    pub fn retain_for_task(
        &self,
        requirements: impl IntoIterator<Item = String>,
    ) -> Result<ExtensionLeases> {
        self.retain(requirements, false)
    }

    /// Retains versions for an operation already committed by an owner. This
    /// remains available after admission is disabled so reconciliation and
    /// observation cannot be mistaken for a new dispatch.
    pub fn retain_for_existing(
        &self,
        requirements: impl IntoIterator<Item = String>,
    ) -> Result<ExtensionLeases> {
        self.retain(requirements, true)
    }

    fn retain(
        &self,
        requirements: impl IntoIterator<Item = String>,
        existing: bool,
    ) -> Result<ExtensionLeases> {
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
        let selected = self
            .selected
            .iter()
            .map(ExtensionIdentity::dependency)
            .collect::<BTreeSet<_>>();
        let mut leases = Vec::new();
        for dependency in required {
            if !selected.contains(&dependency) {
                return Err(Error::Unsupported(format!(
                    "extension {}@{} is not active",
                    dependency.name, dependency.version
                )));
            }
            let identity = self
                .selected
                .iter()
                .find(|identity| {
                    identity.name == dependency.name && identity.version == dependency.version
                })
                .ok_or_else(|| {
                    Error::Storage("selected extension disappeared during retention".into())
                })?;
            let lease = if existing {
                self.registry.pin_exact(identity)?
            } else {
                self.registry.pin_exact_accepting(identity)?
            };
            leases.push(lease);
        }
        Ok(ExtensionLeases(leases))
    }

    /// Checks that a committed metadata selection agrees with linked code.
    pub fn validate_admission(&self, admission: Option<&ExtensionAdmission>) -> Result<()> {
        if let Some(admission) = admission {
            admission.validate()?;
            let selected = admission
                .selected()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let linked = self
                .selected
                .iter()
                .map(ExtensionIdentity::dependency)
                .collect::<BTreeSet<_>>();
            if selected != linked {
                return Err(Error::Conflict(
                    "extension admission differs from linked composition".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Builder-friendly owned link bundle. It keeps executable implementations
/// alive while the resulting harness and its admitted tasks are alive.
pub struct NativeExtensionBundle(Arc<ExtensionRuntime>);

impl NativeExtensionBundle {
    /// Activates exact roots from a registry and retains their linked code.
    pub fn new(
        registry: ExtensionRegistry,
        roots: impl IntoIterator<Item = ExtensionIdentity>,
    ) -> Result<Self> {
        Self::activate(registry, roots)
    }

    /// Activates exact extension roots from a registry.
    pub fn activate(
        registry: ExtensionRegistry,
        roots: impl IntoIterator<Item = ExtensionIdentity>,
    ) -> Result<Self> {
        Ok(Self(ExtensionRuntime::activate(registry, roots)?))
    }

    /// Returns the runtime view used for task admission gating.
    #[must_use]
    pub fn runtime(&self) -> Arc<ExtensionRuntime> {
        Arc::clone(&self.0)
    }

    /// Consumes the bundle and returns its linked runtime.
    pub(crate) fn into_runtime(self) -> Arc<ExtensionRuntime> {
        self.0
    }

    /// Links the bundle into the complete mutable composition.
    pub fn link_into(&self, bindings: &mut Bindings) -> Result<()> {
        self.0.link_into_bindings(bindings)
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
            runtime.retain_for_task(["extension:example.extension@1".into()]),
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
        let retained = runtime.retain_for_existing(["extension:example.extension@1".into()])?;
        assert_eq!(retained.identities().count(), 1);
        Ok(())
    }
}
