//! Version-pinned runtime registrations with drain-before-replacement lifecycle.

pub(crate) use crate::contract::validate_component_label;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

/// Immutable component implementation identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ComponentIdentity {
    /// Stable local or namespaced logical name.
    pub name: String,
    /// Semantic implementation version.
    pub version: String,
    /// Implementation/schema digest.
    pub digest: [u8; 32],
}

struct Entry<T> {
    value: Arc<T>,
    accepting: bool,
    active: usize,
}

struct RegistryState<T> {
    current: BTreeMap<String, ComponentIdentity>,
    versions: BTreeMap<ComponentIdentity, Entry<T>>,
    version_digests: BTreeMap<(String, String), [u8; 32]>,
}

/// Explicit registry that pins active work and drains replaced versions.
pub struct PinnedRegistry<T>(Arc<Mutex<RegistryState<T>>>);

impl<T> Default for PinnedRegistry<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(RegistryState {
            current: BTreeMap::new(),
            versions: BTreeMap::new(),
            version_digests: BTreeMap::new(),
        })))
    }
}

impl<T> Clone for PinnedRegistry<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> PinnedRegistry<T> {
    /// Installs a version for new work and begins draining the previous version.
    pub fn install(&self, identity: ComponentIdentity, value: T) -> Result<()> {
        validate_component_label(&identity.name, "component name")?;
        validate_component_label(&identity.version, "component version")?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))?;
        if state.versions.contains_key(&identity) {
            return Err(Error::Conflict(
                "component version is already installed".into(),
            ));
        }
        let version_key = (identity.name.clone(), identity.version.clone());
        if state
            .version_digests
            .get(&version_key)
            .is_some_and(|digest| digest != &identity.digest)
        {
            return Err(Error::Conflict(
                "component version is pinned to another digest".into(),
            ));
        }
        state.version_digests.insert(version_key, identity.digest);
        if let Some(previous) = state
            .current
            .insert(identity.name.clone(), identity.clone())
            && let Some(entry) = state.versions.get_mut(&previous)
        {
            entry.accepting = false;
        }
        state.versions.insert(
            identity,
            Entry {
                value: Arc::new(value),
                accepting: true,
                active: 0,
            },
        );
        reap(&mut state);
        Ok(())
    }

    /// Alias for callers treating registrations as native executable links.
    pub fn register(&self, identity: ComponentIdentity, value: T) -> Result<()> {
        self.install(identity, value)
    }

    /// Pins the current version for the lifetime of one active invocation.
    pub fn pin(&self, name: &str) -> Result<ComponentLease<T>> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))?;
        let identity = state
            .current
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("component {name}")))?;
        let entry = state
            .versions
            .get_mut(&identity)
            .ok_or_else(|| Error::Storage("current component version is missing".into()))?;
        entry.active = entry
            .active
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("component active count exhausted".into()))?;
        Ok(ComponentLease {
            registry: self.clone(),
            identity,
            value: Arc::clone(&entry.value),
        })
    }

    /// Pins an exact retained version rather than resolving the current one.
    pub fn pin_exact(&self, identity: &ComponentIdentity) -> Result<ComponentLease<T>> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))?;
        let entry = state.versions.get_mut(identity).ok_or_else(|| {
            Error::NotFound(format!("component {}@{}", identity.name, identity.version))
        })?;
        entry.active = entry
            .active
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("component active count exhausted".into()))?;
        Ok(ComponentLease {
            registry: self.clone(),
            identity: identity.clone(),
            value: Arc::clone(&entry.value),
        })
    }

    /// Reports whether a logical component currently accepts new work.
    pub fn accepting(&self, name: &str) -> Result<bool> {
        self.0
            .lock()
            .map(|state| state.current.contains_key(name))
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))
    }

    /// Stops accepting new work for one logical component while preserving
    /// every active exact-version lease until it is released.
    pub fn disable(&self, name: &str) -> Result<()> {
        validate_component_label(name, "component name")?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))?;
        state.current.remove(name);
        for (identity, entry) in &mut state.versions {
            if identity.name == name {
                entry.accepting = false;
            }
        }
        reap(&mut state);
        Ok(())
    }

    /// Removes a disabled version only after all retained invocations drain.
    pub fn remove(&self, identity: &ComponentIdentity) -> Result<Arc<T>> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))?;
        if state.current.get(&identity.name) == Some(identity) {
            return Err(Error::Conflict(
                "current component must be disabled before removal".into(),
            ));
        }
        let entry = state.versions.get(identity).ok_or_else(|| {
            Error::NotFound(format!("component {}@{}", identity.name, identity.version))
        })?;
        if entry.active != 0 {
            return Err(Error::Conflict(
                "component still has retained invocations".into(),
            ));
        }
        let entry = state
            .versions
            .remove(identity)
            .ok_or_else(|| Error::Storage("component disappeared during removal".into()))?;
        Ok(entry.value)
    }

    /// Returns whether a specific version is still installed or draining.
    pub fn contains(&self, identity: &ComponentIdentity) -> Result<bool> {
        self.0
            .lock()
            .map(|state| state.versions.contains_key(identity))
            .map_err(|_| Error::Storage("component registry lock poisoned".into()))
    }
}

/// Active version pin. Dropping the final old lease completes draining.
pub struct ComponentLease<T> {
    registry: PinnedRegistry<T>,
    identity: ComponentIdentity,
    value: Arc<T>,
}

impl<T> ComponentLease<T> {
    /// Returns the exact pinned implementation identity.
    #[must_use]
    pub const fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }
}

impl<T> Deref for ComponentLease<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> Drop for ComponentLease<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.registry.0.lock() {
            if let Some(entry) = state.versions.get_mut(&self.identity) {
                entry.active = entry.active.saturating_sub(1);
            }
            reap(&mut state);
        }
    }
}

fn reap<T>(state: &mut RegistryState<T>) {
    state
        .versions
        .retain(|_, entry| entry.accepting || entry.active != 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrades_pin_active_work_until_it_drains() -> Result<()> {
        let registry = PinnedRegistry::default();
        let first = ComponentIdentity {
            name: "example.model".into(),
            version: "1".into(),
            digest: [1; 32],
        };
        let second = ComponentIdentity {
            name: "example.model".into(),
            version: "2".into(),
            digest: [2; 32],
        };
        registry.install(first.clone(), "old")?;
        let pinned = registry.pin("example.model")?;
        registry.install(second.clone(), "new")?;
        assert_eq!(*pinned, "old");
        assert!(registry.contains(&first)?);
        assert_eq!(*registry.pin("example.model")?, "new");
        drop(pinned);
        assert!(!registry.contains(&first)?);
        assert!(registry.contains(&second)?);
        assert!(matches!(
            registry.install(
                ComponentIdentity {
                    name: "example.model".into(),
                    version: "1".into(),
                    digest: [3; 32],
                },
                "different"
            ),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
}
