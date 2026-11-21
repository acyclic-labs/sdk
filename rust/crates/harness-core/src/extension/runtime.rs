use super::*;
/// Dependency-ordered implementations retained by one composition.
pub struct ExtensionRuntime {
    registry: ExtensionRegistry,
    retained: Vec<ExtensionLease>,
    accepting: Mutex<bool>,
}
impl ExtensionRuntime {
    /// Activates exact roots and their dependency closure.
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
            accepting: Mutex::new(true),
        }))
    }

    /// Disables new admissions for this composition.
    pub fn disable(&self) -> Result<()> {
        *self
            .accepting
            .lock()
            .map_err(|_| Error::Storage("extension admission gate poisoned".into()))? = false;
        Ok(())
    }
    /// Iterates dependency-ordered retained identities.
    pub fn selected(&self) -> impl ExactSizeIterator<Item = &ExtensionIdentity> {
        self.retained.iter().map(ExtensionLease::identity)
    }
    /// Retains required identities, allowing disabled versions for committed operations.
    pub fn retain(
        &self,
        requirements: impl IntoIterator<Item = String>,
        existing: bool,
    ) -> Result<Vec<ExtensionLease>> {
        let _gate = self
            .accepting
            .lock()
            .map_err(|_| Error::Storage("extension admission gate poisoned".into()))?;
        if !existing && !*_gate {
            return Err(Error::Conflict("extension admissions are disabled".into()));
        }
        let required = requirements
            .into_iter()
            .filter_map(|requirement| parse_requirement(&requirement).transpose())
            .collect::<Result<Vec<_>>>()?;
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
