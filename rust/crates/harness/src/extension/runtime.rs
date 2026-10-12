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

    /// Restores exact versions with the reducer's committed digest bindings.
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
            if reducer.schemas().implementation_digest(
                &dependency.name,
                dependency.version,
                &mut None,
            )? != identity.digest
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
            .map(|identity| registry.pin_exact(identity, false))
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
    /// Whether this composition accepts new admissions.
    pub fn accepts_new_admissions(&self) -> bool {
        *self
            .accepting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    /// Checks that committed metadata selects the linked code.
    pub(crate) fn validate_admission(
        &self,
        admission: Option<&ExtensionAdmission>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if let Some(admission) = admission {
            admission.validate(work)?;
            if admission.selected().len() != self.retained.len() {
                return Err(crate::runtime::scope_metadata_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "extension admission differs from linked composition",
                ));
            }
            for selected in admission.selected() {
                let mut found = false;
                for linked in self.selected() {
                    if let Some(io) = work.as_deref_mut() {
                        io.codec_charge(crate::contract::JsonCodecCharge::Work {
                            bytes: 0,
                            items: 1,
                        })?;
                    }
                    if selected.name == linked.name && selected.version == linked.version {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Err(crate::runtime::scope_metadata_boundary(
                        work,
                        crate::wire::ErrorCode::Conflict,
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
