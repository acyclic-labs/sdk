//! The host-facing half of the native work ledger: root admission after
//! process-global services load, and root observation through `native_host`.
use super::NativeTreeMaterializationError;
use crate::ledger::{DemandWork, NativeMaterializationWork};
use crate::{WorkBudget, WorkCounters};
use std::path::Path;

impl NativeMaterializationWork {
    /// Opens a no-follow directory capability on this original native ledger.
    pub fn open_root_directory(
        &self,
        path: &Path,
    ) -> Result<(cap_std::fs::Dir, crate::NativeRootIdentity), NativeTreeMaterializationError> {
        crate::native_host::open_root_directory(path, &mut &*self)
    }

    /// Prepares process-global services, then admits the original ledger before
    /// sharing it. Global framework bootstrap is not invocation payload.
    pub fn new(
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, (NativeTreeMaterializationError, Box<WorkCounters>)> {
        Self::from_demand(DemandWork::new(budget, cancellation))
    }

    /// Retains the same admitted demand ledger when a native producer outlives
    /// preparation. Preparation work and cancellation are never reset.
    pub(crate) fn from_demand(
        state: DemandWork,
    ) -> Result<Self, (NativeTreeMaterializationError, Box<WorkCounters>)> {
        let state = Self::checked(state)?;
        // Every constructor crosses this boundary before publishing original
        // custody. Loading a process framework from an already original leaf is
        // deliberately forbidden, so a root ledger requires the loaded proof.
        let services = match crate::watch::native_services() {
            Ok(services) => services,
            Err(error) => {
                return Err((
                    NativeTreeMaterializationError::Io(std::io::Error::other(error)),
                    Box::new(state.work),
                ));
            }
        };
        Self::admitted_root(state, &services)
    }

    /// Observes the original root through `native_host`.
    pub fn root_identity(
        &self,
        path: &Path,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open(path, &mut &*self)?.identity())
    }
}
