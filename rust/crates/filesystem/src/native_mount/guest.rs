//! Source-owner enrollment for a guest, independent of the socket or VMM.

use super::{MountFilesystem, NativeMountError};
use std::sync::Arc;

/// POSIX identities explicitly enrolled by a trusted local source owner.
/// Entries are identity mappings into the volume's virtual POSIX metadata;
/// they never select host users or grant host filesystem access.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestPosixPolicy {
    /// Permitted `(uid, gid)` pairs. An empty map admits no guest requests.
    pub identities: Vec<(u32, u32)>,
    /// Permit chmod, chown, timestamp changes and named-attribute mutations.
    pub metadata: bool,
}

/// Trusted source-side enrollment, implemented by the authenticated host
/// composition (for example Harness `hosting::Bound`), never by a guest.
/// Implementors retain the funded source, its revocable callback guard, exact
/// volume/view and enrolled identity policy together for the entire session.
pub trait GuestSource: Send + Sync + 'static {
    /// Exact authenticated canonical filesystem source.
    fn filesystem(&self) -> &Arc<dyn MountFilesystem>;
    /// Source-authenticated POSIX enrollment.
    fn posix(&self) -> &GuestPosixPolicy;
    /// Whether the original source admission permits authored effects.
    fn writable(&self) -> bool;
}

/// Trusted physical owner of one original guest incarnation and its private
/// memory. This is an in-process capability, never a request or operation ID.
/// Implementors retain the original process handle, not a lookup by name.
/// Dropping the owner must fence and join that same incarnation; an unproved
/// join must fail closed and must never release live private mappings.
pub trait GuestMappingFence: Send + Sync + 'static {
    /// Connects the original cancellation to irreversible physical fencing,
    /// including while no filesystem request is running. Physical owner/input
    /// loss must cancel this same work. Registration must precede volume IO,
    /// retain its own funded event custody, and refuse an already lost owner.
    fn attach(&mut self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError>;

    /// Joins the original physical workers and releases their private memory.
    /// Success is positive evidence for that held incarnation only. Failure
    /// retains the original owner for its fail-closed Drop; it is not retirement.
    fn retire(&mut self) -> Result<(), NativeMountError>;
}

/// A physical fence connected to the original native invocation before IO.
pub struct GuestMappingAdmission {
    pub(super) fence: Box<dyn GuestMappingFence>,
    work: crate::NativeMaterializationWork,
}

impl GuestMappingAdmission {
    /// Consumes the original physical custodian and registers its loss events.
    pub fn new(
        mut fence: Box<dyn GuestMappingFence>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        fence.attach(work)?;
        work.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(Self {
            fence,
            work: work.clone(),
        })
    }

    pub(super) fn require_work(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if !self.work.shares_invocation_context(work) {
            return Err(super::MountSourceError::AccessDenied.into());
        }
        work.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(())
    }
}

impl GuestPosixPolicy {
    pub(super) fn require_identity(&self, owner: (u32, u32)) -> Result<(), NativeMountError> {
        if self.identities.contains(&owner) {
            Ok(())
        } else {
            Err(super::MountSourceError::AccessDenied.into())
        }
    }
}
