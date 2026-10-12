use super::*;
use crate::{
    AgentId,
    conversation::{VolumeClass, VolumeOperation},
};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod access;
#[cfg(feature = "filesystem")]
/// Borrowed original ROOT authentication, sealed by the actual owner.
#[derive(Clone, Copy)]
pub struct RootBootstrapContentContext<'a> {
    pub(super) original_root: &'a TaskAdmissionRecord,
    pub(super) owner: &'a Authority,
    binding: &'a crate::store::VerifiedRootBinding,
    pub(super) admission_digest: [u8; 32],
}
#[cfg(feature = "filesystem")]
impl<'a> RootBootstrapContentContext<'a> {
    /// Original admitted ROOT.
    pub const fn original_root(&self) -> &'a TaskAdmissionRecord {
        self.original_root
    }
    /// Trusted coordinator owner.
    pub const fn owner(&self) -> &'a Authority {
        self.owner
    }
    /// Canonical RootBind audience.
    pub fn audience(&self) -> &'a Authority {
        self.binding.audience()
    }
    /// Canonical RootBind agent.
    pub fn agent(&self) -> AgentId {
        self.binding.agent()
    }
    /// Complete original admission digest.
    pub const fn original_admission_digest(&self) -> [u8; 32] {
        self.admission_digest
    }
    pub(crate) fn authenticated(
        original_root: &'a TaskAdmissionRecord,
        owner: &'a Authority,
        binding: &'a crate::store::VerifiedRootBinding,
        admission_digest: [u8; 32],
    ) -> Self {
        Self {
            original_root,
            owner,
            binding,
            admission_digest,
        }
    }
}
