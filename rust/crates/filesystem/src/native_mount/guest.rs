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
/// A hosted caller must not implement this local-root authority from request
/// fields: transports cannot fence private guest mappings on revocation.
pub trait LocalGuestSource: Send + Sync + 'static {
    /// Exact authenticated canonical filesystem source.
    fn filesystem(&self) -> &Arc<dyn MountFilesystem>;
    /// Source-authenticated POSIX enrollment.
    fn posix(&self) -> &GuestPosixPolicy;
    /// Whether the original source admission permits authored effects.
    fn writable(&self) -> bool;
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
