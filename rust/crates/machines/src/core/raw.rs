//! Raw provider primitives. Replay, ordering and resource ownership live in core.
use crate::ProviderError;
use async_trait::async_trait;

/// Native machine adapter. The remaining lifecycle primitives are being migrated
/// into this same boundary; implementations must never add their own operation registry.
#[async_trait]
pub trait RawMachines: Send + Sync {
    /// Delete one opaque resource once. A missing resource may return NotFound;
    /// a lost native reply remains Unavailable. Retry decisions belong to core.
    async fn destroy(&self, resource: &str) -> Result<(), ProviderError>;
}
