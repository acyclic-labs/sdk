//! Durable publication after one complete model/tool batch.
use crate::{
    OperationId, Result, conversation::FileRef, core::EffectGuarantee, registry::ComponentIdentity,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

/// Exact admission for child activation after all ordered tool results exist.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBatchPublication {
    /// Stable effect identity derived from the parent turn and step.
    pub operation_id: OperationId,
    /// Owning turn identity.
    pub parent_operation: OperationId,
    /// Zero-based model step.
    pub step: u32,
    /// Pinned original request before this batch's calls and results.
    pub request: FileRef,
    /// Pinned completed request and immutable inherited prefix.
    pub boundary: FileRef,
    /// Pinned implementation and contract.
    pub publisher: ComponentIdentity,
    /// Guarantee authorized before dispatch.
    pub guarantee: EffectGuarantee,
}

/// Harness-owned bridge to typed fork publication and task admission.
/// Implementations retain stable operation identities for every child and
/// use existing provider-owned preparation/publication/reconciliation APIs.
pub trait ModelBatchPublisher: Send + Sync {
    /// Immutable implementation identity.
    fn identity(&self) -> ComponentIdentity;
    /// Retry behavior backed by the actual provider.
    fn guarantee(&self) -> EffectGuarantee;
    /// Whether exactly-once reconciliation is linearizable.
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    /// Publishes an already admitted completed batch.
    fn publish<'a>(&'a self, request: ModelBatchPublication) -> BoxFuture<'a, Result<()>>;
    /// Observes the exact original admission without repeating an unsafe effect.
    fn reconcile<'a>(&'a self, request: ModelBatchPublication)
    -> BoxFuture<'a, Result<Option<()>>>;
}
