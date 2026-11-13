//! Sender-local routing context; credentials remain in the owning host.
use super::{AgentPath, AgentRecord, Error, PeerAdmission};
use acyclic_actors::{addressing::ActorReference, route};

pub(super) const OUTBOX: &str = "acyclic.actors.route.outbox.v1";
pub(super) const ADMISSION: &str = "acyclic.harness.peer-admission.v1";

impl AgentRecord for PeerAdmission {}

/// Routing subtree and source identity admitted by this node's host.
#[derive(Clone, Debug)]
pub struct Routing {
    /// Account routing subtree on the sender's local provider.
    pub paths: route::Paths,
    /// This node's identity, bound by the authenticated account connection.
    pub node: route::NodeId,
}

pub(super) fn error(error: route::Error) -> Error {
    match error {
        route::Error::Stream(error) => Error::Stream(error),
        route::Error::Actor(error) => Error::Actor(Box::new(error)),
        _ => Error::Record("invalid route record"),
    }
}

impl Routing {
    /// Lower one sender publication, staging only targets known to be remote.
    /// The host determines locality through its actor store, never directory
    /// presence or a caller-supplied network location. All other effects retain
    /// the ordinary lowering path.
    pub fn lower(
        &self,
        within: &ActorReference,
        remote: &std::collections::BTreeSet<AgentPath>,
        step: &super::Step,
        cursor: u64,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<acyclic_stream::CommitRequest, Error> {
        super::lower::lower_routed(step, cursor, key, Some((self, within, remote)))
    }
}
