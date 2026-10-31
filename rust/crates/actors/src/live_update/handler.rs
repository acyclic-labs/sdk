use super::{CallEnvelope, DurableAwait, Error, Pin, Publication, Selection, SelectionFence};
use crate::guest::{
    CancellationToken, Checkpoint, Delivery, GuestError, GuestFuture, Handler, Manifest, Transition,
};
use std::sync::Arc;

/// Immutable artifact context, validated and shared once per host/version. It
/// owns no Actor task, timer or subscription and conveys no execution grant.
pub struct VersionedHandler {
    handler: Arc<dyn Handler>,
    deployment: super::DeploymentId,
    version: super::Version,
    schema: [u8; 32],
    handoff: super::HandoffRequirements,
}

impl VersionedHandler {
    /// Verify the artifact and declared codec against the canonical upload.
    /// The host separately authenticates and admits that immutable publication.
    pub fn new(
        handler: Arc<dyn Handler>,
        publication: &Publication,
        namespace: &str,
    ) -> Result<Self, Error> {
        let version = publication.version()?;
        let artifact = publication
            .artifacts
            .get(namespace)
            .ok_or(Error::Invalid("namespace"))?;
        if artifact.code != *handler.manifest().code.as_bytes()
            || handler.manifest().codec != publication.compatibility.handoff.writes
        {
            return Err(Error::Invalid("handler differs from publication"));
        }
        Ok(Self {
            handler,
            deployment: publication.deployment.clone(),
            version,
            schema: artifact.schema,
            handoff: publication.compatibility.handoff.clone(),
        })
    }
}

/// One original Handler behind an admitted selection fence. Local and hosted
/// runtimes use this same adapter and the same Actor transaction. The host must
/// suspend old continuations/effect admission before publishing a new selection;
/// this fence prevents a late old transition from committing after that cut.
pub struct FencedHandler {
    package: Arc<VersionedHandler>,
    fence: SelectionFence,
    envelope: CallEnvelope,
}

impl FencedHandler {
    /// Bind the original authenticated publication, pin and selection receipt.
    /// This validates identity, not authority: Cloud supplies the original
    /// admitted cut and refuses stale/untrusted host metadata before guest entry.
    pub fn new(
        package: Arc<VersionedHandler>,
        selection: &Selection,
        pin: Pin,
        fence: SelectionFence,
    ) -> Result<Self, Error> {
        let version = selection.version(&pin)?;
        if package.deployment != selection.deployment
            || package.version != *version
            || fence.tail == 0
        {
            return Err(Error::Invalid("handler publication differs from selection"));
        }
        Ok(Self {
            fence,
            envelope: CallEnvelope {
                deployment: selection.deployment.clone(),
                pin,
                epoch: selection.epoch,
                version: version.clone(),
                contract: package.schema,
            },
            package,
        })
    }

    /// Host-bound outgoing provenance. Forwarders retain this exact identity,
    /// rather than replacing it with the receiver's selection epoch.
    pub const fn envelope(&self) -> &CallEnvelope {
        &self.envelope
    }
}

impl Handler for FencedHandler {
    fn manifest(&self) -> &Manifest {
        self.package.handler.manifest()
    }

    fn publication_conditions(&self) -> Vec<acyclic_stream::CommitCondition> {
        let mut conditions = self.package.handler.publication_conditions();
        conditions.push(self.fence.condition());
        conditions
    }

    fn handoff(&self, checkpoint: &Checkpoint) -> Result<Option<DurableAwait>, GuestError> {
        if !self.package.handoff.durable_await {
            return Ok(None);
        }
        let point = self.package.handler.handoff(checkpoint)?;
        if point.as_ref().is_some_and(|point| {
            point.continuation != *checkpoint
                || !self
                    .package
                    .handoff
                    .reads
                    .contains(&point.continuation.codec)
                || point
                    .pending
                    .iter()
                    .any(|operation| operation.operation.is_empty())
        }) {
            return Err(GuestError::Decode(
                "handoff differs from the committed continuation".into(),
            ));
        }
        Ok(point)
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.package
            .handler
            .on_record(delivery, checkpoint, cancellation)
    }
}
