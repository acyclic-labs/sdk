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
    fn accepts_interrupt<'a>(
        &'a self,
        delivery: &'a Delivery,
        checkpoint: Option<&'a Checkpoint>,
        subscription: &'a str,
        record: &'a acyclic_stream::Record,
    ) -> GuestFuture<'a, bool> {
        self.package
            .handler
            .accepts_interrupt(delivery, checkpoint, subscription, record)
    }
    fn manifest(&self) -> &Manifest {
        self.package.handler.manifest()
    }

    fn publication_conditions(
        &self,
        actor: &crate::addressing::ActorReference,
    ) -> Vec<acyclic_stream::CommitCondition> {
        let mut conditions = self.package.handler.publication_conditions(actor);
        conditions.push(self.fence.condition());
        conditions
    }

    fn admission_class(
        &self,
        actor: &crate::addressing::ActorReference,
    ) -> crate::guest::AdmissionClass {
        self.package.handler.admission_class(actor)
    }

    fn external_subscriptions(&self) -> &[String] {
        self.package.handler.external_subscriptions()
    }
    fn failure_owner(
        &self,
        actor: &crate::addressing::ActorReference,
        checkpoint: Option<&Checkpoint>,
    ) -> Option<crate::addressing::ActorReference> {
        self.package.handler.failure_owner(actor, checkpoint)
    }

    fn retain_paused(
        &self,
        actor: &crate::addressing::ActorReference,
        checkpoint: Option<&Checkpoint>,
    ) -> bool {
        self.package.handler.retain_paused(actor, checkpoint)
    }

    fn ordered_subscriptions(&self) -> &[String] {
        self.package.handler.ordered_subscriptions()
    }

    fn subscription_key(
        &self,
        id: &str,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Option<bytes::Bytes>, GuestError> {
        self.package.handler.subscription_key(id, checkpoint)
    }

    fn revalidate_foreign_conflicts(&self, actor: &crate::addressing::ActorReference) -> bool {
        self.package.handler.revalidate_foreign_conflicts(actor)
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

    fn on_failure<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        failure: String,
    ) -> GuestFuture<'a, Option<Transition>> {
        self.package
            .handler
            .on_failure(delivery, checkpoint, failure)
    }
}
