//! A guest changes delivery execution, never its host's admission metadata.
use super::*;
use crate::addressing::ActorReference;
use acyclic_stream::CommitCondition;
use std::sync::Arc;

/// Keep host-owned actors outside a guest boundary while ordinary deliveries
/// cross it. Both use the same runtime and publication contract. The host is
/// the authority for subscription selection, scheduling and admission fences;
/// the guest receives only deliveries the host explicitly routes to it.
pub struct HostedHandler<F> {
    host: Arc<dyn Handler>,
    guest: Arc<dyn Handler>,
    host_owned: F,
}

impl<F> HostedHandler<F>
where
    F: Fn(&ActorReference) -> bool + acyclic_stream::ProviderPlatform,
{
    /// `host_owned` is host configuration, never a guest-supplied designation.
    pub fn new(host: Arc<dyn Handler>, guest: Arc<dyn Handler>, host_owned: F) -> Self {
        Self {
            host,
            guest,
            host_owned,
        }
    }

    fn executor(&self, actor: &ActorReference) -> &dyn Handler {
        if (self.host_owned)(actor) {
            &*self.host
        } else {
            &*self.guest
        }
    }
}

impl<F> Handler for HostedHandler<F>
where
    F: Fn(&ActorReference) -> bool + acyclic_stream::ProviderPlatform,
{
    fn manifest(&self) -> &Manifest {
        self.host.manifest()
    }

    fn accepts_interrupt<'a>(
        &'a self,
        delivery: &'a Delivery,
        checkpoint: Option<&'a Checkpoint>,
        subscription: &'a str,
        record: &'a acyclic_stream::Record,
    ) -> GuestFuture<'a, bool> {
        self.host
            .accepts_interrupt(delivery, checkpoint, subscription, record)
    }

    fn admission_class(&self, actor: &ActorReference) -> AdmissionClass {
        self.host.admission_class(actor)
    }

    fn retain_paused(&self, actor: &ActorReference, checkpoint: Option<&Checkpoint>) -> bool {
        self.host.retain_paused(actor, checkpoint)
    }

    fn external_subscriptions(&self) -> &[String] {
        self.host.external_subscriptions()
    }

    fn ordered_subscriptions(&self) -> &[String] {
        self.host.ordered_subscriptions()
    }

    fn subscription_key(
        &self,
        id: &str,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Option<Bytes>, GuestError> {
        self.host.subscription_key(id, checkpoint)
    }

    fn publication_conditions(&self, actor: &ActorReference) -> Vec<CommitCondition> {
        let mut conditions = self.host.publication_conditions(actor);
        if !(self.host_owned)(actor) {
            conditions.extend(self.guest.publication_conditions(actor));
        }
        conditions
    }

    fn revalidate_foreign_conflicts(&self, actor: &ActorReference) -> bool {
        self.host.revalidate_foreign_conflicts(actor)
    }

    fn handoff(
        &self,
        checkpoint: &Checkpoint,
    ) -> Result<Option<crate::live_update::DurableAwait>, GuestError> {
        self.host.handoff(checkpoint)
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.executor(&delivery.actor)
            .on_record(delivery, checkpoint, cancellation)
    }

    fn on_failure<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        failure: String,
    ) -> GuestFuture<'a, Option<Transition>> {
        self.executor(&delivery.actor)
            .on_failure(delivery, checkpoint, failure)
    }
}
