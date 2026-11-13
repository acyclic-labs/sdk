//! One actor per event owner, using the shared delivery and publication engine.
//!
//! The platform admits the actor and injects its request ledger, Store, Handler and clock. It
//! routes committed input changes and the returned alarm to [`Host::wake`],
//! retaining its event until that call finishes. No background task is started.

use crate::{
    addressing::ActorReference,
    guest::Handler,
    local::{
        Clock, Error, FailedDelivery, FailureDisposition, PreparedPublish, Runtime, RuntimeLimits,
        Store,
    },
};
use acyclic_stream::{IdempotencyKey, StreamPath, StreamProvider};
use std::sync::Arc;

/// Production event host for any native or strict-wire guest Handler.
/// Opening/reopening does no IO and executes no guest work. Actor creation
/// remains a companion mutation on the injected canonical Store.
pub struct Host<P: StreamProvider + ?Sized> {
    actor: ActorReference,
    runtime: Runtime<P>,
    clock: Arc<dyn Clock>,
    events: tokio::sync::Mutex<()>,
}

impl<P: StreamProvider + ?Sized> Host<P> {
    /// Bind exactly one actor, without selecting a backend or an application.
    /// Bind the guest capability bridge to a clone of `requests` before
    /// constructing this host.
    pub fn new(
        requests: crate::guest::Requests,
        store: Store<P>,
        actor: ActorReference,
        handler: Arc<dyn Handler>,
        clock: Arc<dyn Clock>,
        limits: RuntimeLimits,
    ) -> Self {
        Self {
            runtime: Runtime::new(requests, store, handler, Arc::clone(&clock)).with_limits(limits),
            actor,
            clock,
            events: tokio::sync::Mutex::new(()),
        }
    }

    /// Serve under an external placement owner: this host never issues its
    /// own placement command, and admits only through [`Self::place`].
    #[must_use]
    pub fn with_external_placement(mut self) -> Self {
        self.runtime = self.runtime.externally_placed();
        self
    }

    /// The exact actor admitted by the platform that opened this host.
    pub const fn actor(&self) -> &ActorReference {
        &self.actor
    }

    /// The original injected store, including its root and provider.
    pub const fn store(&self) -> &Store<P> {
        self.runtime.store()
    }

    /// The original host clock, also used for alarm delivery.
    pub fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    /// Admit this event host through the actor's explicit placement command.
    /// Wakes never claim ownership on their own. The placement owner supplies
    /// the actor-local command index and retains it through retries.
    pub async fn place(&self, index: u64) -> Result<(), Error> {
        self.runtime.place(self.actor.clone(), index).await
    }

    /// Observe the committed placement index and whether this host owns it.
    /// An unpublished command leaves the index unchanged. Retain the exact
    /// placement command through an uncertain publication result.
    pub async fn placement_state(&self) -> Result<(u64, bool), Error> {
        self.runtime.placement_state(&self.actor).await
    }
    /// Commit a pause/resume fence without waiting for an active event.
    /// This neither delivers nor consumes input. Resume needs a platform wake.
    pub async fn pause(&self, paused: bool) -> Result<(), Error> {
        self.runtime.pause(&self.actor, paused).await
    }

    /// Prepare an ordinary Stream publication for this admitted Actor's exact
    /// selected failure. No read, guest invocation or submission occurs here.
    /// The caller retains the resulting request through uncertain submission.
    pub fn prepare_disposition(
        &self,
        failed: &FailedDelivery,
        disposition: FailureDisposition<'_>,
        key: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
        self.store()
            .prepare_disposition(&self.actor, failed, disposition, key)
    }

    /// State and current subscription paths the platform must route here.
    /// Register these before the wake's final committed-input recheck. Changes
    /// to subscriptions require updating routing from this original state.
    pub async fn wake_paths(&self) -> Result<Vec<StreamPath>, Error> {
        let snapshot = self.store().load(&self.actor).await?;
        let state = snapshot
            .state()
            .ok_or(Error::Invalid("actor state was never created"))?;
        let mut paths = vec![snapshot.path().clone()];
        paths.extend(
            state
                .subscriptions()
                .values()
                .map(|value| value.path.clone()),
        );
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    /// Drain committed input and due alarms to quiescence, retaining exact
    /// Unknown publication custody on failure. Controls can run concurrently;
    /// delivery events serialize. The platform persists/arms the returned
    /// deadline before completing its event, or clears its alarm for None.
    pub async fn wake(&self) -> Result<Option<u64>, Error> {
        let _event = self.events.lock().await;
        self.runtime.drain_event(&self.actor).await
    }
}
