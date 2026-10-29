//! One actor per event owner, using the shared delivery and publication engine.
//!
//! The platform admits the actor and injects its Store, Handler and clock. It
//! routes committed input changes and the returned alarm to [`Host::wake`],
//! retaining its event until that call finishes. Idle actors own no background
//! task. Native deliveries run on their fixed shard.

use crate::{
    addressing::ActorReference,
    guest::Handler,
    local::{Clock, Error, Runtime, RuntimeLimits, Store},
};
use acyclic_stream::{StreamPath, StreamProvider};
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
    pub fn new(
        store: Store<P>,
        actor: ActorReference,
        handler: Arc<dyn Handler>,
        clock: Arc<dyn Clock>,
        limits: RuntimeLimits,
    ) -> Self {
        Self {
            runtime: Runtime::configured(store, handler, Arc::clone(&clock), limits),
            actor,
            clock,
            events: tokio::sync::Mutex::new(()),
        }
    }

    /// The exact actor admitted by the platform that opened this host.
    pub const fn actor(&self) -> &ActorReference {
        &self.actor
    }

    /// The original injected store, including its root and provider.
    pub fn store(&self) -> &Store<P> {
        self.runtime.store()
    }

    /// The original host clock, also used for alarm delivery.
    pub fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    /// Commit a pause/resume fence without waiting for an active event.
    /// This neither delivers nor consumes input. Resume needs a platform wake.
    pub async fn pause(&self, paused: bool) -> Result<(), Error> {
        self.runtime.pause(&self.actor, paused).await
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
