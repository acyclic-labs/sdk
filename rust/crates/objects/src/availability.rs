//! Coalesced physical-availability invalidations, scoped by the owning account.
use crate::{Digest, Error, wire::ErrorCode};
use acyclic_stream::{BoxProviderStream, ProviderTask};
use futures::{Stream, StreamExt, stream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;

fn unavailable() -> Error {
    ErrorCode::Unavailable.into()
}

/// One selected key's observation cut. This is neither an Objects revision nor
/// evidence that bytes exist, have been verified, or have become durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AvailabilityCursor {
    incarnation: [u8; 16],
    sequence: u64,
}

impl AvailabilityCursor {
    /// Decodes a source-issued cursor. An all-zero incarnation is invalid.
    pub fn new(incarnation: [u8; 16], sequence: u64) -> Result<Self, Error> {
        if incarnation == [0; 16] {
            return Err(ErrorCode::InvalidArgument.into());
        }
        Ok(Self {
            incarnation,
            sequence,
        })
    }

    /// The selected watch's incarnation, replaced after source restart or the
    /// last subscriber leaves. It reveals no other account's activity.
    pub const fn incarnation(&self) -> &[u8; 16] {
        &self.incarnation
    }

    /// Coalesced invalidation position within this incarnation.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl From<AvailabilityCursor> for crate::wire::AvailabilityCursor {
    fn from(cursor: AvailabilityCursor) -> Self {
        Self {
            incarnation: cursor.incarnation.to_vec(),
            sequence: cursor.sequence,
        }
    }
}

impl TryFrom<crate::wire::AvailabilityCursor> for AvailabilityCursor {
    type Error = Error;
    fn try_from(cursor: crate::wire::AvailabilityCursor) -> Result<Self, Error> {
        let incarnation = cursor.incarnation.try_into().map_err(|_| unavailable())?;
        Self::new(incarnation, cursor.sequence).map_err(|_| unavailable())
    }
}

/// A registered exact-key observation. Subscribe **before** verifying the
/// original object or closure, then await [`Self::changed`]. Changes racing
/// verification remain pending. Every new subscription, including reconnect,
/// requires verification after its initial cursor; an old cursor cannot skip
/// that handshake. There is no retained event history or readiness polling.
///
/// An invalidation asks the consumer to repeat its existing verification. It
/// never certifies successful repair, existence, integrity or durability.
pub struct Availability {
    cursor: AvailabilityCursor,
    changes: BoxProviderStream<'static, Result<AvailabilityCursor, Error>>,
}

impl Availability {
    /// Adapts an already registered producer. `initial` must be captured after
    /// registration, before the caller can begin verification. Source loss must
    /// end `changes` or yield a typed error, never an empty healthy subscription.
    pub fn new(
        initial: AvailabilityCursor,
        changes: impl Stream<Item = Result<AvailabilityCursor, Error>> + ProviderTask + 'static,
    ) -> Self {
        Self {
            cursor: initial,
            changes: Box::pin(changes),
        }
    }

    /// The cut the next verification must cover. Reading it consumes no change.
    pub const fn cursor(&self) -> AvailabilityCursor {
        self.cursor
    }

    /// Waits for this key's original producer or its terminal failure. Duplicate,
    /// regressing and foreign-incarnation transport frames fail closed.
    pub async fn changed(&mut self) -> Result<AvailabilityCursor, Error> {
        let result = match self.changes.next().await {
            Some(Ok(next))
                if next.incarnation == self.cursor.incarnation
                    && next.sequence > self.cursor.sequence =>
            {
                self.cursor = next;
                return Ok(next);
            }
            Some(Err(error)) => error,
            _ => unavailable(),
        };
        self.changes = Box::pin(stream::empty());
        Err(result)
    }
}

type Signal = watch::Sender<Result<AvailabilityCursor, Error>>;
type Keys = HashMap<Digest, Signal>;

/// The original physical producer's account-scoped publication capability.
/// Share it between the immutable engine and the service that actually restores
/// bytes. A catalog-only observer cannot implement this contract. An external
/// push bridge must call [`Self::fail`] if it loses its producer; arbitrary raw
/// storage edits outside that producer are not observable through this source.
///
/// Only actively observed keys occupy the index. Publication looks up one key
/// and wakes only its subscribers; it allocates no buffer and records no history.
#[derive(Clone)]
pub struct AvailabilitySource {
    state: Arc<Mutex<Option<Keys>>>,
}

impl Default for AvailabilitySource {
    fn default() -> Self {
        Self::new()
    }
}

impl AvailabilitySource {
    /// Opens one physical source, without allocating per-key state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(Some(HashMap::new()))),
        }
    }

    fn keys(&self) -> Result<std::sync::MutexGuard<'_, Option<Keys>>, Error> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let failed = state.take();
                drop(state);
                drop(failed);
                Err(unavailable())
            }
        }
    }

    /// Registers before capturing the current cut. The returned observer does
    /// not keep the producer alive; dropping its last owner wakes it with
    /// `Unavailable`. This does not read, list or verify any stored bytes.
    pub fn subscribe(&self, digest: Digest) -> Result<Availability, Error> {
        self.register(digest)?.open()
    }

    fn register(&self, digest: Digest) -> Result<Observation, Error> {
        let receiver = {
            let mut state = self.keys()?;
            let keys = state.as_mut().ok_or_else(unavailable)?;
            let sender = match keys.entry(digest) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let mut incarnation = [0; 16];
                    getrandom::fill(&mut incarnation).map_err(|_| unavailable())?;
                    let cursor = AvailabilityCursor::new(incarnation, 0)?;
                    entry.insert(watch::channel(Ok(cursor)).0)
                }
            };
            sender.subscribe()
        };
        Ok(Observation {
            receiver,
            _registration: Registration {
                source: Arc::downgrade(&self.state),
                digest,
            },
        })
    }

    /// Invalidates the exact identity after its physical producer completed a
    /// write/restoration, including a repair with no catalog/layout change.
    /// Wakes outside the index lock. Coalescing preserves a pending verification.
    pub fn restored(&self, digest: Digest) -> Result<(), Error> {
        let sender = self
            .keys()?
            .as_ref()
            .ok_or_else(unavailable)?
            .get(&digest)
            .cloned();
        if let Some(sender) = sender {
            sender.send_modify(|current| {
                if let Ok(cursor) = current {
                    match cursor.sequence.checked_add(1) {
                        Some(next) => cursor.sequence = next,
                        None => *current = Err(unavailable()),
                    }
                }
            });
        }
        Ok(())
    }

    /// Ends every active observation when the physical source fails. Reopening
    /// needs a new source and the mandatory subscribe/verify handshake.
    pub fn fail(&self) {
        let failed = self.keys().ok().and_then(|mut state| state.take());
        for sender in failed.into_iter().flat_map(HashMap::into_values) {
            sender.send_modify(|current| *current = Err(unavailable()));
        }
    }
}

// A detached physical write can finish after its caller is cancelled. Until
// that completion is observed, this authority cannot promise a healthy feed.
pub(crate) struct Restoration<'a> {
    source: &'a AvailabilitySource,
    digest: Digest,
    outcome: RestorationOutcome,
}

enum RestorationOutcome {
    NoEffect,
    Restored,
    Indeterminate,
}

impl<'a> Restoration<'a> {
    pub(crate) async fn write(
        source: &'a AvailabilitySource,
        digest: Digest,
        write: impl Future<Output = Result<bool, crate::MediumError>>,
    ) -> Result<bool, crate::MediumError> {
        // Only a polled physical write can be indeterminate on cancellation.
        // Classify its original result before any caller can propagate it.
        let mut restoration = Self {
            source,
            digest,
            outcome: RestorationOutcome::Indeterminate,
        };
        let result = write.await;
        restoration.outcome = match &result {
            Ok(true) => RestorationOutcome::Restored,
            Ok(false) | Err(crate::MediumError::Failed(_)) => RestorationOutcome::NoEffect,
            Err(crate::MediumError::Ambiguous(_)) => RestorationOutcome::Indeterminate,
        };
        result
    }
}

impl Drop for Restoration<'_> {
    fn drop(&mut self) {
        match self.outcome {
            RestorationOutcome::NoEffect => {}
            RestorationOutcome::Restored => {
                if self.source.restored(self.digest).is_err() {
                    self.source.fail();
                }
            }
            RestorationOutcome::Indeterminate => self.source.fail(),
        }
    }
}

// Field order releases the receiver before the last-registration check, both
// when a change future is cancelled and when its producer terminates.
struct Observation {
    receiver: watch::Receiver<Result<AvailabilityCursor, Error>>,
    _registration: Registration,
}

impl Observation {
    fn open(mut self) -> Result<Availability, Error> {
        let initial = (*self.receiver.borrow_and_update())?;
        let changes = stream::try_unfold(self, |mut observation| async move {
            observation
                .receiver
                .changed()
                .await
                .map_err(|_| unavailable())?;
            let cursor = (*observation.receiver.borrow_and_update())?;
            Ok(Some((cursor, observation)))
        });
        Ok(Availability::new(initial, changes))
    }
}

struct Registration {
    source: Weak<Mutex<Option<Keys>>>,
    digest: Digest,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(source) = self.source.upgrade() {
            let mut state = source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(keys) = state.as_mut()
                && keys
                    .get(&self.digest)
                    .is_some_and(|sender| sender.receiver_count() == 0)
            {
                keys.remove(&self.digest);
            }
        }
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) mod client;
#[cfg(test)]
mod tests;
