//! The one batch driver every provider and native entry point shares.
use crate::{ByteRange, Digest, Sealed};
use futures::stream::{self, StreamExt};
use std::sync::{Mutex, PoisonError};

/// One batch member's place in the batch's dependency order.
pub(crate) trait Member {
    /// The object this member publishes, when later members may depend on it.
    fn publishes(&self) -> Option<Digest>;
    /// The objects this member needs published before it starts.
    fn needs(&self) -> &[Digest];
}

impl Member for (Sealed, Vec<Digest>) {
    fn publishes(&self) -> Option<Digest> {
        Some(Digest::sealed(&self.0))
    }
    fn needs(&self) -> &[Digest] {
        &self.1
    }
}

impl Member for (Digest, ByteRange, u64) {
    fn publishes(&self) -> Option<Digest> {
        None
    }
    fn needs(&self) -> &[Digest] {
        &[]
    }
}

/// A started member that publishes, at slot `index % window`.
struct Slot {
    index: usize,
    digest: Digest,
    done: bool,
}

/// Fixed bound on what a task node of the in-flight set holds beside its
/// future: its links, counts, flags and order.
const NODE: usize = 16 * std::mem::size_of::<usize>();

/// The byte size of what `f` returns, before anything is built.
const fn returned<A, F, R>(_: &F) -> usize
where
    F: Fn(usize, A) -> R,
{
    std::mem::size_of::<R>()
}

/// Runs `start` over every member with at most `window` in flight, handing
/// each outcome to `each` in request order.
///
/// A member that needs or republishes an earlier member's object starts once
/// that member completed, so outcomes never depend on timing; every other
/// member overlaps the rest. The batch holds `window` members' state at
/// once whatever its length; `admit` is charged every byte of it before any
/// is allocated, and holds the charge until that state is freed.
pub(crate) async fn drive<R, T, G, E, Fut>(
    requests: Vec<R>,
    window: usize,
    admit: impl FnOnce(usize) -> Result<G, E>,
    start: impl Fn(R) -> Fut,
    mut each: impl FnMut(T),
) -> Result<(), E>
where
    R: Member,
    Fut: Future<Output = T>,
{
    let window = window.clamp(1, requests.len().max(1));
    let slots = Mutex::new(Vec::<Option<Slot>>::new());
    let published = tokio::sync::Notify::new();
    // A member starts only once the one `window` places earlier completed,
    // so its unfinished dependencies all hold a slot.
    let ready = |index: usize, digest: Option<Digest>, needs: &[Digest]| {
        slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .flatten()
            .filter(|slot| slot.index < index && !slot.done)
            .all(|slot| Some(slot.digest) != digest && !needs.contains(&slot.digest))
    };
    let member = |index: usize, request: R| {
        let digest = request.publishes();
        let (slots, published, ready, start) = (&slots, &published, &ready, &start);
        async move {
            loop {
                let notified = published.notified();
                if ready(index, digest, request.needs()) {
                    break;
                }
                notified.await;
            }
            let outcome = start(request).await;
            if digest.is_some() {
                let mut slots = slots.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(Some(slot)) = slots.get_mut(index % window) {
                    slot.done = true;
                }
                drop(slots);
                published.notify_waiters();
            }
            outcome
        }
    };
    let member_bytes =
        returned(&member) + std::mem::size_of::<T>() + NODE + std::mem::size_of::<Option<Slot>>();
    let _grant = admit(window.checked_mul(member_bytes).unwrap_or(usize::MAX))?;
    let mut started = 0;
    let mut outcomes = stream::iter(requests)
        .map(|request| {
            let index = started;
            started += 1;
            if let Some(digest) = request.publishes() {
                let mut slots = slots.lock().unwrap_or_else(PoisonError::into_inner);
                let slot = Slot {
                    index,
                    digest,
                    done: false,
                };
                if slots.is_empty() {
                    slots.resize_with(window, || None);
                }
                // At most `window` members are in flight, so the member this
                // one displaces completed.
                let old = slots[index % window].replace(slot);
                assert!(
                    old.is_none_or(|old| old.done),
                    "a displaced member completed"
                );
            }
            member(index, request)
        })
        .buffered(window);
    while let Some(outcome) = outcomes.next().await {
        each(outcome);
    }
    Ok(())
}
