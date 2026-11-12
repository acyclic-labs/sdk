//! Explicit per-actor placement, committed with the actor's publication fence.
//!
//! The ONLY way a node gains an actor's epoch is an admitted placement
//! command: an actor-local record `{index, node, epoch}` appended to
//! `<actor>/placement` under the conditional cut that raises the epoch.
//! Cloud's placement directory supplies explicit indexes through [`Runtime::place`].
//! A local single-node runtime, which holds its store's exclusive claim, is
//! also that store's placement owner and issues the next command through the
//! same record and admission path ([`Runtime::with_external_placement`] turns
//! this off). Neither delivery nor namespace discovery decides ownership.
use super::*;
use acyclic_stream::{CommitCondition, CommitEffect, CommitMutation};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Placement {
    index: u64,
    node: [u8; 16],
    epoch: u64,
}

impl<P: StreamProvider + ?Sized> Runtime<P> {
    /// Admit this node through one explicit host placement command. The index
    /// belongs to this actor: first is one, then each successor is contiguous.
    /// Exact replay admits the recorded cut; an older index, another node's
    /// command at this index or a gap refuses with [`Error::Conflict`].
    pub async fn place(&self, actor: ActorReference, index: u64) -> Result<(), Error> {
        if index == 0 {
            return Err(Error::Invalid("invalid actor placement"));
        }
        let placement = self.placement(&actor)?;
        let slot = &placement.0;
        // Serialize local commands with execution before changing durable
        // ownership. Cancellation after commit leaves the old local epoch
        // fenced; replay of this exact command admits the recorded cut.
        let mut inner = slot.inner.lock().await;
        self.command(&actor, Some(index), &mut inner).await?;
        slot.refresh.store(false, Ordering::Release);
        slot.waker.wake_by_ref();
        Ok(())
    }

    fn placement(&self, actor: &ActorReference) -> Result<EventDelivery, Error> {
        self.track(actor.clone())?;
        self.pin(actor)?
            .ok_or(Error::Invalid("actor placement retired during admission"))
    }

    /// Resolve this node's admitted epoch for a resident slot: the existing
    /// command if it names this node incarnation, otherwise (as the local
    /// placement owner only) the next command. External placement refuses.
    pub(super) async fn admit(
        &self,
        actor: &ActorReference,
        inner: &mut Inner,
    ) -> Result<(), Error> {
        self.command(actor, None, inner).await
    }

    async fn command(
        &self,
        actor: &ActorReference,
        index: Option<u64>,
        inner: &mut Inner,
    ) -> Result<(), Error> {
        let owner = Owner {
            node: self.instance,
            places: self.places,
        };
        command(&self.store, owner, actor, index, inner).await
    }
}

/// This node incarnation, and whether it is its store's placement owner.
#[derive(Clone, Copy)]
pub(super) struct Owner {
    pub(super) node: uuid::Uuid,
    pub(super) places: bool,
}

/// Resolve or issue one placement command under the slot's delivery lock.
pub(super) async fn command<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    owner: Owner,
    actor: &ActorReference,
    index: Option<u64>,
    inner: &mut Inner,
) -> Result<(), Error> {
    if inner.pending.is_some() {
        return Err(Error::Invalid(
            "placement has unsettled publication custody",
        ));
    }
    let previous = store.load(actor).await?;
    let state = previous
        .state()
        .ok_or(Error::Invalid("actor state was never created"))?;
    if state.deleted() {
        return Err(Error::Deleted);
    }
    let path = StreamPath::new(format!("{}/placement", previous.path()))?;
    let head = match store.provider().head(path.clone()).await {
        Ok(head) => Some(head),
        Err(error) if error.category() == StreamError::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let tail = head.as_ref().map_or(0, |head| head.tail);
    let placed = head
        .as_ref()
        .and_then(|head| head.last.as_ref())
        .map(|record| serde_json::from_slice::<Placement>(&record.value))
        .transpose()
        .map_err(|_| Error::Invalid("invalid placement record"))?;
    let current = placed.as_ref().is_some_and(|placed| {
        placed.index == tail
            && placed.node == *owner.node.as_bytes()
            && placed.epoch == state.epoch()
            && previous.owns_epoch(placed.epoch)
    });
    let index = match index {
        Some(index) if index == tail => {
            // Exact replay of the latest command, or a stale duplicate.
            if !current {
                return Err(Error::Conflict);
            }
            None
        }
        Some(index) if tail.checked_add(1) == Some(index) => Some(index),
        Some(_) => return Err(Error::Conflict),
        None if current => None,
        None if owner.places => Some(
            tail.checked_add(1)
                .ok_or(Error::Invalid("placement index overflow"))?,
        ),
        None => return Err(Error::Conflict),
    };
    let snapshot = match index {
        None => previous,
        Some(index) => {
            let epoch = state
                .epoch()
                .checked_add(1)
                .ok_or(Error::Invalid("actor epoch overflow"))?;
            let command = Placement {
                index,
                node: *owner.node.as_bytes(),
                epoch,
            };
            let condition = match head.and_then(|head| head.last.map(|last| (head.tail, last))) {
                Some((expected, last)) => CommitCondition::Head {
                    path: path.clone(),
                    expected,
                    commit_id: last.commit_id,
                },
                None => CommitCondition::Absent { path: path.clone() },
            };
            let effect = CommitEffect::hard(
                vec![condition],
                vec![CommitMutation::Append {
                    path,
                    records: vec![
                        serde_json::to_vec(&command)
                            .map_err(|_| Error::Invalid("invalid placement record"))?
                            .into(),
                    ],
                }],
            );
            store
                .claim_control(
                    &previous,
                    epoch,
                    None,
                    vec![effect],
                    key(&previous, &format!("placement/{index}/{}", owner.node))?,
                )
                .await?
        }
    };
    // A committed command is not admission: recovery may have failed or been
    // cancelled after its durable cut. Exact replay must finish it too.
    store.recover_custody(&snapshot).await?;
    inner.epoch = snapshot.state().ok_or(Error::Conflict)?.epoch();
    inner.observed = Some(Arc::new(snapshot));
    Ok(())
}

#[cfg(all(test, loom))]
mod model {
    use super::*;

    #[test]
    fn placement_pins_before_retirement() {
        loom::model(|| {
            let result = race();
            assert!(result.is_ok(), "{result:?}");
        });
    }

    fn race() -> Result<(), Box<dyn std::error::Error>> {
        let actor = ActorReference::new(b"account", b"agents", b"placement")?;
        let runtime = Arc::new(Runtime::new(
            Store::new(
                Arc::new(acyclic_stream::MemoryStream::default()),
                StreamPath::new("actors")?,
            ),
            Arc::new(crate::conformance::Package::new()?),
            Arc::new(SystemClock),
        ));
        runtime.track(actor.clone())?;
        let placing = Arc::clone(&runtime);
        let address = actor.clone();
        let placement = loom::thread::spawn(move || {
            if let Ok(placement) = placing.placement(&address) {
                // Admission holds this guard across all asynchronous work.
                // Release cannot retire either this slot or its replacement.
                assert!(!placing.release(&address));
                assert!(matches!(placement.0.inner.try_lock(), Ok(inner) if !inner.released));
                let resident = placing
                    .slot(&address)
                    .ok_or(Error::Invalid("pinned actor was retired"))?;
                assert!(Arc::ptr_eq(&placement.0, &resident));
            }
            Ok::<_, Error>(())
        });
        let retiring = loom::thread::spawn(move || {
            runtime.release(&actor);
        });
        placement
            .join()
            .map_err(|_| "placement thread panicked")??;
        retiring.join().map_err(|_| "retirement thread panicked")?;
        Ok(())
    }
}
