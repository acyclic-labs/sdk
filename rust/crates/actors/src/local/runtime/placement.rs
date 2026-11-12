//! Explicit per-actor placement, committed with the actor's publication fence.
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
    /// Local and remote placement owners call this same path; a delivery or
    /// namespace discovery never calls it. Caller admission is host-owned.
    pub async fn place(&self, actor: ActorReference, index: u64) -> Result<(), Error> {
        if !self.includes(&actor) || index == 0 {
            return Err(Error::Invalid("invalid actor placement"));
        }
        self.track(actor.clone())?;
        let slot = self
            .slot(&actor)
            .ok_or(Error::Invalid("placed actor is not registered"))?;
        // Serialize local commands with execution before changing durable
        // ownership. Cancellation after commit leaves the old local epoch
        // fenced; replay of this exact command admits the recorded cut.
        let mut inner = slot.inner.lock().await;
        if inner.pending.is_some() {
            return Err(Error::Invalid(
                "placement has unsettled publication custody",
            ));
        }
        let previous = self.store.load(&actor).await?;
        let state = previous
            .state()
            .ok_or(Error::Invalid("actor state was never created"))?;
        let path = StreamPath::new(format!("{}/placement", previous.path()))?;
        let head = match self.store.provider().head(path.clone()).await {
            Ok(head) => Some(head),
            Err(StreamError::NotFound) => None,
            Err(error) => return Err(error.into()),
        };
        let tail = head.as_ref().map_or(0, |head| head.tail);
        let snapshot = if tail == index {
            let record = head
                .as_ref()
                .and_then(|head| head.last.as_ref())
                .ok_or(Error::Invalid("placement has no committed command"))?;
            let placed: Placement = serde_json::from_slice(&record.value)
                .map_err(|_| Error::Invalid("invalid placement record"))?;
            if placed.index != index
                || placed.node != *self.instance.as_bytes()
                || placed.epoch != state.epoch()
            {
                return Err(Error::Conflict);
            }
            previous
        } else {
            if tail.checked_add(1) != Some(index) {
                return Err(Error::Conflict);
            }
            let epoch = state
                .epoch()
                .checked_add(1)
                .ok_or(Error::Invalid("actor epoch overflow"))?;
            let command = Placement {
                index,
                node: *self.instance.as_bytes(),
                epoch,
            };
            let condition = if let Some(head) = head {
                let last = head
                    .last
                    .ok_or(Error::Invalid("placement has no committed command"))?;
                CommitCondition::Head {
                    path: path.clone(),
                    expected: tail,
                    commit_id: last.commit_id,
                }
            } else {
                CommitCondition::Absent { path: path.clone() }
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
            self.store
                .claim_control(
                    &previous,
                    epoch,
                    None,
                    vec![effect],
                    key(&previous, &format!("placement/{index}/{}", self.instance))?,
                )
                .await?
        };
        inner.epoch = snapshot.state().ok_or(Error::Conflict)?.epoch();
        inner.observed = Some(Arc::new(snapshot));
        slot.refresh.store(false, Ordering::Release);
        slot.waker.wake_by_ref();
        Ok(())
    }
}
