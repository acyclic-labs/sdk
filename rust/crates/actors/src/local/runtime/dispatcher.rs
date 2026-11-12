//! Each stable scheduling shard checkpoints its activation cursor and its
//! consumed deadline-window watermark as a small actor in a separate root.
//! Opening reads it; the first advance creates it, so an idle shard costs one
//! head read and never a write.
//! Placement commands, not a root child scan, admit resident execution.
use super::*;
use crate::local::Subscription;

impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) fn dispatcher_store(&self, shard: u16) -> Result<(Store<P>, ActorReference), Error> {
        let name = format!("dispatcher-{shard:04x}");
        let actor = match &self.scope {
            None => ActorReference::new(b"runtime", b"dispatch", name.as_bytes()),
            Some((account, namespace)) => ActorReference::new(
                account.as_bytes(),
                namespace.as_str().as_bytes(),
                name.as_bytes(),
            ),
        }
        .map_err(|_| Error::Invalid("invalid dispatcher identity"))?;
        Ok((
            Store::new(
                Arc::clone(self.store.provider()),
                StreamPath::new(format!("{}/_dispatcher", self.store.root()))?,
            ),
            actor,
        ))
    }

    pub(super) async fn advance_dispatcher(
        &self,
        store: &Store<P>,
        previous: Snapshot,
        shard: u16,
        seen: u64,
        deadline: u64,
    ) -> Result<Snapshot, Error> {
        let mut state = match previous.state() {
            Some(state) => state.clone(),
            None => State {
                placement_branch: None,
                escrow: None,
                metadata: Default::default(),
                journal_tail: 0,
                epoch: 0,
                checkpoint: None,
                alarm: Default::default(),
                paused: false,
                subscriptions: ["activation", "deadline"]
                    .into_iter()
                    .map(|name| {
                        Ok((
                            name.into(),
                            Subscription {
                                path: self.store.scheduling_path(shard)?,
                                cursor: 0,
                            },
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, Error>>()?
                    .into(),
            },
        };
        let mut advanced = false;
        // The activation cursor only advances; the window watermark may
        // return to a window holding an alarm armed already past due.
        for (name, cursor, regresses) in [("activation", seen, false), ("deadline", deadline, true)]
        {
            let subscription = state
                .subscriptions
                .get_mut(name)
                .ok_or(Error::Invalid("dispatcher has no scheduling cursor"))?;
            if subscription.cursor < cursor || (regresses && subscription.cursor != cursor) {
                subscription.cursor = cursor;
                advanced = true;
            }
        }
        if !advanced {
            return Ok(previous);
        }
        // Competing dispatchers can observe different activation suffixes
        // from the same state cut. Bind the key to the proposed cursor so
        // their CAS conflict is not misclassified as changed request bytes.
        let publication = store.prepare(
            &previous,
            state,
            Vec::new(),
            key(&previous, &format!("dispatch/{seen}/{deadline}"))?,
        )?;
        // Conflict returns the original conditional result. Unknown retains
        // the exact publication and participant custody in the returned error.
        store
            .submit_retained(publication)
            .await?
            .ok_or(Error::Invalid("dispatcher receipt has no state"))
    }
}

/// The confirmed activation cursor of a shard checkpoint; zero before creation.
pub(super) fn activation_cursor(snapshot: &Snapshot) -> u64 {
    cursor(snapshot, "activation")
}

/// The first deadline window not yet delivered; zero before creation.
pub(super) fn deadline_cursor(snapshot: &Snapshot) -> u64 {
    cursor(snapshot, "deadline")
}

fn cursor(snapshot: &Snapshot, name: &str) -> u64 {
    snapshot
        .state()
        .and_then(|state| state.cursor(name))
        .unwrap_or(0)
}
