//! Activation discovery is a checkpointed actor in a separate state root.
//! Placement commands, not a root child scan, admit resident execution.
use super::*;
use crate::local::Subscription;

impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) fn dispatcher_store(&self, shard: u8) -> Result<(Store<P>, ActorReference), Error> {
        let name = format!("dispatcher-{shard:02x}");
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

    pub(super) async fn dispatcher(
        &self,
        store: &Store<P>,
        actor: &ActorReference,
        shard: u8,
    ) -> Result<Snapshot, Error> {
        let snapshot = store.load(actor).await?;
        if snapshot.state().is_some() {
            return Ok(snapshot);
        }
        let state = State {
            placement_branch: None,
            escrow: None,
            metadata: Default::default(),
            journal_tail: 0,
            epoch: 0,
            checkpoint: None,
            alarm: Default::default(),
            paused: false,
            subscriptions: BTreeMap::from([(
                "activation".into(),
                Subscription {
                    path: self.store.scheduling_path(shard)?,
                    cursor: 0,
                },
            )])
            .into(),
        };
        let request = store.prepare(
            &snapshot,
            state,
            Vec::new(),
            key(&snapshot, "dispatcher-open")?,
        )?;
        store
            .submit_retained(request)
            .await?
            .ok_or(Error::Invalid("dispatcher receipt has no state"))
    }

    pub(super) async fn advance_dispatcher(
        &self,
        store: &Store<P>,
        previous: Snapshot,
        seen: u64,
    ) -> Result<Snapshot, Error> {
        let mut state = previous
            .state()
            .cloned()
            .ok_or(Error::Invalid("dispatcher state disappeared"))?;
        let subscription = state
            .subscriptions
            .get_mut("activation")
            .ok_or(Error::Invalid("dispatcher has no activation cursor"))?;
        if subscription.cursor >= seen {
            return Ok(previous);
        }
        subscription.cursor = seen;
        // Competing dispatchers can observe different activation suffixes
        // from the same state cut. Bind the key to the proposed cursor so
        // their CAS conflict is not misclassified as changed request bytes.
        let publication = store.prepare(
            &previous,
            state,
            Vec::new(),
            key(&previous, &format!("dispatch/{seen}"))?,
        )?;
        // Conflict returns the original conditional result. Unknown retains
        // the exact publication and participant custody in the returned error.
        store
            .submit_retained(publication)
            .await?
            .ok_or(Error::Invalid("dispatcher receipt has no state"))
    }
}
