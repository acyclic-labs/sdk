//! Activation discovery is a checkpointed actor in a separate state root.
//! Its cursor never replaces the current resident set: restart restores that
//! set from state names before following the captured activation cut.
use super::*;
use crate::local::Subscription;
use acyclic_stream::ChildrenPageRequest;

impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) fn dispatcher_store(&self) -> Result<(Store<P>, ActorReference), Error> {
        let actor = match &self.claims {
            None => ActorReference::new(b"runtime", b"dispatch", b"all"),
            Some((account, namespace)) => ActorReference::new(
                account.as_bytes(),
                namespace.as_str().as_bytes(),
                b"dispatcher",
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
    ) -> Result<Snapshot, Error> {
        loop {
            let snapshot = store.load(actor).await?;
            if snapshot.state().is_some() {
                return Ok(snapshot);
            }
            let state = State {
                epoch: 0,
                checkpoint: None,
                alarm: Default::default(),
                paused: false,
                subscriptions: BTreeMap::from([(
                    "activation".into(),
                    Subscription {
                        path: self.store.root().clone(),
                        cursor: 0,
                    },
                )]),
            };
            let request = store.prepare(
                &snapshot,
                &state,
                Vec::new(),
                key(&snapshot, "dispatcher-open")?,
            )?;
            match store.submit_retained(request).await {
                Ok(Some(committed)) => return Ok(committed),
                Ok(None) => return Err(Error::Invalid("dispatcher receipt has no state")),
                Err(Error::Conflict) => {}
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) async fn advance_dispatcher(
        &self,
        store: &Store<P>,
        mut previous: Snapshot,
        seen: u64,
    ) -> Result<Snapshot, Error> {
        loop {
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
                &state,
                Vec::new(),
                key(&previous, &format!("dispatch/{seen}"))?,
            )?;
            match store.submit_retained(publication).await {
                Ok(Some(committed)) => return Ok(committed),
                Ok(None) => return Err(Error::Invalid("dispatcher receipt has no state")),
                Err(Error::Conflict) => {}
                // Unknown retains the exact publication in the returned error.
                // Reopen reads the decided cursor or reconstructs the identical
                // request/key; it never spins while storage is unavailable.
                Err(error) => return Err(error),
            }
            previous = store.load_path(previous.path.clone()).await?;
        }
    }

    pub(super) async fn restore(&self) -> Result<(), Error> {
        let mut after = None;
        let mut version = None;
        loop {
            let page = self
                .store
                .provider()
                .children_page(ChildrenPageRequest {
                    parent: Some(self.store.root().clone()),
                    after: after.clone(),
                    hierarchy_version: version,
                    limit: 256,
                    tail_stream: None,
                })
                .await;
            let page = match page {
                Ok(page) => page,
                // Current state names are authoritative. Repeated names are
                // harmless; restart a changed traversal, never skip its suffix.
                Err(StreamError::HierarchyChanged) => {
                    after = None;
                    version = None;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            for child in page.children {
                if let Ok(actor) = self.store.actor(&child.path)
                    && self.claims(&actor)
                {
                    self.track(actor)?;
                }
            }
            match page.next_after {
                Some(next) => {
                    after = Some(next);
                    version = Some(page.hierarchy_version);
                }
                None => return Ok(()),
            }
        }
    }
}
