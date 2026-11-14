//! Root control and account presence share the original caller-owned commit.
use super::{AgentHost, AgentPath, CommitRequest, Error, StreamError};
use crate::agent::{SessionProfile, SessionPublication, SessionRow};
use acyclic_actors::local::PreparedPublish;
use acyclic_stream::{
    CommitMutation, CommittedMutation, IdempotencyOutcome, Record, StreamProviderExt as _,
};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

impl<P: crate::store::JournalProvider + ?Sized + 'static> AgentHost<P> {
    /// Retain the command's original semantic record, rather than preparing
    /// another row revision after an uncertain or repeated user operation.
    async fn retained_control(
        &self,
        root: &AgentPath,
        base: &CommitRequest,
    ) -> Result<Option<Record>, Error> {
        let Some(observed) = self
            .provider
            .inspect_idempotency(base.idempotency_key.clone())
            .await
            .map_err(Error::Stream)?
        else {
            return Ok(None);
        };
        if observed.idempotency_key != base.idempotency_key {
            return Err(Error::Stream(StreamError::InvalidResponse));
        }
        let IdempotencyOutcome::Commit(envelope) = observed.outcome else {
            return Err(Error::Stream(StreamError::IdempotencyMismatch));
        };
        let Some(CommitMutation::Append { path, records }) = base
            .effects
            .first()
            .and_then(|effect| effect.mutations.first())
        else {
            return Err(Error::Record("control has no original append"));
        };
        let [expected] = records.as_slice() else {
            return Err(Error::Record("control has no unique original record"));
        };
        for mutation in envelope.mutations {
            if let CommittedMutation::Append(append) = mutation
                && &append.path == path
            {
                let [record] = append.records.as_slice() else {
                    return Err(Error::Stream(StreamError::IdempotencyMismatch));
                };
                let same = if path == &root.peer_membership()? {
                    use crate::agent::AgentRecord as _;
                    // Epoch selection is host-owned preparation. Replaying the
                    // user's same boolean must retain its original interval.
                    crate::agent::PeerMembership::decode(&record.value)?.enabled()
                        == crate::agent::PeerMembership::decode(&expected.value)?.enabled()
                } else {
                    record.value == expected.value
                };
                if !same || record.headers != expected.headers {
                    return Err(Error::Stream(StreamError::IdempotencyMismatch));
                }
                return Ok(Some(record.clone()));
            }
        }
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    }

    /// The existing control publisher remains the writer. Only routed roots
    /// acquire a row companion; children and ordinary local hosts do no IO here.
    pub(super) async fn publish_presence_control<T, F, R>(
        &self,
        root: &AgentPath,
        base: CommitRequest,
        enabled: bool,
        publish: F,
        original: R,
    ) -> Result<T, Error>
    where
        F: AsyncFnOnce(CommitRequest) -> Result<T, Error>,
        R: FnOnce(Record) -> Result<T, Error>,
    {
        if root.depth() != 0 {
            return publish(base).await;
        }
        if let Some(record) = self.retained_control(root, &base).await? {
            return original(record);
        }
        if self.agents.routing.is_none() {
            return publish(base).await;
        }
        let request = self.presence_control(root, base.clone(), enabled).await?;
        match publish(request).await {
            Err(Error::Stream(error)) if error.category() == StreamError::IdempotencyMismatch => {
                // The original may have committed after the first observation.
                // Reconcile that terminal result once; never resubmit a rewrite.
                let record = self
                    .retained_control(root, &base)
                    .await?
                    .ok_or(Error::Stream(error))?;
                original(record)
            }
            outcome => outcome,
        }
    }

    async fn presence_control(
        &self,
        root: &AgentPath,
        base: CommitRequest,
        enabled: bool,
    ) -> Result<CommitRequest, Error> {
        let routing = self
            .agents
            .routing
            .as_ref()
            .ok_or(Error::Record("root has no routing context"))?;
        let actor = self.actor(root)?;
        let (current, row_guard) =
            SessionPublication::observe(self.provider.as_ref(), &actor, &routing.node).await?;
        let mut guard = None;
        let changed = if !enabled {
            match current {
                Some(row) => row.changed(None)?,
                None => None,
            }
        } else if current.as_ref().is_some_and(|row| row.profile.is_some()) {
            None
        } else {
            // Removal leaves no public metadata in its head. The immediately
            // preceding live row retains display fields; refresh phase and the
            // workspace table from one fenced original actor observation.
            let previous = if let Some(row) = &current
                && row.rev > 1
            {
                let record = self
                    .provider
                    .record_at(row_guard.path().clone(), row.rev - 2)
                    .await
                    .map_err(Error::Stream)?
                    .ok_or(Error::Record("removed session has no preceding row"))?;
                let previous = SessionRow::from_record(row_guard.path(), row.rev - 1, &record)?;
                if previous.node != routing.node {
                    return Err(Error::Record("removed session provenance differs"));
                }
                Some(
                    previous
                        .profile
                        .ok_or(Error::Record("consecutive session removals"))?,
                )
            } else {
                None
            };
            let (head, _, observed) = self.agents.head_with_guard(&actor, root).await?;
            let state = head.load(self.provider.as_ref()).await?;
            guard = Some(observed);
            let (title, goal) = previous.map_or_else(
                || (String::new(), String::new()),
                |profile| (profile.title, profile.goal_preview),
            );
            let profile =
                SessionProfile::project(&state.phase, title, &state.volumes, &goal, goal.len());
            match current {
                Some(row) => row.changed(Some(profile))?,
                None => Some(SessionRow::new(routing.node.clone(), profile)),
            }
        };
        let mut joined = PreparedPublish::foreign(base.idempotency_key.clone());
        let intent = base.idempotency_key.clone();
        for effect in base.effects {
            joined.join(effect).map_err(super::actor_error)?;
        }
        joined
            .require(vec![row_guard])
            .map_err(super::actor_error)?;
        if let Some(guard) = guard {
            joined.require(vec![guard]).map_err(super::actor_error)?;
        }
        if let Some(row) = changed {
            let publication = SessionPublication { actor, row, intent };
            for effect in publication.effects(&routing.paths)? {
                joined.join(effect).map_err(super::actor_error)?;
            }
        }
        joined.into_request().map_err(super::actor_error)
    }
}
