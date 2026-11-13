//! Root control and account presence share the original caller-owned commit.
use super::{AgentHost, AgentPath, CommitRequest, Error, StreamError};
use crate::agent::{SessionProfile, SessionPublication, SessionRow};
use acyclic_actors::local::PreparedPublish;
use acyclic_stream::{
    CommitMutation, CommittedMutation, IdempotencyOutcome, Record, StreamProviderExt as _,
};

impl<P: crate::store::JournalProvider + ?Sized + 'static> AgentHost<P> {
    /// Retain the command's original semantic record, rather than preparing
    /// another row revision after an uncertain or repeated user operation.
    async fn retained_control(&self, base: &CommitRequest) -> Result<Option<Record>, Error> {
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
                if record.value != expected.value || record.headers != expected.headers {
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
        if root.depth() != 0 || self.agents.routing.is_none() {
            return publish(base).await;
        }
        if let Some(record) = self.retained_control(&base).await? {
            return original(record);
        }
        let request = self.presence_control(root, base.clone(), enabled).await?;
        match publish(request).await {
            Err(Error::Stream(error)) if error.category() == StreamError::IdempotencyMismatch => {
                // The original may have committed after the first observation.
                // Reconcile that terminal result once; never resubmit a rewrite.
                let record = self
                    .retained_control(&base)
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
        let path = SessionPublication::row_path(&actor)?;
        let current = match self.provider.head(path.clone()).await {
            Ok(head) => {
                let record = head.last.ok_or(Error::Record("session row is empty"))?;
                let row = SessionRow::decode(&record.value)?;
                if row.rev != head.tail || row.node != routing.node {
                    return Err(Error::Record("session row ownership or revision differs"));
                }
                Some(row)
            }
            Err(error) if error.category() == StreamError::NotFound => None,
            Err(error) => return Err(Error::Stream(error)),
        };
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
                    .record_at(path, row.rev - 2)
                    .await
                    .map_err(Error::Stream)?
                    .ok_or(Error::Record("removed session has no preceding row"))?;
                let previous = SessionRow::decode(&record.value)?;
                if previous.node != routing.node || previous.rev.checked_add(1) != Some(row.rev) {
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
        let Some(row) = changed else {
            return Ok(base);
        };
        let publication = SessionPublication {
            actor,
            row,
            intent: base.idempotency_key.clone(),
        };
        let mut joined = PreparedPublish::foreign(base.idempotency_key.clone());
        for effect in base.effects {
            joined.join(effect).map_err(super::actor_error)?;
        }
        if let Some(guard) = guard {
            joined.require(vec![guard]).map_err(super::actor_error)?;
        }
        for effect in publication.effects(&routing.paths)? {
            joined.join(effect).map_err(super::actor_error)?;
        }
        joined.into_request().map_err(super::actor_error)
    }
}
