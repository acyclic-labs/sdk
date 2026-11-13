use super::*;
use crate::guest::SubscriptionChange;
use prost::Message as _;
use sha2::{Digest as _, Sha256};

const INTENT: &str = "acyclic.actor.subscription";

impl<P: StreamProvider + ?Sized> Store<P> {
    /// Add a subscription at one exact actor cut, preserving its checkpoint and
    /// all other subscriptions. Head starts bind once at acceptance. Replaying
    /// the caller's identity recovers that acceptance before reading any heads;
    /// changing its request is an idempotency mismatch. The same commit wakes
    /// a dormant actor. Concurrent deliveries are fenced by the actor-state CAS.
    pub async fn add_subscription(
        &self,
        account: &[u8],
        request: &crate::wire::AddSubscriptionRequest,
    ) -> Result<ActorReference, Error> {
        let admitted = crate::domain::AddSubscriptionRequest::try_from(request.clone())
            .map_err(|_| Error::Invalid("invalid add subscription request"))?;
        let actor = admitted
            .reference(account)
            .map_err(|_| Error::Invalid("invalid actor reference"))?;
        let spec = admitted.subscription();
        self.edit_subscription(
            &actor,
            b"add",
            admitted.idempotency_key(),
            &request.encode_to_vec(),
            SubscriptionChange::Add {
                id: spec.subscription_id().to_owned(),
                path: StreamPath::new(spec.stream_path().to_owned())?,
                from: spec.start().cursor_value(),
            },
        )
        .await?;
        Ok(actor)
    }

    /// Remove a subscription under the same publication and replay guarantees
    /// as [`Self::add_subscription`]. Removing it cannot consume a guest input.
    pub async fn remove_subscription(
        &self,
        account: &[u8],
        request: &crate::wire::RemoveSubscriptionRequest,
    ) -> Result<ActorReference, Error> {
        let admitted = crate::domain::RemoveSubscriptionRequest::try_from(request.clone())
            .map_err(|_| Error::Invalid("invalid remove subscription request"))?;
        let actor = admitted
            .reference(account)
            .map_err(|_| Error::Invalid("invalid actor reference"))?;
        self.edit_subscription(
            &actor,
            b"remove",
            admitted.idempotency_key(),
            &request.encode_to_vec(),
            SubscriptionChange::Remove {
                id: admitted.subscription_id().to_owned(),
            },
        )
        .await?;
        Ok(actor)
    }

    /// Resumes a held input without advancing its cursor or changing its
    /// checkpoint. The same commit activates delivery, and an exact replay
    /// recovers its original acceptance before inspecting the current binding.
    pub async fn resume_subscription(
        &self,
        account: &[u8],
        request: &crate::wire::ResumeSubscriptionRequest,
    ) -> Result<ActorReference, Error> {
        let admitted = crate::domain::ResumeSubscriptionRequest::try_from(request.clone())
            .map_err(|_| Error::Invalid("invalid resume subscription request"))?;
        let actor = admitted
            .reference(account)
            .map_err(|_| Error::Invalid("invalid actor reference"))?;
        self.edit_subscription(
            &actor,
            b"resume",
            admitted.idempotency_key(),
            &request.encode_to_vec(),
            SubscriptionChange::Resume {
                id: admitted.subscription_id().to_owned(),
            },
        )
        .await?;
        Ok(actor)
    }
    async fn edit_subscription(
        &self,
        actor: &ActorReference,
        operation: &[u8],
        identity: &str,
        request: &[u8],
        change: SubscriptionChange,
    ) -> Result<(), Error> {
        if identity.is_empty() {
            return Err(Error::Invalid("subscription edit has no retry identity"));
        }
        let path = self.path(actor)?;
        let mut hash = Sha256::new();
        hash.update(b"acyclic.actors.subscription.v1\0");
        hash.update(path.as_str().as_bytes());
        hash.update([0]);
        hash.update(operation);
        hash.update([0]);
        hash.update(identity.as_bytes());
        let key = IdempotencyKey::new(hash.finalize().to_vec())?;
        let intent: [u8; 32] = Sha256::digest(request).into();
        if self.subscription_replay(&key, &path, &intent).await? {
            return Ok(());
        }
        let previous = self.load(actor).await?;
        let mut state = previous
            .state()
            .filter(|state| !state.deleted())
            .ok_or(Error::Invalid("subscription actor does not exist"))?
            .clone();
        let mut changes = vec![change];
        for change in &mut changes {
            if let SubscriptionChange::Add {
                path,
                from: from @ None,
                ..
            } = change
            {
                *from = Some(self.head(path).await?.map_or(0, |head| head.tail));
            }
        }
        subscribe(&mut state.subscriptions, &path, &changes)?;
        let mut prepared = self.prepare(
            &previous,
            state,
            vec![self.activation(actor)?],
            key.clone(),
            false,
        )?;
        // The original accepted envelope binds the caller's intent without
        // putting control receipts in guest metadata or adding a second ledger.
        for effect in &mut prepared.request.effects {
            for mutation in &mut effect.mutations {
                if let CommitMutation::Append {
                    path: target,
                    records,
                } = mutation
                    && *target == path
                {
                    records[0].headers = acyclic_stream::RecordHeaders::new(
                        Vec::new(),
                        vec![acyclic_stream::Attribute {
                            name: INTENT.to_owned(),
                            value: intent.to_vec().into(),
                        }],
                    )?;
                }
            }
        }
        match self.submit(prepared).await {
            Ok(()) => Ok(()),
            Err(
                error @ (Error::Conflict
                | Error::Stream(StreamError::IdempotencyMismatch)
                | Error::Unknown { .. }),
            ) => match self.subscription_replay(&key, &path, &intent).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(error),
                Err(_) if matches!(&error, Error::Unknown { .. }) => Err(error),
                Err(recovery) => Err(recovery),
            },
            Err(error) => Err(error),
        }
    }

    async fn subscription_replay(
        &self,
        key: &IdempotencyKey,
        path: &StreamPath,
        intent: &[u8; 32],
    ) -> Result<bool, Error> {
        let Some(observed) = self.provider.inspect_idempotency(key.clone()).await? else {
            return Ok(false);
        };
        if observed.idempotency_key == *key
            && let acyclic_stream::IdempotencyOutcome::Commit(envelope) = observed.outcome
            && let Some(record) = envelope
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    acyclic_stream::CommittedMutation::Append(append) if append.path == *path => {
                        append.records.first()
                    }
                    _ => None,
                })
            && record
                .headers
                .attributes()
                .iter()
                .any(|attribute| attribute.name == INTENT && attribute.value.as_ref() == intent)
        {
            if let Err(source) = self.confirm_durability().await {
                return Err(Error::SubscriptionDurabilityUnknown {
                    envelope: Box::new(envelope),
                    source,
                });
            }
            return Ok(true);
        }
        Err(StreamError::IdempotencyMismatch.into())
    }
}
