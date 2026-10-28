use super::{Decision, Grant, OwnerEvidence, Request, decode};
use crate::{
    addressing::ActorReference,
    invocation::Invocation,
    local::{CommittedInvocation, Error, Store},
};
use acyclic_stream::StreamProvider;
use bytes::Bytes;

/// Public reservation client for an admitted actor running the shared Book.
/// The provider and pool actor are original host-injected capabilities.
pub struct Pool<'a, P: StreamProvider + ?Sized> {
    store: &'a Store<P>,
    actor: &'a ActorReference,
    subscription: &'a str,
}

/// Original reservation decision with its committed revision and receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    decision: Decision,
    invocation: CommittedInvocation,
}
impl Publication {
    /// Original immutable decision, including a recorded refusal.
    pub const fn decision(&self) -> &Decision {
        &self.decision
    }
    /// Original publication evidence; replay returns the same commit.
    pub const fn invocation(&self) -> &CommittedInvocation {
        &self.invocation
    }
}

impl<'a, P: StreamProvider + ?Sized> Pool<'a, P> {
    /// Bind to the original actor and its declared reservation subscription.
    pub const fn new(
        store: &'a Store<P>,
        actor: &'a ActorReference,
        subscription: &'a str,
    ) -> Self {
        Self {
            store,
            actor,
            subscription,
        }
    }

    /// Reserve explicit units under a request identity, owner evidence and
    /// logical expiry. Changed arguments under the same identity are refused.
    pub async fn reserve(
        &self,
        units: u64,
        request_id: Bytes,
        owner: OwnerEvidence,
        deadline_unix_ms: u64,
    ) -> Result<Publication, Error> {
        self.call(Request::Reserve {
            units,
            request_id,
            owner,
            deadline_unix_ms,
        })
        .await
    }

    /// Release only the exact original generation and owner. A stale or
    /// altered grant cannot release a newer grant, even after capacity reuse.
    pub async fn release(&self, grant: &Grant) -> Result<Publication, Error> {
        if grant.pool.as_ref() != self.actor.encode().as_slice()
            || grant.state_path != self.store.path(self.actor)?
        {
            return Err(Error::Conflict);
        }
        self.call(Request::Release {
            grant: grant.clone(),
        })
        .await
    }

    async fn call(&self, request: Request) -> Result<Publication, Error> {
        let invocation: Invocation =
            request.invocation(self.actor.clone(), self.subscription.into())?;
        let committed = self.store.invoke(&invocation).await?;
        let decision = decode(committed.response())?;
        let matches = match (&request, &decision) {
            (
                Request::Reserve {
                    request_id,
                    units,
                    owner,
                    deadline_unix_ms,
                },
                Decision::Granted(grant),
            ) => {
                grant.pool.as_ref() == self.actor.encode().as_slice()
                    && grant.state_path == self.store.path(self.actor)?
                    && grant.request_id == *request_id
                    && grant.units == *units
                    && grant.owner == *owner
                    && grant.deadline_unix_ms == *deadline_unix_ms
                    && grant.generation > 0
            }
            (Request::Reserve { .. }, Decision::Exhausted | Decision::Expired)
            | (Request::Release { .. }, Decision::Released | Decision::Stale) => true,
            _ => false,
        };
        if !matches {
            return Err(Error::Invalid("reservation response changes its request"));
        }
        Ok(Publication {
            decision,
            invocation: committed,
        })
    }
}
