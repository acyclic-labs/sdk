//! Generation-fenced unit reservations in an actor's existing checkpoint.
//! Request decisions use committed invocation; cleanup uses its sole alarm.
use crate::{
    guest::{AlarmChange, Delivery, GuestError},
    invocation::Invocation,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod client;
pub use client::{Pool, Publication};
mod package;
pub use package::{Package, codec, initial_checkpoint};

/// Companion creation for a lazy reservation actor. Its empty initialization
/// record creates the inbox in the same commit as state and discovery, so a
/// continuous host can subscribe before the first reserve request arrives.
pub fn create<P: acyclic_stream::StreamProvider + ?Sized>(
    store: &crate::local::Store<P>,
    actor: &crate::addressing::ActorReference,
    inbox: acyclic_stream::StreamPath,
    capacity: u64,
) -> Result<[crate::guest::Effect; 3], crate::local::Error> {
    use acyclic_stream::{CommitCondition, CommitMutation};
    let [state, activation] = store.create(
        actor,
        BTreeMap::from([(
            "inbox".into(),
            crate::guest::Subscription {
                path: inbox.clone(),
                cursor: 0,
            },
        )]),
        Some(initial_checkpoint(capacity)?),
    )?;
    Ok([
        state,
        activation,
        crate::guest::Effect {
            conditions: vec![CommitCondition::Absent {
                path: inbox.clone(),
            }],
            mutation: CommitMutation::Append {
                path: inbox,
                records: vec![Bytes::new().into()],
            },
        },
    ])
}

/// Exact external owner identity and its host-admitted evidence. Actors retain
/// these bytes; the admitting host verifies their authority and physical owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerEvidence {
    identity: Bytes,
    evidence: Bytes,
}
impl OwnerEvidence {
    /// Retain nonempty original identity and evidence, without reinterpreting it.
    pub fn new(identity: Bytes, evidence: Bytes) -> Result<Self, GuestError> {
        if identity.is_empty() || evidence.is_empty() {
            return Err(invalid());
        }
        Ok(Self { identity, evidence })
    }
    /// Original placement or executor identity.
    pub const fn identity(&self) -> &Bytes {
        &self.identity
    }
    /// Original evidence supplied by the admitting host.
    pub const fn evidence(&self) -> &Bytes {
        &self.evidence
    }
}

/// One grant. Its generation never reuses an earlier grant's fence in this pool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pool: Bytes,
    state_path: acyclic_stream::StreamPath,
    request_id: Bytes,
    generation: u64,
    units: u64,
    owner: OwnerEvidence,
    deadline_unix_ms: u64,
}
impl Grant {
    /// Original pool actor's canonical identity bytes.
    pub const fn pool(&self) -> &Bytes {
        &self.pool
    }
    /// Original pool state path, including the host-injected Store root.
    pub const fn state_path(&self) -> &acyclic_stream::StreamPath {
        &self.state_path
    }
    /// Original reserve request identity.
    pub const fn request_id(&self) -> &Bytes {
        &self.request_id
    }
    /// Monotonically increasing pool fence.
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    /// Units retained until release or logical expiry.
    pub const fn units(&self) -> u64 {
        self.units
    }
    /// Original owner and authority evidence.
    pub const fn owner(&self) -> &OwnerEvidence {
        &self.owner
    }
    /// Logical lease deadline; this does not attest physical execution death.
    pub const fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }
}

/// Durable result returned with the actor's committed revision and receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "grant",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum Decision {
    /// Units assigned to the original request and owner.
    Granted(Grant),
    /// Explicit pool capacity could not fund the request.
    Exhausted,
    /// The requested deadline was already elapsed at delivery.
    Expired,
    /// The exact active grant was released.
    Released,
    /// A release does not name the exact active generation and owner.
    Stale,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum Request {
    Reserve {
        request_id: Bytes,
        units: u64,
        owner: OwnerEvidence,
        deadline_unix_ms: u64,
    },
    Release {
        grant: Grant,
    },
}
impl Request {
    fn operation(&self) -> Result<Bytes, GuestError> {
        match self {
            Self::Reserve { request_id, .. } if !request_id.is_empty() => {
                let mut id = b"reserve\0".to_vec();
                id.extend_from_slice(request_id);
                Ok(id.into())
            }
            Self::Release { grant } => {
                use sha2::{Digest, Sha256};
                let mut id = b"release\0".to_vec();
                id.extend_from_slice(&Sha256::digest(encode(grant)?));
                Ok(id.into())
            }
            _ => Err(invalid()),
        }
    }
    fn invocation(
        &self,
        actor: crate::addressing::ActorReference,
        subscription: String,
    ) -> Result<Invocation, GuestError> {
        if let Self::Reserve {
            units,
            request_id,
            owner,
            ..
        } = self
        {
            if *units == 0
                || request_id.is_empty()
                || owner.identity.is_empty()
                || owner.evidence.is_empty()
            {
                return Err(invalid());
            }
        }
        Invocation::new(actor, subscription, self.operation()?, encode(self)?)
    }
}

/// Reservation state embedded in the owning actor's original checkpoint.
/// There is no independent journal, scheduler or replay implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Book {
    capacity: u64,
    used: u64,
    generation: u64,
    // Deadline first: cleanup visits due grants, and release addresses one grant.
    active: BTreeMap<u64, BTreeMap<u64, Grant>>,
}

/// One reservation transition to join to the artifact's existing checkpoint.
pub struct Step {
    /// Response for the original committed invocation; absent on an alarm.
    pub response: Option<Bytes>,
    /// Earliest remaining expiry on the actor's generation-fenced alarm.
    pub alarm: AlarmChange,
}

impl Book {
    /// Explicit positive capacity; there is no SDK-selected pool limit.
    pub fn new(capacity: u64) -> Result<Self, GuestError> {
        if capacity == 0 {
            return Err(invalid());
        }
        Ok(Self {
            capacity,
            used: 0,
            generation: 0,
            active: BTreeMap::new(),
        })
    }
    /// Explicit total units in this pool.
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }
    /// Units in the committed active grants, including overdue cleanup.
    pub const fn used(&self) -> u64 {
        self.used
    }
    /// Earliest pending logical expiry, for alarm/restart observation.
    pub fn deadline(&self) -> Option<u64> {
        self.active.first_key_value().map(|(deadline, _)| *deadline)
    }

    /// Validate decoded checkpoint ownership before using the book. The host's
    /// existing checkpoint allowance bounds decoding and retained state.
    pub fn validate(&self) -> Result<(), GuestError> {
        let mut used = 0u64;
        let mut generations = std::collections::BTreeSet::new();
        for (deadline, grants) in &self.active {
            if *deadline == 0 || grants.is_empty() {
                return Err(invalid());
            }
            for (generation, grant) in grants {
                if *generation == 0
                    || *generation > self.generation
                    || *generation != grant.generation
                    || !generations.insert(*generation)
                    || *deadline != grant.deadline_unix_ms
                    || grant.units == 0
                    || grant.request_id.is_empty()
                    || grant.owner.identity.is_empty()
                    || grant.owner.evidence.is_empty()
                {
                    return Err(invalid());
                }
                used = used.checked_add(grant.units).ok_or_else(invalid)?;
                crate::addressing::ActorReference::decode(&grant.pool).map_err(|_| invalid())?;
            }
        }
        if self.capacity == 0 || used != self.used || used > self.capacity {
            return Err(invalid());
        }
        Ok(())
    }

    /// Apply an admitted invocation or its original alarm. Commit the changed
    /// book, returned response and alarm together through Handler::Transition.
    /// The artifact delegates its alarm to this book for a reservation owner.
    pub fn step(&mut self, delivery: &Delivery) -> Result<Step, GuestError> {
        let request = match delivery.invocation()? {
            Some(invocation) => {
                let request: Request = decode(invocation.request())?;
                if request.operation()? != *invocation.operation() {
                    return Err(invalid());
                }
                Some(request)
            }
            None if matches!(delivery.trigger, crate::guest::Trigger::Alarm { .. }) => None,
            None if delivery
                .record
                .as_ref()
                .map(crate::invocation::Invocation::decode)
                .transpose()?
                .flatten()
                .is_some() =>
            {
                None
            }
            None if matches!(delivery.trigger, crate::guest::Trigger::Record { .. })
                && delivery.record.as_ref().is_some_and(|record| {
                    record.value.is_empty()
                        && record.headers.keys().is_empty()
                        && record.headers.attributes().is_empty()
                }) =>
            {
                None
            }
            None => return Err(invalid()),
        };
        let previous_deadline = self.deadline();
        while self
            .deadline()
            .is_some_and(|deadline| deadline <= delivery.now_unix_ms)
        {
            let (_, grants) = self.active.pop_first().ok_or_else(invalid)?;
            for grant in grants.into_values() {
                self.used = self.used.checked_sub(grant.units).ok_or_else(invalid)?;
            }
        }
        let decision = match request {
            None => None,
            Some(Request::Reserve {
                request_id,
                units,
                owner,
                deadline_unix_ms,
            }) => {
                if units == 0
                    || request_id.is_empty()
                    || owner.identity.is_empty()
                    || owner.evidence.is_empty()
                {
                    return Err(invalid());
                }
                Some(if deadline_unix_ms <= delivery.now_unix_ms {
                    Decision::Expired
                } else if units > self.capacity.checked_sub(self.used).ok_or_else(invalid)? {
                    Decision::Exhausted
                } else {
                    self.generation = self.generation.checked_add(1).ok_or_else(invalid)?;
                    let grant = Grant {
                        pool: delivery.actor.encode().into(),
                        state_path: delivery.subscriptions.state_path().clone(),
                        request_id,
                        generation: self.generation,
                        units,
                        owner,
                        deadline_unix_ms,
                    };
                    self.active
                        .entry(deadline_unix_ms)
                        .or_default()
                        .insert(self.generation, grant.clone());
                    self.used += units;
                    Decision::Granted(grant)
                })
            }
            Some(Request::Release { grant }) => {
                let grants = self.active.get_mut(&grant.deadline_unix_ms);
                Some(match grants {
                    Some(grants)
                        if grant.pool.as_ref() == delivery.actor.encode().as_slice()
                            && grant.state_path == *delivery.subscriptions.state_path()
                            && grants.get(&grant.generation) == Some(&grant) =>
                    {
                        grants.remove(&grant.generation);
                        self.used = self.used.checked_sub(grant.units).ok_or_else(invalid)?;
                        if grants.is_empty() {
                            self.active.remove(&grant.deadline_unix_ms);
                        }
                        Decision::Released
                    }
                    _ => Decision::Stale,
                })
            }
        };
        let deadline = self.deadline();
        Ok(Step {
            response: decision.as_ref().map(encode).transpose()?,
            alarm: if deadline == previous_deadline {
                AlarmChange::Keep
            } else {
                deadline.map_or(AlarmChange::Clear, |deadline_unix_ms| AlarmChange::Set {
                    deadline_unix_ms,
                })
            },
        })
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Bytes, GuestError> {
    serde_json::to_vec(value)
        .map(Into::into)
        .map_err(|_| invalid())
}
fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T, GuestError> {
    let value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if encode(&value)?.as_ref() != bytes {
        return Err(invalid());
    }
    Ok(value)
}
fn invalid() -> GuestError {
    GuestError::Decode("invalid fenced reservation".into())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
