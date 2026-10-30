//! The actor's original state cut owns metadata, journal and alarm publication.
use super::{Alarm, Error, Publication, Snapshot, State, Store, change_alarm, owned};
use crate::guest::{AlarmChange, Effect};
use acyclic_stream::{
    CommitCondition, CommitMutation, IdempotencyKey, ReadRequest, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;

/// Synchronous transaction borrowed from one admitted actor. Failure discards
/// its metadata, journal appends and alarm together. No borrow escapes the call.
pub struct ActorStateTransaction<'a> {
    state: &'a mut State,
    journal: &'a mut Vec<Bytes>,
    undo: Vec<(String, Option<Bytes>)>,
    alarm: Alarm,
    journal_tail: u64,
    journal_len: usize,
    accepted: bool,
}
impl ActorStateTransaction<'_> {
    /// Read this transaction's own writes and its original committed cut.
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.state.metadata.get(key)
    }
    /// Stage metadata; only a successful publication confirms durability.
    pub fn put(&mut self, key: String, value: Bytes) {
        let previous = self.state.metadata.insert(key.clone(), value);
        self.undo.push((key, previous));
    }
    /// Enumerate this actor's metadata in canonical key order.
    pub fn entries(&self) -> impl ExactSizeIterator<Item = (&str, &Bytes)> {
        self.state.metadata()
    }
    /// Retained and staged journal frames in this transaction.
    pub const fn journal_tail(&self) -> u64 {
        self.state.journal_tail
    }
    /// Append opaque journal frames at the exact expected ordinal. The journal
    /// path is host-derived from this actor; the guest cannot select a stream.
    pub fn append_journal(&mut self, expected: u64, frames: Vec<Bytes>) -> Result<(), Error> {
        if expected != self.state.journal_tail {
            return Err(Error::Conflict);
        }
        if frames.is_empty() {
            return Err(Error::Invalid("empty actor journal append"));
        }
        let count =
            u64::try_from(frames.len()).map_err(|_| Error::Invalid("journal tail overflow"))?;
        self.state.journal_tail = expected
            .checked_add(count)
            .ok_or(Error::Invalid("journal tail overflow"))?;
        self.journal.extend(frames);
        Ok(())
    }
    /// Replace or clear the original Actor alarm. The original alarm owner
    /// delivers its admitted generation as GuestEvent; no guest timer is made.
    pub fn alarm(&mut self, deadline_unix_ms: Option<u64>) -> Result<(), Error> {
        change_alarm(
            &mut self.state.alarm,
            match deadline_unix_ms {
                Some(deadline_unix_ms) => AlarmChange::Set { deadline_unix_ms },
                None => AlarmChange::Clear,
            },
        )
    }
}
impl Drop for ActorStateTransaction<'_> {
    fn drop(&mut self) {
        if self.accepted {
            return;
        }
        for (key, previous) in self.undo.drain(..).rev() {
            match previous {
                Some(value) => {
                    self.state.metadata.insert(key, value);
                }
                None => {
                    self.state.metadata.remove(&key);
                }
            }
        }
        self.state.alarm = self.alarm.clone();
        self.state.journal_tail = self.journal_tail;
        self.journal.truncate(self.journal_len);
    }
}

/// Local producer for one admitted actor's public durable state binding.
/// Metadata and the journal's retained tail share its original exact state cut.
/// Journal history stays paged in the canonical Streams subtree, never copied
/// into a growing checkpoint. One conditional commit publishes metadata, alarm
/// and all staged journal frames. Cancellation or failed publication fences the
/// binding; Error::Unknown retains the original publication, never redispatch.
pub struct ActorStateBinding<'a, P: StreamProvider + ?Sized> {
    store: &'a Store<P>,
    snapshot: Snapshot,
    epoch: u64,
    journal: Vec<Bytes>,
    committed_alarm: Alarm,
    committed_journal_tail: u64,
    dirty: bool,
    fenced: bool,
}
impl<P: StreamProvider + ?Sized> Store<P> {
    /// Project an already admitted actor cut; this grants no new authority.
    pub fn bind_state(
        &self,
        snapshot: Snapshot,
        epoch: u64,
    ) -> Result<ActorStateBinding<'_, P>, Error> {
        let state = owned(&snapshot, epoch)?;
        let committed_alarm = state.alarm.clone();
        let committed_journal_tail = state.journal_tail;
        self.actor(snapshot.path())?;
        Ok(ActorStateBinding {
            store: self,
            snapshot,
            epoch,
            journal: Vec::new(),
            committed_alarm,
            committed_journal_tail,
            dirty: false,
            fenced: false,
        })
    }
}
impl<P: StreamProvider + ?Sized> ActorStateBinding<'_, P> {
    /// Seed the shared JavaScript projection from this exact admitted cut.
    pub fn seed(&self) -> Result<crate::guest::ActorStateSeed, Error> {
        if self.fenced || self.dirty {
            return Err(Error::Conflict);
        }
        let state = owned(&self.snapshot, self.epoch)?;
        Ok(crate::guest::ActorStateSeed {
            metadata: state
                .metadata
                .iter()
                .map(|(key, value)| crate::guest::ActorMetadataWrite {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            journal_tail: state.journal_tail.to_string(),
        })
    }

    /// Apply the public JavaScript projection's values through the SAME native
    /// transaction path. The packet supplies no actor, epoch, stream or key.
    /// The host then calls flush once under its retained invocation identity.
    pub fn apply(&mut self, commit: crate::guest::ActorStateCommit) -> Result<(), Error> {
        let from = decimal(&commit.journal_from)?;
        self.transaction(|state| {
            if state.journal_tail() != from {
                return Err(Error::Conflict);
            }
            for write in commit.writes {
                state.put(write.key, write.value);
            }
            if !commit.journal.is_empty() {
                state.append_journal(from, commit.journal)?;
            }
            match commit.alarm {
                crate::guest::ActorStateAlarm::Keep => Ok(()),
                crate::guest::ActorStateAlarm::Clear => state.alarm(None),
                crate::guest::ActorStateAlarm::Set { deadline } => {
                    state.alarm(Some(decimal(&deadline)?))
                }
            }
        })
    }

    /// Return the updated cut to the original delivery/checkpoint owner.
    /// Pending or uncertain work cannot disappear behind an older snapshot.
    pub fn finish(self) -> Result<Snapshot, Error> {
        if self.fenced || self.dirty {
            return Err(Error::Conflict);
        }
        Ok(self.snapshot)
    }
    /// Run synchronously. Callback failure leaves the previous pending group
    /// intact, including on unwind. Reads borrow the retained cut without
    /// allocation, I/O or marking it dirty. Publication confirms durability.
    pub fn transaction<T>(
        &mut self,
        action: impl FnOnce(&mut ActorStateTransaction<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.fenced {
            return Err(Error::Conflict);
        }
        let state = self.snapshot.state.as_mut().ok_or(Error::Conflict)?;
        if state.paused {
            return Err(Error::Paused);
        }
        let mut transaction = ActorStateTransaction {
            alarm: state.alarm.clone(),
            journal_tail: state.journal_tail,
            journal_len: self.journal.len(),
            state,
            journal: &mut self.journal,
            undo: Vec::new(),
            accepted: false,
        };
        let value = action(&mut transaction)?;
        self.dirty |= !transaction.undo.is_empty()
            || transaction.alarm != transaction.state.alarm
            || transaction.journal_tail != transaction.state.journal_tail;
        transaction.accepted = true;
        Ok(value)
    }
    /// Page only the journal frames at this binding's committed cut. Later
    /// owner epochs cannot leak into a stale binding's replay. Staged frames
    /// remain transaction-local until flush confirms their publication.
    pub async fn read_journal(
        &self,
        from: u64,
        limit: u32,
    ) -> Result<Vec<acyclic_stream::OwnedBytes>, Error> {
        if self.fenced {
            return Err(Error::Conflict);
        }
        if from > self.committed_journal_tail || limit == 0 {
            return Err(Error::Invalid("invalid actor journal page"));
        }
        let count = (self.committed_journal_tail - from).min(u64::from(limit));
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut records = self
            .store
            .provider()
            .read(ReadRequest {
                path: self.journal_path()?,
                from,
                limit: u32::try_from(count)
                    .map_err(|_| Error::Invalid("invalid journal page bound"))?,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        let mut frames = Vec::new();
        for sequence in from..from + count {
            let record = records
                .try_next()
                .await?
                .ok_or(Error::Invalid("actor journal gap"))?;
            if record.sequence != sequence {
                return Err(Error::Invalid("actor journal sequence changed"));
            }
            frames.push(record.value);
        }
        if records.try_next().await?.is_some() {
            return Err(Error::Invalid("actor journal page exceeds cut"));
        }
        Ok(frames)
    }
    /// Resolve after ONE original conditional commit of metadata, journal and
    /// alarm. No input cursor is consumed. The host supplies the invocation's
    /// stable key and must retain this returned cut for its next checkpoint.
    pub async fn flush(&mut self, key: IdempotencyKey) -> Result<(), Error> {
        if self.fenced {
            return Err(Error::Conflict);
        }
        if !self.dirty {
            return Ok(());
        }
        self.fenced = true;
        let effects = self.effects(AlarmChange::Keep)?;
        let state = self.snapshot.state.take().ok_or(Error::Conflict)?;
        let prepared = self.store.prepare(&self.snapshot, state, effects, key)?;
        self.snapshot = self
            .store
            .submit_retained(prepared)
            .await?
            .ok_or(Error::Invalid("state commit omitted its original receipt"))?;
        let state = owned(&self.snapshot, self.epoch)?;
        self.committed_alarm = state.alarm.clone();
        self.committed_journal_tail = state.journal_tail;
        self.dirty = false;
        self.fenced = false;
        Ok(())
    }

    /// Join staged metadata, journal and alarm with the delivery's checkpoint,
    /// consumed cursor, response and effects. Submit the returned publication
    /// once through the original Store; no preceding flush is needed.
    pub fn prepare_publish(
        mut self,
        trigger: &crate::guest::Trigger,
        delivered: Option<&acyclic_stream::Record>,
        mut transition: crate::guest::Transition,
        key: IdempotencyKey,
    ) -> Result<super::PreparedPublish, Error> {
        if self.fenced {
            return Err(Error::Conflict);
        }
        let journal = self.journal_path()?;
        if transition
            .effects
            .iter()
            .any(|effect| super::mutation_targets(&effect.mutation, &journal))
        {
            return Err(Error::Invalid(
                "guest effect targets the host's actor journal",
            ));
        }
        transition.effects.extend(self.effects(transition.alarm)?);
        let state = self.snapshot.state.take().ok_or(Error::Conflict)?;
        let mut prepared = self.store.prepare_publication(
            &self.snapshot,
            state,
            &self.committed_alarm,
            Publication {
                trigger,
                delivered,
                transition,
                key,
            },
        )?;
        prepared.journal = Some(journal);
        Ok(prepared)
    }

    fn effects(&mut self, alarm: AlarmChange) -> Result<Vec<Effect>, Error> {
        let mut effects = if self.journal.is_empty() {
            Vec::new()
        } else {
            let path = self.journal_path()?;
            vec![Effect {
                conditions: vec![CommitCondition::Tail {
                    path: path.clone(),
                    expected: self.committed_journal_tail,
                }],
                mutation: CommitMutation::Append {
                    path,
                    records: std::mem::take(&mut self.journal)
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                },
            }]
        };
        if alarm != AlarmChange::Keep
            || owned(&self.snapshot, self.epoch)?.alarm != self.committed_alarm
        {
            // The same durable activation feed drives the original runtime's
            // ready index and sole timer. A new deadline (or clear) must wake
            // an already idle actor, whether staged here or in the delivery's
            // transition. Both changes share one activation entry and commit.
            effects.push(
                self.store
                    .activation(&self.store.actor(self.snapshot.path())?),
            );
        }
        Ok(effects)
    }
    fn journal_path(&self) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/journal",
            self.snapshot.path()
        ))?)
    }
}

fn decimal(value: &str) -> Result<u64, Error> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| Error::Invalid("invalid actor state position"))?;
    if parsed.to_string() != value {
        return Err(Error::Invalid("noncanonical actor state position"));
    }
    Ok(parsed)
}
