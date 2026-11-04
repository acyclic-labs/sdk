//! Keyed admission and settled results on the canonical Streams commit boundary.
use super::journal_codec::{Completed, Intent, decode as decode_bytes, encode};
use super::*;
use acyclic_actors::call::{self, CallPath};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitReceipt,
    CommitRequest, PathHead, ReadRequest, StreamError, StreamPath, StreamProvider,
    StreamProviderExt as _,
};

/// Storage of one admitted Machine incarnation. No process-local result map,
/// resource scan, periodic observer or second durability barrier participates.
pub(crate) struct Journal {
    streams: Arc<dyn StreamProvider>,
    root: StreamPath,
    machine: MachineId,
    // At most one physical transition runs under the incarnation's admission
    // lock. This is its live producer, never an operation/result registry.
    producer: Mutex<Option<(OperationId, tokio::sync::watch::Receiver<bool>)>>,
    // Followers borrow lifetime, never retain the incarnation's producer.
    lifetime: std::sync::OnceLock<tokio::sync::watch::Sender<()>>,
}

pub(crate) enum Admission {
    Original(Option<Publication>),
    Replay(MutationOutcome),
}

pub(crate) struct Publication {
    commit: CommitId,
    machine_tail: u64,
    producer: tokio::sync::watch::Sender<bool>,
}

// Machine lifecycle calls have one acceptance and one terminal, with their
// request delivered inline. The shared call fold alone owns protocol state.
struct CallSnapshot {
    journal: call::Journal,
    digest: Option<[u8; 32]>,
    completed: Option<MutationOutcome>,
    preceding: Option<CommitId>,
}

impl CallSnapshot {
    fn new(path: &CallPath) -> Self {
        Self {
            journal: call::Journal::new(path.log()),
            digest: None,
            completed: None,
            preceding: None,
        }
    }

    fn apply(&mut self, record: &acyclic_stream::Record) -> Result<(), ProviderError> {
        let digest = match self.journal.apply(record).map_err(call_error)? {
            call::Entry::Accepted(accepted) if accepted.fence.is_none() => {
                Some(decode_bytes::<Intent>(accepted.executor.as_ref())?.digest)
            }
            call::Entry::Terminal(call::Outcome::Ok(body)) => {
                let completed = decode_bytes::<Completed>(body.as_ref())?;
                self.completed = Some(completed.outcome);
                Some(completed.digest)
            }
            call::Entry::Terminal(_) => None,
            _ => {
                return Err(ProviderError::Rejected(
                    "native Machine call requires its original inline acceptance and terminal"
                        .into(),
                ));
            }
        };
        if let Some(digest) = digest {
            if self.digest.is_some_and(|original| original != digest) {
                return Err(ProviderError::Rejected(
                    "Machine call intent changed at completion".into(),
                ));
            }
            self.digest = Some(digest);
        }
        self.preceding = Some(record.commit_id);
        Ok(())
    }

    fn phase(&self) -> OperationPhase {
        match self.journal.outcome() {
            None => OperationPhase::Pending,
            Some(call::Outcome::Ok(_)) => OperationPhase::Succeeded,
            Some(call::Outcome::Error(_)) => OperationPhase::Failed,
            Some(call::Outcome::Cancelled(_)) => OperationPhase::Cancelled,
            Some(call::Outcome::Indeterminate(_)) => OperationPhase::Indeterminate,
        }
    }

    fn outcome(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        match self.journal.outcome() {
            Some(call::Outcome::Ok(_)) => self.completed.clone().ok_or_else(|| {
                ProviderError::Rejected("Machine call terminal has no decoded completion".into())
            }),
            Some(call::Outcome::Cancelled(_)) => Err(ProviderError::Cancelled),
            Some(call::Outcome::Error(_)) => Err(ProviderError::Failed),
            Some(call::Outcome::Indeterminate(_)) | None => Err(ProviderError::Indeterminate(key)),
        }
    }
}

impl Journal {
    pub(crate) fn new(
        streams: Arc<dyn StreamProvider>,
        root: StreamPath,
        machine: MachineId,
    ) -> Self {
        Self {
            streams,
            root,
            machine,
            producer: Mutex::new(None),
            lifetime: std::sync::OnceLock::new(),
        }
    }

    fn path(&self, suffix: impl fmt::Display) -> Result<StreamPath, ProviderError> {
        StreamPath::new(format!("{}/{suffix}", self.root)).map_err(storage)
    }

    fn call(&self, key: IdempotencyKey) -> Result<CallPath, ProviderError> {
        CallPath::new(&self.root, &key.to_string()).map_err(call_error)
    }

    async fn read_call(&self, path: &CallPath) -> Result<CallSnapshot, ProviderError> {
        let mut snapshot = CallSnapshot::new(path);
        let mut records = match self
            .streams
            .read(ReadRequest {
                path: path.log().clone(),
                from: 0,
                // Two lifecycle records plus one malformed-extra-record control.
                limit: 3,
                at_least: Default::default(),
                key: None,
            })
            .await
        {
            Err(StreamError::NotFound) => return Ok(snapshot),
            result => result.map_err(storage)?.take(3),
        };
        while let Some(record) = records.next().await {
            snapshot.apply(&record.map_err(storage)?)?;
        }
        Ok(snapshot)
    }

    async fn existing_call(&self, path: &CallPath) -> Result<CallSnapshot, ProviderError> {
        let snapshot = self.read_call(path).await?;
        if snapshot.journal.next() == 0 {
            return Err(ProviderError::NotFound("Machine call has no original record; recovery: retain its original supplied journal and key".into()));
        }
        Ok(snapshot)
    }

    async fn head(&self, path: StreamPath) -> Result<PathHead, ProviderError> {
        match self.streams.head(path).await {
            Err(StreamError::NotFound) => Ok(PathHead {
                tail: 0,
                last: None,
                durable: 0,
                retained_through: None,
                origin: None,
            }),
            value => value.map_err(storage),
        }
    }

    pub(crate) fn machine_id(&self) -> MachineId {
        self.machine
    }

    /// Keys a provider-initiated transition by the incarnation's publication
    /// tail: fresh for each lifecycle position, and shared by every request that
    /// observed the same position, so concurrent triggers join one operation.
    pub(crate) async fn implicit_key(
        &self,
        purpose: &[u8],
    ) -> Result<IdempotencyKey, ProviderError> {
        let path = self.path(format_args!("machines/{}", self.machine))?;
        let tail = self.head(path).await?.tail;
        let mut digest = Sha256::new();
        digest.update(b"acyclic-machines-implicit-transition-v1");
        digest.update(purpose);
        digest.update(self.machine.as_bytes());
        digest.update(tail.to_be_bytes());
        Ok(IdempotencyKey::from_digest(digest.finalize().into()))
    }

    pub(crate) async fn machine(&self) -> Result<MachineObservation, ProviderError> {
        let path = self.path(format_args!("machines/{}", self.machine))?;
        decode(self.head(path).await?)
    }

    pub(crate) async fn enroll(
        &self,
        machine: MachineObservation,
    ) -> Result<MachineObservation, ProviderError> {
        match self.machine().await {
            Ok(retained) if retained.contract != machine.contract => Err(ProviderError::Conflict(
                "enrollment differs from its retained incarnation; recovery: retain the original contract".into(),
            )),
            Ok(retained) if retained.state == MachineState::Destroyed => Ok(retained),
            Ok(_) => Err(ProviderError::Conflict(
                "Machine incarnation already has a physical owner; recovery: clone its original provider; after owner loss retain uncertainty and enroll a new incarnation".into(),
            )),
            Err(ProviderError::NotFound(_)) => {
                let path = self.path(format_args!("machines/{}", self.machine))?;
                self.commit(
                    IdempotencyKey::new(),
                    b"enroll",
                    vec![CommitCondition::Absent { path: path.clone() }],
                    vec![append(path, &machine)?],
                    || {},
                ).await?;
                Ok(machine)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn begin<C: FnOnce()>(
        &self,
        key: IdempotencyKey,
        digest: [u8; 32],
        physical: Option<&MachineObservation>,
        validate: impl FnOnce() -> Result<C, ProviderError>,
    ) -> Result<Admission, ProviderError> {
        let path = self.call(key)?;
        let original = self.read_call(&path).await?;
        if original.journal.next() != 0 {
            if original.digest.is_some_and(|original| original != digest) {
                return Err(ProviderError::Conflict("mutation key names another intent; recovery: retain the original arguments or use a new key".into()));
            }
            return original.outcome(key).map(Admission::Replay);
        }
        let close_admission = validate()?;
        let Some(machine) = physical else {
            return Ok(Admission::Original(None));
        };
        let machine_path = self.path(format_args!("machines/{}", self.machine))?;
        let current = self.head(machine_path.clone()).await?;
        let mut accepted = path
            .accept(
                &call::Accepted {
                    executor: encode(&Intent { digest })?.into(),
                    fence: None,
                },
                false,
            )
            .map_err(call_error)?;
        accepted
            .conditions
            .push(condition(machine_path.clone(), &current)?);
        accepted.mutations.push(append(machine_path, machine)?);
        // Only original physical admission needs a live completion owner.
        // Replay and already-settled logical changes allocate no producer.
        let (producer, receiver) = tokio::sync::watch::channel(false);
        *self.producer.lock().await = Some((OperationId(key.0), receiver));
        let receipt = self
            .commit(
                key,
                b"admit",
                accepted.conditions,
                accepted.mutations,
                close_admission,
            )
            .await?;
        Ok(Admission::Original(Some(Publication {
            commit: receipt.commit_id,
            machine_tail: current.tail + 1,
            producer,
        })))
    }

    pub(crate) async fn finish(
        &self,
        key: IdempotencyKey,
        digest: [u8; 32],
        admitted: Option<Publication>,
        machine: &MachineObservation,
        outcome: MutationOutcome,
    ) -> Result<MutationOutcome, ProviderError> {
        let path = self.call(key)?;
        let machine_path = self.path(format_args!("machines/{}", self.machine))?;
        let (machine_condition, operation_condition) = match &admitted {
            Some(publication) => (
                CommitCondition::Head {
                    path: machine_path.clone(),
                    expected: publication.machine_tail,
                    commit_id: publication.commit,
                },
                CommitCondition::Head {
                    path: path.log().clone(),
                    expected: 1,
                    commit_id: publication.commit,
                },
            ),
            None => (
                condition(
                    machine_path.clone(),
                    &self
                        .head(machine_path.clone())
                        .await
                        .map_err(|_| ProviderError::Indeterminate(key))?,
                )?,
                CommitCondition::Tail {
                    path: path.log().clone(),
                    expected: 0,
                },
            ),
        };
        let mut terminal = path
            .settle(
                None,
                &call::Outcome::Ok(
                    encode(&Completed {
                        digest,
                        outcome: &outcome,
                    })?
                    .into(),
                ),
            )
            .map_err(call_error)?;
        terminal
            .conditions
            .extend([machine_condition, operation_condition]);
        terminal.mutations.push(append(machine_path, machine)?);
        self.commit(
            key,
            b"settle",
            terminal.conditions,
            terminal.mutations,
            || {},
        )
        .await
        .map_err(|_| ProviderError::Indeterminate(key))?;
        if let Some(publication) = admitted {
            publication.producer.send_replace(true);
        }
        Ok(outcome)
    }

    async fn commit(
        &self,
        key: IdempotencyKey,
        phase: &[u8],
        conditions: Vec<CommitCondition>,
        mutations: Vec<CommitMutation>,
        on_submit: impl FnOnce(),
    ) -> Result<CommitReceipt, ProviderError> {
        let mut identity = Sha256::new();
        identity.update(self.root.as_str().as_bytes());
        identity.update(key.as_bytes());
        identity.update(phase);
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions,
            mutations,
            idempotency_key: acyclic_stream::IdempotencyKey::new(identity.finalize().to_vec())
                .map_err(storage)?,
        };
        // All reads, serialization, key admission and lock waits precede this
        // cut. Closing admission and submitting the original commit cannot be
        // separated by cancellation or a fallible preparation step.
        on_submit();
        match self
            .streams
            .commit(request)
            .await
            .map_err(|_| ProviderError::Indeterminate(key))?
        {
            CommitOutcome::Committed(receipt) => Ok(receipt),
            CommitOutcome::Conflict(_) => Err(ProviderError::Conflict(
                "Machine publication changed; recovery: recover the original mutation key".into(),
            )),
        }
    }

    pub(crate) async fn recover(
        &self,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.existing_call(&self.call(key)?).await?.outcome(key)
    }

    pub(crate) async fn inspect(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        let key = IdempotencyKey(operation.0);
        let value = self.existing_call(&self.call(key)?).await?;
        Ok(OperationObservation {
            id: operation,
            phase: value.phase(),
        })
    }

    pub(crate) async fn cancel(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        let key = IdempotencyKey(operation.0);
        let path = self.call(key)?;
        let original = self.existing_call(&path).await?;
        if original.journal.outcome().is_some() {
            return Ok(OperationObservation {
                id: operation,
                phase: original.phase(),
            });
        }
        let cancelled = path
            .cancel("original Machine caller cancelled")
            .map_err(call_error)?;
        match self
            .commit(
                key,
                b"cancel",
                cancelled.conditions,
                cancelled.mutations,
                || {},
            )
            .await
        {
            Ok(_) => Ok(OperationObservation {
                id: operation,
                phase: OperationPhase::Indeterminate,
            }),
            // Only a real terminal race justifies reading the winning outcome.
            Err(ProviderError::Conflict(_)) => self.inspect(operation).await,
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn watch(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let key = IdempotencyKey(operation.0);
        let path = self.call(key)?;
        // Retain the original producer before reading its publication. A later
        // transition can replace the slot, but cannot replace this handle.
        // Storage observation must never hold up lifecycle admission.
        let mut producer = {
            let active = self.producer.lock().await;
            active
                .as_ref()
                .filter(|(id, _)| *id == operation)
                .map(|(_, receiver)| receiver.clone())
        };
        let value = match &mut producer {
            Some(producer) => tokio::select! {
                biased;
                result = self.existing_call(&path) => result?,
                () = producer_lost(producer) => return Err(ProviderError::Indeterminate(key)),
            },
            None => self.existing_call(&path).await?,
        };
        let mut request =
            acyclic_stream::FollowRequest::new(path.log().clone(), value.journal.next());
        request.preceding = value.preceding;
        let initial = OperationObservation {
            id: operation,
            phase: value.phase(),
        };
        if initial.phase != OperationPhase::Pending {
            return Ok(stream::once(async move { Ok(initial) }).boxed());
        }
        let mut producer = producer.ok_or(ProviderError::Indeterminate(key))?;
        if !*producer.borrow() && producer.has_changed().is_err() {
            return Err(ProviderError::Indeterminate(key));
        }
        let records = tokio::select! {
            biased;
            result = self.streams.follow(request) => result.map_err(|_| ProviderError::Indeterminate(key))?,
            () = producer_lost(&mut producer) => return Err(ProviderError::Indeterminate(key)),
        };
        let updates = stream::try_unfold(
            (
                acyclic_stream::records_only(records),
                producer,
                value,
                false,
            ),
            move |(mut records, mut producer, mut value, done)| async move {
                if done {
                    return Ok(None);
                }
                let record = tokio::select! {
                    biased;
                    record = records.next() => record.and_then(Result::ok)
                        .ok_or(ProviderError::Indeterminate(key))?,
                    () = producer_lost(&mut producer) => return Err(ProviderError::Indeterminate(key)),
                };
                value.apply(&record)?;
                let phase = value.phase();
                Ok(Some((
                    OperationObservation {
                        id: operation,
                        phase,
                    },
                    (records, producer, value, phase != OperationPhase::Pending),
                )))
            },
        );
        Ok(stream::once(async move { Ok(initial) })
            .chain(updates)
            .boxed())
    }

    pub(crate) async fn follow_events(
        &self,
        after: Option<u64>,
    ) -> Result<EventStream, ProviderError> {
        let machine = self.machine;
        let path = self.path(format_args!("machines/{machine}"))?;
        let head = self.head(path.clone()).await?;
        let from = after.unwrap_or(0);
        if from > head.tail {
            return Err(ProviderError::Invalid(
                "Machine event cursor exceeds its tail".into(),
            ));
        }
        if from == head.tail && decode::<EventObservation>(head)?.state == MachineState::Destroyed {
            return Err(ProviderError::Conflict(
                "Machine event cursor is already terminal".into(),
            ));
        }
        let mut lifetime = self
            .lifetime
            .get_or_init(|| tokio::sync::watch::Sender::new(()))
            .subscribe();
        let request = acyclic_stream::FollowRequest::new(path, from);
        let records = tokio::select! {
            records = self.streams.follow(request) => records.map_err(storage)?,
            _ = lifetime.changed() => return Err(ProviderError::Unavailable),
        };
        let records = acyclic_stream::records_only(records);
        Ok(super::events::checked(
            machine,
            after,
            (records, lifetime),
            move |(mut records, mut lifetime)| async move {
                let record = tokio::select! {
                    biased;
                    record = records.next() => record.ok_or(ProviderError::Unavailable)?.map_err(storage)?,
                    _ = lifetime.changed() => return Err(ProviderError::Unavailable),
                };
                let event = machine_event(machine, record)?;
                Ok(Some((event, (records, lifetime))))
            },
        ))
    }

    pub(crate) async fn events(
        &self,
        after: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "event page is empty; recovery: provide positive page credit".into(),
            ));
        }
        let path = self.path(format_args!("machines/{}", self.machine))?;
        let head = self.head(path.clone()).await?;
        let from = after.unwrap_or(0);
        if from >= head.tail {
            return Ok(EventPage {
                events: Vec::new(),
                next_sequence: None,
            });
        }
        let mut records = self
            .streams
            .read(ReadRequest {
                path,
                from,
                limit,
                at_least: Default::default(),
                key: None,
            })
            .await
            .map_err(storage)?;
        let mut events = Vec::new();
        while let Some(record) = records.next().await {
            let record = record.map_err(storage)?;
            events.push(machine_event(self.machine, record)?);
        }
        let next_sequence = events
            .last()
            .and_then(|last| (last.sequence < head.tail).then_some(last.sequence));
        Ok(EventPage {
            events,
            next_sequence,
        })
    }
}

#[cfg(test)]
mod tests;

// Read-only projection of the original versioned observation envelope.
#[derive(Deserialize)]
struct EventObservation {
    id: MachineId,
    state: MachineState,
    changed_at_unix_ms: u64,
}

fn machine_event(
    machine: MachineId,
    record: acyclic_stream::Record,
) -> Result<MachineEvent, ProviderError> {
    // The journal owns the full contract. Event readers need only its state
    // projection and must not allocate a second contract or endpoint vector.
    let value: EventObservation = decode_bytes(record.value.as_ref())?;
    if value.id != machine {
        return Err(ProviderError::Rejected(
            "Machine event journal identity differs".into(),
        ));
    }
    Ok(MachineEvent {
        machine,
        sequence: record
            .sequence
            .checked_add(1)
            .ok_or_else(|| ProviderError::Rejected("Machine event sequence exhausted".into()))?,
        observed_at_unix_ms: value.changed_at_unix_ms,
        fact: EventFact::State(value.state),
    })
}

async fn producer_lost(producer: &mut tokio::sync::watch::Receiver<bool>) {
    let published = producer.wait_for(|published| *published).await.is_ok();
    if published {
        // Successful publication transferred progress to the canonical follow.
        // Delivery may follow the commit receipt; it needs no live mutator.
        futures::future::pending::<()>().await;
    }
}

fn condition(path: StreamPath, head: &PathHead) -> Result<CommitCondition, ProviderError> {
    Ok(match &head.last {
        Some(last) => CommitCondition::Head {
            path,
            expected: head.tail,
            commit_id: last.commit_id,
        },
        None => CommitCondition::Absent { path },
    })
}

fn append(path: StreamPath, value: &impl Serialize) -> Result<CommitMutation, ProviderError> {
    Ok(CommitMutation::Append {
        path,
        records: vec![AppendRecord::from(encode(value)?)],
    })
}

fn decode<T: serde::de::DeserializeOwned>(head: PathHead) -> Result<T, ProviderError> {
    let record = head.last.ok_or_else(|| ProviderError::NotFound("Machine journal has no original record; recovery: use the original admitted provider and key".into()))?;
    decode_bytes(record.value.as_ref())
}

fn storage(error: StreamError) -> ProviderError {
    match error {
        StreamError::Unavailable | StreamError::Refused(_) => ProviderError::Unavailable,
        error => ProviderError::Rejected(format!(
            "Machine journal access failed: {error}; recovery: restore the original Streams authority and publication"
        )),
    }
}

fn call_error(error: call::Error) -> ProviderError {
    match error {
        call::Error::Stream(error) => storage(error),
        error => ProviderError::Rejected(format!(
            "Machine call refused: {error}; recovery: retain its original caller journal"
        )),
    }
}
