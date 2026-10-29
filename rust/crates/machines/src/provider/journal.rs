//! Keyed admission and settled results on the canonical Streams commit boundary.
use super::*;
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitReceipt,
    CommitRequest, PathHead, ReadRequest, StreamError, StreamPath, StreamProvider,
    StreamProviderExt as _,
};

#[derive(Serialize, Deserialize)]
struct Operation {
    digest: [u8; 32],
    phase: OperationPhase,
    outcome: Option<MutationOutcome>,
}

#[derive(Serialize, Deserialize)]
enum Version {
    #[serde(rename = "1")]
    V1,
}
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    version: Version,
    value: T,
}

/// Storage of one admitted Machine incarnation. No process-local result map,
/// resource scan, periodic observer or second durability barrier participates.
pub(crate) struct Journal {
    streams: Arc<dyn StreamProvider>,
    root: StreamPath,
    machine: MachineId,
    // At most one physical transition runs under the incarnation's admission
    // lock. This is its live producer, never an operation/result registry.
    producer: Mutex<Option<(OperationId, tokio::sync::watch::Receiver<bool>)>>,
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
        }
    }

    fn path(&self, suffix: impl fmt::Display) -> Result<StreamPath, ProviderError> {
        StreamPath::new(format!("{}/{suffix}", self.root)).map_err(storage)
    }

    fn operation(&self, key: IdempotencyKey) -> Result<StreamPath, ProviderError> {
        self.path(format_args!("operations/{key}"))
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

    pub(crate) async fn machine(&self) -> Result<MachineObservation, ProviderError> {
        decode(
            self.head(self.path(format_args!("machines/{}", self.machine))?)
                .await?,
        )
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
        let path = self.operation(key)?;
        let head = self.head(path.clone()).await?;
        if head.last.is_some() {
            let original: Operation = decode(head)?;
            if original.digest != digest {
                return Err(ProviderError::Conflict("mutation key names another intent; recovery: retain the original arguments or use a new key".into()));
            }
            return original
                .outcome
                .map(Admission::Replay)
                .ok_or(ProviderError::Indeterminate(key));
        }
        let close_admission = validate()?;
        let Some(machine) = physical else {
            return Ok(Admission::Original(None));
        };
        let machine_path = self.path(format_args!("machines/{}", self.machine))?;
        let current = self.head(machine_path.clone()).await?;
        let value = Operation {
            digest,
            phase: OperationPhase::Pending,
            outcome: None,
        };
        // Only original physical admission needs a live completion owner.
        // Replay and already-settled logical changes allocate no producer.
        let (producer, receiver) = tokio::sync::watch::channel(false);
        *self.producer.lock().await = Some((OperationId(key.0), receiver));
        let receipt = self
            .commit(
                key,
                b"admit",
                vec![
                    condition(machine_path.clone(), &current)?,
                    CommitCondition::Absent { path: path.clone() },
                ],
                vec![append(machine_path, machine)?, append(path, &value)?],
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
        let operation = self.operation(key)?;
        let machine_path = self.path(format_args!("machines/{}", self.machine))?;
        let (machine_condition, operation_condition) = match &admitted {
            Some(publication) => (
                CommitCondition::Head {
                    path: machine_path.clone(),
                    expected: publication.machine_tail,
                    commit_id: publication.commit,
                },
                CommitCondition::Head {
                    path: operation.clone(),
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
                CommitCondition::Absent {
                    path: operation.clone(),
                },
            ),
        };
        let conditions = vec![machine_condition, operation_condition];
        let value = Operation {
            digest,
            phase: OperationPhase::Succeeded,
            outcome: Some(outcome.clone()),
        };
        self.commit(
            key,
            b"settle",
            conditions,
            vec![append(machine_path, machine)?, append(operation, &value)?],
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
        let mut identity = self.root.as_str().as_bytes().to_vec();
        identity.extend_from_slice(&key.as_bytes());
        identity.extend_from_slice(phase);
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions,
            mutations,
            idempotency_key: acyclic_stream::IdempotencyKey::new(Sha256::digest(identity).to_vec())
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
        let operation: Operation = decode(self.head(self.operation(key)?).await?)?;
        operation.outcome.ok_or(ProviderError::Indeterminate(key))
    }

    pub(crate) async fn inspect(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        let key = IdempotencyKey(operation.0);
        let value: Operation = decode(self.head(self.operation(key)?).await?)?;
        Ok(OperationObservation {
            id: operation,
            phase: value.phase,
        })
    }

    pub(crate) async fn watch(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let key = IdempotencyKey(operation.0);
        let path = self.operation(key)?;
        // Pin producer selection across this head cut. Completion needs no
        // lock; a subsequent transition cannot replace its live source before
        // this observer retains it. No provider lock survives follow opening.
        let active = self.producer.lock().await;
        let head = self.head(path.clone()).await?;
        let mut request = acyclic_stream::FollowRequest::new(path, head.tail);
        request.preceding = head.last.as_ref().map(|record| record.commit_id);
        let value: Operation = decode(head)?;
        let initial = OperationObservation {
            id: operation,
            phase: value.phase,
        };
        if initial.phase != OperationPhase::Pending {
            return Ok(stream::once(async move { Ok(initial) }).boxed());
        }
        let mut producer = active
            .as_ref()
            .filter(|(id, _)| *id == operation)
            .map(|(_, receiver)| receiver.clone())
            .ok_or(ProviderError::Indeterminate(key))?;
        drop(active);
        if !*producer.borrow() && producer.has_changed().is_err() {
            return Err(ProviderError::Indeterminate(key));
        }
        let records = tokio::select! {
            biased;
            result = self.streams.follow(request) => result.map_err(|_| ProviderError::Indeterminate(key))?,
            () = producer_lost(&mut producer) => return Err(ProviderError::Indeterminate(key)),
        };
        let updates = stream::try_unfold(
            (acyclic_stream::records_only(records), producer, false),
            move |(mut records, mut producer, done)| async move {
                if done {
                    return Ok(None);
                }
                let record = tokio::select! {
                    biased;
                    record = records.next() => record.and_then(Result::ok)
                        .ok_or(ProviderError::Indeterminate(key))?,
                    () = producer_lost(&mut producer) => return Err(ProviderError::Indeterminate(key)),
                };
                let value: Envelope<Operation> = serde_json::from_slice(record.value.as_ref())
                    .map_err(|_| ProviderError::Indeterminate(key))?;
                let phase = value.value.phase;
                Ok(Some((
                    OperationObservation {
                        id: operation,
                        phase,
                    },
                    (records, producer, phase != OperationPhase::Pending),
                )))
            },
        );
        Ok(stream::once(async move { Ok(initial) })
            .chain(updates)
            .boxed())
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
            let value: Envelope<MachineObservation> =
                serde_json::from_slice(record.value.as_ref()).map_err(encoding)?;
            let value = value.value;
            events.push(MachineEvent {
                machine: self.machine,
                sequence: record.sequence + 1,
                observed_at_unix_ms: value.changed_at_unix_ms,
                fact: EventFact::State(value.state),
            });
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
        records: vec![AppendRecord::from(
            serde_json::to_vec(&Envelope {
                version: Version::V1,
                value,
            })
            .map_err(encoding)?,
        )],
    })
}

fn decode<T: serde::de::DeserializeOwned>(head: PathHead) -> Result<T, ProviderError> {
    let record = head.last.ok_or_else(|| ProviderError::NotFound("Machine journal has no original record; recovery: use the original admitted provider and key".into()))?;
    let envelope: Envelope<T> = serde_json::from_slice(record.value.as_ref()).map_err(encoding)?;
    Ok(envelope.value)
}

fn encoding(error: serde_json::Error) -> ProviderError {
    ProviderError::Rejected(format!(
        "Machine journal encoding failed: {error}; recovery: restore the original version-one records"
    ))
}

fn storage(error: StreamError) -> ProviderError {
    ProviderError::Rejected(format!(
        "Machine journal access failed: {error}; recovery: restore the original Streams authority and publication"
    ))
}
