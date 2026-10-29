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
}

pub(crate) enum Admission {
    Original(Option<Publication>),
    Replay(MutationOutcome),
}

pub(crate) struct Publication {
    commit: CommitId,
    machine_tail: u64,
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

    pub(crate) async fn begin(
        &self,
        key: IdempotencyKey,
        digest: [u8; 32],
        physical: Option<&MachineObservation>,
        validate: impl FnOnce() -> Result<(), ProviderError>,
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
        validate()?;
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
        let receipt = self
            .commit(
                key,
                b"admit",
                vec![
                    condition(machine_path.clone(), &current)?,
                    CommitCondition::Absent { path: path.clone() },
                ],
                vec![append(machine_path, machine)?, append(path, &value)?],
            )
            .await?;
        Ok(Admission::Original(Some(Publication {
            commit: receipt.commit_id,
            machine_tail: current.tail + 1,
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
        let (machine_condition, admitted) = match admitted {
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
        let conditions = vec![machine_condition, admitted];
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
        )
        .await
        .map_err(|_| ProviderError::Indeterminate(key))?;
        Ok(outcome)
    }

    async fn commit(
        &self,
        key: IdempotencyKey,
        phase: &[u8],
        conditions: Vec<CommitCondition>,
        mutations: Vec<CommitMutation>,
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
        let head = self.head(path.clone()).await?;
        let value: Operation = decode(head)?;
        let initial = OperationObservation {
            id: operation,
            phase: value.phase,
        };
        // A pending producer that lost its physical owner has no authority to
        // promise future completion. Recovery never repeats that mutation.
        if initial.phase == OperationPhase::Pending {
            return Err(ProviderError::Indeterminate(key));
        }
        Ok(stream::once(async move { Ok(initial) }).boxed())
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
