use super::*;
#[cfg(feature = "filesystem")]
use crate::filesystem::PreparationIo;
use crate::{NativeCustody, store::HistoryContentFuture, workflow::WorkflowRecord};
use acyclic_stream::OwnedBytes as Bytes;
use acyclic_stream::{
    CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitRequest, ReadRequest,
    StreamError,
};
#[cfg(not(feature = "filesystem"))]
type PreparationIo = ();
use futures::TryStreamExt;

const PAGE: u32 = 64;
mod admission;
mod subscription;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Bootstrap {
    pub manifest: Manifest,
    pub input: Value,
    pub admission: WorkflowAdmission,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Entry {
    pub snapshot: Snapshot,
    pub bootstrap: Option<Bootstrap>,
    pub workflow: Option<WorkflowRecord>,
    pub event: Option<Event>,
    pub cancelling: bool,
    pub moving: Option<Placement>,
}

pub(super) struct Loaded {
    pub tail: u64,
    pub head: CommitId,
    pub bootstrap: Bootstrap,
    pub last: Entry,
}

pub(super) struct Journal<P: StreamProvider> {
    stream: Arc<P>,
    manifest: Manifest,
    placement: Placement,
    path: StreamPath,
    inbox: StreamPath,
}

pub(super) fn stream_error(error: &StreamError) -> Error {
    match error {
        StreamError::IdempotencyMismatch | StreamError::AlreadyExists => {
            Error::Conflict(error.to_string())
        }
        StreamError::AccessDenied => Error::Unauthorized(error.to_string()),
        StreamError::NotFound => Error::NotFound(error.to_string()),
        _ => Error::Storage(error.to_string()),
    }
}

pub(super) fn codec_error(error: &serde_json::Error) -> Error {
    Error::Invalid(error.to_string())
}

impl<P: StreamProvider> Journal<P> {
    pub fn new(stream: Arc<P>, manifest: Manifest, placement: Placement) -> Result<Self> {
        let path = StreamPath::new(format!(
            "{}/{}",
            manifest.bindings.streams,
            Manifest::workflow(placement.actor)
        ))
        .map_err(|error| stream_error(&error))?;
        let inbox = StreamPath::new(format!(
            "{}/{}",
            manifest.bindings.streams,
            Manifest::inbox(placement.actor)
        ))
        .map_err(|error| stream_error(&error))?;
        Ok(Self {
            stream,
            manifest,
            placement,
            path,
            inbox,
        })
    }

    async fn entry(&self, sequence: u64) -> Result<(Entry, CommitId)> {
        let mut records = self
            .stream
            .read(ReadRequest {
                path: self.path.clone(),
                from: sequence,
                limit: 1,
            })
            .await
            .map_err(|error| stream_error(&error))?;
        let record = records
            .try_next()
            .await
            .map_err(|error| stream_error(&error))?
            .ok_or_else(|| Error::Storage("actor journal has a missing record".into()))?;
        if record.sequence != sequence {
            return Err(Error::Storage("actor journal cursor differs".into()));
        }
        Ok((
            serde_json::from_slice(&record.value).map_err(|error| codec_error(&error))?,
            record.commit_id,
        ))
    }

    pub async fn load_optional(&self) -> Result<Option<Loaded>> {
        let tail = match self.stream.tail(self.path.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(stream_error(&error)),
        };
        if tail == 0 {
            return Err(Error::Storage("empty actor journal".into()));
        }
        let first = self.entry(0).await?.0;
        let bootstrap = first
            .bootstrap
            .ok_or_else(|| Error::Storage("actor journal has no admission".into()))?;
        if bootstrap.manifest != self.manifest {
            return Err(Error::Conflict(
                "actor package identity or bindings changed".into(),
            ));
        }
        let (last, head) = self.entry(tail - 1).await?;
        Ok(Some(Loaded {
            tail,
            head,
            bootstrap,
            last,
        }))
    }

    pub async fn load(&self) -> Result<Loaded> {
        self.load_optional()
            .await?
            .ok_or_else(|| Error::NotFound("actor is not placed".into()))
    }

    pub fn require_owner(&self, loaded: &Loaded) -> Result<()> {
        if loaded.last.snapshot.placement != self.placement || loaded.last.moving.is_some() {
            return Err(Error::Conflict("stale actor placement fence".into()));
        }
        Ok(())
    }

    pub async fn durable_tail(&self) -> Result<u64> {
        Ok(self
            .stream
            .watermarks(self.path.clone())
            .await
            .map_err(|error| stream_error(&error))?
            .durable)
    }

    pub async fn require_durable(&self, tail: u64) -> Result<()> {
        if self.durable_tail().await? < tail {
            return Err(Error::Storage(
                "actor publication has not reached durable storage".into(),
            ));
        }
        Ok(())
    }

    pub fn claim_effects(&self) -> Result<acyclic_stream::OperationClaim<'_, ()>> {
        self.claim("effects")
    }

    pub fn claim_handler(&self) -> Result<acyclic_stream::OperationClaim<'_, ()>> {
        self.claim("handler")
    }

    pub fn claim_supervisor(&self) -> Result<acyclic_stream::OperationClaim<'_, ()>> {
        self.claim("supervisor")
    }

    fn claim(&self, kind: &str) -> Result<acyclic_stream::OperationClaim<'_, ()>> {
        let mut key = [0; 48];
        let (actor, domain) = key.split_at_mut(16);
        actor.copy_from_slice(&self.placement.actor.into_bytes());
        domain.copy_from_slice(
            blake3::hash(format!("{kind}:{}", self.path.as_str()).as_bytes()).as_bytes(),
        );
        self.stream
            .operation_claims()
            .claim(key, || Ok::<_, Error>(()), |_| Ok(()))?
            .ok_or_else(|| Error::Conflict("actor work already has a live runtime owner".into()))
    }

    pub async fn create(&self, entry: Entry) -> Result<()> {
        let value = Bytes::from(serde_json::to_vec(&entry).map_err(|error| codec_error(&error))?);
        self.commit_transaction(
            vec![CommitCondition::Absent {
                path: self.path.clone(),
            }],
            vec![CommitMutation::Append {
                path: self.path.clone(),
                records: vec![value],
            }],
        )
        .await
    }

    pub async fn publish(
        &self,
        head: &CommitId,
        tail: u64,
        entry: Entry,
        event: Option<Event>,
    ) -> Result<()> {
        let value = Bytes::from(serde_json::to_vec(&entry).map_err(|error| codec_error(&error))?);
        let mut conditions = vec![CommitCondition::Head {
            path: self.path.clone(),
            expected: tail,
            commit_id: *head,
        }];
        let mutations = vec![CommitMutation::Append {
            path: self.path.clone(),
            records: vec![value],
        }];
        if let Some(event) = event {
            let consumed = entry
                .snapshot
                .consumed
                .checked_sub(1)
                .ok_or_else(|| Error::Invalid("invalid consumed actor cursor".into()))?;
            conditions.push(
                self.input_condition(&event, entry.cancelling, consumed)
                    .await?,
            );
        }
        self.commit_transaction(conditions, mutations).await
    }

    async fn commit_transaction(
        &self,
        conditions: Vec<CommitCondition>,
        mutations: Vec<CommitMutation>,
    ) -> Result<()> {
        let mut request = CommitRequest {
            conditions,
            mutations,
            idempotency_key: acyclic_stream::IdempotencyKey::new("actor-transaction")
                .map_err(|error| stream_error(&error))?,
        };
        let digest = acyclic_stream::request::commit_digest(&mut request)
            .map_err(|error| stream_error(&error))?;
        request.idempotency_key = acyclic_stream::IdempotencyKey::new(format!(
            "actor:{}",
            blake3::hash(&digest).to_hex()
        ))
        .map_err(|error| stream_error(&error))?;
        match self
            .stream
            .commit(request)
            .await
            .map_err(|error| stream_error(&error))?
        {
            CommitOutcome::Committed(_) => Ok(()),
            CommitOutcome::Conflict(_) => Err(Error::Conflict(
                "actor journal changed before commit".into(),
            )),
        }
    }

    async fn page(&self, from: u64, maximum: u32) -> Result<Vec<Entry>> {
        let mut records = self
            .stream
            .read(ReadRequest {
                path: self.path.clone(),
                from,
                limit: maximum,
            })
            .await
            .map_err(|error| stream_error(&error))?;
        let mut entries = Vec::new();
        let mut expected = from;
        while let Some(record) = records
            .try_next()
            .await
            .map_err(|error| stream_error(&error))?
        {
            if record.sequence != expected {
                return Err(Error::Storage("actor history lost its cursor".into()));
            }
            expected += 1;
            entries
                .push(serde_json::from_slice(&record.value).map_err(|error| codec_error(&error))?);
        }
        Ok(entries)
    }

    pub async fn retry(&self, event: &Event) -> Result<Option<Entry>> {
        let tail = self.load().await?.tail;
        let mut from = 0;
        while from < tail {
            let page = self
                .page(from, PAGE.min(u32::try_from(tail - from).unwrap_or(PAGE)))
                .await?;
            if page.is_empty() {
                return Err(Error::Storage("actor retry history is incomplete".into()));
            }
            from += page.len() as u64;
            for entry in page {
                if entry.workflow.as_ref().is_some_and(|record| {
                    record
                        .transition
                        .commands
                        .iter()
                        .any(|command| command.operation_id == event.operation_id)
                }) {
                    return Err(Error::Conflict(
                        "actor event reuses a committed effect identity".into(),
                    ));
                }
                if let Some(existing) = &entry.event
                    && (existing.operation_id == event.operation_id
                        || existing.idempotency_key == event.idempotency_key)
                {
                    if existing != event {
                        return Err(Error::Conflict(
                            "actor retry identity is already bound to another input".into(),
                        ));
                    }
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }

    pub async fn check_commands(&self, proposed: &MachineTransition) -> Result<()> {
        if proposed.commands.is_empty() {
            return Ok(());
        }
        let tail = self.load().await?.tail;
        let mut from = 1;
        while from < tail {
            let page = self
                .page(from, PAGE.min(u32::try_from(tail - from).unwrap_or(PAGE)))
                .await?;
            if page.is_empty() {
                return Err(Error::Storage("actor command history is incomplete".into()));
            }
            from += page.len() as u64;
            for record in page.into_iter().filter_map(|entry| entry.workflow) {
                if proposed.commands.iter().any(|command| {
                    record
                        .operation_ids()
                        .any(|operation| operation == command.operation_id)
                }) {
                    return Err(Error::Conflict(
                        "actor effect identity was already committed".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl<P: StreamProvider> WorkflowJournal for Journal<P> {
    fn admit(
        &self,
        admission: WorkflowAdmission,
    ) -> BoxProviderFuture<'_, Result<WorkflowAdmission>> {
        Box::pin(async move {
            let retained = self.load().await?.bootstrap.admission;
            if retained != admission {
                return Err(Error::Conflict("workflow admission changed".into()));
            }
            Ok(retained)
        })
    }

    fn admission<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<HistoryContentFuture<'a, (Option<WorkflowAdmission>, NativeCustody)>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "actor journal requires ordinary workflow callbacks".into(),
            ));
        }
        Ok(HistoryContentFuture::Ordinary {
            future: Box::pin(
                async move { Ok((Some(self.load().await?.bootstrap.admission), None)) },
            ),
            frame_bytes: 0,
        })
    }

    fn replay<'a>(
        &'a self,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<HistoryContentFuture<'a, (Vec<WorkflowRecord>, NativeCustody)>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "actor journal requires ordinary workflow callbacks".into(),
            ));
        }
        Ok(HistoryContentFuture::Ordinary {
            future: Box::pin(async move {
                if maximum == 0 {
                    return Err(Error::Invalid("invalid actor replay page size".into()));
                }
                let tail = self.load().await?.tail;
                let mut from = 1;
                let mut result = Vec::new();
                while from < tail && result.len() < maximum as usize {
                    let page = self
                        .page(from, PAGE.min(u32::try_from(tail - from).unwrap_or(PAGE)))
                        .await?;
                    if page.is_empty() {
                        return Err(Error::Storage("actor replay history is incomplete".into()));
                    }
                    from += page.len() as u64;
                    for entry in page {
                        if let Some(record) = entry.workflow
                            && record.prior.revision >= after
                        {
                            result.push(record);
                            if result.len() == maximum as usize {
                                break;
                            }
                        }
                    }
                }
                Ok((result, None))
            }),
            frame_bytes: 0,
        })
    }

    fn commit(
        &self,
        expected_revision: u64,
        key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxProviderFuture<'_, Result<WorkflowRecord>> {
        Box::pin(async move {
            record.validate(None)?;
            let current = self.load().await?;
            self.require_owner(&current)?;
            let event = Event {
                operation_id: record.operation_id,
                idempotency_key: key.clone(),
                input: record.input.clone(),
            };
            if let Some(retry) = self.retry(&event).await? {
                return retry
                    .workflow
                    .filter(|existing| existing == &record)
                    .ok_or_else(|| Error::Conflict("workflow retry contents changed".into()));
            }
            if current.last.cancelling
                || current.last.moving.is_some()
                || record.prior != current.last.snapshot.checkpoint
                || record.prior.revision != expected_revision
                || record.idempotency_key != key
            {
                return Err(Error::Conflict(
                    "actor checkpoint or placement changed".into(),
                ));
            }
            self.manifest.validate_transition(&record.transition)?;
            let mut snapshot = current.last.snapshot;
            snapshot.checkpoint = MachineCheckpoint {
                machine: record.prior.machine.clone(),
                revision: expected_revision
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("actor revision overflow".into()))?,
                state: record.transition.state.clone(),
            };
            snapshot.consumed = snapshot
                .consumed
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("actor cursor overflow".into()))?;
            snapshot.status = record.transition.status.clone();
            snapshot.pending.retain(|command| {
                effect_result_operation(command.operation_id) != record.operation_id
            });
            snapshot.pending.extend(record.transition.commands.clone());
            if !matches!(snapshot.status, MachineStatus::Suspended) && !snapshot.pending.is_empty()
            {
                return Err(Error::Conflict(
                    "terminal actor still has pending effects".into(),
                ));
            }
            self.publish(
                &current.head,
                current.tail,
                Entry {
                    snapshot,
                    bootstrap: None,
                    workflow: Some(record.clone()),
                    event: Some(event.clone()),
                    cancelling: false,
                    moving: None,
                },
                Some(event),
            )
            .await?;
            Ok(record)
        })
    }
}
