//! Durable outbox and replay reducer. A record is committed before publication.

use super::{
    TaskFileViews,
    content::MessageContent,
    task_compat::views::{authority_key, payload},
};
use crate::{
    Error, Result, wire,
    wire_api::{current_protocol, validate_admission, validate_resume_protocol},
};
use std::collections::{BTreeMap, HashMap, HashSet};

const CHECKPOINT_RECORDS: usize = 64;

/// One atomically appended recovery record. Contains private command grants.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionRecord {
    /// Initial replay cut or one queued command.
    #[prost(message, optional, tag = "1")]
    pub request: Option<wire::ClientFrame>,
    /// One validated admission or delivery.
    #[prost(message, optional, tag = "2")]
    pub response: Option<wire::ServerFrame>,
    /// Complete recovery cut replacing the older journal prefix.
    #[prost(message, optional, tag = "3")]
    pub checkpoint: Option<SessionCheckpoint>,
}

/// Version-bound durable cursor and pending command recovery cut.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionCheckpoint {
    /// Protocol identity that produced this state.
    #[prost(message, optional, tag = "1")]
    pub protocol: Option<wire::ProtocolIdentity>,
    /// Durable per-authority cuts.
    #[prost(message, repeated, tag = "2")]
    pub cursors: Vec<wire::ReplayCursor>,
    /// Exact pending envelopes in submission order.
    #[prost(message, repeated, tag = "3")]
    pub pending: Vec<wire::CommandEnvelope>,
}

/// Private, single-writer durable journal. A failed append must leave no record.
pub trait ClientStateStore {
    /// Reads committed records in order. Reject corruption rather than skipping it.
    fn load(&mut self) -> Result<Vec<SessionRecord>>;
    /// Atomically commits one record before returning success.
    fn append(&mut self, record: &SessionRecord) -> Result<()>;
    /// Atomically replaces the journal with one complete checkpoint.
    fn replace(&mut self, checkpoint: &SessionRecord) -> Result<()>;
}

/// In-process store for embedded consumers and tests.
#[derive(Default)]
pub struct MemoryClientState {
    records: Vec<SessionRecord>,
}

impl ClientStateStore for MemoryClientState {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        Ok(self.records.clone())
    }
    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        self.records.push(record.clone());
        Ok(())
    }
    fn replace(&mut self, checkpoint: &SessionRecord) -> Result<()> {
        self.records.clear();
        self.records.push(checkpoint.clone());
        Ok(())
    }
}

/// Durable outbox and cursors with disposable renderer state.
pub struct ClientSession<S> {
    store: S,
    cursors: BTreeMap<String, wire::ReplayCursor>,
    pending: Vec<Option<wire::CommandEnvelope>>,
    operations: HashMap<String, usize>,
    idempotency: HashSet<(String, String)>,
    views: TaskFileViews,
    view_revisions: BTreeMap<String, u64>,
    tail_records: usize,
}

impl<S: ClientStateStore> ClientSession<S> {
    /// Recovers durable cursors and pending commands. Disposable views rebuild from
    /// owner history when replay opens; the journal never snapshots a transcript.
    pub fn open(mut store: S) -> Result<Self> {
        let records = store.load()?;
        let mut session = Self {
            store,
            cursors: BTreeMap::new(),
            pending: Vec::new(),
            operations: HashMap::new(),
            idempotency: HashSet::new(),
            views: TaskFileViews::default(),
            view_revisions: BTreeMap::new(),
            tail_records: 0,
        };
        for mut record in records {
            if let Some(checkpoint) = record.checkpoint.take() {
                if record.request.is_some() || record.response.is_some() {
                    return Err(Error::Storage("mixed checkpoint record".into()));
                }
                session.restore(checkpoint)?;
            } else {
                session.record(&record, false)?;
                session.tail_records += 1;
            }
        }
        Ok(session)
    }

    /// Adds an authority/generation once; reconnect retains its durable cut.
    pub fn track(&mut self, cursor: wire::ReplayCursor) -> Result<()> {
        self.record(
            &SessionRecord {
                request: Some(wire::ClientFrame {
                    frame: Some(wire::client_frame::Frame::Resume(wire::ResumeRequest {
                        protocol: Some(current_protocol()),
                        cursors: vec![cursor],
                    })),
                }),
                response: None,
                checkpoint: None,
            },
            true,
        )?;
        self.compact_if_needed()
    }

    /// Persists exact intent and retry identity before a transport may send it.
    pub fn queue(&mut self, command: wire::CommandEnvelope) -> Result<()> {
        self.record(
            &SessionRecord {
                request: Some(wire::ClientFrame {
                    frame: Some(wire::client_frame::Frame::Command(command)),
                }),
                response: None,
                checkpoint: None,
            },
            true,
        )?;
        self.compact_if_needed()
    }

    /// Commits an admission or gapless delivery before returning its acknowledgement.
    pub fn receive(&mut self, frame: wire::ServerFrame) -> Result<Option<wire::Acknowledge>> {
        let acknowledge = match &frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(wire::Acknowledge {
                authority: delivery.authority.clone(),
                generation: delivery.generation.clone(),
                through_revision: delivery.through_revision,
            }),
            _ => None,
        };
        self.record(
            &SessionRecord {
                request: None,
                response: Some(frame),
                checkpoint: None,
            },
            true,
        )?;
        self.compact_if_needed()?;
        Ok(acknowledge)
    }

    /// Validates and reserves maintenance before publication. Retry intent is
    /// removed only after the sink accepts the admission and its record commits.
    pub(super) fn receive_admission(
        &mut self,
        admission: wire::Admission,
        publish: impl FnOnce(&wire::Admission) -> Result<()>,
    ) -> Result<()> {
        let record = SessionRecord {
            request: None,
            response: Some(wire::ServerFrame {
                frame: Some(wire::server_frame::Frame::Admission(admission)),
            }),
            checkpoint: None,
        };
        let Some(wire::ServerFrame {
            frame: Some(wire::server_frame::Frame::Admission(admission)),
        }) = record.response.as_ref()
        else {
            return Err(Error::Storage("admission record missing".into()));
        };
        self.admit(admission, &record, true, publish)
    }

    /// Resolves canonical files, publishes the complete frame, then commits its cursor.
    /// File and validation failures leave the views and durable cut unchanged.
    pub(super) async fn receive_delivery(
        &mut self,
        delivery: wire::Delivery,
        files: Option<&dyn crate::conversation::ContentResidencyVerifier>,
        before_commit: impl FnOnce(&wire::ServerFrame) -> Result<()>,
    ) -> Result<wire::Delivery> {
        let authority = valid_authority(delivery.authority.as_ref())?;
        let view_revision = self.view_revision(authority);
        let mut messages = Vec::with_capacity(delivery.events.len());
        for event in &delivery.events {
            let payload = payload(event)?;
            let content = if event.revision > view_revision {
                if let crate::core::EventPayload::ConversationMessageAppended { message } = &payload
                {
                    MessageContent::read(message, files).await?
                } else {
                    None
                }
            } else {
                None
            };
            messages.push((payload, content));
        }
        let record = SessionRecord {
            request: None,
            response: Some(wire::ServerFrame {
                frame: Some(wire::server_frame::Frame::Delivery(delivery)),
            }),
            checkpoint: None,
        };
        let Some(wire::ServerFrame {
            frame: Some(wire::server_frame::Frame::Delivery(delivery)),
        }) = record.response.as_ref()
        else {
            return Err(Error::Storage("delivery record missing".into()));
        };
        self.deliver(delivery, &record, true, messages, before_commit)?;
        match record.response.and_then(|response| response.frame) {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Ok(delivery),
            _ => Err(Error::Storage("delivery record missing".into())),
        }
    }

    /// Exact resume request, including the compiled protocol identity.
    #[must_use]
    pub fn resume(&self) -> wire::ResumeRequest {
        wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: self.cursors.values().cloned().collect(),
        }
    }

    pub(super) fn cursor(&self, authority: &wire::Authority) -> Option<&wire::ReplayCursor> {
        self.cursors.get(&authority_key(authority))
    }

    /// Current disposable projection cut. It starts at zero after reopening.
    #[must_use]
    pub fn view_revision(&self, authority: &wire::Authority) -> u64 {
        self.view_revisions
            .get(&authority_key(authority))
            .copied()
            .unwrap_or(0)
    }

    /// Owner replay cut, independent of the task-file presentation cache.
    #[must_use]
    pub fn replay_revision(&self, authority: &wire::Authority) -> u64 {
        self.view_revision(authority)
    }

    /// Snapshot-plus-follow starts at the projection cut, retaining durable acknowledgements.
    #[must_use]
    pub fn replay_request(&self) -> wire::ResumeRequest {
        let mut request = self.resume();
        for cursor in &mut request.cursors {
            if let Some(authority) = &cursor.authority {
                cursor.revision = self.view_revision(authority);
            }
        }
        request
    }

    /// Folds one verified platform-follow update into the shared disposable views.
    /// Query cursors remain owned by the platform follow, independently of the
    /// durable wire outbox and acknowledgements.
    pub fn apply_update(&mut self, update: super::ViewUpdate) -> Result<()> {
        match update {
            super::ViewUpdate::File(change) => self.query_change(change).map(|_| ()),
            super::ViewUpdate::DocumentChange { authority, change } => {
                self.apply_documents(&authority, |documents| documents.apply_change(&change))
            }
            super::ViewUpdate::Model {
                authority,
                turn,
                event,
            } => self.model_event(&authority, turn, event),
            super::ViewUpdate::ToolOutput {
                authority,
                call_id,
                delta,
            } => self.tool_output(&authority, &call_id, &delta),
            super::ViewUpdate::DocumentSnapshot {
                authority,
                query,
                snapshot,
            } => self.apply_documents(&authority, |documents| documents.replace(&query, &snapshot)),
            super::ViewUpdate::DocumentFile { authority, file } => {
                self.apply_documents(&authority, |documents| documents.apply(&file))
            }
        }
    }

    /// Replace a captured-volume query fold at its authenticated snapshot cut.
    pub fn replace_diff(
        &mut self,
        authority: &wire::Authority,
        view: super::DiffView,
    ) -> Result<()> {
        self.require_tracked(authority)?;
        self.views.diffs.insert(authority_key(authority), view);
        Ok(())
    }

    /// Fold one exact-path publication before the query adapter advances its cursor.
    pub async fn diff_publication(
        &mut self,
        authority: &wire::Authority,
        publication: &super::DiffPublication,
        reader: &dyn crate::conversation::ContentResidencyVerifier,
        limits: super::DiffLimits,
    ) -> Result<Vec<String>> {
        self.require_tracked(authority)?;
        self.views
            .diffs
            .get_mut(&authority_key(authority))
            .ok_or_else(|| Error::Invalid("open the diff query snapshot before its follow".into()))?
            .apply(publication, reader, limits)
            .await
    }

    fn apply_documents(
        &mut self,
        authority: &wire::Authority,
        apply: impl FnOnce(&mut super::DocumentViews) -> Result<()>,
    ) -> Result<()> {
        self.require_tracked(authority)?;
        let key = authority_key(authority);
        if let Some(documents) = self.views.documents.get_mut(&key) {
            return apply(documents);
        }
        let mut documents = super::DocumentViews::default();
        apply(&mut documents)?;
        self.views.documents.insert(key, documents);
        Ok(())
    }
    /// Folds an incoming model item into the session's shared renderer state.
    pub fn model_event(
        &mut self,
        authority: &wire::Authority,
        turn: crate::OperationId,
        event: crate::model::ModelEvent,
    ) -> Result<()> {
        self.require_tracked(authority)?;
        self.views.model_event(authority, turn, event)
    }

    /// Folds an incoming tool chunk without a separate renderer-owned registry.
    pub fn tool_output(
        &mut self,
        authority: &wire::Authority,
        call_id: &str,
        delta: &str,
    ) -> Result<()> {
        self.require_tracked(authority)?;
        self.views.tool_output(authority, call_id, delta);
        Ok(())
    }

    fn require_tracked(&self, authority: &wire::Authority) -> Result<()> {
        valid_authority(Some(authority))?;
        if !self.cursors.contains_key(&authority_key(authority)) {
            return Err(Error::Conflict(
                "streaming authority was not requested".into(),
            ));
        }
        Ok(())
    }

    /// Exact envelopes awaiting definitive admission; never regenerate their identities.
    pub fn pending(&self) -> impl Iterator<Item = &wire::CommandEnvelope> {
        self.pending.iter().filter_map(Option::as_ref)
    }

    /// Replaces a disposable file view at the query's atomic snapshot cut.
    pub fn query_snapshot(
        &mut self,
        query: crate::task_volume::Query,
        snapshot: crate::task_volume::Snapshot,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<()> {
        self.views
            .query_snapshot(query, snapshot, maximum_files, maximum_bytes)
    }

    /// Applies one pushed file record; query payloads are disposable, not journaled.
    pub fn query_change(&mut self, change: crate::task_volume::Change) -> Result<bool> {
        self.views.query_change(change)
    }

    /// Selects both file sources required for goal status and continuation counts.
    pub async fn select_goal_view(
        &mut self,
        goal: &(impl crate::task_volume::FileQuerySource + ?Sized),
        request: &(impl crate::task_volume::FileQuerySource + ?Sized),
        maximum_bytes: usize,
    ) -> Result<crate::task_volume::ChangeStream> {
        self.views
            .select_goal_view(goal, request, maximum_bytes)
            .await
    }

    /// Joins observed GOAL.md and request-head cuts without persisting file payloads.
    pub fn bind_goal_sources(
        &mut self,
        goal: ([u8; 32], [u8; 32]),
        request: ([u8; 32], [u8; 32]),
    ) -> Result<()> {
        self.views.bind_goal_sources(goal, request)
    }

    /// State shared by every renderer.
    #[must_use]
    pub fn views(&self) -> &TaskFileViews {
        &self.views
    }

    /// Returns storage after transport shutdown, for reopening or explicit disposal.
    pub fn into_store(self) -> S {
        self.store
    }

    fn record(&mut self, record: &SessionRecord, persist: bool) -> Result<()> {
        match (&record.request, &record.response) {
            (Some(request), None) => match &request.frame {
                Some(wire::client_frame::Frame::Resume(request)) => {
                    validate_resume_protocol(request)?;
                    let [cursor] = request.cursors.as_slice() else {
                        return Err(Error::Invalid("track requires one cursor".into()));
                    };
                    let authority = valid_authority(cursor.authority.as_ref())?;
                    if cursor.generation.is_empty() || cursor.revision != 0 {
                        return Err(Error::Invalid(
                            "initial cursor requires a generation and revision zero".into(),
                        ));
                    }
                    let key = authority_key(authority);
                    if let Some(existing) = self.cursors.get(&key) {
                        return if existing.generation == cursor.generation {
                            Ok(())
                        } else {
                            Err(Error::Conflict("replay generation changed".into()))
                        };
                    }
                    if persist {
                        self.append_record(record)?;
                    }
                    self.cursors.insert(key, cursor.clone());
                }
                Some(wire::client_frame::Frame::Command(command)) => {
                    let (identity, key) = command_identity(command)?;
                    if let Some(index) = self.operations.get(&identity.operation_id) {
                        return if self.pending.get(*index).and_then(Option::as_ref) == Some(command)
                        {
                            Ok(())
                        } else {
                            Err(Error::Conflict("outbox operation identity reused".into()))
                        };
                    }
                    if self.idempotency.contains(&key) {
                        return Err(Error::Conflict("outbox idempotency identity reused".into()));
                    }
                    if persist {
                        self.append_record(record)?;
                    }
                    self.operations
                        .insert(identity.operation_id.clone(), self.pending.len());
                    self.idempotency.insert(key);
                    self.pending.push(Some(command.clone()));
                }
                _ => return Err(Error::Invalid("unsupported client journal request".into())),
            },
            (None, Some(response)) => match &response.frame {
                Some(wire::server_frame::Frame::Admission(admission)) => {
                    self.admit(admission, record, persist, |_| Ok(()))?;
                }
                Some(wire::server_frame::Frame::Delivery(delivery)) => {
                    let messages = delivery
                        .events
                        .iter()
                        .map(|event| {
                            let payload = payload(event)?;
                            if persist
                                && event.revision
                                    > self.view_revision(valid_authority(
                                        delivery.authority.as_ref(),
                                    )?)
                                && let crate::core::EventPayload::ConversationMessageAppended {
                                    message,
                                } = &payload
                                && MessageContent::requires_files(message)
                            {
                                return Err(Error::Unsupported(
                                    "message projection requires a filesystem provider".into(),
                                ));
                            }
                            Ok((payload, None))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    self.deliver(delivery, record, persist, messages, |_| Ok(()))?;
                }
                _ => return Err(Error::Invalid("unsupported client journal response".into())),
            },
            _ => {
                return Err(Error::Invalid(
                    "client journal record needs one frame".into(),
                ));
            }
        }
        Ok(())
    }

    /// Replaces recovery metadata without visiting or serializing disposable views.
    pub fn checkpoint(&mut self) -> Result<()> {
        let checkpoint = SessionRecord {
            request: None,
            response: None,
            checkpoint: Some(SessionCheckpoint {
                protocol: Some(current_protocol()),
                cursors: self.cursors.values().cloned().collect(),
                pending: self.pending().cloned().collect(),
            }),
        };
        self.store.replace(&checkpoint)?;
        self.pending.retain(Option::is_some);
        for (index, command) in self.pending.iter().enumerate() {
            if let Some(identity) = command
                .as_ref()
                .and_then(|command| command.operation.as_ref())
            {
                self.operations.insert(identity.operation_id.clone(), index);
            }
        }
        self.tail_records = 0;
        Ok(())
    }

    fn append_record(&mut self, record: &SessionRecord) -> Result<()> {
        self.store.append(record)?;
        self.tail_records += 1;
        Ok(())
    }

    pub(super) fn compact_if_needed(&mut self) -> Result<()> {
        if self.tail_records >= CHECKPOINT_RECORDS {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn restore(&mut self, checkpoint: SessionCheckpoint) -> Result<()> {
        if checkpoint.protocol.as_ref() != Some(&current_protocol()) {
            return Err(Error::Conflict("client checkpoint protocol changed".into()));
        }
        let mut cursors = BTreeMap::new();
        for cursor in &checkpoint.cursors {
            let authority = valid_authority(cursor.authority.as_ref())?;
            if cursor.generation.is_empty()
                || cursors
                    .insert(authority_key(authority), cursor.clone())
                    .is_some()
            {
                return Err(Error::Storage("invalid checkpoint cursor".into()));
            }
        }
        let mut operations = HashMap::with_capacity(checkpoint.pending.len());
        let mut idempotency = HashSet::with_capacity(checkpoint.pending.len());
        for (index, command) in checkpoint.pending.iter().enumerate() {
            let (identity, key) = command_identity(command)?;
            if operations
                .insert(identity.operation_id.clone(), index)
                .is_some()
                || !idempotency.insert(key)
            {
                return Err(Error::Storage(
                    "duplicate checkpoint command identity".into(),
                ));
            }
        }
        self.cursors = cursors;
        self.pending = checkpoint.pending.into_iter().map(Some).collect();
        self.operations = operations;
        self.idempotency = idempotency;
        self.views = TaskFileViews::default();
        self.view_revisions.clear();
        self.tail_records = 0;
        Ok(())
    }

    fn admit(
        &mut self,
        admission: &wire::Admission,
        record: &SessionRecord,
        persist: bool,
        publish: impl FnOnce(&wire::Admission) -> Result<()>,
    ) -> Result<()> {
        // Reserve this record before removing retry intent. A maintenance failure
        // must leave the command pending, including when replace committed first.
        if persist && self.tail_records >= CHECKPOINT_RECORDS - 1 {
            self.checkpoint()?;
        }
        let operation = admission
            .operation
            .as_ref()
            .ok_or_else(|| Error::Conflict("admission identity missing".into()))?;
        let index = *self
            .operations
            .get(&operation.operation_id)
            .ok_or_else(|| Error::Conflict("unsolicited admission".into()))?;
        let command = self
            .pending
            .get(index)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::Conflict("pending command disappeared".into()))?;
        validate_admission(command, admission)?;
        match wire::AdmissionState::try_from(admission.state) {
            Ok(
                wire::AdmissionState::Accepted
                | wire::AdmissionState::Rejected
                | wire::AdmissionState::Indeterminate,
            ) => {}
            _ => return Err(Error::Invalid("invalid admission state".into())),
        }
        publish(admission)?;
        if admission.state == wire::AdmissionState::Indeterminate as i32 {
            return Ok(());
        }
        let (_, key) = command_identity(command)?;
        if persist {
            self.append_record(record)?;
        }
        *self
            .pending
            .get_mut(index)
            .ok_or_else(|| Error::Conflict("pending command disappeared".into()))? = None;
        self.operations.remove(&operation.operation_id);
        self.idempotency.remove(&key);
        if self.operations.is_empty() {
            self.pending.clear();
        }
        Ok(())
    }

    fn deliver(
        &mut self,
        delivery: &wire::Delivery,
        record: &SessionRecord,
        persist: bool,
        messages: Vec<(crate::core::EventPayload, Option<MessageContent>)>,
        before_commit: impl FnOnce(&wire::ServerFrame) -> Result<()>,
    ) -> Result<()> {
        let authority = valid_authority(delivery.authority.as_ref())?;
        let key = authority_key(authority);
        let cursor = self
            .cursors
            .get(&key)
            .ok_or_else(|| Error::Conflict("delivery authority was not requested".into()))?;
        if cursor.generation != delivery.generation
            || delivery.through_revision < delivery.from_revision
        {
            return Err(Error::Conflict(
                "delivery generation or range mismatch".into(),
            ));
        }
        let mut revision = delivery.from_revision;
        let protocol = current_protocol();
        for event in &delivery.events {
            revision = revision
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("revision overflow".into()))?;
            if event.authority.as_ref() != Some(authority)
                || event.revision != revision
                || event.protocol.as_ref() != Some(&protocol)
            {
                return Err(Error::Conflict(
                    "delivery event identity or sequence mismatch".into(),
                ));
            }
        }
        if revision != delivery.through_revision {
            return Err(Error::Conflict("delivery is not contiguous".into()));
        }
        let durable_revision = cursor.revision;
        let view_revision = self.view_revision(authority);
        if delivery.through_revision > durable_revision && delivery.from_revision > durable_revision
        {
            return Err(Error::Conflict(
                "delivery does not follow durable cursor".into(),
            ));
        }
        if persist
            && delivery.through_revision > view_revision
            && delivery.from_revision > view_revision
        {
            return Err(Error::Conflict(
                "delivery does not follow view cursor".into(),
            ));
        }
        let skip = delivery
            .events
            .iter()
            .take_while(|event| event.revision <= view_revision)
            .count();
        let usage = if persist {
            self.views
                .prepare_usage(authority, messages.iter().skip(skip))?
        } else {
            None
        };
        before_commit(
            record
                .response
                .as_ref()
                .ok_or_else(|| Error::Storage("delivery record missing".into()))?,
        )?;
        if persist && delivery.through_revision > durable_revision {
            self.append_record(record)?;
        }
        // Recovery reads only durable metadata. Replaying history rehydrates the
        // projection without re-appending or moving the durable cursor backwards.
        if persist && delivery.through_revision > view_revision {
            for (event, (payload, content)) in delivery
                .events
                .iter()
                .skip(skip)
                .zip(messages.into_iter().skip(skip))
            {
                self.views
                    .apply(authority, &event.operation_id, payload, content);
            }
            if let Some((usage, total)) = usage {
                self.views.set_usage(authority, usage, total);
            }
            self.view_revisions.insert(key.clone(), revision);
        }
        if let Some(cursor) = self.cursors.get_mut(&key) {
            cursor.revision = cursor.revision.max(revision);
        }
        Ok(())
    }
}

fn command_identity(
    command: &wire::CommandEnvelope,
) -> Result<(&wire::OperationIdentity, (String, String))> {
    crate::wire_api::validate_command_protocol(command)?;
    let authority = valid_authority(command.authority.as_ref())?;
    let identity = command
        .operation
        .as_ref()
        .ok_or_else(|| Error::Invalid("command identity missing".into()))?;
    crate::OperationId::parse(&identity.operation_id)?;
    crate::IdempotencyKey::new(identity.idempotency_key.clone())?;
    Ok((
        identity,
        (authority_key(authority), identity.idempotency_key.clone()),
    ))
}

fn valid_authority(authority: Option<&wire::Authority>) -> Result<&wire::Authority> {
    let authority = authority.ok_or_else(|| Error::Invalid("authority missing".into()))?;
    if authority.id.is_empty()
        || matches!(authority.id.as_str(), "." | "..")
        || authority.id.contains(['/', '\\'])
        || authority.id.chars().any(char::is_control)
        || wire::AggregateKind::try_from(authority.kind)
            .map_or(true, |kind| kind == wire::AggregateKind::Unspecified)
    {
        return Err(Error::Invalid("invalid authority".into()));
    }
    Ok(authority)
}

#[path = "session_tasks.rs"]
mod task_views;
