//! Durable local Harness composition using public Stream and Filesystem providers.
use super::{FilesystemForkVerifier, FilesystemHost, HarnessStorage};
use crate::{
    AgentId, Capabilities, ConversationId, Error, OperationId, Result, SessionId, TaskId,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, Limits, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, EffectGuarantee, EffectStatus, Scope},
    durable_host::task_interaction_id,
    effects::{EffectDispatch, EffectProvider, EffectRegistry},
    executor::{ExecutionJournal, SwarmProviderAdmission, TurnOutput},
    fork::{CompositeForkVerifier, ForkSeed, ForkSeedVerifier, StreamHistoryForkVerifier},
    host_execution::{
        ExecutionApproval, ExecutionClaim, ExecutionClaimHandle, ExecutionEnvironment,
        ExecutionReceipt, ExecutionReceiptKey, ExecutionReceiptRecord, ExecutionReceiptStore,
        ExecutionResolutionCapability, ExecutionSpec, NativeExecutionProvider,
    },
    interaction::{Interaction, InteractionOutcome},
    model::{Model, ModelProvider},
    resources::ProviderRef,
    store::StreamAggregate,
    runtime::ToolContext,
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry, ToolResult},
};
use acyclic_fs::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions};
use acyclic_stream::{
    AppendOutcome, LocalStream, LocalStreamLimits, Stream, StreamClient, StreamError,
};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Persistent providers own durability; storage semantics are shared with memory.
pub type DurableHarnessStorage =
    HarnessStorage<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

const EXECUTION_RECEIPT_STREAM: &str = "harness/system/execution-receipts";
const EXECUTION_RECEIPT_MAX_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ExecutionReceiptEvent {
    Pending {
        key: ExecutionReceiptKey,
        owner_token: [u8; 32],
        generation: u64,
    },
    CancellationRequested {
        key: ExecutionReceiptKey,
    },
    Completed {
        key: ExecutionReceiptKey,
        result: FileRef,
        owner_token: [u8; 32],
        generation: u64,
        operator_resolution: bool,
        #[serde(default)]
        operator_principal: Option<String>,
        #[serde(default)]
        operator_authenticated: bool,
    },
}

/// Host-owned receipt journal for approved local process execution.
///
/// The stream is the durable compare-and-swap boundary. A caller first
/// appends a `Pending` event at the observed tail, then publishes a terminal
/// receipt at a later tail. Reopened providers therefore observe the pending
/// claim and refuse to spawn the command again until an operator resolves it.
/// The receipt content is staged under the reserved internal path and never
/// resolved from a model-supplied workspace reference.
pub struct FilesystemExecutionReceiptStore<A, O> {
    stream: Stream<LocalStream>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    read: ContentGrant,
    write: ContentGrant,
    session_id: SessionId,
    resolution_token: [u8; 32],
    maximum_bytes: u64,
}

impl<A, O> FilesystemExecutionReceiptStore<A, O>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    /// Binds one local stream and owner-authorized private volume.
    pub fn new(
        stream: StreamClient<LocalStream>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        session_id: SessionId,
        owner_scope: &crate::core::Scope,
        read: ContentGrant,
        write: ContentGrant,
        maximum_bytes: u64,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_bytes == 0
        {
            return Err(Error::Invalid(
                "execution receipts require a local agent-private volume".into(),
            ));
        }
        write.require(&volume, VolumeOperation::Write)?;
        read.require(&volume, VolumeOperation::Read)?;
        let resolution_token =
            ExecutionResolutionCapability::owner_token(session_id, &volume, owner_scope)?;
        let stream_name = format!(
            "{EXECUTION_RECEIPT_STREAM}/{}/{}",
            volume.storage_name()?,
            session_id
        );
        let stream = stream
            .stream(stream_name)
            .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            stream,
            host,
            volume,
            read,
            write,
            session_id,
            resolution_token,
            maximum_bytes: maximum_bytes.min(EXECUTION_RECEIPT_MAX_BYTES),
        })
    }

    fn validate_key(&self, key: &ExecutionReceiptKey) -> Result<()> {
        key.validate()
    }

    async fn events(&self) -> Result<(u64, Vec<ExecutionReceiptEvent>)> {
        let mut replay = self.stream.replay(0);
        let mut events = Vec::new();
        while let Some(page) = replay
            .next_page()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
        {
            for record in page {
                let event: ExecutionReceiptEvent =
                    serde_json::from_slice(&record.value).map_err(|error| {
                        Error::Storage(format!("invalid execution receipt journal: {error}"))
                    })?;
                events.push(event);
            }
        }
        self.validate_events(&events).await?;
        Ok((replay.cursor(), events))
    }

    async fn validate_events(&self, events: &[ExecutionReceiptEvent]) -> Result<()> {
        let mut pending = Vec::<(ExecutionReceiptKey, [u8; 32], u64)>::new();
        let mut cancellation_requested = Vec::<ExecutionReceiptKey>::new();
        let mut finalized = Vec::<ExecutionReceiptKey>::new();
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending {
                    key,
                    owner_token,
                    generation,
                } => {
                    self.validate_key(key)?;
                    if finalized.iter().any(|candidate| candidate == key)
                        || pending.iter().any(|(candidate, _, _)| candidate == key)
                    {
                        return Err(Error::Storage(
                            "execution receipt journal contains a duplicate claim".into(),
                        ));
                    }
                    pending.push((key.clone(), *owner_token, *generation));
                }
                ExecutionReceiptEvent::CancellationRequested { key } => {
                    self.validate_key(key)?;
                    if finalized.iter().any(|candidate| candidate == key)
                        || cancellation_requested
                            .iter()
                            .any(|candidate| candidate == key)
                        || !pending.iter().any(|(candidate, _, _)| candidate == key)
                    {
                        return Err(Error::Storage(
                            "execution receipt journal contains an invalid cancellation intent"
                                .into(),
                        ));
                    }
                    cancellation_requested.push(key.clone());
                }
                ExecutionReceiptEvent::Completed {
                    key,
                    result,
                    owner_token,
                    generation,
                    operator_resolution,
                    operator_principal,
                    operator_authenticated,
                } => {
                    self.validate_key(key)?;
                    self.validate_result_ref(key, result)?;
                    let bytes = self
                        .host
                        .read_internal_content(
                            result,
                            &self.volume,
                            &self.read,
                            super::InternalContentClass::Execution,
                            self.maximum_bytes,
                        )
                        .await?
                        .to_vec();
                    result.descriptor().verify(&bytes)?;
                    let receipt: ExecutionReceipt = serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    receipt.validate()?;
                    let explicitly_resolved_unknown = *operator_resolution
                        && *operator_authenticated
                        && matches!(receipt, ExecutionReceipt::Unknown { .. });
                    if cancellation_requested
                        .iter()
                        .any(|candidate| candidate == key)
                        && !matches!(receipt, ExecutionReceipt::Cancelled { .. })
                        && !explicitly_resolved_unknown
                    {
                        return Err(Error::Conflict(
                            "execution receipt completed after cancellation was requested".into(),
                        ));
                    }
                    if *operator_resolution != matches!(receipt, ExecutionReceipt::Unknown { .. })
                        || (*operator_authenticated && operator_principal.is_none())
                        || (!*operator_authenticated && operator_principal.is_some())
                        || operator_principal.as_deref().is_some_and(str::is_empty)
                    {
                        return Err(Error::Conflict(
                            "execution receipt operator marker does not match its typed outcome"
                                .into(),
                        ));
                    }
                    if finalized.iter().any(|candidate| candidate == key) {
                        return Err(Error::Storage(
                            "execution receipt terminal record is orphaned or duplicated".into(),
                        ));
                    }
                    let Some((_, pending_token, pending_generation)) =
                        pending.iter().find(|(candidate, _, _)| candidate == key)
                    else {
                        return Err(Error::Storage(
                            "execution receipt terminal record is orphaned or duplicated".into(),
                        ));
                    };
                    if pending_token != owner_token || pending_generation != generation {
                        return Err(Error::Conflict(
                            "execution receipt terminal owner does not match its claim".into(),
                        ));
                    }
                    pending.retain(|(candidate, _, _)| candidate != key);
                    cancellation_requested.retain(|candidate| candidate != key);
                    finalized.push(key.clone());
                }
            }
        }
        Ok(())
    }

    fn validate_result_ref(&self, key: &ExecutionReceiptKey, result: &FileRef) -> Result<()> {
        result.validate()?;
        if result.volume() != &self.volume
            || result.descriptor().media_type() != "application/json"
            || !result
                .path()
                .starts_with(&format!(".system/execution/{}/", key.attempt_id))
        {
            return Err(Error::Unauthorized(
                "execution receipt result is outside its host-owned attempt path".into(),
            ));
        }
        Ok(())
    }

    fn same_operation(left: &ExecutionReceiptKey, right: &ExecutionReceiptKey) -> bool {
        left.operation_id == right.operation_id && left.effect_id == right.effect_id
    }

    async fn append_at_tail(&self, tail: u64, event: &ExecutionReceiptEvent) -> Result<bool> {
        let bytes = crate::contract::canonical_json_bytes(event)?;
        match self
            .stream
            .append_at(bytes, tail)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
        {
            AppendOutcome::Committed(_) => Ok(true),
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }

    /// Lists claims that remain unresolved after a provider restart.
    #[cfg(test)]
    pub(crate) async fn pending_claims(
        &self,
        resolution: &ExecutionResolutionCapability,
        resolver: &ContentGrant,
    ) -> Result<Vec<(ExecutionReceiptKey, ExecutionClaimHandle)>> {
        if !resolution.matches(self.session_id, &self.volume, &self.resolution_token, None) {
            return Err(Error::Unauthorized(
                "execution receipt resolver is not authenticated for this session".into(),
            ));
        }
        resolver.require(&self.volume, VolumeOperation::Write)?;
        let (_, events) = self.events().await?;
        let mut pending: Vec<(ExecutionReceiptKey, ExecutionClaimHandle)> = Vec::new();
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending {
                    key,
                    owner_token,
                    generation,
                } => {
                    if !pending.iter().any(|(candidate, _)| candidate == &key) {
                        pending.push((
                            key.clone(),
                            ExecutionClaimHandle::from_owner_parts(key, owner_token, generation),
                        ));
                    }
                }
                ExecutionReceiptEvent::Completed { key, result, .. } => {
                    result.validate()?;
                    self.validate_result_ref(&key, &result)?;
                    pending.retain(|(candidate, _)| *candidate != key);
                }
                ExecutionReceiptEvent::CancellationRequested { .. } => {}
            }
        }
        Ok(pending)
    }

    /// Returns one pending claim for an externally authenticated operator.
    ///
    /// Operation capabilities are deliberately non-enumerating: a caller must
    /// present a grant for this exact operation, session, and private volume
    /// before the journal reveals the pending claim token.
    pub async fn pending_claim_for_operator(
        &self,
        key: &ExecutionReceiptKey,
        resolution: &ExecutionResolutionCapability,
        resolver: &ContentGrant,
    ) -> Result<ExecutionClaimHandle> {
        self.validate_key(key)?;
        if !resolution.is_operator_for(self.session_id, &self.volume, key.operation_id) {
            return Err(Error::Unauthorized(
                "operator capability is not bound to this execution operation".into(),
            ));
        }
        resolver.require(&self.volume, VolumeOperation::Write)?;
        let (_, events) = self.events().await?;
        if events.iter().any(|event| {
            matches!(event, ExecutionReceiptEvent::Completed { key: candidate, .. } if candidate == key)
        }) {
            return Err(Error::Conflict(
                "execution attempt already has a terminal receipt".into(),
            ));
        }
        let Some((owner_token, generation)) = events.iter().find_map(|event| {
            let ExecutionReceiptEvent::Pending {
                key: candidate,
                owner_token,
                generation,
            } = event
            else {
                return None;
            };
            (candidate == key).then_some((*owner_token, *generation))
        }) else {
            return Err(Error::NotFound(
                "execution attempt is not pending operator resolution".into(),
            ));
        };
        ExecutionClaimHandle::from_operator_parts(
            key.clone(),
            owner_token,
            generation,
            resolution.principal().to_owned(),
        )
    }

    /// Explicitly resolves a pending claim as unknown after operator review.
    /// This is the only supported path for clearing uncertainty; retrying the
    /// command itself is never inferred from a process or storage failure.
    pub async fn resolve_unknown(
        &self,
        key: &ExecutionReceiptKey,
        resolution: &ExecutionResolutionCapability,
        resolver: &ContentGrant,
        handle: &ExecutionClaimHandle,
        reason: impl Into<String>,
    ) -> Result<FileRef> {
        if !resolution.is_operator_for(self.session_id, &self.volume, key.operation_id) {
            return Err(Error::Unauthorized(
                "execution receipt resolver is not authenticated for this session".into(),
            ));
        }
        resolver.require(&self.volume, VolumeOperation::Write)?;
        if !handle.is_operator() {
            return Err(Error::Unauthorized(
                "pending execution resolution requires an operator handle".into(),
            ));
        }
        if handle.operator_principal() != Some(resolution.principal()) {
            return Err(Error::Unauthorized(
                "operator handle is bound to a different authenticated principal".into(),
            ));
        }
        self.publish(
            key,
            handle,
            &ExecutionReceipt::Unknown {
                reason: reason.into(),
            },
        )
        .await
    }

    #[cfg(test)]
    async fn resolve_unknown_owner(
        &self,
        key: &ExecutionReceiptKey,
        resolution: &ExecutionResolutionCapability,
        resolver: &ContentGrant,
        handle: &ExecutionClaimHandle,
        reason: impl Into<String>,
    ) -> Result<FileRef> {
        if !resolution.matches(self.session_id, &self.volume, &self.resolution_token, None) {
            return Err(Error::Unauthorized(
                "owner execution resolver is not authenticated for this session".into(),
            ));
        }
        resolver.require(&self.volume, VolumeOperation::Write)?;
        if !handle.is_operator() || handle.operator_authenticated() {
            return Err(Error::Unauthorized(
                "owner recovery requires the internal owner resolution handle".into(),
            ));
        }
        self.publish(
            key,
            handle,
            &ExecutionReceipt::Unknown {
                reason: reason.into(),
            },
        )
        .await
    }

    async fn terminal_for(
        &self,
        events: &[ExecutionReceiptEvent],
        key: &ExecutionReceiptKey,
    ) -> Result<Option<ExecutionReceiptRecord>> {
        let mut pending = false;
        let mut terminal = None;
        for event in events {
            match event {
                ExecutionReceiptEvent::Pending { key: candidate, .. } if candidate == key => {
                    pending = true;
                }
                ExecutionReceiptEvent::Completed {
                    key: candidate,
                    result,
                    operator_principal,
                    operator_authenticated,
                    ..
                } if candidate == key => {
                    self.validate_result_ref(candidate, result)?;
                    let bytes = self
                        .host
                        .read_internal_content(
                            result,
                            &self.volume,
                            &self.read,
                            super::InternalContentClass::Execution,
                            self.maximum_bytes,
                        )
                        .await?
                        .to_vec();
                    result.descriptor().verify(&bytes)?;
                    let receipt: ExecutionReceipt = serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    receipt.validate()?;
                    let record = ExecutionReceiptRecord {
                        key: candidate.clone(),
                        result: result.clone(),
                        receipt,
                        operator_principal: operator_principal.clone(),
                        operator_authenticated: *operator_authenticated,
                    };
                    if !pending {
                        return Err(Error::Storage(
                            "execution receipt completed without a durable claim".into(),
                        ));
                    }
                    if terminal
                        .as_ref()
                        .is_some_and(|prior: &ExecutionReceiptRecord| prior != &record)
                    {
                        return Err(Error::Conflict(
                            "execution receipt journal contains conflicting terminal records"
                                .into(),
                        ));
                    }
                    terminal = Some(record);
                }
                _ => {}
            }
        }
        Ok(terminal)
    }
}

impl<A, O> ExecutionReceiptStore for FilesystemExecutionReceiptStore<A, O>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    fn claim<'a>(&'a self, key: &'a ExecutionReceiptKey) -> BoxFuture<'a, Result<ExecutionClaim>> {
        Box::pin(async move {
            self.validate_key(key)?;
            loop {
                let (tail, events) = self.events().await?;
                if let Some(record) = self.terminal_for(&events, key).await? {
                    return Ok(ExecutionClaim::Completed(record));
                }
                if events.iter().any(|event| {
                    matches!(event, ExecutionReceiptEvent::Pending { key: candidate, .. } if candidate == key)
                }) {
                    return Ok(ExecutionClaim::Pending);
                }
                let fenced_keys: Vec<ExecutionReceiptKey> = events
                    .iter()
                    .filter_map(|event| match event {
                        ExecutionReceiptEvent::Pending { key: candidate, .. }
                        | ExecutionReceiptEvent::CancellationRequested { key: candidate }
                            if Self::same_operation(candidate, key) =>
                        {
                            Some(candidate.clone())
                        }
                        ExecutionReceiptEvent::Completed { key: candidate, .. }
                            if Self::same_operation(candidate, key) =>
                        {
                            Some(candidate.clone())
                        }
                        _ => None,
                    })
                    .collect();
                for candidate in fenced_keys {
                    if let Some(record) = self.terminal_for(&events, &candidate).await? {
                        return Ok(
                            if matches!(record.receipt, ExecutionReceipt::Unknown { .. }) {
                                ExecutionClaim::Completed(record)
                            } else {
                                ExecutionClaim::Pending
                            },
                        );
                    }
                    if events.iter().any(|event| {
                        matches!(
                            event,
                            ExecutionReceiptEvent::Pending { key: pending, .. }
                                if pending == &candidate
                        )
                    }) {
                        return Ok(ExecutionClaim::Pending);
                    }
                }
                let handle = ExecutionClaimHandle::issue(key.clone(), tail);
                if self
                    .append_at_tail(
                        tail,
                        &ExecutionReceiptEvent::Pending {
                            key: key.clone(),
                            owner_token: *handle.token(),
                            generation: handle.generation(),
                        },
                    )
                    .await?
                {
                    return Ok(ExecutionClaim::Acquired { handle });
                }
            }
        })
    }

    fn load<'a>(
        &'a self,
        key: &'a ExecutionReceiptKey,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
        Box::pin(async move {
            self.validate_key(key)?;
            let (_, events) = self.events().await?;
            self.terminal_for(&events, key).await
        })
    }

    fn load_attempt<'a>(
        &'a self,
        attempt_id: crate::EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
        Box::pin(async move {
            if attempt_id.into_bytes() == [0; 16] {
                return Err(Error::Invalid(
                    "execution receipt attempt identity cannot be empty".into(),
                ));
            }
            let (_, events) = self.events().await?;
            let mut record = None;
            for event in &events {
                if let ExecutionReceiptEvent::Completed { key: candidate, .. } = event
                    && candidate.attempt_id == attempt_id
                {
                    let Some(candidate) = self.terminal_for(&events, candidate).await? else {
                        continue;
                    };
                    if record.as_ref().is_some_and(|prior| prior != &candidate) {
                        return Err(Error::Conflict(
                            "execution receipt attempt has conflicting terminal records".into(),
                        ));
                    }
                    record = Some(candidate);
                }
            }
            Ok(record)
        })
    }

    fn request_cancel<'a>(&'a self, key: &'a ExecutionReceiptKey) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.validate_key(key)?;
            loop {
                let (tail, events) = self.events().await?;
                if self.terminal_for(&events, key).await?.is_some() {
                    return Ok(());
                }
                if events.iter().any(|event| {
                    matches!(event, ExecutionReceiptEvent::CancellationRequested { key: candidate } if candidate == key)
                }) {
                    return Ok(());
                }
                if !events.iter().any(|event| {
                    matches!(event, ExecutionReceiptEvent::Pending { key: candidate, .. } if candidate == key)
                }) {
                    return Err(Error::Conflict(
                        "execution cancellation has no durable pending claim".into(),
                    ));
                }
                if self
                    .append_at_tail(
                        tail,
                        &ExecutionReceiptEvent::CancellationRequested { key: key.clone() },
                    )
                    .await?
                {
                    return Ok(());
                }
            }
        })
    }

    fn publish<'a>(
        &'a self,
        key: &'a ExecutionReceiptKey,
        handle: &'a ExecutionClaimHandle,
        receipt: &'a ExecutionReceipt,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            self.validate_key(key)?;
            receipt.validate()?;
            let bytes =
                serde_json::to_vec(receipt).map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid("execution receipt exceeds its bound".into()));
            }
            if !handle.matches(key) {
                return Err(Error::Unauthorized(
                    "execution receipt handle is bound to another key".into(),
                ));
            }
            if handle.is_operator() && !matches!(receipt, ExecutionReceipt::Unknown { .. }) {
                return Err(Error::Unauthorized(
                    "operator handles may only resolve execution as unknown".into(),
                ));
            }
            if !handle.is_operator() && matches!(receipt, ExecutionReceipt::Unknown { .. }) {
                return Err(Error::Unauthorized(
                    "unknown execution outcomes require operator resolution".into(),
                ));
            }
            if handle.is_operator()
                && handle.operator_authenticated()
                && handle.operator_principal().is_none()
            {
                return Err(Error::Unauthorized(
                    "operator receipt publication lacks an authenticated principal".into(),
                ));
            }
            loop {
                let (tail, events) = self.events().await?;
                if let Some(record) = self.terminal_for(&events, key).await? {
                    if record.receipt == *receipt {
                        return Ok(record.result);
                    }
                    return Err(Error::Conflict(
                        "execution receipt claim was already finalized".into(),
                    ));
                }
                let Some((owner_token, generation)) = events.iter().find_map(|event| {
                    let ExecutionReceiptEvent::Pending {
                        key: candidate,
                        owner_token,
                        generation,
                    } = event
                    else {
                        return None;
                    };
                    (candidate == key).then_some((*owner_token, *generation))
                }) else {
                    return Err(Error::Conflict(
                        "execution receipt publication has no durable claim".into(),
                    ));
                };
                if owner_token != *handle.token() || generation != handle.generation() {
                    return Err(Error::Conflict(
                        "execution receipt handle is stale or owned by another dispatcher".into(),
                    ));
                }
                if events.iter().any(|event| {
                    matches!(
                        event,
                        ExecutionReceiptEvent::CancellationRequested { key: candidate }
                            if candidate == key
                    )
                }) && !matches!(receipt, ExecutionReceipt::Cancelled { .. })
                    && !(handle.is_operator()
                        && handle.operator_authenticated()
                        && matches!(receipt, ExecutionReceipt::Unknown { .. }))
                {
                    return Err(Error::Conflict(
                        "execution receipt publication lost a cancellation race".into(),
                    ));
                }
                let result = self
                    .host
                    .put_internal_content(
                        &self.volume,
                        &self.write,
                        &format!(
                            ".system/execution/{}/{}.json",
                            key.attempt_id,
                            if handle.is_operator() {
                                format!("resolve-{}", hex::encode(handle.token()))
                            } else {
                                "dispatch".into()
                            }
                        ),
                        &bytes,
                        "application/json",
                        "execution-result.json",
                        self.maximum_bytes,
                        &crate::IdempotencyKey::new(format!(
                            "execution-receipt:{}:{}",
                            key.attempt_id,
                            hex::encode(handle.token())
                        ))?,
                        super::InternalContentClass::Execution,
                    )
                    .await?;
                let record = ExecutionReceiptRecord {
                    key: key.clone(),
                    result,
                    receipt: receipt.clone(),
                    operator_principal: handle.operator_principal().map(ToOwned::to_owned),
                    operator_authenticated: handle.operator_authenticated(),
                };
                record.validate()?;
                let published = record.result.clone();
                if self
                    .append_at_tail(
                        tail,
                        &ExecutionReceiptEvent::Completed {
                            key: record.key,
                            result: record.result,
                            owner_token: *handle.token(),
                            generation: handle.generation(),
                            operator_resolution: handle.is_operator(),
                            operator_principal: handle.operator_principal().map(ToOwned::to_owned),
                            operator_authenticated: handle.operator_authenticated(),
                        },
                    )
                    .await?
                {
                    return Ok(published);
                }
            }
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionDescriptor {
    version: u32,
    agent: AgentId,
    conversation: Authority,
    private_volume: VolumeRef,
    signing_key: [u8; 32],
    #[serde(default)]
    session_id: Option<SessionId>,
    model: Model,
    limits: Limits,
    /// Optional owner-selected project volume used by the recursive local
    /// allocator. It is pinned in the descriptor so reopen cannot silently
    /// switch the source workspace.
    #[serde(default)]
    project: Option<VolumeRef>,
}

impl SessionDescriptor {
    fn fresh_with_provider(
        model: Model,
        limits: Limits,
        project: Option<VolumeRef>,
        filesystem_provider: ProviderRef,
    ) -> Result<Self> {
        filesystem_provider.validate()?;
        if filesystem_provider.family() != "filesystem" {
            return Err(Error::Invalid(
                "local session requires a filesystem provider identity".into(),
            ));
        }
        if let Some(project) = &project
            && project.provider() != &filesystem_provider
        {
            return Err(Error::Invalid(
                "local session project belongs to another filesystem provider".into(),
            ));
        }
        let agent = AgentId::new();
        let descriptor = Self {
            version: 1,
            agent,
            conversation: Authority {
                kind: AggregateKind::Conversation,
                id: ConversationId::new().to_string(),
            },
            private_volume: VolumeRef::new(
                filesystem_provider,
                OperationId::new().to_string(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            signing_key: *blake3::hash(&OperationId::new().into_bytes()).as_bytes(),
            session_id: Some(SessionId::new()),
            model,
            limits,
            project,
        };
        Ok(descriptor)
    }
}

fn descriptor_session_id(descriptor: &SessionDescriptor) -> Result<SessionId> {
    let session_id = if let Some(session_id) = descriptor.session_id {
        session_id
    } else {
        let mut input = Vec::with_capacity(40);
        input.extend_from_slice(b"acyclic:harness:legacy-session:v1");
        input.extend_from_slice(&descriptor.signing_key);
        let bytes: [u8; 16] = blake3::hash(&input).as_bytes()[..16]
            .try_into()
            .map_err(|_| Error::Invalid("legacy session identity has invalid length".into()))?;
        SessionId::from_bytes(bytes)
    };
    if session_id.into_bytes() == [0; 16] {
        return Err(Error::Invalid("session identity cannot be zero".into()));
    }
    Ok(session_id)
}

fn validate_descriptor(
    descriptor: &SessionDescriptor,
    model: &Model,
    limits: Limits,
    project: Option<&VolumeRef>,
) -> Result<()> {
    if descriptor.agent.into_bytes() == [0; 16]
        || descriptor.signing_key == [0; 32]
        || descriptor.conversation.kind != AggregateKind::Conversation
    {
        return Err(Error::Conflict(
            "local session descriptor has an invalid owner identity".into(),
        ));
    }
    descriptor.conversation.stream_path()?;
    descriptor.private_volume.validate()?;
    if descriptor.private_volume.class() != VolumeClass::AgentPrivate
        || descriptor.private_volume.owner() != &VolumeOwner::Agent(descriptor.agent)
    {
        return Err(Error::Conflict(
            "local session descriptor private volume is not owned by its agent".into(),
        ));
    }
    if let Some(descriptor_project) = &descriptor.project {
        descriptor_project.validate()?;
        if descriptor_project.class() != VolumeClass::Project
            || descriptor_project.provider() != descriptor.private_volume.provider()
        {
            return Err(Error::Conflict(
                "local session descriptor project binding is inconsistent".into(),
            ));
        }
    }
    if descriptor.version != 1
        || &descriptor.model != model
        || crate::contract::canonical_json_digest(&descriptor.limits)?
            != crate::contract::canonical_json_digest(&limits)?
    {
        return Err(Error::Conflict(
            "local session composition differs from its pinned descriptor".into(),
        ));
    }
    if let Some(project) = project
        && descriptor.project.as_ref() != Some(project)
    {
        return Err(Error::Conflict(
            "local session project differs from its pinned descriptor".into(),
        ));
    }
    Ok(())
}

/// Ready-to-run, reopenable local agent with pinned composition.
pub struct PersistentLocalHarness {
    storage: DurableHarnessStorage,
    bundle: crate::Harness,
}

/// Optional owner supplied tools shared by every session in one local swarm.
///
/// The registry is copied into each immutable Harness composition. Executors
/// retain their authenticated host and wait sources; model input only sees
/// the version pinned definitions selected by the registry.
#[derive(Clone, Default)]
pub struct LocalHarnessTools {
    tools: ToolRegistry,
    batch_publisher: Option<Arc<dyn crate::batch_publication::ModelBatchPublisher>>,
    authenticated_task: Option<TaskId>,
}

impl LocalHarnessTools {
    /// Creates an empty extension set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tools: ToolRegistry::new(),
            batch_publisher: None,
            authenticated_task: None,
        }
    }

    /// Retains one explicitly assembled tool registry.
    #[must_use]
    pub fn from_registry(tools: ToolRegistry) -> Self {
        Self {
            tools,
            batch_publisher: None,
            authenticated_task: None,
        }
    }

    /// Binds the owner mediated completed batch publisher used by model turns.
    #[must_use]
    pub fn with_batch_publisher(
        mut self,
        publisher: Arc<dyn crate::batch_publication::ModelBatchPublisher>,
    ) -> Self {
        self.batch_publisher = Some(publisher);
        self
    }

    /// Carries the durable task selected by the owning swarm composition.
    #[must_use]
    pub(crate) fn with_authenticated_task(mut self, task_id: TaskId) -> Self {
        self.authenticated_task = Some(task_id);
        self
    }

    /// Adds one owner-constructed tool while preserving the registry's pinned
    /// definition and revision.  Local swarm composition uses this to install
    /// the filesystem Git facade after it has bound the task's authenticated
    /// project workspace.
    pub(crate) fn with_tool(mut self, tool: crate::tool::Tool) -> Result<Self> {
        self.tools.register(tool)?;
        Ok(self)
    }

    pub(crate) fn install_into(
        &self,
        mut builder: crate::bundle::HarnessBuilder,
    ) -> Result<crate::bundle::HarnessBuilder> {
        for definition in self.tools.definitions()? {
            let tool = self
                .tools
                .get_version(&definition.name, &definition.revision)
                .cloned()
                .ok_or_else(|| {
                    Error::Storage("local tool registry lost selected revision".into())
                })?;
            builder = builder.tool(tool)?;
            builder = builder.grant(format!("tool:call:{}", definition.name));
        }
        if let Some(publisher) = &self.batch_publisher {
            builder = builder.batch_publisher(publisher.clone());
        }
        if let Some(task_id) = self.authenticated_task {
            // These grants accompany the authenticated communication tools,
            // so admission records the capabilities of the actual bundle.
            builder = builder
                .grant("mail:send")
                .grant("mail:read")
                .grant("timer:wait");
            builder = builder.authenticated_task(task_id);
        }
        Ok(builder)
    }
}

const LOCAL_EXECUTION_TOOL: &str = "acyclic.shell";
pub(super) const LOCAL_EXECUTION_CAPABILITY: &str = "tool:call:acyclic.shell";
pub(super) const INTERACTION_ROUTE_CAPABILITY: &str = "interaction:route";
const LOCAL_EXECUTION_PROVIDER: &str = "harness.native-execution.v1";
const LOCAL_EXECUTION_KIND: &str = "host.process";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalExecutionInput {
    executable: String,
    arguments: Vec<String>,
    working_directory: String,
    environment: ExecutionEnvironment,
    timeout_ms: Option<u64>,
    max_output_bytes: u32,
}

impl LocalExecutionInput {
    fn spec(self) -> Result<ExecutionSpec> {
        let spec = ExecutionSpec {
            executable: self.executable,
            arguments: self.arguments,
            working_directory: self.working_directory,
            environment: self.environment,
            timeout_ms: self.timeout_ms,
            max_output_bytes: self.max_output_bytes,
        };
        spec.validate()?;
        Ok(spec)
    }
}

/// Harness-owned adapter for exact approved local process execution.
///
/// The tool only opens an authenticated approval interaction and dispatches
/// through the host execution provider. It never accepts an ambient command
/// string, inherits the host environment, or manufactures an approval.
struct LocalExecutionTool {
    journal: Arc<dyn ExecutionJournal>,
    provider: Arc<dyn EffectProvider>,
    volume: VolumeRef,
    session_id: SessionId,
}

/// Host-only request reader for the native provider. Execution journal refs
/// live under `.system/execution`; they must never be opened through the
/// model-facing private-content reader.
struct ExecutionJournalRequestReader {
    journal: Arc<dyn ExecutionJournal>,
}

impl ContentResidencyVerifier for ExecutionJournalRequestReader {
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = self.journal.load(reference).await?;
            reference.descriptor().verify(&bytes)
        })
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = self.journal.load(reference).await?;
            reference.descriptor().verify(&bytes)?;
            Ok(bytes)
        })
    }
}

fn execution_request_key(operation_id: OperationId) -> String {
    format!("native-execution-request:{operation_id}")
}

fn local_interaction_id(session_id: SessionId, operation_id: OperationId) -> crate::InteractionId {
    let digest = blake3::hash(
        &[
            b"harness/local-execution-interaction:v1".as_slice(),
            session_id.into_bytes().as_slice(),
            operation_id.into_bytes().as_slice(),
        ]
        .concat(),
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    crate::InteractionId::from_bytes(bytes)
}

impl LocalExecutionTool {
    fn definition() -> ToolDefinition {
        ToolDefinition {
            name: LOCAL_EXECUTION_TOOL.into(),
            revision: "1".into(),
            description: "Execute one exact absolute host command after owner approval".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "executable": {"type": "string", "minLength": 1},
                    "arguments": {"type": "array", "items": {"type": "string"}},
                    "working_directory": {"type": "string", "minLength": 1},
                    "environment": {
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {"kind": {"const": "clear"}},
                                "required": ["kind"],
                                "additionalProperties": false
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "kind": {"const": "explicit"},
                                    "variables": {"type": "object", "additionalProperties": {"type": "string"}}
                                },
                                "required": ["kind", "variables"],
                                "additionalProperties": false
                            }
                        ]
                    },
                    "timeout_ms": {"type": ["integer", "null"], "minimum": 1},
                    "max_output_bytes": {"type": "integer", "minimum": 1}
                },
                "required": ["executable", "arguments", "working_directory", "environment", "timeout_ms", "max_output_bytes"],
                "additionalProperties": false
            }),
            output_schema: Self::output_schema(),
            model_output_schema: Self::output_schema(),
        }
    }

    fn output_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "operation_id": {"type": "string"},
                "receipt": {
                    "type": "object",
                    "required": ["kind"],
                    "properties": {"kind": {"type": "string"}},
                    "additionalProperties": true
                }
            },
            "required": ["operation_id", "receipt"],
            "additionalProperties": false
        })
    }

    fn attempt_id(operation_id: OperationId) -> crate::EffectAttemptId {
        let digest = blake3::hash(
            &[
                b"harness/local-execution-attempt:v1".as_slice(),
                operation_id.into_bytes().as_slice(),
            ]
            .concat(),
        );
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        crate::EffectAttemptId::from_bytes(bytes)
    }

    async fn stage_request(
        &self,
        operation_id: OperationId,
        interaction_id: crate::InteractionId,
        spec: ExecutionSpec,
    ) -> Result<(FileRef, [u8; 32])> {
        // The journal owns the host-only `.system/execution` path and its
        // idempotency fence. The returned immutable ref still binds every
        // request byte and is authenticated by the provider.
        let mut approval = ExecutionApproval::approve_for(
            self.session_id,
            interaction_id,
            operation_id,
            spec,
        )?;
        let request_key = execution_request_key(operation_id);
        let path = super::execution_journal::execution_content_path(operation_id, &request_key);
        approval.bind_request_location(&self.volume, &path)?;
        let bytes = serde_json::to_vec(&approval)
            .map_err(|error| Error::Invalid(format!("execution approval is not serializable: {error}")))?;
        let reference = self
            .journal
            .stage(
                operation_id,
                request_key,
                bytes,
                "application/json",
            )
            .await?;
        let digest = crate::core::effect_request_digest(
            LOCAL_EXECUTION_PROVIDER,
            EffectGuarantee::AtMostOnce,
            LOCAL_EXECUTION_KIND,
            &reference,
        )?;
        Ok((reference, digest))
    }

    async fn execute_scoped(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> Result<ToolResult> {
        let task = context.task().durable_task_id().ok_or_else(|| {
            Error::Unsupported("local host execution requires an admitted durable task".into())
        })?;
        let input: LocalExecutionInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input is invalid: {error}")))?;
        let spec = input.spec()?;
        let interaction_id = task_interaction_id(task, invocation.operation_id);
        let (request, request_digest) = self
            .stage_request(invocation.operation_id, interaction_id, spec)
            .await?;
        let interaction = Interaction::approval(
            "Approve this exact host executable, argv, working directory, and environment",
            invocation.operation_id,
            request_digest,
        )?;
        match context
            .interact(invocation.operation_id, interaction)
            .await?
        {
            InteractionOutcome::Indeterminate { .. } => {
                Err(Error::Indeterminate(invocation.operation_id))
            }
            InteractionOutcome::Declined
            | InteractionOutcome::Cancelled
            | InteractionOutcome::Expired
            | InteractionOutcome::Denied => Ok(ToolResult {
                value: json!({
                    "operation_id": invocation.operation_id.to_string(),
                    "receipt": {"kind": "denied", "reason": "owner did not approve execution"}
                }),
            }),
            InteractionOutcome::Approved => {
                self.dispatch_approved(invocation.operation_id, request, request_digest)
                    .await
            }
            InteractionOutcome::Answered { .. } => Err(Error::Conflict(
                "execution approval resolved with a non-approval answer".into(),
            )),
        }
    }

    async fn execute_local(&self, invocation: ToolInvocation) -> Result<ToolResult> {
        let input: LocalExecutionInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input is invalid: {error}")))?;
        let spec = input.spec()?;
        let interaction_id = local_interaction_id(self.session_id, invocation.operation_id);
        let (request, request_digest) = self
            .stage_request(invocation.operation_id, interaction_id, spec)
            .await?;
        let interaction = Interaction::approval(
            "Approve this exact host executable, argv, working directory, and environment",
            invocation.operation_id,
            request_digest,
        )?;
        self.journal
            .open_interaction(interaction_id, interaction)
            .await?;
        let outcome = self
            .journal
            .interaction_outcome(interaction_id)
            .await?
            .unwrap_or(InteractionOutcome::Indeterminate {
                operation_id: invocation.operation_id,
            });
        match outcome {
            InteractionOutcome::Approved => {
                self.dispatch_approved(invocation.operation_id, request, request_digest)
                    .await
            }
            InteractionOutcome::Declined
            | InteractionOutcome::Cancelled
            | InteractionOutcome::Expired
            | InteractionOutcome::Denied => Ok(ToolResult {
                value: json!({
                    "operation_id": invocation.operation_id.to_string(),
                    "receipt": {"kind": "denied", "reason": "owner did not approve execution"}
                }),
            }),
            InteractionOutcome::Indeterminate { .. } => {
                Err(Error::Indeterminate(invocation.operation_id))
            }
            InteractionOutcome::Answered { .. } => Err(Error::Conflict(
                "execution approval resolved with a non-approval answer".into(),
            )),
        }
    }

    async fn dispatch_approved(
        &self,
        operation_id: OperationId,
        request: FileRef,
        request_digest: [u8; 32],
    ) -> Result<ToolResult> {
        let dispatch = EffectDispatch {
            provider: LOCAL_EXECUTION_PROVIDER.into(),
            effect_id: crate::EffectId::from_bytes(operation_id.into_bytes()),
            attempt_id: Self::attempt_id(operation_id),
            effect_kind: LOCAL_EXECUTION_KIND.into(),
            request,
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let observation = self.provider.dispatch(dispatch).await?;
        self.observation_result(operation_id, observation).await
    }

    async fn observation_result(
        &self,
        operation_id: OperationId,
        observation: crate::effects::EffectObservation,
    ) -> Result<ToolResult> {
        if observation.provider != LOCAL_EXECUTION_PROVIDER
            || observation.effect_id != crate::EffectId::from_bytes(operation_id.into_bytes())
            || observation.guarantee != EffectGuarantee::AtMostOnce
        {
            return Err(Error::Conflict(
                "native execution observation is bound to a different effect".into(),
            ));
        }
        let receipt = match observation.status {
            EffectStatus::Succeeded { result }
            | EffectStatus::FailedWithReceipt { result, .. } => {
                let bytes = self.journal.load(&result).await?;
                let receipt: ExecutionReceipt = serde_json::from_slice(&bytes).map_err(|error| {
                    Error::Storage(format!("execution receipt is invalid: {error}"))
                })?;
                receipt.validate()?;
                receipt
            }
            EffectStatus::Indeterminate | EffectStatus::Planned | EffectStatus::Dispatched => {
                return Err(Error::Indeterminate(operation_id));
            }
            EffectStatus::Failed { message } => {
                return Err(Error::Storage(format!("native execution failed: {message}")));
            }
        };
        Ok(ToolResult {
            value: json!({
                "operation_id": operation_id.to_string(),
                "receipt": receipt,
            }),
        })
    }

    async fn reconcile_scoped(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> Result<Option<ToolResult>> {
        let task = context.task().durable_task_id().ok_or_else(|| {
            Error::Unsupported("local host execution requires an admitted durable task".into())
        })?;
        let input: LocalExecutionInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input is invalid: {error}")))?;
        let spec = input.spec()?;
        let interaction_id = task_interaction_id(task, invocation.operation_id);
        let (_request, request_digest) = self
            .stage_request(invocation.operation_id, interaction_id, spec)
            .await?;
        let attempt_id = Self::attempt_id(invocation.operation_id);
        let Some(observation) = self.provider.reconcile(attempt_id).await? else {
            // No receipt means dispatch has not produced an observable
            // attempt yet. Re-open the exact durable approval ticket so an
            // operator can resolve a pending request after restart. Once an
            // attempt exists, the provider branch above is the only path.
            return self.execute_scoped(context, invocation).await.map(Some);
        };
        if observation.attempt_id != attempt_id || observation.request_digest != request_digest {
            return Err(Error::Conflict(
                "native execution reconciliation is bound to a different request".into(),
            ));
        }
        self.observation_result(invocation.operation_id, observation)
            .await
            .map(Some)
    }

    async fn reconcile_local(&self, invocation: ToolInvocation) -> Result<Option<ToolResult>> {
        let input: LocalExecutionInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input is invalid: {error}")))?;
        let spec = input.spec()?;
        let interaction_id = local_interaction_id(self.session_id, invocation.operation_id);
        let (_request, request_digest) = self
            .stage_request(invocation.operation_id, interaction_id, spec)
            .await?;
        let attempt_id = Self::attempt_id(invocation.operation_id);
        let Some(observation) = self.provider.reconcile(attempt_id).await? else {
            return self.execute_local(invocation).await.map(Some);
        };
        if observation.attempt_id != attempt_id || observation.request_digest != request_digest {
            return Err(Error::Conflict(
                "native execution reconciliation is bound to a different request".into(),
            ));
        }
        self.observation_result(invocation.operation_id, observation)
            .await
            .map(Some)
    }
}

impl ToolExecutor for LocalExecutionTool {
    fn authorize(
        &self,
        scope: Option<&crate::runtime::RuntimeScope>,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        let scope = scope.ok_or_else(|| Error::Unauthorized("host execution requires scope".into()))?;
        if !scope.grants().contains(INTERACTION_ROUTE_CAPABILITY) {
            return Err(Error::Unauthorized(format!(
                "host execution requires {INTERACTION_ROUTE_CAPABILITY}"
            )));
        }
        let input: LocalExecutionInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input is invalid: {error}")))?;
        input.spec().map(|_| ())
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(self.execute_local(invocation))
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(self.execute_scoped(context, invocation))
    }

    fn reconcile<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.reconcile_local(invocation))
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.reconcile_scoped(context, invocation))
    }
}

pub(super) fn local_execution_tool(storage: &DurableHarnessStorage) -> Result<Tool> {
    let journal = storage.journal();
    let request_reader = Arc::new(ExecutionJournalRequestReader {
        journal: journal.clone(),
    });
    let (host, stream, read, write, maximum_bytes) = storage.execution_binding();
    let receipt_store = Arc::new(FilesystemExecutionReceiptStore::new(
        stream,
        host,
        storage.volume().clone(),
        storage.session_id(),
        storage.owner_scope(),
        read,
        write,
        maximum_bytes,
    )?);
    let provider = NativeExecutionProvider::native_with_receipt_store(
        request_reader,
        receipt_store,
        storage.execution_approval_verifier(),
    )?;
    Ok(Tool {
        definition: LocalExecutionTool::definition(),
        executor: Arc::new(LocalExecutionTool {
            journal,
            provider: Arc::new(provider),
            volume: storage.volume().clone(),
            session_id: storage.session_id(),
        }),
        projection: Arc::new(LocalExecutionToolProjection),
    })
}

pub(super) fn register_local_execution_tool(
    tools: &mut ToolRegistry,
    storage: &DurableHarnessStorage,
) -> Result<()> {
    tools.register(local_execution_tool(storage)?)
}

struct LocalExecutionToolProjection;

impl ToolProjection for LocalExecutionToolProjection {
    fn project(&self, _invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn default_local_bundle(
    storage: &DurableHarnessStorage,
    model: Model,
    provider: Arc<dyn ModelProvider>,
    limits: Limits,
    extension: LocalHarnessTools,
) -> Result<crate::Harness> {
    let mut tools = storage.default_tools(limits)?;
    register_local_execution_tool(&mut tools, storage)?;
    let builder = storage
        .builder()
        .model(model, provider)
        .tools(tools)
        .grant("model:generate")
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.stage_file")
        .grant("tool:call:acyclic.list_files")
        .grant(LOCAL_EXECUTION_CAPABILITY)
        .grant(INTERACTION_ROUTE_CAPABILITY)
        .limits(limits);
    extension.install_into(builder)?.build()
}

impl PersistentLocalHarness {
    /// Finishes a crate-owned composition without reopening its storage.
    /// Call only before exposing the session or admitting a model turn.
    pub(crate) fn with_local_tools(
        mut self,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        self.bundle = default_local_bundle(&self.storage, model, provider, limits, extension)?;
        Ok(self)
    }

    /// Composes a durable harness from provider and identity descriptors that
    /// the application has already persisted.
    pub async fn from_providers(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        agent: AgentId,
        volume: VolumeRef,
        conversation: Authority,
        issuer: AuthorityIssuer,
    ) -> Result<Self> {
        Self::from_providers_with_tools(
            model,
            provider,
            limits,
            host,
            stream,
            agent,
            volume,
            conversation,
            issuer,
            LocalHarnessTools::new(),
        )
        .await
    }

    /// Composes a durable local agent with owner supplied model tools.
    pub async fn from_providers_with_tools(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        agent: AgentId,
        volume: VolumeRef,
        conversation: Authority,
        issuer: AuthorityIssuer,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        Self::from_providers_with_tools_and_project(
            model,
            provider,
            limits,
            host,
            stream,
            agent,
            volume,
            conversation,
            issuer,
            extension,
            None,
        )
        .await
    }

    /// Composes a durable local agent with an owner-selected project volume.
    /// Project read/write capabilities are signed into the owner scope and
    /// pinned by the session descriptor; no model supplied reference can add
    /// project access.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider and authority boundaries remain explicit"
    )]
    pub async fn from_providers_with_tools_and_project(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        agent: AgentId,
        volume: VolumeRef,
        conversation: Authority,
        issuer: AuthorityIssuer,
        extension: LocalHarnessTools,
        project: Option<&VolumeRef>,
    ) -> Result<Self> {
        limits.validate()?;
        crate::model::validate_model_options(&model.options, provider.model_option_policy())?;
        let project_capabilities = match project {
            Some(project) => {
                if project.class() != VolumeClass::Project || project.provider() != &host.provider {
                    return Err(Error::Invalid(
                        "local session project belongs to another provider or class".into(),
                    ));
                }
                Capabilities::new([
                    project.capability(VolumeOperation::Read)?,
                    project.capability(VolumeOperation::Write)?,
                ])
            }
            None => Capabilities::new(std::iter::empty::<String>()),
        };
        let storage = DurableHarnessStorage::from_providers_with_reads(
            agent,
            limits.file_bytes,
            host.clone(),
            stream,
            volume,
            conversation,
            issuer,
            project_capabilities,
        )
        .await?
        .with_fork_verifier(local_fork_verifier(host.clone(), limits.file_bytes)?);
        let bundle = default_local_bundle(&storage, model, provider, limits, extension)?;
        Ok(Self { storage, bundle })
    }

    /// Composes a child harness from an already published typed fork. The
    /// caller owns the shared local providers and must retain the parent
    /// aggregate used by `spawn_from_report`; this constructor only binds the
    /// exact child seed and its immutable reference grants.
    pub async fn from_published_fork(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
    ) -> Result<Self> {
        Self::from_published_fork_with_tools(
            model,
            provider,
            limits,
            host,
            stream,
            issuer,
            parent,
            seed,
            LocalHarnessTools::new(),
        )
        .await
    }

    /// Composes a child from a published fork while retaining owner supplied
    /// model tools from the parent swarm.
    pub async fn from_published_fork_with_tools(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        Self::from_published_fork_with_tools_and_stream_provider(
            model,
            provider,
            limits,
            host,
            stream,
            issuer,
            parent,
            seed,
            extension,
            ProviderRef::new("local", "stream", "2")?,
        )
        .await
    }

    /// Composes a published child while pinning the stream provider used by
    /// its fork verifier to the surrounding local composition.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider identity is an explicit durable binding"
    )]
    pub async fn from_published_fork_with_tools_and_stream_provider(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
        extension: LocalHarnessTools,
        stream_provider: ProviderRef,
    ) -> Result<Self> {
        limits.validate()?;
        crate::model::validate_model_options(&model.options, provider.model_option_policy())?;
        let storage = DurableHarnessStorage::from_published_fork(
            limits.file_bytes,
            host.clone(),
            stream,
            issuer,
            parent,
            seed,
        )
        .await?
        .with_fork_verifier(local_fork_verifier_with_stream_provider(
            host.clone(),
            stream_provider,
            limits.file_bytes,
        )?);
        let bundle = default_local_bundle(&storage, model, provider, limits, extension)?;
        Ok(Self { storage, bundle })
    }

    /// Opens an exclusive session root, preserving stable identities and configuration.
    pub async fn open(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<Self> {
        Self::open_with_tools(root, model, provider, limits, LocalHarnessTools::new()).await
    }

    /// Reopens a local session with the same owner supplied tool registry.
    pub async fn open_with_tools(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        extension: LocalHarnessTools,
    ) -> Result<Self> {
        Self::open_with_tools_and_project(root, model, provider, limits, extension, None).await
    }

    /// Opens a durable local session with an owner-selected project binding.
    /// The project identity is persisted with the session descriptor and must
    /// match on every reopen.
    pub async fn open_with_tools_and_project(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        extension: LocalHarnessTools,
        project: Option<VolumeRef>,
    ) -> Result<Self> {
        Self::open_with_tools_and_project_for_provider(
            root,
            model,
            provider,
            limits,
            extension,
            project,
            ProviderRef::new("local", "filesystem", "2")?,
        )
        .await
    }

    /// Opens a durable local session using an explicit authenticated
    /// filesystem provider identity for fresh descriptor creation. Reopens
    /// always use the provider pinned in the descriptor.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider identity is an explicit durable binding"
    )]
    pub async fn open_with_tools_and_project_for_provider(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        extension: LocalHarnessTools,
        project: Option<VolumeRef>,
        filesystem_provider: ProviderRef,
    ) -> Result<Self> {
        limits.validate()?;
        crate::model::validate_model_options(&model.options, provider.model_option_policy())?;
        let root = root.as_ref();
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(root.join("history"), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let metadata = stream
            .stream("harness/session")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let missing = match metadata.tail().await {
            Ok(1) => false,
            Ok(0) | Err(StreamError::NotFound) => true,
            Ok(_) => {
                return Err(Error::Storage(
                    "invalid local session descriptor tail".into(),
                ));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if missing {
            let descriptor = SessionDescriptor::fresh_with_provider(
                model.clone(),
                limits,
                project.clone(),
                filesystem_provider,
            )?;
            match metadata
                .append_at(crate::contract::canonical_json_bytes(&descriptor)?, 0)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
            {
                AppendOutcome::Committed(_) => {}
                // Another opener published the descriptor. Read its durable
                // winner below and validate it against this request.
                AppendOutcome::TailConflict { .. } => {}
            }
        }
        let mut records = metadata
            .read(0, 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let record = records
            .next()
            .await
            .ok_or_else(|| Error::Storage("local descriptor is missing".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        let descriptor: SessionDescriptor = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        validate_descriptor(&descriptor, &model, limits, project.as_ref())?;
        let session_id = descriptor_session_id(&descriptor)?;
        let fs = LocalFs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = Arc::new(FilesystemHost::new(
            fs,
            descriptor.private_volume.provider().clone(),
        )?);
        host.create_volume(&descriptor.private_volume).await?;
        if let Some(project) = &descriptor.project {
            host.create_volume(project).await?;
        }
        let issuer = AuthorityIssuer::new(
            "local-harness",
            descriptor.signing_key,
            descriptor.conversation.clone(),
        );
        let project_capabilities = match descriptor.project.as_ref() {
            Some(project) => Capabilities::new([
                project.capability(VolumeOperation::Read)?,
                project.capability(VolumeOperation::Write)?,
            ]),
            None => Capabilities::new(std::iter::empty::<String>()),
        };
        let storage = DurableHarnessStorage::from_providers_with_session_and_reads(
            descriptor.agent,
            limits.file_bytes,
            host.clone(),
            stream,
            descriptor.private_volume,
            descriptor.conversation,
            issuer,
            project_capabilities,
            session_id,
        )
        .await?
        .with_fork_verifier(local_fork_verifier(host, limits.file_bytes)?);
        let bundle = default_local_bundle(&storage, model, provider, limits, extension)?;
        Ok(Self { storage, bundle })
    }

    /// Opens a durable session using providers owned by a surrounding
    /// composition.  Session metadata remains in `root/history`, while the
    /// conversation aggregate and filesystem volumes use the supplied
    /// provider instances.  This is required when a swarm resolver and its
    /// task harnesses share one authenticated local provider domain.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider identities are explicit durable bindings"
    )]
    pub async fn open_with_tools_and_project_on_providers(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        extension: LocalHarnessTools,
        project: Option<VolumeRef>,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        stream_provider: ProviderRef,
    ) -> Result<Self> {
        limits.validate()?;
        crate::model::validate_model_options(&model.options, provider.model_option_policy())?;
        if stream_provider.family() != "stream" {
            return Err(Error::Invalid(
                "local session requires a stream provider identity".into(),
            ));
        }
        if let Some(project) = &project
            && (project.class() != VolumeClass::Project || project.provider() != &host.provider)
        {
            return Err(Error::Invalid(
                "local session project belongs to another provider or class".into(),
            ));
        }
        let root = root.as_ref();
        let descriptor_stream = stream
            .stream(shared_session_descriptor_path(root))
            .map_err(|error| Error::Storage(error.to_string()))?;
        let missing = match descriptor_stream.tail().await {
            Ok(1) => false,
            Ok(0) | Err(StreamError::NotFound) => true,
            Ok(_) => {
                return Err(Error::Storage(
                    "invalid local session descriptor tail".into(),
                ));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if missing {
            let descriptor = SessionDescriptor::fresh_with_provider(
                model.clone(),
                limits,
                project.clone(),
                host.provider.clone(),
            )?;
            match descriptor_stream
                .append_at(crate::contract::canonical_json_bytes(&descriptor)?, 0)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
            {
                AppendOutcome::Committed(_) => {}
                // Another opener published the descriptor. Read its durable
                // winner below and validate it against this request.
                AppendOutcome::TailConflict { .. } => {}
            }
        }
        let mut records = descriptor_stream
            .read(0, 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let record = records
            .next()
            .await
            .ok_or_else(|| Error::Storage("local descriptor is missing".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        let descriptor: SessionDescriptor = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        validate_descriptor(&descriptor, &model, limits, project.as_ref())?;
        if descriptor.private_volume.provider() != &host.provider
            || descriptor
                .project
                .as_ref()
                .is_some_and(|project| project.provider() != &host.provider)
        {
            return Err(Error::Conflict(
                "local session descriptor belongs to another filesystem provider".into(),
            ));
        }
        let session_id = descriptor_session_id(&descriptor)?;
        host.create_volume(&descriptor.private_volume).await?;
        if let Some(project) = &descriptor.project {
            host.create_volume(project).await?;
        }
        let issuer = AuthorityIssuer::new(
            "local-harness",
            descriptor.signing_key,
            descriptor.conversation.clone(),
        );
        let project_capabilities = match descriptor.project.as_ref() {
            Some(project) => Capabilities::new([
                project.capability(VolumeOperation::Read)?,
                project.capability(VolumeOperation::Write)?,
            ]),
            None => Capabilities::new(std::iter::empty::<String>()),
        };
        let storage = DurableHarnessStorage::from_providers_with_session_and_reads(
            descriptor.agent,
            limits.file_bytes,
            host.clone(),
            stream,
            descriptor.private_volume,
            descriptor.conversation,
            issuer,
            project_capabilities,
            session_id,
        )
        .await?
        .with_fork_verifier(local_fork_verifier_with_stream_provider(
            host,
            stream_provider,
            limits.file_bytes,
        )?);
        let bundle = default_local_bundle(&storage, model, provider, limits, extension)?;
        Ok(Self { storage, bundle })
    }
    /// Runs or recovers an exact prompt with a caller-retained operation identity.
    pub async fn run(&self, operation: OperationId, prompt: &str) -> Result<TurnOutput> {
        let max_steps = u32::try_from(self.bundle.limits().model_steps)
            .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?;
        self.run_with_max_steps(operation, prompt, max_steps).await
    }

    /// Runs an exact prompt under an owner-pinned model step budget.
    pub async fn run_with_max_steps(
        &self,
        operation: OperationId,
        prompt: &str,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        self.run_with_bundle(&self.bundle, operation, prompt, max_steps)
            .await
    }

    pub(crate) async fn run_with_bundle(
        &self,
        bundle: &crate::Harness,
        operation: OperationId,
        prompt: &str,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        let content = self
            .storage
            .stage(
                operation,
                &format!("turns/{operation}/user.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        self.storage
            .run_conversation(bundle, operation, content, vec![], max_steps)
            .await
    }

    /// Runs a turn only under the exact owner admission that was persisted for
    /// this operation. The local runtime remains the execution engine, while
    /// this check prevents a reopened handle from substituting prompt, limits,
    /// or operation identity after budget admission.
    pub(crate) async fn run_with_admission(
        &self,
        bundle: &crate::Harness,
        admission: &crate::runtime::TaskAdmissionRecord,
        prompt: &str,
        max_steps: u32,
        budget: &mut dyn SwarmProviderAdmission,
    ) -> Result<TurnOutput> {
        if admission.input != serde_json::Value::String(prompt.to_owned())
            || bundle.limits() != admission.limits
            || !admission.grants.is_subset_of(bundle.capabilities())
            || admission
                .run_limits
                .max_steps
                .is_some_and(|limit| {
                    usize::try_from(max_steps).map_or(true, |steps| steps > limit)
                })
        {
            return Err(Error::Conflict(
                "local execution no longer matches its owner task admission".into(),
            ));
        }
        let current_policy = bundle
            .execution_contract()
            .and_then(|contract| contract.get("model_option_policy"))
            .filter(|policy| !policy.is_null())
            .map(|policy| {
                serde_json::from_value::<crate::registry::ComponentIdentity>(policy.clone())
                    .map_err(|error| {
                        Error::Conflict(format!(
                            "local execution policy contract is invalid: {error}"
                        ))
                    })
            })
            .transpose()?;
        if current_policy != admission.policy {
            return Err(Error::Conflict(
                "local execution provider policy changed after admission".into(),
            ));
        }
        let operation = admission.operation_id;
        let content = self
            .storage
            .stage(
                operation,
                &format!("turns/{operation}/user.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        self.storage
            .run_conversation_with_provider_budget(
                bundle,
                operation,
                content,
                vec![],
                max_steps,
                Some(budget),
            )
            .await
    }
    /// Provider-bound storage for tools and recovery.
    #[must_use]
    pub fn storage(&self) -> &DurableHarnessStorage {
        &self.storage
    }

    /// Returns host-managed session signing material to crate-owned durable
    /// compositions. It is never exposed to model providers or wire callers.
    pub(crate) fn signing_key(&self) -> [u8; 32] {
        self.storage.signing_key()
    }

    /// Reads the current authoritative conversation projection for host
    /// adapters without starting a model worker.
    pub async fn conversation_state(
        &self,
        limits: crate::conversation::Limits,
    ) -> Result<crate::conversation::ConversationState> {
        self.storage.conversation_state(limits).await
    }

    /// Opens this session's owner-bound conversation aggregate for typed fork
    /// publication. The aggregate verifier comes from the session descriptor.
    pub async fn conversation_aggregate(
        &self,
        limits: crate::conversation::Limits,
    ) -> Result<StreamAggregate<LocalStream>> {
        self.storage.conversation_aggregate(limits).await
    }

    /// Reads a bounded authoritative event page after a revision cursor.
    pub async fn conversation_events(
        &self,
        after_revision: u64,
        limit: usize,
        limits: crate::conversation::Limits,
    ) -> Result<Vec<crate::core::Event>> {
        self.storage
            .conversation_events(after_revision, limit, limits)
            .await
    }

    /// Reads one private file at an owner authenticated generation.
    pub async fn read_private_path(
        &self,
        path: &str,
        generation: Option<&crate::resources::GenerationRef>,
    ) -> Result<(FileRef, Vec<u8>)> {
        self.storage.read_private_path(path, generation).await
    }

    /// Lists one private directory page at an owner authenticated generation.
    pub async fn list_private_directory(
        &self,
        path: &str,
        generation: Option<&crate::resources::GenerationRef>,
        after: Option<&str>,
        maximum_entries: u32,
    ) -> Result<crate::conversation::PrivateDirectoryPage> {
        self.storage
            .list_private_directory_page(path, generation, after, maximum_entries)
            .await
    }

    /// Resolves one owner-authenticated interaction in this task's journal.
    pub async fn resolve_interaction(
        &self,
        id: crate::InteractionId,
        response: crate::interaction::InteractionResponse,
        responder: &Scope,
    ) -> Result<crate::interaction::InteractionOutcome> {
        self.storage
            .resolve_interaction_with_scope(id, response, responder)
            .await
    }
    /// Runtime shared with other local host compositions.
    #[must_use]
    pub fn bundle(&self) -> &crate::Harness {
        &self.bundle
    }

    /// Returns a host-owned receipt store bound to this reopened session.
    /// The store uses the session's private owner grant and system stream;
    /// callers cannot redirect it to a model-visible workspace path.
    pub fn execution_receipt_store(
        &self,
    ) -> Result<Arc<FilesystemExecutionReceiptStore<LocalAuthorityBackend, LocalObjectBackend>>>
    {
        let (host, stream, read, write, maximum_bytes) = self.storage.execution_binding();
        Ok(Arc::new(FilesystemExecutionReceiptStore::new(
            stream,
            host,
            self.storage.volume().clone(),
            self.storage.session_id(),
            self.storage.owner_scope(),
            read,
            write,
            maximum_bytes,
        )?))
    }

    /// Returns the host-only signer used after an explicit operator approval
    /// to authorize one exact pending execution resolution.
    pub fn execution_operator_authorizer(
        &self,
    ) -> crate::host_execution::ExecutionOperatorAuthorizer {
        self.storage.execution_operator_authorizer()
    }

    /// Returns the host application's separately authenticated authority for
    /// resolving uncertain process attempts after review. This constructor is
    /// crate-private; applications must obtain an operator capability from a
    /// host authority and use [`Self::resolve_unknown_execution_with_capability`]
    /// so model-visible code cannot mint an operator capability.
    #[cfg(test)]
    pub(crate) fn execution_resolution_capability(&self) -> Result<ExecutionResolutionCapability> {
        ExecutionResolutionCapability::for_test_owner(
            self.storage.session_id(),
            self.storage.volume(),
            self.storage.owner_scope(),
        )
    }

    /// Resolves one protected pending execution after an authenticated host
    /// operator has reviewed its outcome. The provider remains unable to
    /// retry the command until this explicit transition is durable.
    #[cfg(test)]
    pub(crate) async fn resolve_unknown_execution(
        &self,
        key: &ExecutionReceiptKey,
        reason: impl Into<String>,
    ) -> Result<FileRef> {
        let store = self.execution_receipt_store()?;
        let resolution = self.execution_resolution_capability()?;
        let (_, _, _, resolver, _) = self.storage.execution_binding();
        let pending = store.pending_claims(&resolution, &resolver).await?;
        let Some((candidate, handle)) = pending.into_iter().find(|(candidate, _)| candidate == key)
        else {
            return Err(Error::NotFound(
                "execution attempt is not pending operator resolution".into(),
            ));
        };
        store
            .resolve_unknown_owner(&candidate, &resolution, &resolver, &handle, reason)
            .await
    }

    /// Resolves one uncertain execution with a separately authenticated
    /// operator capability bound to the exact operation identity. The
    /// capability must be issued from a host-verified scope carrying
    /// [`ExecutionResolutionCapability::capability_for`] for the exact
    /// session, canonical volume identity, and operation tuple.
    pub async fn resolve_unknown_execution_with_capability(
        &self,
        key: &ExecutionReceiptKey,
        resolution: &ExecutionResolutionCapability,
        reason: impl Into<String>,
    ) -> Result<FileRef> {
        let store = self.execution_receipt_store()?;
        let (_, _, _, resolver, _) = self.storage.execution_binding();
        let handle = store
            .pending_claim_for_operator(key, resolution, &resolver)
            .await?;
        store
            .resolve_unknown(key, resolution, &resolver, &handle, reason)
            .await
    }

    /// Composes the production native provider around this session's
    /// host-owned receipt journal and authenticated interaction verifier.
    /// The provider uses the native runner and never falls back to a
    /// model-writable receipt path.
    pub fn native_execution_provider(&self) -> Result<NativeExecutionProvider> {
        NativeExecutionProvider::native_with_receipt_store(
            self.storage.content_verifier(),
            self.execution_receipt_store()?,
            self.storage.execution_approval_verifier(),
        )
    }

    /// Authenticates an externally issued operator grant against this
    /// reopened session's configured issuer and conversation audience.  The
    /// resulting capability is bound to this exact session, private volume,
    /// and operation; callers cannot redirect it to another local session.
    pub fn authenticate_execution_resolution(
        &self,
        operator: &Scope,
        operation_id: OperationId,
    ) -> Result<ExecutionResolutionCapability> {
        ExecutionResolutionCapability::authenticate(
            &self.storage.verifier(),
            operator,
            self.storage.session_id(),
            self.storage.volume(),
            operation_id,
        )
    }

    /// Creates the effect registry with the authenticated host process provider.
    pub fn effect_registry_with_native_execution(&self) -> Result<EffectRegistry> {
        let mut registry =
            EffectRegistry::default().with_result_resolver(self.storage.content_verifier());
        registry.register(Arc::new(self.native_execution_provider()?))?;
        Ok(registry)
    }
}

fn local_fork_verifier(
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    maximum_bytes: u64,
) -> Result<Arc<CompositeForkVerifier>> {
    let filesystem = Arc::new(FilesystemForkVerifier::new(host, maximum_bytes)?);
    let stream = Arc::new(StreamHistoryForkVerifier::new(ProviderRef::new(
        "local", "stream", "2",
    )?)?);
    Ok(Arc::new(CompositeForkVerifier::new(vec![
        filesystem as Arc<dyn ForkSeedVerifier>,
        stream as Arc<dyn ForkSeedVerifier>,
    ])?))
}

fn local_fork_verifier_with_stream_provider(
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    stream_provider: ProviderRef,
    maximum_bytes: u64,
) -> Result<Arc<CompositeForkVerifier>> {
    let filesystem = Arc::new(FilesystemForkVerifier::new(host, maximum_bytes)?);
    let stream = Arc::new(StreamHistoryForkVerifier::new(stream_provider)?);
    Ok(Arc::new(CompositeForkVerifier::new(vec![
        filesystem as Arc<dyn ForkSeedVerifier>,
        stream as Arc<dyn ForkSeedVerifier>,
    ])?))
}

/// Returns the descriptor stream path for a composed task. Composed swarms
/// share one Stream provider, so the task identity must be part of the path;
/// hashing the supplied root keeps arbitrary caller paths out of the stream
/// namespace while remaining stable across reopen.
fn shared_session_descriptor_path(root: &Path) -> String {
    let root = normalized_descriptor_path(root);
    let identity = path_identity_bytes(&root);
    let digest = blake3::hash(&identity).to_hex();
    format!("harness/session/{digest}")
}

fn normalized_descriptor_path(path: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn path_identity_bytes(path: &Path) -> Vec<u8> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        path.as_os_str()
            .encode_wide()
            .flat_map(|unit| unit.to_le_bytes())
            .collect()
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::ffi::OsStrExt as _;
        path.as_os_str().as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelAttempt, ModelEvent, ModelOptionPolicy};
    use crate::registry::ComponentIdentity;
    use crate::{EffectAttemptId, EffectId, core::EffectGuarantee};
    use futures::{future::BoxFuture, stream::BoxStream};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct Mock(AtomicUsize);
    impl ModelProvider for Mock {
        fn generate<'a>(
            &'a self,
            _: crate::model_input::PreparedModelInput,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "persisted".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]))
        }
        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[test]
    fn session_descriptor_rejects_inconsistent_owner_bindings() -> Result<()> {
        let model = Model::new("mock", "descriptor", "1", json!({}))?;
        let limits = Limits::default();
        let provider = ProviderRef::new("descriptor", "filesystem", "1")?;
        let project = VolumeRef::new(
            provider.clone(),
            "project",
            VolumeClass::Project,
            VolumeOwner::Project("owner".into()),
        )?;
        let mut descriptor = SessionDescriptor::fresh_with_provider(
            model.clone(),
            limits,
            Some(project.clone()),
            provider.clone(),
        )?;
        validate_descriptor(&descriptor, &model, limits, Some(&project))?;

        descriptor.signing_key = [0; 32];
        assert!(matches!(
            validate_descriptor(&descriptor, &model, limits, Some(&project)),
            Err(Error::Conflict(message)) if message.contains("invalid owner identity")
        ));
        descriptor.signing_key = [7; 32];

        descriptor.private_volume = VolumeRef::new(
            provider.clone(),
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::new()),
        )?;
        assert!(matches!(
            validate_descriptor(&descriptor, &model, limits, Some(&project)),
            Err(Error::Conflict(message)) if message.contains("private volume")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_local_openers_reopen_the_descriptor_winner() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "descriptor-race", "1", json!({}))?;
        let (first, second) = tokio::join!(
            PersistentLocalHarness::open_with_tools(
                root.path(),
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            ),
            PersistentLocalHarness::open_with_tools(
                root.path(),
                model,
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            ),
        );
        let first = first?;
        let second = second?;
        assert_eq!(first.storage.session_id(), second.storage.session_id());
        assert_eq!(first.signing_key(), second.signing_key());
        Ok(())
    }

    struct RecordingMock {
        calls: AtomicUsize,
        prepared: Mutex<Vec<(Vec<u8>, crate::model_input::ModelInputManifest)>>,
        policy: ModelOptionPolicy,
    }

    impl ModelProvider for RecordingMock {
        fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
            Some(&self.policy)
        }

        fn generate<'a>(
            &'a self,
            prepared: crate::model_input::PreparedModelInput,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut captured) = self.prepared.lock() {
                captured.push((prepared.bytes().to_vec(), prepared.manifest().clone()));
            }
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "persisted".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[test]
    fn local_execution_contract_is_exact_and_closed() -> Result<()> {
        let definition = LocalExecutionTool::definition();
        definition.validate()?;
        assert_eq!(definition.name, LOCAL_EXECUTION_TOOL);
        assert_eq!(definition.revision, "1");
        for field in [
            "executable",
            "arguments",
            "working_directory",
            "environment",
            "timeout_ms",
            "max_output_bytes",
        ] {
            assert!(definition.input_schema["required"]
                .as_array()
                .is_some_and(|required| required.iter().any(|value| value == field)));
        }
        assert_eq!(definition.input_schema["additionalProperties"], false);
        Ok(())
    }

    #[test]
    fn local_execution_input_rejects_ambiguous_host_resolution() {
        let input = serde_json::from_value::<LocalExecutionInput>(json!({
            "executable": "echo",
            "arguments": [],
            "working_directory": ".",
            "environment": {"kind": "clear"},
            "timeout_ms": null,
            "max_output_bytes": 1024,
        }))
        .expect("input shape is valid before exact spec admission");
        assert!(input.spec().is_err());
    }

    #[tokio::test]
    async fn denied_model_options_do_not_create_session_storage() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let unregistered = Arc::new(Mock(AtomicUsize::new(0)));
        let registered = Arc::new(RecordingMock {
            calls: AtomicUsize::new(0),
            prepared: Mutex::new(Vec::new()),
            policy: ModelOptionPolicy::new(
                ComponentIdentity {
                    name: "test.local-construction-options".into(),
                    version: "1".into(),
                    digest: [62; 32],
                },
                serde_json::json!({"type": "object", "additionalProperties": false}),
            )?,
        });
        let providers: [Arc<dyn ModelProvider>; 2] = [unregistered.clone(), registered.clone()];
        for (index, provider) in providers.into_iter().enumerate() {
            let session_root = root.path().join(format!("denied-{index}"));
            let model = Model::new(
                "mock",
                "durable",
                "1",
                serde_json::json!({"api_key": "private-provider-state"}),
            )?;
            assert!(matches!(
                PersistentLocalHarness::open(&session_root, model, provider, Limits::default(),)
                    .await,
                Err(Error::Invalid(_))
            ));
            assert!(
                !session_root.exists(),
                "denied model created durable storage"
            );
        }
        assert_eq!(unregistered.0.load(Ordering::SeqCst), 0);
        assert_eq!(registered.calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn authoritative_history_overflow_refuses_dispatch_after_restart() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(Mock(AtomicUsize::new(0)));
        let model = Model::new("mock", "context-overflow", "1", serde_json::json!({}))?;
        let limits = Limits {
            context_messages: 2,
            ..Limits::default()
        };
        let operation = OperationId::new();
        {
            let session =
                PersistentLocalHarness::open(root.path(), model.clone(), provider.clone(), limits)
                    .await?;
            assert_eq!(
                session.run(OperationId::new(), "first").await?.text,
                "persisted"
            );
            assert!(matches!(
                session.run(operation, "second").await,
                Err(Error::Invalid(message)) if message.contains("no history was omitted")
            ));
            assert_eq!(provider.0.load(Ordering::SeqCst), 1);
        }
        let reopened =
            PersistentLocalHarness::open(root.path(), model, provider.clone(), limits).await?;
        assert!(matches!(
            reopened.run(operation, "second").await,
            Err(Error::Invalid(message)) if message.contains("no history was omitted")
        ));
        assert_eq!(provider.0.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reopen_recovers_completed_turn_without_dispatch() -> Result<()> {
        let root = std::env::temp_dir().join(format!("harness-reopen-{}", OperationId::new()));
        let policy = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "test.local-restart-options".into(),
                version: "1".into(),
                digest: [61; 32],
            },
            serde_json::json!({
                "type": "object",
                "additionalProperties": false,
            }),
        )?;
        let provider = Arc::new(RecordingMock {
            calls: AtomicUsize::new(0),
            prepared: Mutex::new(Vec::new()),
            policy: policy.clone(),
        });
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let operation = OperationId::new();
        let file;
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await?;
            file = session
                .storage()
                .stage(
                    OperationId::new(),
                    "example.txt",
                    b"original",
                    "text/plain",
                    "example.txt",
                )
                .await?;
            assert_eq!(
                session.run(operation, "exact prompt\r\n").await?.text,
                "persisted"
            );
        }
        let (expected_bytes, expected_manifest) = provider
            .prepared
            .lock()
            .map_err(|_| Error::Storage("recording model lock poisoned".into()))?
            .first()
            .cloned()
            .ok_or_else(|| Error::Storage("recording model did not receive input".into()))?;
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await?;
            assert_eq!(session.storage().read(&file).await?, b"original");
            assert_eq!(
                session.run(operation, "exact prompt\r\n").await?.text,
                "persisted"
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            assert!(session.run(operation, "different").await.is_err());

            let prepared = session
                .storage()
                .journal()
                .replay(operation)
                .await?
                .into_iter()
                .find_map(|record| match record.event {
                    crate::executor::ExecutionEvent::ModelInputPrepared {
                        manifest,
                        request,
                        ..
                    } => Some((manifest, request)),
                    _ => None,
                })
                .ok_or_else(|| Error::Storage("persisted model input is missing".into()))?;
            // Request evidence belongs to the host journal, not the model's
            // workspace content grants.
            let journal = session.storage().journal();
            let request_bytes = journal.load(&prepared.1).await?;
            let manifest_bytes = journal.load(&prepared.0).await?;
            let manifest: crate::model_input::ModelInputManifest =
                serde_json::from_slice(&manifest_bytes)
                    .map_err(|error| Error::Storage(error.to_string()))?;
            assert_eq!(request_bytes, expected_bytes);
            assert_eq!(manifest, expected_manifest);
            assert_eq!(manifest.model_option_policy, Some(policy.identity.clone()));
            assert_eq!(
                manifest.model_option_schema_digest,
                Some(policy.schema_digest()?)
            );
        }
        let changed = Model::new("mock", "changed", "1", serde_json::json!({}))?;
        assert!(
            PersistentLocalHarness::open(&root, changed, provider, Limits::default())
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn reopened_receipt_providers_share_atomic_claim_and_terminal_record() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("harness-receipt-reopen-{}", OperationId::new()));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([61; 16]),
            effect_id: EffectId::from_bytes([62; 16]),
            attempt_id: EffectAttemptId::from_bytes([63; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [64; 32],
        };
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            assert!(matches!(
                store.claim(&key).await?,
                ExecutionClaim::Acquired { .. }
            ));
        }
        let first = {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            assert_eq!(store.claim(&key).await?, ExecutionClaim::Pending);
            let (_, _, _, resolver, _) = session.storage().execution_binding();
            let resolution = session.execution_resolution_capability()?;
            let pending = store.pending_claims(&resolution, &resolver).await?;
            assert_eq!(pending.len(), 1);
            let (_, operator) = pending.into_iter().next().unwrap();
            store
                .resolve_unknown_owner(
                    &key,
                    &resolution,
                    &resolver,
                    &operator,
                    "operator resolved after restart",
                )
                .await?
        };
        {
            let session = PersistentLocalHarness::open(
                &root,
                model,
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            let claim = store.claim(&key).await?;
            let ExecutionClaim::Completed(record) = claim else {
                return Err(Error::Storage("reopened receipt was not terminal".into()));
            };
            assert_eq!(record.result, first);
            assert!(matches!(record.receipt, ExecutionReceipt::Unknown { .. }));
            assert_eq!(store.load_attempt(key.attempt_id).await?, Some(record));
            assert!(
                session
                    .storage()
                    .content_verifier()
                    .read(&first)
                    .await
                    .is_err()
            );
        }
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn receipt_store_fences_new_attempt_after_pending_or_unknown() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "harness-receipt-attempt-fence-{}",
            OperationId::new()
        ));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([81; 16]),
            effect_id: EffectId::from_bytes([82; 16]),
            attempt_id: EffectAttemptId::from_bytes([83; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [84; 32],
        };
        let fresh = ExecutionReceiptKey {
            attempt_id: EffectAttemptId::from_bytes([85; 16]),
            ..key.clone()
        };
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            assert!(matches!(
                store.claim(&key).await?,
                ExecutionClaim::Acquired { .. }
            ));
            assert_eq!(store.claim(&fresh).await?, ExecutionClaim::Pending);

            let (_, _, _, resolver, _) = session.storage().execution_binding();
            let resolution = session.execution_resolution_capability()?;
            let pending = store.pending_claims(&resolution, &resolver).await?;
            let (_, operator) = pending
                .into_iter()
                .find(|(candidate, _)| candidate == &key)
                .ok_or_else(|| Error::Storage("pending attempt fence claim missing".into()))?;
            store
                .resolve_unknown_owner(
                    &key,
                    &resolution,
                    &resolver,
                    &operator,
                    "attempt outcome remained unknown",
                )
                .await?;
            let ExecutionClaim::Completed(record) = store.claim(&fresh).await? else {
                return Err(Error::Storage(
                    "unknown operation fence did not retain the terminal record".into(),
                ));
            };
            assert!(matches!(record.receipt, ExecutionReceipt::Unknown { .. }));
            assert_eq!(record.key.attempt_id, key.attempt_id);
        }
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn receipt_store_rejects_success_after_cancellation_request() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "harness-receipt-cancel-fence-{}",
            OperationId::new()
        ));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([86; 16]),
            effect_id: EffectId::from_bytes([87; 16]),
            attempt_id: EffectAttemptId::from_bytes([88; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [89; 32],
        };
        let session = PersistentLocalHarness::open(
            &root,
            model,
            Arc::new(Mock(AtomicUsize::new(0))),
            Limits::default(),
        )
        .await?;
        let store = session.execution_receipt_store()?;
        let ExecutionClaim::Acquired { handle } = store.claim(&key).await? else {
            return Err(Error::Storage("cancellation claim was not acquired".into()));
        };
        store.request_cancel(&key).await?;
        let result = store
            .publish(
                &key,
                &handle,
                &ExecutionReceipt::Succeeded {
                    status_code: 0,
                    stdout: b"late success".to_vec(),
                    stderr: Vec::new(),
                },
            )
            .await;
        assert!(
            matches!(result, Err(Error::Conflict(message)) if message.contains("cancellation"))
        );
        assert_eq!(store.claim(&key).await?, ExecutionClaim::Pending);
        drop(session);
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn operator_can_close_cancelled_unknown_without_permitting_retry() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "harness-receipt-cancel-unknown-resolution-{}",
            OperationId::new()
        ));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([146; 16]),
            effect_id: EffectId::from_bytes([147; 16]),
            attempt_id: EffectAttemptId::from_bytes([148; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [149; 32],
        };
        let session = PersistentLocalHarness::open(
            &root,
            model,
            Arc::new(Mock(AtomicUsize::new(0))),
            Limits::default(),
        )
        .await?;
        let store = session.execution_receipt_store()?;
        let ExecutionClaim::Acquired { .. } = store.claim(&key).await? else {
            return Err(Error::Storage(
                "cancelled unknown claim was not acquired".into(),
            ));
        };
        store.request_cancel(&key).await?;

        let operator = session.execution_operator_authorizer().authenticate(
            "reviewer",
            session.storage().session_id(),
            session.storage().volume(),
            key.operation_id,
        )?;
        let (_, _, _, resolver, _) = session.storage().execution_binding();
        let handle = store
            .pending_claim_for_operator(&key, &operator, &resolver)
            .await?;
        store
            .resolve_unknown(
                &key,
                &operator,
                &resolver,
                &handle,
                "process remained unresolved after cancellation",
            )
            .await?;

        let ExecutionClaim::Completed(record) = store.claim(&key).await? else {
            return Err(Error::Storage(
                "operator resolution did not produce a terminal receipt".into(),
            ));
        };
        assert!(matches!(record.receipt, ExecutionReceipt::Unknown { .. }));
        assert_eq!(record.operator_principal.as_deref(), Some("reviewer"));
        assert!(record.operator_authenticated);
        assert_eq!(store.claim(&key).await?, ExecutionClaim::Completed(record));
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn receipt_store_rejects_resolution_capability_from_another_session() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("harness-receipt-capability-{}", OperationId::new()));
        let other_root = std::env::temp_dir().join(format!(
            "harness-receipt-capability-other-{}",
            OperationId::new()
        ));
        let model = Model::new("mock", "capability", "1", serde_json::json!({}))?;
        let first = PersistentLocalHarness::open(
            &root,
            model.clone(),
            Arc::new(Mock(AtomicUsize::new(0))),
            Limits::default(),
        )
        .await?;
        let second = PersistentLocalHarness::open(
            &other_root,
            model,
            Arc::new(Mock(AtomicUsize::new(0))),
            Limits::default(),
        )
        .await?;
        let forged = second.execution_resolution_capability()?;
        assert_ne!(forged, first.execution_resolution_capability()?);
        drop(second);
        drop(first);
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::remove_dir_all(other_root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn reopened_local_receipt_provider_replays_success_ack() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("harness-receipt-success-{}", OperationId::new()));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let key = ExecutionReceiptKey {
            operation_id: OperationId::from_bytes([71; 16]),
            effect_id: EffectId::from_bytes([72; 16]),
            attempt_id: EffectAttemptId::from_bytes([73; 16]),
            provider: "harness.native-execution.v1".into(),
            effect_kind: "host.process".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: [74; 32],
        };
        let result = {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(Mock(AtomicUsize::new(0))),
                Limits::default(),
            )
            .await?;
            let store = session.execution_receipt_store()?;
            let ExecutionClaim::Acquired { handle } = store.claim(&key).await? else {
                return Err(Error::Conflict(
                    "local receipt claim was not acquired".into(),
                ));
            };
            let receipt = ExecutionReceipt::Succeeded {
                status_code: 0,
                stdout: b"ack".to_vec(),
                stderr: Vec::new(),
            };
            let result = store.publish(&key, &handle, &receipt).await?;
            // A caller may lose the publication acknowledgement after the
            // terminal event is durable. Retrying the exact protected
            // receipt is an idempotent replay and returns the retained ref.
            assert_eq!(store.publish(&key, &handle, &receipt).await?, result);
            result
        };
        let session = PersistentLocalHarness::open(
            &root,
            model,
            Arc::new(Mock(AtomicUsize::new(0))),
            Limits::default(),
        )
        .await?;
        let store = session.execution_receipt_store()?;
        let ExecutionClaim::Completed(record) = store.claim(&key).await? else {
            return Err(Error::Storage(
                "reopened success receipt was not terminal".into(),
            ));
        };
        assert_eq!(record.result, result);
        assert!(matches!(record.receipt, ExecutionReceipt::Succeeded { .. }));
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }
}
