//! Direct Stream persistence for durable aggregate histories.

mod checkpoints;
mod effect_heads;
mod effects;
pub(crate) mod history;
pub use checkpoints::{DEFAULT_PROJECTION_EVENTS, default_projection_read_limits};
pub(crate) mod operations;
#[cfg(feature = "filesystem")]
mod guarded;
pub use history::*;
pub use effects::effect_events;

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, Limits, ReferencedAttachments,
        verified_attachment_manifest, verified_content_bytes,
    },
    core::{
        Action, ApplyResult, Authority, AuthorityVerifier, Command, EffectStatus, EventPayload,
        EventReference, ExtensionDependency, Reducer, SchemaRegistry, Scope, Snapshot,
    },
    effects::{validate_result_bytes, validate_schema_bytes},
    fork::{CompositeForkVerifier, ForkPreparer, ForkReport, ForkRequest, ForkSeed},
    interaction::InteractionOutcome,
    merge::ProjectMergeVerifier,
    wire_codec::decode_event,
};
use acyclic_stream::{
    CommitOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamError, StreamProvider,
};
use acyclic_stream::OwnedBytes;
use futures::TryStreamExt as _;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Finite recent terminal cache.
pub const DEFAULT_TERMINAL_EFFECTS: usize = 64;

pub(crate) fn guarded_boundary(
    work: &Option<&mut dyn history::HistoryIo>, kind: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
        return Error::NativeBoundary { code: kind, reason };
    }
    let _ = work;
    match kind {
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

fn fork_child_binding_with_work(
    seed: &ForkSeed,
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<(OperationId, EventReference)> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
    }
    let revision = seed
        .parent_revision
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("fork parent revision overflow".into()))?;
    if let Some(observer) = work.as_deref_mut() {
        observer.hashed_bytes(b"harness/v2/fork-child-bind\0".len() as u64 + 16)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/fork-child-bind\0");
    hash.update(&seed.operation_id.into_bytes());
    if let Some(observer) = work.as_deref_mut() { observer.copy_bytes(16)?; }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok((
        OperationId::from_bytes(identity),
        EventReference {
            authority: match work.as_deref_mut() {
                Some(observer) => observer.clone_authority(&seed.parent)?,
                None => seed.parent.clone(),
            },
            revision,
        },
    ))
}

fn fork_command_key_with_work(
    prefix: &str,
    operation: OperationId,
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<(IdempotencyKey, u64)> {
    use std::fmt::Write as _;
    let capacity = prefix.len().checked_add(36)
        .ok_or_else(|| Error::Invalid("fork command identity length overflows".into()))?;
    let mut text = String::new();
    if let Some(observer) = work.as_deref_mut() {
        observer.reserve_text(&mut text, capacity)?;
        if let Err(error) = observer.copy_bytes(capacity as u64) {
            let held = text.capacity() as u64;
            drop(text);
            observer.release_owned(held)?;
            return Err(error);
        }
    } else {
        text.try_reserve_exact(capacity)
            .map_err(|_| Error::Invalid("fork command identity allocation failed".into()))?;
    }
    let held = if work.is_some() { text.capacity() as u64 } else { 0 };
    let result = write!(&mut text, "{prefix}{operation}")
        .map_err(|_| Error::Invalid("fork command identity formatting failed".into()))
        .and_then(|()| IdempotencyKey::new(text));
    if result.is_err() && let Some(observer) = work.as_deref_mut() {
        observer.release_owned(held)?;
    }
    result.map(|key| (key, held))
}

/// Exact-version native migration registry for local and customer-hosted runs.
#[derive(Default)]
pub struct NativeExtensionMigrations {
    implementations: BTreeMap<(String, u32, u32, [u8; 32]), Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>>,
}

impl NativeExtensionMigrations {
    /// Pins one installed target implementation to a pure state transformation.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        from_version: u32,
        to_version: u32,
        target_implementation_digest: [u8; 32],
        migration: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>,
    ) -> Result<()> {
        let name = name.into();
        ExtensionDependency {
            name: name.clone(),
            version: from_version,
        }
        .validate()?;
        ExtensionDependency {
            name: name.clone(),
            version: to_version,
        }
        .validate()?;
        if from_version == to_version || target_implementation_digest == [0; 32] {
            return Err(Error::Invalid(
                "extension migration identity is invalid".into(),
            ));
        }
        let key = (name, from_version, to_version, target_implementation_digest);
        if self.implementations.contains_key(&key) {
            return Err(Error::Conflict(
                "extension migration is already registered".into(),
            ));
        }
        self.implementations.insert(key, migration);
        Ok(())
    }

    fn migrate(
        &self,
        name: &str,
        from_version: u32,
        to_version: u32,
        target_implementation_digest: [u8; 32],
        previous: &Value,
    ) -> Result<Value> {
        self.implementations
            .get(&(
                name.to_owned(),
                from_version,
                to_version,
                target_implementation_digest,
            ))
            .ok_or_else(|| {
                Error::Unsupported("exact extension migration is not installed".into())
            })?(previous)
    }
}

/// Caller-retained identity of one exact state migration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionMigrationRequest {
    /// Stable migration operation.
    pub operation_id: OperationId,
    /// Aggregate revision observed when migration was planned.
    pub expected_revision: u64,
    /// Exact prior state event, independent of later extension changes.
    pub previous: EventReference,
    /// Namespaced extension state identity.
    pub name: String,
    /// Installed target implementation version.
    pub to_version: u32,
}

fn extension_migration_command(
    request: ExtensionMigrationRequest,
    scope: Scope,
    content: crate::conversation::FileRef,
) -> Result<Command> {
    Ok(Command {
        operation_id: request.operation_id,
        idempotency_key: IdempotencyKey::new(format!(
            "extension-migration:{}",
            request.operation_id
        ))?,
        expected_revision: request.expected_revision,
        scope,
        causal_parent: None,
        action: Action::MigrateExtensionState {
            name: request.name,
            previous: request.previous,
            to_version: request.to_version,
            content,
        },
    })
}
/// Original canonical ROOT binding custody, constructed only by this store.
#[cfg(feature = "filesystem")]
pub(crate) struct VerifiedRootBinding {
    original_task: crate::TaskId,
    admission_digest: [u8; 32],
    fence: crate::scheduler::LeaseFence,
    agent: crate::AgentId,
    audience: Authority,
    bind_operation: OperationId,
    intent_digest: [u8; 32],
}

#[cfg(feature = "filesystem")]
impl VerifiedRootBinding {
    pub(crate) fn original_task(&self) -> crate::TaskId { self.original_task }
    pub(crate) fn admission_digest(&self) -> [u8; 32] { self.admission_digest }
    pub(crate) fn fence(&self) -> &crate::scheduler::LeaseFence { &self.fence }
    pub(crate) fn agent(&self) -> crate::AgentId { self.agent }
    pub(crate) fn audience(&self) -> &Authority { &self.audience }
    pub(crate) fn bind_operation(&self) -> OperationId { self.bind_operation }
    pub(crate) fn intent_digest(&self) -> [u8; 32] { self.intent_digest }
}

/// One authoritative reducer whose canonical history is stored directly in Stream.
pub struct StreamAggregate<P> {
    client: std::sync::Arc<P>,
    stream: Stream<P>,
    reducer: Reducer,
    content_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    fork_verifier: Option<Arc<CompositeForkVerifier>>,
    merge_verifier: Option<Arc<dyn ProjectMergeVerifier>>,
    extension_migrations: Option<Arc<NativeExtensionMigrations>>,
    limits: Limits,
    effect_history_limits: HistoryReadLimits,
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Executes a pinned pure migration, stages its bytes through the owning provider, then publishes only the resulting ref.
    pub async fn migrate_extension_state(
        &mut self,
        request: ExtensionMigrationRequest,
        scope: Scope,
        publisher: &dyn ContentPublisher,
    ) -> Result<ApplyResult> {
        if request.previous.authority != *self.reducer.authority()
            || request.previous.revision == 0
            || request.previous.revision > request.expected_revision
            || request.to_version == 0
        {
            return Err(Error::Invalid(
                "extension migration request is invalid".into(),
            ));
        }
        if let Some(revision) = self.operation_revision(request.operation_id).await? {
            return self
                .replay_extension_migration(request, scope, publisher, revision)
                .await;
        }
        self.reducer.preflight_extension_migration(
            &scope,
            request.expected_revision,
            &request.previous,
            &request.name,
            request.to_version,
            publisher.volume(),
        )?;
        let output = self.extension_migration_output(&request).await?;
        let path = format!("extensions/migration-{}.json", request.operation_id);
        let content = publisher
            .stage(
                request.operation_id,
                &path,
                &output,
                "application/json",
                "state.json",
                None,
            )?
            .await?.0;
        if content.volume() != publisher.volume() {
            return Err(Error::Unauthorized(
                "extension publisher returned another volume".into(),
            ));
        }
        self.execute(extension_migration_command(request, scope, content)?, #[cfg(feature = "filesystem")] None, &mut None)
            .await
    }

    async fn replay_extension_migration(
        &mut self,
        request: ExtensionMigrationRequest,
        scope: Scope,
        publisher: &dyn ContentPublisher,
        revision: u64,
    ) -> Result<ApplyResult> {
        let event = self.read_event_at(revision, &mut None).await?.0;
        let EventPayload::ExtensionStateMigrated { migration } = &event.payload else {
            return Err(Error::Conflict(
                "operation belongs to another action".into(),
            ));
        };
        if migration.previous != request.previous
            || migration.record.name != request.name
            || migration.record.version != request.to_version
        {
            return Err(Error::Conflict(
                "operation belongs to another migration".into(),
            ));
        }
        if publisher.volume() != migration.record.content.volume() {
            return Err(Error::Unauthorized(
                "extension migration publisher owns another volume".into(),
            ));
        }
        self.execute(extension_migration_command(
            request,
            scope,
            migration.record.content.clone(),
        )?, #[cfg(feature = "filesystem")] None, &mut None)
        .await
    }

    async fn extension_migration_output(
        &self,
        request: &ExtensionMigrationRequest,
    ) -> Result<Vec<u8>> {
        let prior_event = self.read_event_at(request.previous.revision, &mut None).await?.0;
        if prior_event.revision != request.previous.revision {
            return Err(Error::Conflict(
                "extension migration source event changed".into(),
            ));
        }
        let prior = match &prior_event.payload {
            EventPayload::Custom { record } => record,
            EventPayload::ExtensionStateMigrated { migration } => &migration.record,
            _ => {
                return Err(Error::Invalid(
                    "migration source is not an extension state".into(),
                ));
            }
        };
        if prior.name != request.name {
            return Err(Error::Conflict(
                "migration source belongs to another extension".into(),
            ));
        }
        if self
            .reducer
            .extension_state(&request.name)
            .map(|(source, _)| source)
            != Some(&request.previous)
        {
            return Err(Error::Conflict("extension migration source changed".into()));
        }
        let verifier = self
            .content_verifier
            .as_ref()
            .ok_or_else(|| Error::Unsupported("extension content verifier is not bound".into()))?;
        let migrations = self.extension_migrations.as_ref().ok_or_else(|| {
            Error::Unsupported("extension migration executor is not bound".into())
        })?;
        self.limits.validate_file(&prior.content, &mut None)?;
        let bytes = verified_content_bytes(verifier.as_ref(), &prior.content, (&prior.content).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes)?;
        self.reducer
            .validate_custom_bytes(&request.name, prior.version, &prior.content, &bytes)?;
        let value: Value = crate::contract::json_from_slice(&bytes).map_err(|error| {
            Error::Invalid(format!("previous extension state is invalid: {error}"))
        })?;
        let target_digest = self
            .reducer
            .extension_implementation_digest(&request.name, request.to_version)?;
        let migrated = migrations.migrate(
            &request.name,
            prior.version,
            request.to_version,
            target_digest,
            &value,
        )?;
        let output = crate::contract::canonical_json_bytes(&migrated)?;
        if output.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid(
                "extension migration exceeds file limit".into(),
            ));
        }
        Ok(output)
    }

    /// Asks an explicit provider to prepare each selected resource at the parent's exact current revision.
    pub async fn prepare_fork(
        &self,
        provider: &dyn ForkPreparer,
        request: ForkRequest,
    ) -> Result<ForkReport> {
        request.validate(&mut None)?;
        if request.parent != *self.reducer.authority()
            || request.parent_revision != self.reducer.revision()
        {
            return Err(Error::Conflict(
                "fork preparation is not at the parent revision".into(),
            ));
        }
        let report = provider.prepare(request.clone()).await?;
        report.validate(&mut None)?;
        if report.request != request {
            return Err(Error::Conflict(
                "fork provider returned a different request".into(),
            ));
        }
        Ok(report)
    }

    /// Reconciles the original preparation identity after an unknown reply.
    pub async fn reconcile_fork(
        &self,
        provider: &dyn ForkPreparer,
        request: ForkRequest,
    ) -> Result<Option<ForkReport>> {
        request.validate(&mut None)?;
        if request.parent != *self.reducer.authority() {
            return Err(Error::Unauthorized(
                "fork request belongs to another parent".into(),
            ));
        }
        let report = provider.reconcile(request.clone()).await?;
        if let Some(report) = &report {
            report.validate(&mut None)?;
            if report.request != request {
                return Err(Error::Conflict(
                    "reconciled fork belongs to a different request".into(),
                ));
            }
        }
        Ok(report)
    }

    /// Publishes a fork report under the admitted owner's atomic fence.
    #[cfg(feature = "filesystem")]
    pub async fn publish_fork_report_measured(
        &mut self,
        report: ForkReport,
        scope: Scope,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<ApplyResult>, Error> {
        let mut work = history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.publish_fork_report(
                report, scope, Some(owner), &mut Some(&mut work)).await
        }.await;
        work.finish(result)
    }

    /// Publishes a captured fork through the parent append.
    pub async fn publish_fork_report(
        &mut self, report: ForkReport, scope: Scope,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        if report.request.parent != *self.reducer.authority() {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "fork report belongs to another parent"));
        }
        #[cfg(feature = "filesystem")]
        if let Some(owner) = owner {
            owner.authenticate_fork_parent(self.reducer.authority(), work)?;
            if scope.agent() != Some(owner.authenticated_fork_agent(work)?) {
                return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized,
                    "fork publication scope differs from its admitted caller"));
            }
        }
        let mut custody = crate::wire_codec::CodecQuota::observed(work)?;
        let seed = report.into_seed(custody.work())?;
        let vector_slots = seed.resources.capacity() as u64 * std::mem::size_of::<crate::fork::CapturedResource>() as u64
            + seed.omissions.capacity() as u64 * std::mem::size_of::<crate::fork::ForkOmission>() as u64;
        custody.adopt(vector_slots)?;
        let (key, key_slots) = fork_command_key_with_work(
            "fork-publication:", seed.operation_id, custody.work())?;
        custody.adopt(key_slots)?;
        let box_slots = std::mem::size_of::<ForkSeed>() as u64;
        if let Some(observer) = custody.work().as_deref_mut() {
            observer.admit_allocation(box_slots)?;
        }
        custody.retain(box_slots)?;
        let seed = Box::new(seed);
        let command = Command {
            operation_id: seed.operation_id,
            idempotency_key: key,
            expected_revision: seed.parent_revision,
            scope,
            causal_parent: None,
            action: Action::PublishFork { seed },
        };
        #[cfg(feature = "filesystem")]
        if owner.is_some() {
            return self.publish(command, owner, crate::distributed::JournalWrite::Fresh,
                false, custody.work()).await.map(|(result, _)| result);
        }
        self.execute(command, #[cfg(feature = "filesystem")] None, custody.work()).await

    }

    /// Admits a prepared child through the parent, then binds its fresh conversation to the published seed.
    pub async fn spawn_from_report(
        &mut self,
        parent: &mut Self,
        report: ForkReport,
        parent_scope: Scope,
        child_scope: Scope,
    ) -> Result<ForkSeed> {
        report.validate(&mut None)?;
        if report.request.child != *self.reducer.authority()
            || report.request.parent != *parent.reducer.authority()
        {
            return Err(Error::Unauthorized(
                "fork report does not match parent and child aggregates".into(),
            ));
        }
        let seed = report.clone().into_seed(&mut None)?;
        if child_scope.agent() != Some(seed.child_agent)
            || (self.reducer.revision() != 0 && parent.reducer.fork(&seed.child) != Some(&seed))
        {
            return Err(Error::Conflict(
                "child binding is not fresh or previously published by this parent".into(),
            ));
        }
        parent.publish_fork_report(report, parent_scope, #[cfg(feature = "filesystem")] None, &mut None).await?;
        self.bind_published_child(parent, &seed, child_scope, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        Ok(seed)
    }

    /// Binds only the exact child of an authenticated canonical parent fork.
    #[cfg(feature = "filesystem")]
    pub async fn bind_published_child_measured(
        &mut self,
        parent: &Self,
        seed: &ForkSeed,
        scope: Scope,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let mut work = history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.bind_published_child(
                parent, seed, scope, Some(owner), &mut Some(&mut work)).await
        }.await;
        work.finish(result)
    }

    /// Binds a child to its exact published parent seed.
    pub async fn bind_published_child(
        &mut self, parent: &Self, seed: &ForkSeed, scope: Scope,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let mut custody = crate::wire_codec::CodecQuota::observed(work)?;
        seed.validate(custody.work())?;
        #[cfg(feature = "filesystem")]
        let parent_agent = if let Some(owner) = owner {
            owner.authenticate_fork_parent(&seed.parent, custody.work())?;
            Some(owner.authenticated_fork_agent(custody.work())?)
        } else { None };
        #[cfg(not(feature = "filesystem"))]
        let parent_agent: Option<crate::AgentId> = None;
        if parent.reducer.authority() != &seed.parent
            || parent.reducer.fork(&seed.child) != Some(seed)
            || self.reducer.authority() != &seed.child {
            return Err(guarded_boundary(custody.work(), crate::wire::ErrorCode::Unauthorized,
                "child binding has no published parent fork"));
        }
        if scope.agent() != Some(seed.child_agent) {
            return Err(guarded_boundary(custody.work(), crate::wire::ErrorCode::Unauthorized,
                "child binding scope belongs to another agent"));
        }
        let fork_revision = seed.parent_revision.checked_add(1).ok_or_else(||
            guarded_boundary(custody.work(), crate::wire::ErrorCode::Invalid,
                "fork parent revision overflow"))?;
        let (fork_event, fork_event_owned) = parent.read_event_at(
            fork_revision, custody.work()).await?;
        custody.adopt(fork_event_owned)?;
        if fork_event.revision != fork_revision
            || fork_event.operation_id != seed.operation_id
            || parent_agent.is_some_and(|agent| fork_event.scope.agent() != Some(agent))
            || !matches!(&fork_event.payload, crate::core::EventPayload::ForkPublished { seed: published } if published.as_ref() == seed) {
            return Err(guarded_boundary(custody.work(), crate::wire::ErrorCode::Conflict,
                "parent fork event does not match the child seed"));
        }
        let (bind_operation, causal_parent) = fork_child_binding_with_work(seed, custody.work())?;
        custody.adopt(causal_parent.authority.id.capacity() as u64)?;
        let conversation = self.reducer.conversation().ok_or_else(||
            guarded_boundary(custody.work(), crate::wire::ErrorCode::Invalid,
                "fork child is not a conversation"))?;
        if conversation.agent == Some(seed.child_agent) {
            let (event, event_owned) = self.read_event_at(1, custody.work()).await?;
            custody.adopt(event_owned)?;
            if event.revision == 1 && event.operation_id == bind_operation
                && event.causal_parent.as_ref() == Some(&causal_parent)
                && matches!(event.payload, crate::core::EventPayload::ConversationBound { agent } if agent == seed.child_agent) {
                #[cfg(feature = "filesystem")]
                if let Some(owner) = owner { owner.verify(false, custody.work()).await?; }
                drop(event);
                custody.release(event_owned)?;
                drop(fork_event);
                custody.release(fork_event_owned)?;
                return Ok(());
            }
            return Err(guarded_boundary(custody.work(), crate::wire::ErrorCode::Conflict,
                "child conversation was bound outside its published fork"));
        }
        if self.reducer.revision() != 0 || conversation.agent.is_some() {
            return Err(guarded_boundary(custody.work(), crate::wire::ErrorCode::Conflict,
                "fork child already has a different history"));
        }
        let (key, key_slots) = fork_command_key_with_work(
            "fork-child-bind:", seed.operation_id, custody.work())?;
        custody.adopt(key_slots)?;
        let command = Command {
            operation_id: bind_operation,
            idempotency_key: key,
            expected_revision: 0,
            scope,
            causal_parent: Some(causal_parent),
            action: Action::BindConversation { agent: seed.child_agent },
        };
        #[cfg(feature = "filesystem")]
        let (result, returned_owned) = if owner.is_some() || custody.work().is_some() {
            self.publish(command, owner, crate::distributed::JournalWrite::Fresh,
                false, custody.work()).await?
        } else { (self.execute(command, None, custody.work()).await?, 0) };
        #[cfg(not(feature = "filesystem"))]
        let (result, returned_owned) = (self.execute(command, custody.work()).await?, 0);
        custody.adopt(returned_owned)?;
        drop(result);
        custody.release(returned_owned)?;
        drop(fork_event);
        custody.release(fork_event_owned)?;
        Ok(())
    }

    /// Restores the same authenticated checkpoint and suffix on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_with_native_work(
        client: &std::sync::Arc<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        original_work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut native_io = history::native_work_io(original_work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        history::HistoryIo::check_cancellation(&native_io)?;
        let aggregate = Self::open(client, authority, authority_verifier, schemas, None,
            default_projection_read_limits(), DEFAULT_TERMINAL_EFFECTS, None,
            &mut Some(&mut native_io)).await?;
        native_io.return_staged_output()?;
        Ok((aggregate, native_io.into_custody()))
    }

    /// Moves the reducer out without copying or replaying it.
    pub fn into_reducer(self) -> Reducer {
        self.reducer
    }

    /// Restores a bounded committed cut with a finite work receipt.
    #[cfg(feature = "filesystem")]
    #[allow(clippy::too_many_arguments, reason = "original exact-cut open arguments plus the cumulative work receipt")]
    pub async fn open_measured(
        client: &std::sync::Arc<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        original_revision: Option<u64>,
        limits: HistoryReadLimits,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Self>, Error> {
        let mut work = history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            Self::open(
                client,
                authority,
                authority_verifier,
                schemas, None, limits, DEFAULT_TERMINAL_EFFECTS, original_revision, &mut Some(&mut work)).await
        }.await;
        work.finish(result)
    }

    /// Restores an authenticated aggregate at a bounded committed cut.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.open",
            level = "info",
            skip_all,
            fields(rev = crate::obs::Empty, items = crate::obs::Empty, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    #[allow(clippy::too_many_arguments, reason = "canonical restore inputs and observer")]
    pub async fn open(
        client: &std::sync::Arc<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Option<Snapshot>,
        limits: HistoryReadLimits,
        maximum_terminal_effects: usize,
        original_revision: Option<u64>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Self> {
        authority_verifier.verify_audience(&authority, &mut None)?;
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 || maximum_terminal_effects == 0
        {
            return Err(Error::Invalid(
                "projection read bounds must be positive".into(),
            ));
        }
        let reader = HistoryReader::new(client, &authority, &authority_verifier, work)?;
        let reader_bytes = if work.is_some() { reader.logical_owned_bytes()? } else { 0 };
        let loaded = match snapshot {
            Some(snapshot) => checkpoints::supplied(client, &authority, snapshot, limits, original_revision, work).await?,
            None => checkpoints::load_with_work(client, &authority, &authority_verifier, limits, original_revision, work).await?,
        };
        let stream = match work.as_deref_mut() {
            None => {
                let path = authority.stream_path(&mut None)?;
                acyclic_stream::Stream::new(std::sync::Arc::clone(&client), path)?
            },
            Some(observer) => {
                let path = authority.stream_path(&mut Some(&mut *observer))?;
                let temporary_bytes = path.capacity() as u64;
                let path_bytes = path.len() as u64;
                let stream: Result<_> = (|| {
                    observer.admit_allocation(path_bytes)?;
                    observer.copy_bytes(path_bytes)?;
                    Ok(acyclic_stream::Stream::new(std::sync::Arc::clone(&client), &path)?)
                })();
                drop(path);
                let released = observer.release_owned(temporary_bytes);
                match stream {
                    Ok(stream) => {
                        released?;
                        observer.retain_owned(path_bytes)?;
                        stream
                    },
                    Err(error) => {
                        let _ = released;
                        return Err(error);
                    },
                }
            },
        };
        let mut reducer = if let Some(snapshot) = loaded.snapshot {
            if snapshot.authority != authority {
                return Err(Error::Invalid(
                    "snapshot authority does not match requested aggregate".into(),
                ));
            }
            Reducer::restore_with_work(snapshot, authority_verifier, schemas, work)?
        } else {
            Reducer::new(authority, authority_verifier, schemas)
        };
        reducer.set_resident_event_limit(1024)?;
        reducer.set_resident_terminal_effect_limit(maximum_terminal_effects)?;
        let start = reducer.revision();
        let remaining_events = limits.maximum_events - loaded.consumed_events;
        if loaded.through_revision < start
            || loaded.through_revision - start > u64::from(remaining_events)
        {
            return Err(Error::Invalid(
                "projection suffix exceeds event allowance".into(),
            ));
        }
        if start < loaded.through_revision {
            Self::restore_suffix(
                client,
                &reader,
                &mut reducer,
                loaded.through_revision,
                remaining_events,
                limits.maximum_bytes.saturating_sub(loaded.consumed_bytes),
                work,
            )
            .await?;
        }
        drop(reader);
        if let Some(work) = work.as_deref_mut() { work.release_owned(reader_bytes)?; }
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        crate::obs::obs_record!(
            "rev" = reducer.revision(),
            "items" = reducer.revision().saturating_sub(start)
        );
        Ok(Self {
            client: client.clone(),
            stream,
            reducer,
            content_verifier: None,
            fork_verifier: None,
            merge_verifier: None,
            extension_migrations: None,
            limits: Limits::default(),
            effect_history_limits: default_projection_read_limits(),
        })
    }

    async fn restore_suffix(
        client: &std::sync::Arc<P>,
        reader: &HistoryReader<P>,
        reducer: &mut Reducer,
        through: u64,
        maximum_events: u32,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "projection suffix exceeds byte allowance".into(),
            ));
        }
        let cursor = HistoryCursor {
            authority: match work.as_deref_mut() {
                Some(work) => work.clone_authority(reducer.authority())?,
                None => reducer.authority().clone(),
            },
            after_revision: reducer.revision(),
            through_revision: through,
        };
        let (page, records, mut used, events_retained_bytes) = reader
            .read_page_with_work(
                &cursor,
                HistoryReadLimits { maximum_events, maximum_bytes },
                true,
                work.as_mut().map(|observer| &mut **observer as &mut dyn history::HistoryIo),
            )
            .await?;
        let cursor_bytes = cursor.authority.id.capacity() as u64;
        drop(cursor);
        if let Some(work) = work.as_deref_mut() { work.release_owned(cursor_bytes)?; }
        if page.cursor.after_revision != through {
            return Err(Error::Invalid(
                "projection suffix exceeds byte allowance".into(),
            ));
        }
        let event_bytes = (page.events.capacity() * std::mem::size_of::<crate::core::Event>()) as u64;
        let record_capacity = (records.capacity() * std::mem::size_of::<operations::ObservedRecord>()) as u64;
        let page_cursor_bytes = page.cursor.authority.id.capacity() as u64;
        drop(page.cursor);
        if let Some(work) = work.as_deref_mut() { work.release_owned(page_cursor_bytes)?; }
        for (event, record) in page.events.into_iter().zip(records) {
            let (indexed, bytes) = effects::retirement_proof_with_work(
                client,
                reducer.authority(),
                &event,
                &record,
                maximum_bytes.saturating_sub(used),
                work,
            )
            .await?;
            used = used
                .checked_add(bytes)
                .filter(|total| *total <= maximum_bytes)
                .ok_or_else(|| Error::Invalid("projection suffix exceeds byte allowance".into()))?;
            let (cloned, clone_owned) = crate::core::native_event_clone_with_custody(&event, work)?;
            drop(reducer.apply_committed(cloned, work)?);
            if let Some(work) = work.as_deref_mut() { work.release_owned(clone_owned)?; }
            if indexed {
                reducer.mark_indexed_terminal_effect_with_work(&event, work)?;
            }
            reducer.enforce_terminal_effect_limit()?;
            drop(event);
            let record_bytes = operations::sdk_record_retained_bytes(&record, work);
            drop(record);
            if let Some(work) = work.as_deref_mut() { work.release_owned(record_bytes)?; }
        }
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(events_retained_bytes)?;
            work.release_owned(event_bytes)?;
            work.release_owned(record_capacity)?;
            work.check_cancellation()?;
        }
        Ok(())
    }

    /// Configures retirement of originally indexed successful/failed effects.
    pub fn with_resident_terminal_effect_limit(mut self, maximum: usize) -> Result<Self> {
        self.reducer.set_resident_terminal_effect_limit(maximum)?;
        Ok(self)
    }

    /// Sets the archive allowance for effect checks and retries.
    pub fn with_effect_history_read_limits(mut self, limits: HistoryReadLimits) -> Result<Self> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "effect history bounds must be positive".into(),
            ));
        }
        self.effect_history_limits = limits;
        Ok(self)
    }

    /// Reads an effect at the current cut and returns its retained bytes.
    pub async fn effect(
        &self,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<(crate::core::EffectState, u64)>> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "effect read bounds must be positive",
                });
            }
            return Err(Error::Invalid("effect read bounds must be positive".into()));
        }
        if let Some(state) = self.reducer.effect(effect) {
            crate::contract::validate_trusted_json_byte_bound(state, limits.maximum_bytes, work)?;
            return crate::core::native_effect_clone_with_custody(state, work).map(Some);
        }
        let reader = HistoryReader::new(&self.client, self.reducer.authority(), self.reducer.event_verifier(), work)?;
        let reader_bytes = if work.is_some() { reader.logical_owned_bytes()? } else { 0 };
        let authority = match work.as_deref_mut() {
            Some(observer) => observer.clone_authority(self.reducer.authority()),
            None => Ok(self.reducer.authority().clone()),
        };
        let authority = match authority {
            Ok(authority) => authority,
            Err(error) => {
                drop(reader);
                if let Some(observer) = work.as_deref_mut() { let _ = observer.release_owned(reader_bytes); }
                return Err(error);
            },
        };
        let cursor = HistoryCursor {
            authority,
            after_revision: 0,
            through_revision: self.reducer.revision(),
        };
        let cursor_bytes = if work.is_some() { cursor.authority.id.capacity() as u64 } else { 0 };
        let result = reader.effect(
            &cursor, effect, limits,
            work.as_mut().map(|observer| &mut **observer as &mut dyn history::HistoryIo),
        ).await;
        drop(cursor);
        drop(reader);
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(cursor_bytes + reader_bytes),
            None => Ok(()),
        };
        match result {
            Ok(state) => { released?; Ok(state) },
            Err(error) => {
                let _ = released;
                Err(error)
            },
        }
    }


    /// Configures the event/retry cache; canonical Stream history and live projections remain authoritative.
    pub fn with_resident_event_limit(mut self, maximum: usize) -> Result<Self> {
        self.set_resident_event_limit(maximum)?;
        Ok(self)
    }

    /// Reconfigures the bounded event/retry cache without moving a borrowed aggregate.
    pub fn set_resident_event_limit(&mut self, maximum: usize) -> Result<()> {
        self.reducer.set_resident_event_limit(maximum)
    }

    /// Retires only closed, checkpoint-covered conversation records from memory.
    pub fn compact_conversation_projection(&mut self) -> Result<()> {
        self.reducer.compact_conversation_projection(&mut None)
    }

    /// Installs the provider boundary that verifies every message file before admission.
    #[must_use]
    pub fn with_content_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.content_verifier = Some(verifier);
        self
    }

    /// Installs the provider boundary that checks a prepared child before publication.
    #[must_use]
    pub fn with_fork_verifier(mut self, verifier: Arc<CompositeForkVerifier>) -> Self {
        self.fork_verifier = Some(verifier);
        self
    }

    /// Installs the provider proof for parent-controlled project joins.
    #[must_use]
    pub fn with_merge_verifier(mut self, verifier: Arc<dyn ProjectMergeVerifier>) -> Self {
        self.merge_verifier = Some(verifier);
        self
    }

    /// Requires an installed, exact-digest migration implementation before accepting any `MigrateExtensionState` event.
    #[must_use]
    pub fn with_extension_migrations(
        mut self,
        migrations: Arc<NativeExtensionMigrations>,
    ) -> Self {
        self.extension_migrations = Some(migrations);
        self
    }

    /// Configures content and projection bounds for future admissions.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        self.set_limits(limits)?;
        Ok(self)
    }

    /// Updates admission bounds without discarding the verified projection.
    pub fn set_limits(&mut self, limits: Limits) -> Result<()> {
        limits.validate(&mut None)?;
        self.limits = limits;
        Ok(())
    }

    /// Returns the current deterministic projection.
    #[must_use]
    pub const fn reducer(&self) -> &Reducer {
        &self.reducer
    }

    /// Resolves one archived operation through its atomically published canonical location.
    pub async fn operation_event(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::core::Event>> {
        operations::find_operation_bounded_with_work(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
            operation, 2 * acyclic_stream::MAX_RECORD_BYTES as u64, true, &mut None,
        ).await.map(|(event, _)| event)
    }

    /// Exact historical operation revision, independent of the resident retry index.
    pub async fn operation_revision(&self, operation: OperationId) -> Result<Option<u64>> {
        Ok(self
            .operation_event(operation)
            .await?
            .map(|event| event.revision))
    }

    /// Exact committed model selection, including operations archived from resident state.
    pub async fn context_selection_for_operation(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::conversation::ModelContextSelection>> {
        Ok(self
            .operation_event(operation)
            .await?
            .and_then(|event| match event.payload {
                EventPayload::ModelContextSelected { selection } => Some(selection),
                _ => None,
            }))
    }

    async fn replay(
        &mut self, command: &Command, return_event: bool, maximum_bytes: u64,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<(ApplyResult, u64)>> {
        let mut owned = crate::wire_codec::CodecQuota::from_retained(work, 0);
        self.reducer.verify_command_scope_with_work(command, owned.work())?;
        let (event, _) = operations::bounded_operation_with_retained_quota(
            &self.client, self.reducer.authority(), self.reducer.event_verifier(),
            command.operation_id, maximum_bytes, return_event, owned.work(),
        ).await?;
        let Some((event, original_owned)) = event else {
            if self.reducer.operation_revision(command.operation_id).is_some() {
                return Err(guarded_boundary(owned.work(), crate::wire::ErrorCode::Storage,
                    "resident operation has no atomic canonical location"));
            }
            return Ok(None);
        };
        owned.adopt(original_owned)?;
        self.reducer.verify_retained_command_with_work(command, &event, owned.work())?;
        if event.revision > self.reducer.revision() {
            if event.revision != crate::contract::next_revision(self.reducer.revision())? {
                return Err(guarded_boundary(owned.work(), crate::wire::ErrorCode::Conflict,
                    "aggregate must refresh to the indexed operation boundary"));
            }
            let indexed = effects::retirement_proof_for_event_with_work(
                &self.client, self.reducer.authority(), &event, owned.work()).await?;
            let (cloned, returned_owned) = crate::core::native_event_clone_with_custody(&event, owned.work())?;
            owned.adopt(returned_owned)?;
            let result = self.reducer.apply_committed(cloned, owned.work())?;
            if indexed { self.reducer.mark_indexed_terminal_effect_with_work(&event, owned.work())?; }
            self.reducer.enforce_terminal_effect_limit()?;
            drop(event);
            owned.relinquish(original_owned)?;
            owned.transfer();
            return Ok(Some((result, returned_owned)));
        }
        owned.transfer();
        Ok(Some((ApplyResult::Replayed { event }, original_owned)))
    }


    /// Opens an archival reader without replaying or cloning the resident event history.
    pub fn history_reader(&self) -> Result<HistoryReader<P>> {
        HistoryReader::new(&self.client, self.reducer.authority(), self.reducer.event_verifier(), &mut None)
    }

    /// Resolves one attested event within this projection's committed boundary.
    pub async fn read_event_at(
        &self, revision: u64, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(crate::core::Event, u64)> {
        let after_revision = revision.checked_sub(1)
            .ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                "event revision must be positive"))?;
        let mut owned = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let cursor = HistoryCursor {
            authority: match owned.work().as_deref_mut() {
                Some(work) => work.clone_authority(self.reducer.authority())?,
                None => self.reducer.authority().clone(),
            },
            after_revision, through_revision: self.reducer.revision(),
        };
        let cursor_bytes = if owned.work().is_some() { cursor.authority.id.capacity() as u64 } else { 0 };
        owned.adopt(cursor_bytes)?;
        let reader = HistoryReader::new(&self.client, self.reducer.authority(), self.reducer.event_verifier(), owned.work())?;
        if owned.work().is_some() { owned.adopt(reader.logical_owned_bytes()?)?; }
        let (page, _, _, retained) = reader.read_page_with_work(&cursor,
            HistoryReadLimits { maximum_events: 1, maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64 },
            false, owned.work().as_mut().map(|work| &mut **work as &mut dyn history::HistoryIo)).await?;
        if owned.work().is_some() {
            owned.adopt(retained + page.cursor.authority.id.capacity() as u64
                + (page.events.capacity() * std::mem::size_of::<crate::core::Event>()) as u64)?;
        }
        let event = page.events.into_iter().next();
        drop(page.cursor);
        drop(cursor);
        drop(reader);
        let event = event.ok_or_else(|| guarded_boundary(owned.work(), crate::wire::ErrorCode::NotFound,
            "committed event is unavailable"))?;
        let temporary = owned.retained() - retained;
        owned.relinquish(temporary)?;
        owned.transfer();
        Ok((event, retained))
    }

    /// Captures the authoritative tail for a finite incremental refresh.
    pub async fn tail_revision(&self) -> Result<u64> {
        Ok(self.stream.tail().await?)
    }

    /// Advances the existing verified projection by at most one caller-sized page.
    pub async fn refresh_through(
        &mut self,
        through_revision: u64,
        maximum_events: u32,
    ) -> Result<bool> {
        let current = self.reducer.revision();
        if maximum_events == 0 || through_revision < current {
            return Err(Error::Invalid("aggregate refresh cursor is invalid".into()));
        }
        if through_revision == current {
            return Ok(true);
        }
        if through_revision > self.stream.tail().await? {
            return Err(Error::Invalid(
                "aggregate refresh exceeds committed tail".into(),
            ));
        }
        let count = u32::try_from((through_revision - current).min(u64::from(maximum_events)))
            .map_err(|_| Error::Invalid("aggregate refresh page exceeds platform size".into()))?;
        let mut records = self.stream.read(current, count).await?;
        let mut consumed = 0;
        while let Some(record) = records.try_next().await? {
            if consumed >= count || record.sequence != self.reducer.revision() {
                return Err(Error::Storage(
                    "aggregate refresh page is not contiguous".into(),
                ));
            }
            let ((authority, event), _) = decode_event(&record.value, &mut None)?;
            if &authority != self.reducer.authority()
                || event.revision
                    != record.sequence.checked_add(1).ok_or_else(|| {
                        Error::Storage("aggregate refresh revision overflows".into())
                    })?
            {
                return Err(Error::Storage(
                    "aggregate refresh event binding is invalid".into(),
                ));
            }
            let (indexed, _) = effects::retirement_proof_with_work(
                &self.client,
                self.reducer.authority(),
                &event,
                &record,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None)
            .await?;
            self.reducer.apply_committed(event.clone(), &mut None)?;
            if indexed {
                self.reducer.mark_indexed_terminal_effect_with_work(&event, &mut None)?;
            }
            self.reducer.enforce_terminal_effect_limit()?;
            consumed += 1;
        }
        if consumed == 0 {
            return Err(Error::Storage(
                "aggregate refresh stopped before captured tail".into(),
            ));
        }
        Ok(self.reducer.revision() == through_revision)
    }

    /// Plans, CAS-appends, and only then applies one command.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.execute",
            level = "info",
            skip_all,
            fields(rev = command.expected_revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn execute(
        &mut self, command: Command,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        #[cfg(feature = "filesystem")]
        if owner.is_some() || work.is_some() {
            if let Some(owner) = owner
                && matches!(command.action, Action::BindConversation { .. })
            {
                return self.execute_root_bind_with_work(command, owner, work).await;
            }
            let write = if let Some(owner) = owner {
                match &command.action {
                    Action::OpenInteraction { .. } => {
                        owner.require_interaction_grant(work)?;
                        crate::distributed::JournalWrite::Fresh
                    }
                    Action::PlanEffect { provider, request, result_schema, .. } => {
                        owner.require_effect_grants(provider, true, work)?;
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
                            owner.validate_input_file(request, Some(&crate::filesystem::PreparationIo::from_native_work(native)?))?;
                            owner.validate_input_file(result_schema, Some(&crate::filesystem::PreparationIo::from_native_work(native)?))?;
                        } else {
                            owner.validate_input_file(request, None)?;
                            owner.validate_input_file(result_schema, None)?;
                        }
                        #[cfg(target_arch = "wasm32")]
                        {
                            owner.validate_input_file(request, None)?;
                            owner.validate_input_file(result_schema, None)?;
                        }
                        crate::distributed::JournalWrite::Fresh
                    }
                    Action::MarkEffectDispatched { effect_id, .. }
                    | Action::ResolveEffect { observation: crate::core::EffectAttestation { effect_id, .. } } => {
                        let mut retained = crate::wire_codec::CodecQuota::from_retained(work, 0);
                        let (effect, bytes) = self.effect(*effect_id, self.effect_history_limits, retained.work())
                            .await?.ok_or_else(|| guarded_boundary(retained.work(),
                                crate::wire::ErrorCode::NotFound, "effect is not found"))?;
                        retained.adopt(bytes)?;
                        owner.require_effect_grants(&effect.provider, false, retained.work())?;
                        drop(effect);
                        retained.relinquish(bytes)?;
                        if matches!(command.action, Action::ResolveEffect { .. }) {
                            crate::distributed::JournalWrite::Settlement
                        } else { crate::distributed::JournalWrite::Fresh }
                    }
                    _ => return Err(guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                        "task journal action is not an admitted request or effect")),
                }
            } else { crate::distributed::JournalWrite::Fresh };
            return self.publish(
                command, owner, write, true, work,
            ).await.map(|(result, _)| result);
        }
        if work.is_some() {
            return Err(Error::Unsupported("aggregate work observers require filesystem support".into()));
        }
        crate::obs::outcome(async {
            if let Some((result, _)) = self.replay(&command, true, 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await? {
                if let Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence(seed).await?;
                }
                return Ok(result);
            }
            let idempotency_key =
                stream_idempotency_key_with_work(self.stream.path().as_str(), &command.idempotency_key, &mut None)?;
            let fresh_migration = matches!(&command.action, Action::MigrateExtensionState { .. })
                && self
                    .reducer
                    .operation_revision(command.operation_id)
                    .is_none();
            let planned = self.plan_command(&command, fresh_migration).await?;
            let ApplyResult::Applied { event } = planned else {
                if let crate::core::Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence(seed).await?;
                }
                return Ok(planned);
            };
            if !fresh_migration {
                self.validate_admission_content(&command.action).await?;
            }
            self.validate_causal_reference(event.causal_parent.as_ref(), &mut None)
                .await?;
            let publication = checkpoints::publication_with_work(&self.client, &self.reducer, &event, &mut None).await?;
            let request = publication.request(&self.client, idempotency_key, &mut None).await?.0;
            let fork_guard = if let crate::core::Action::PublishFork { seed } = &command.action {
                let verifier = self
                    .fork_verifier
                    .as_ref()
                    .ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?;
                Some(verifier.activate(seed, &mut None).await?)
            } else {
                None
            };
            let append = self.client.commit(request).await;
            let outcome = match append {
                Ok(outcome) => outcome,
                Err(StreamError::Unavailable) => {
                    return match self.reconcile_inner(&command).await {
                        Ok(Some(result)) => {
                            if let Some(guard) = fork_guard {
                                crate::fork::release_fork_guard_with_work(guard.guard.as_ref(), &mut None)
                                    .await
                                    .map_err(|_| Error::Indeterminate(command.operation_id))?;
                            }
                            Ok(result)
                        }
                        Ok(None) | Err(Error::Storage(_)) => {
                            Err(Error::Indeterminate(command.operation_id))
                        }
                        Err(error) => Err(error),
                    };
                }
                Err(StreamError::IdempotencyMismatch) => {
                    if let Some(guard) = fork_guard {
                        crate::fork::release_fork_guard_with_work(guard.guard.as_ref(), &mut None)
                                    .await
                            .map_err(|_| Error::Indeterminate(command.operation_id))?;
                    }
                    return Err(Error::Conflict(
                        "retry identity is already bound to another append".into(),
                    ));
                }
                Err(error) => {
                    if let Some(guard) = fork_guard {
                        crate::fork::release_fork_guard_with_work(guard.guard.as_ref(), &mut None)
                                    .await
                            .map_err(|_| Error::Indeterminate(command.operation_id))?;
                    }
                    return Err(Error::Storage(error.to_string()));
                }
            };
            let result = match outcome {
                CommitOutcome::Committed(envelope) => {
                    if publication.verify(&envelope, &mut None).is_err() {
                        return Err(Error::Indeterminate(publication.operation_id));
                    }
                    match self.reducer.apply_committed(event.clone(), &mut None).and_then(|result|
                        self.reducer.mark_indexed_terminal_effect_with_work(&event, &mut None).map(|()| result)) {
                        Ok(result) => Ok(result),
                        Err(_) => return Err(Error::Indeterminate(command.operation_id)),
                    }
                }
                CommitOutcome::Conflict(_) => Err(Error::Conflict(
                    "aggregate tail or operation identity changed".into(),
                )),
            };
            if let Some(guard) = fork_guard {
                crate::fork::release_fork_guard_with_work(guard.guard.as_ref(), &mut None)
                                    .await
                    .map_err(|_| Error::Indeterminate(command.operation_id))?;
            }
            result
        }.await)
    }


    async fn plan_command(&self, command: &Command, fresh_migration: bool) -> Result<ApplyResult> {
        if !fresh_migration {
            if let Some(effect_id) = self.reducer.archived_effect_for_command(command)
                && let Some(effect) = self.effect(effect_id, self.effect_history_limits, &mut None).await.map(|state| state.map(|(state, _)| state))?
            {
                return self
                    .reducer
                    .plan_with_archived_effect(command, effect_id, &effect, &mut None).map(|(result, _)| result);
            }
            return self.reducer.plan_indexed_with_retained_quota(command, false, &mut None).map(|(result, _)| result);
        }
        if let Action::MigrateExtensionState {
            name,
            previous,
            to_version,
            content,
        } = &command.action
        {
            self.reducer.preflight_extension_migration(
                &command.scope,
                command.expected_revision,
                previous,
                name,
                *to_version,
                content.volume(),
            )?;
        }
        self.validate_admission_content(&command.action).await?;
        self.reducer.plan_indexed_with_retained_quota(command, true, &mut None).map(|(result, _)| result)
    }


    #[cfg(feature = "filesystem")]
    async fn validate_task_admission_content_with_work(
        &self, action: &Action, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        if work.is_none() { return self.validate_admission_content(action).await; }
        let (reference, reason) = match action {
            Action::OpenInteraction { ticket } => (&ticket.request, "interaction content verifier is not bound"),
            Action::PlanEffect { request, .. } => (request, "effect request content verifier is not bound"),
            Action::ResolveEffect { observation } => {
                let EffectStatus::Succeeded { result } = &observation.status else { return Ok(()); };
                (result, "effect result content verifier is not bound")
            },
            _ => return Ok(()),
        };
        let verifier = self.content_verifier.as_ref().ok_or_else(||
            guarded_boundary(work, crate::wire::ErrorCode::Unsupported, reason))?;
        if let Action::PlanEffect { result_schema, .. } = action {
            match work.as_deref_mut() {
                Some(observer) => history::verify_content_with_work(observer, verifier.as_ref(), reference).await?,
                None => { verified_content_bytes(verifier.as_ref(), reference, (reference).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes)?; },
            }
            let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
            let (bytes, claim) = Self::read_admission_content_with_work(
                verifier.as_ref(), result_schema, scratch.work()).await?;
            scratch.adopt(claim)?;
            let validated = crate::effects::validate_schema_bytes(
                result_schema, &bytes, scratch.work());
            drop(bytes);
            let released = scratch.relinquish(claim);
            return match validated {
                Ok(()) => released,
                Err(error) => { let _ = released; Err(error) },
            };
        }
        if let Action::ResolveEffect { observation } = action {
            let effect = self.reducer.effect(observation.effect_id).ok_or_else(|| {
                #[cfg(not(target_arch = "wasm32"))]
                if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
                    return Error::NativeBoundary {
                        code: crate::wire::ErrorCode::NotFound, reason: "effect is not found",
                    };
                }
                Error::NotFound(format!("effect {}", observation.effect_id))
            })?;
            let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
            let (bytes, claim) = Self::read_admission_content_with_work(
                verifier.as_ref(), reference, scratch.work()).await?;
            scratch.adopt(claim)?;
            let (schema_bytes, schema_claim) = Self::read_admission_content_with_work(
                verifier.as_ref(), &effect.result_schema, scratch.work()).await?;
            scratch.adopt(schema_claim)?;
            let validated = crate::effects::validate_result_bytes(
                &effect.result_schema, &schema_bytes, reference, &bytes, scratch.work());
            drop(schema_bytes);
            drop(bytes);
            let retained = scratch.retained();
            let released = scratch.relinquish(retained);
            return match validated {
                Ok(()) => released,
                Err(error) => { let _ = released; Err(error) },
            };
        }
        if let Action::OpenInteraction { ticket } = action {
            let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
            let (bytes, claim) = Self::read_admission_content_with_work(
                verifier.as_ref(), reference, scratch.work()).await?;
            scratch.adopt(claim)?;
            let validated = ticket.validate_request_bytes(&bytes, scratch.work()).and_then(|request| {
                let measured = request.1;
                drop(request);
                scratch.work().as_deref_mut().map_or(Ok(()), |io| io.release_owned(measured))
            });
            drop(bytes);
            let released = scratch.relinquish(claim);
            return match validated {
                Ok(()) => released,
                Err(error) => { let _ = released; Err(error) },
            };
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    async fn read_admission_content_with_work(
        verifier: &dyn ContentResidencyVerifier, reference: &crate::conversation::FileRef,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(acyclic_stream::OwnedBytes, u64)> {
        match work.as_deref_mut() {
            Some(observer) => history::read_content_with_work(observer, verifier, reference).await,
            None => verifier.read(reference, reference.descriptor().byte_length(), None)?.await.map(|bytes| (bytes, 0)),
        }
    }

    #[cfg(feature = "filesystem")]
    async fn validate_interaction_part_with_work(
        &self, ticket: &crate::interaction::InteractionTicket,
        reference: &crate::conversation::FileRef, detail_outcome: Option<&InteractionOutcome>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let verifier = self.content_verifier.as_ref().ok_or_else(||
            guarded_boundary(work, crate::wire::ErrorCode::Unsupported,
                "interaction content verifier is not bound"))?;
        let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let (request_bytes, request_claim) = Self::read_admission_content_with_work(
            verifier.as_ref(), &ticket.request, scratch.work()).await?;
        scratch.adopt(request_claim)?;
        let request = ticket.validate_request_bytes(&request_bytes, scratch.work())?;
        scratch.adopt(request.1)?;
        let (bytes, claim) = Self::read_admission_content_with_work(
            verifier.as_ref(), reference, scratch.work()).await?;
        scratch.adopt(claim)?;
        match detail_outcome {
            Some(outcome) => ticket.validate_decision_bytes(
                &request.0, outcome, reference, &bytes, scratch.work()),
            None => ticket.validate_answer_bytes(
                &request.0, reference, &bytes, scratch.work()),
        }
    }

    async fn validate_admission_content(&self, action: &crate::core::Action) -> Result<()> {
        if let crate::core::Action::ResolveInteraction { resolution } = action
            && matches!(resolution.outcome, InteractionOutcome::Expired)
        {
            let (ticket, _) = self
                .reducer
                .interaction(&resolution.id)
                .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
            let deadline = ticket
                .deadline_unix_ms
                .ok_or_else(|| Error::Invalid("interaction has no expiry deadline".into()))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| Error::Storage(error.to_string()))?
                .as_millis();
            if now < u128::from(deadline) {
                return Err(Error::Conflict(
                    "interaction deadline has not elapsed".into(),
                ));
            }
        }
        if let crate::core::Action::OpenInteraction { ticket } = action {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("interaction content verifier is not bound".into())
            })?;
            let bytes = verifier.read(&ticket.request, ticket.request.descriptor().byte_length(), None)?.await?;
            ticket.validate_request_bytes(&bytes, &mut None)?;
        }
        if let crate::core::Action::ResolveInteraction { resolution } = action
            && let InteractionOutcome::Answered { answer } = &resolution.outcome
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("interaction content verifier is not bound".into())
            })?;
            let (ticket, _) = self
                .reducer
                .interaction(&resolution.id)
                .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
            let request_bytes = verifier.read(&ticket.request, ticket.request.descriptor().byte_length(), None)?.await?;
            let request = ticket.validate_request_bytes(&request_bytes, &mut None)?.0;
            let answer_bytes = verifier.read(answer, answer.descriptor().byte_length(), None)?.await?;
            ticket.validate_answer_bytes(&request, answer, &answer_bytes, &mut None)?;
        }
        if let crate::core::Action::ResolveInteraction { resolution } = action
            && let Some(detail) = &resolution.detail
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("interaction content verifier is not bound".into())
            })?;
            let (ticket, _) = self
                .reducer
                .interaction(&resolution.id)
                .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
            let request_bytes = verifier.read(&ticket.request, ticket.request.descriptor().byte_length(), None)?.await?;
            let request = ticket.validate_request_bytes(&request_bytes, &mut None)?.0;
            let detail_bytes = verifier.read(detail, detail.descriptor().byte_length(), None)?.await?;
            ticket.validate_decision_bytes(&request, &resolution.outcome, detail, &detail_bytes, &mut None)?;
        }
        if let crate::core::Action::AppendCustom {
            schema,
            version,
            content,
        } = action
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let bytes = verifier.read(content, content.descriptor().byte_length(), None)?.await?;
            self.reducer
                .validate_custom_bytes(schema, *version, content, &bytes)?;
        }
        if let crate::core::Action::MigrateExtensionState {
            name,
            previous,
            to_version,
            content,
        } = action
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let migrations = self.extension_migrations.as_ref().ok_or_else(|| {
                Error::Unsupported("extension migration executor is not bound".into())
            })?;
            let (source, prior) = self
                .reducer
                .extension_state(name)
                .ok_or_else(|| Error::NotFound(format!("extension state {name}")))?;
            if source != previous {
                return Err(Error::Conflict("extension migration source changed".into()));
            }
            self.limits.validate_file(&prior.content, &mut None)?;
            self.limits.validate_file(content, &mut None)?;
            let (old_bytes, _old_hold) = verified_content_bytes(verifier.as_ref(), &prior.content, self.limits.file_bytes, None).await?;
            self.reducer
                .validate_custom_bytes(name, prior.version, &prior.content, &old_bytes)?;
            let previous_value: Value =
                crate::contract::json_from_slice(&old_bytes).map_err(|error| {
                    Error::Invalid(format!("previous extension state is invalid: {error}"))
                })?;
            let target_digest = self
                .reducer
                .extension_implementation_digest(name, *to_version)?;
            let expected = migrations.migrate(
                name,
                prior.version,
                *to_version,
                target_digest,
                &previous_value,
            )?;
            let (bytes, _hold) = verified_content_bytes(verifier.as_ref(), content, self.limits.file_bytes, None).await?;
            self.reducer
                .validate_custom_bytes(name, *to_version, content, &bytes)?;
            if bytes.as_ref() != crate::contract::canonical_json_bytes(&expected)?.as_slice() {
                return Err(Error::Conflict(
                    "staged extension migration differs from its pinned implementation".into(),
                ));
            }
        }
        if let crate::core::Action::ConfigureExtension { extension, content } = action {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let bytes = verifier.read(content, content.descriptor().byte_length(), None)?.await?;
            self.reducer
                .validate_configuration_bytes(extension, content, &bytes, None)?;
        }
        let message = match action {
            crate::core::Action::AppendConversationMessage { message } => Some(message.as_ref()),
            crate::core::Action::PublishProjectMerge { receipt } => Some(&receipt.notice),
            _ => None,
        };
        if let Some(message) = message {
            self.limits.validate_message(message, &mut None)?;
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("conversation content verifier is not bound".into())
            })?;
            verified_content_bytes(verifier.as_ref(), &message.content, (&message.content).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes)?;
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for attachment in items {
                        verified_content_bytes(verifier.as_ref(), &attachment.file, (&attachment.file).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes)?;
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    verified_attachment_manifest(
                        verifier.as_ref(),
                        manifest,
                        *item_count,
                        &self.limits,
                        None,
                    )
                    .await?;
                }
            }
            for reference in message.extensions.values() {
                verified_content_bytes(verifier.as_ref(), reference, (reference).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes)?;
            }
        }
        if let crate::core::Action::PlanEffect {
            request,
            result_schema,
            ..
        } = action
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("effect request content verifier is not bound".into())
            })?;
            verified_content_bytes(verifier.as_ref(), request, (request).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes)?;
            let schema_bytes = verifier.read(result_schema, result_schema.descriptor().byte_length(), None)?.await?;
            validate_schema_bytes(result_schema, &schema_bytes, &mut None)?;
        }
        if let crate::core::Action::ResolveEffect { observation } = action
            && let EffectStatus::Succeeded { result } = &observation.status
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("effect result content verifier is not bound".into())
            })?;
            let effect = self
                .reducer
                .effect(observation.effect_id)
                .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
            let bytes = verifier.read(result, result.descriptor().byte_length(), None)?.await?;
            let schema_bytes = verifier.read(&effect.result_schema, effect.result_schema.descriptor().byte_length(), None)?.await?;
            validate_result_bytes(&effect.result_schema, &schema_bytes, result, &bytes, &mut None)?;
        }
        if let crate::core::Action::PublishProjectMerge { receipt } = action {
            let verifier = self
                .merge_verifier
                .as_ref()
                .ok_or_else(|| Error::Unsupported("project merge verifier is not bound".into()))?;
            verifier.verify(receipt).await?;
        }
        Ok(())
    }



    /// Reconciles a possibly committed append without issuing another append.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.reconcile",
            level = "info",
            skip_all,
            fields(rev = command.expected_revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn reconcile(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        crate::obs::outcome(
            async {
                let result = self.reconcile_inner(command).await?;
                if result.is_some()
                    && let crate::core::Action::PublishFork { seed } = &command.action
                {
                    self.release_replayed_fork_fence(seed).await?;
                }
                Ok(result)
            }
            .await,
        )
    }

    async fn release_replayed_fork_fence(&self, seed: &crate::fork::ForkSeed) -> Result<()> {
        let verifier = self
            .fork_verifier
            .as_ref()
            .ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?;
        verifier
            .release_private_fence(seed, &mut None)
            .await
            .map_err(|_| Error::Indeterminate(seed.operation_id))
    }

    async fn reconcile_inner(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        if let Some((result, _)) = self.replay(command, true, 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await? {
            return Ok(Some(result));
        }
        let key = stream_idempotency_key_with_work(self.stream.path().as_str(), &command.idempotency_key, &mut None)?;
        let Some(observation) = self.client.inspect_idempotency(key).await? else {
            return Ok(None);
        };
        match observation.outcome {
            IdempotencyOutcome::Commit(CommitOutcome::Conflict(_)) => {
                if let Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence(seed).await?;
                }
                Err(Error::Conflict("aggregate publication was rejected".into()))
            }
            IdempotencyOutcome::Commit(CommitOutcome::Committed(_)) => Err(Error::Storage(
                "committed aggregate publication has no atomic operation location".into(),
            )),
            _ => Err(Error::Conflict(
                "retry identity is bound to another operation contract".into(),
            )),
        }
    }

    async fn validate_causal_reference(
        &self, reference: Option<&EventReference>, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let Some(reference) = reference else { return Ok(()); };
        if reference.authority == *self.reducer.authority() { return Ok(()); }
        let path = reference.authority.stream_path(work)?;
        let path_bytes = if work.is_some() { path.capacity() as u64 } else { 0 };
        let read = match operations::stream(&self.client, &path, work) {
            Ok(stream) => operations::one_record(&self.client, stream.0.path(),
                reference.revision.saturating_sub(1), work).await,
            Err(error) => Err(error),
        };
        drop(path);
        let released_path = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(path_bytes), None => Ok(()),
        };
        let record = match read {
            Ok(record) => { released_path?; record },
            Err(error) => { let _ = released_path; return Err(error); },
        }.ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::NotFound, "causal event"))?;
        let raw = operations::sdk_record_retained_bytes(&record, work);
        let decoded = crate::wire_codec::decode_event(&record.value, work);
        drop(record);
        let released_record = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(raw), None => Ok(()),
        };
        let ((authority, event), event_retained) = match decoded {
            Ok(decoded) => { released_record?; decoded },
            Err(error) => { let _ = released_record; return Err(error); },
        };
        let valid = authority == reference.authority && event.revision == reference.revision;
        let owned = if work.is_some() {
            event_retained
                .checked_add(authority.id.capacity() as u64)
                .ok_or_else(|| Error::Invalid("causal record custody overflows".into()))?
        } else { 0 };
        drop(event);
        drop(authority);
        if let Some(work) = work.as_deref_mut() { work.release_owned(owned)?; }
        if !valid {
            return Err(Error::Invalid("causal event reference does not resolve exactly".into()));
        }
        Ok(())
    }
}

fn stream_idempotency_key_with_work(
    path: &str,
    value: &IdempotencyKey,
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<StreamIdempotencyKey> {
    if let Some(work) = work.as_deref_mut() {
        let bytes = b"acyclic-harness-stream-append-v2".len()
            .checked_add(16)
            .and_then(|bytes| bytes.checked_add(path.len()))
            .and_then(|bytes| bytes.checked_add(value.0.len()))
            .ok_or_else(|| Error::Invalid("stream retry digest length overflows".into()))?;
        work.hashed_bytes(bytes as u64)?;
        work.admit_allocation(32)?;
        work.copy_bytes(32)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-stream-append-v2");
    hasher.update(&(path.len() as u64).to_le_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&(value.0.len() as u64).to_le_bytes());
    hasher.update(value.0.as_bytes());
    let key = StreamIdempotencyKey::new(OwnedBytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if let Some(work) = work.as_deref_mut() { work.retain_owned(32)?; }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    include!("store/measured_open_tests.rs");
    #[cfg(feature = "filesystem")]
    mod measured_fork_tests {
        include!("store/measured_fork_tests.rs");
    }
    use crate::wire_codec::encode_event;
    use crate::{
        Capabilities, OperationId,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        core::{Action, AggregateKind, AuthorityIssuer, ExtensionDependency, ExtensionForkPolicy},
        interaction::{
            Interaction, InteractionKind, InteractionOutcome, InteractionResolution,
            InteractionTicket,
        },
        resources::ProviderRef,
    };
    use acyclic_stream::MemoryStream;
    use serde_json::json;
    use std::sync::Arc;

    struct InteractionContent(std::collections::BTreeMap<String, Vec<u8>>);

    impl ContentResidencyVerifier for InteractionContent {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { self.read(reference).await.map(|_| ()) })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<acyclic_stream::OwnedBytes>> + Send + 'a>>
        {
            Box::pin(async move {
                let bytes = self
                    .0
                    .get(reference.path())
                    .ok_or_else(|| Error::NotFound(reference.path().into()))?;
                reference.descriptor().verify(bytes, &mut None)?;
                Ok(bytes.clone().into())
            })
        }
    }

    fn interaction_file(path: &str, bytes: &[u8]) -> Result<FileRef> {
        FileRef::new(
            content()?.volume().clone(),
            path,
            "generation-1",
            FileDescriptor::from_bytes(bytes, "application/json")?,
            "interaction.json",
        )
    }

    fn authority() -> Authority {
        Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        }
    }

    fn command(identity: u8) -> Result<Command> {
        Ok(Command {
            operation_id: OperationId::from_bytes([identity; 16]),
            idempotency_key: IdempotencyKey(format!("append-conversation-1-{identity}")),
            expected_revision: 0,
            scope: issuer().issue("root", &Capabilities::new(["event:append"]), None, None, &mut None).expect("scope proof"),
            causal_parent: None,
            action: Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: content()?,
            },
        })
    }

    const EXTENSION_BYTES: &[u8] = br#"{"text":"hello"}"#;

    fn content() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "extension",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
            )?,
            "extension/event.json",
            "generation-1",
            FileDescriptor::from_bytes(EXTENSION_BYTES, "application/json")?,
            "event.json",
        )
    }

    struct TestContent;

    struct TestPublisher {
        volume: VolumeRef,
        stages: std::sync::atomic::AtomicUsize,
        files: MigrationContentStore,
    }

    #[derive(Clone, Default)]
    struct MigrationContentStore(Arc<std::sync::Mutex<BTreeMap<String, Vec<u8>>>>);

    impl MigrationContentStore {
        fn key(reference: &FileRef) -> Result<String> {
            Ok(format!(
                "{}:{}",
                serde_json::to_string(reference.volume())
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                reference.path()
            ))
        }

        fn insert(&self, reference: &FileRef, bytes: &[u8]) -> Result<()> {
            reference.descriptor().verify(bytes, &mut None)?;
            self.0
                .lock()
                .map_err(|_| Error::Storage("migration content lock poisoned".into()))?
                .insert(Self::key(reference)?, bytes.to_vec());
            Ok(())
        }
    }

    impl ContentResidencyVerifier for MigrationContentStore {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { self.read(reference).await.map(|_| ()) })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<acyclic_stream::OwnedBytes>> + Send + 'a>>
        {
            Box::pin(async move {
                let bytes = self
                    .0
                    .lock()
                    .map_err(|_| Error::Storage("migration content lock poisoned".into()))?
                    .get(&Self::key(reference)?)
                    .cloned()
                    .ok_or_else(|| Error::NotFound("migration content is absent".into()))?;
                reference.descriptor().verify(&bytes, &mut None)?;
                Ok(bytes.into())
            })
        }
    }

    impl ContentPublisher for TestPublisher {
        fn volume(&self) -> &VolumeRef {
            &self.volume
        }

        fn stage<'a>(
            &'a self,
            _operation_id: OperationId,
            path: &'a str,
            bytes: &'a [u8],
            media_type: &'a str,
            display_name: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<FileRef>> + Send + 'a>>
        {
            Box::pin(async move {
                if bytes != EXTENSION_BYTES || media_type != "application/json" {
                    return Err(Error::Invalid("unexpected migration output".into()));
                }
                self.stages
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let reference = FileRef::new(
                    self.volume.clone(),
                    path,
                    "generation-1",
                    FileDescriptor::from_bytes(bytes, media_type)?,
                    display_name,
                )?;
                self.files.insert(&reference, bytes)?;
                Ok(reference)
            })
        }
    }

    impl ContentResidencyVerifier for TestContent {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { reference.descriptor().verify(EXTENSION_BYTES, &mut None) })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<acyclic_stream::OwnedBytes>> + Send + 'a>>
        {
            Box::pin(async move {
                reference.descriptor().verify(EXTENSION_BYTES, &mut None)?;
                Ok(acyclic_stream::OwnedBytes::from_static(EXTENSION_BYTES))
            })
        }
    }

    fn with_content<P: StreamProvider>(aggregate: StreamAggregate<P>) -> StreamAggregate<P> {
        aggregate.with_content_verifier(Arc::new(TestContent))
    }

    fn issuer() -> AuthorityIssuer {
        AuthorityIssuer::new("test", [7; 32], authority())
    }

    fn schemas() -> SchemaRegistry {
        let mut schemas = SchemaRegistry::new();
        assert!(
            schemas
                .register(
                    "example.message",
                    1,
                    json!({"type": "object"}),
                    [9; 32],
                    crate::core::ExtensionForkPolicy::Inherit, [], None
                )
                .is_ok()
        );
        schemas
    }

    #[tokio::test]
    async fn empty_history_cursor_stays_terminal_after_first_publication() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let reader = HistoryReader::new(&client, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?;
        let cursor = reader.pin(0, None).await?;
        let limits = HistoryReadLimits {
            maximum_events: 1,
            maximum_bytes: 65_536,
        };
        assert_eq!(cursor.through_revision, 0);
        assert!(reader.read_page(&cursor, limits).await?.events.is_empty());
        assert!(matches!(reader.pin(1, None).await, Err(Error::Invalid(_))));
        let future = HistoryCursor {
            through_revision: 1,
            ..cursor.clone()
        };
        assert!(matches!(
            reader.read_page(&future, limits).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        writer.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        assert!(reader.read_page(&cursor, limits).await?.events.is_empty());
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        let current = reader.pin(0, None).await?;
        let page = reader.read_page(&current, limits).await?;
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.cursor.after_revision, 1);
        Ok(())
    }

    #[tokio::test]
    async fn operation_lookup_counts_atomic_records_under_one_byte_budget() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?
                .with_resident_event_limit(1)?,
        );
        let original = command(1)?;
        let first = writer.execute(original.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
        let ApplyResult::Applied { event } = first else {
            return Err(Error::Invalid("fixture event was not published".into()));
        };
        let mut later = command(2)?;
        later.expected_revision = 1;
        writer.execute(later, #[cfg(feature = "filesystem")] None, &mut None).await?;
        assert!(
            writer
                .reducer()
                .operation_revision(original.operation_id)
                .is_none()
        );
        let index = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), operations::operation_path(&authority(), original.operation_id)?.as_str())?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let canonical = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let index_bytes = index
            .first()
            .ok_or_else(|| Error::Storage("fixture index missing".into()))?
            .value
            .len() as u64;
        let total_bytes = index_bytes
            + canonical
                .first()
                .ok_or_else(|| Error::Storage("fixture canonical event missing".into()))?
                .value
                .len() as u64;
        let reader = writer.history_reader()?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .operation_event(original.operation_id, 0, &mut None)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        for maximum in [index_bytes - 1, index_bytes] {
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert!(matches!(
                reader
                    .operation_event(original.operation_id, maximum, &mut None)
                    .await,
                Err(Error::Invalid(_))
            ));
        }
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .operation_event(original.operation_id, total_bytes - 1, &mut None)
                .await,
            Err(Error::Invalid(_))
        ));
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader
                .operation_event(original.operation_id, total_bytes, &mut None)
                .await?,
            (Some(event.clone()), total_bytes)
        );
        assert_eq!(
            reader.operation_event(original.operation_id, 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await.map(|(event, _)| event)?,
            Some(event)
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            reader
                .operation_event(OperationId::from_bytes([254; 16]), total_bytes, &mut None)
                .await?,
            (None, 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn conversation_head_pins_racing_append_and_rejects_corrupt_atomic_proof() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        let agent = crate::AgentId::from_bytes([8; 16]);
        let file = content()?;
        let scope = issuer().issue("head-writer", &Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                file.read_capability()?,
            ]), Some(agent), None, &mut None).expect("scope proof");
        let bind = Command {
            operation_id: OperationId::from_bytes([101; 16]),
            idempotency_key: IdempotencyKey::new("head-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation { agent },
        };
        writer.execute(bind.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
        let reader = writer.history_reader()?;
        assert!(reader.latest_conversation_message(65_536).await?.is_none());
        let message = crate::conversation::ConversationMessage {
            id: uuid::Uuid::from_u128(1),
            sequence: 1,
            kind: crate::conversation::MessageKind::User,
            content: file,
            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        let command = Command {
            operation_id: OperationId::from_bytes([102; 16]),
            idempotency_key: IdempotencyKey::new("head-racing-user")?,
            expected_revision: 1,
            scope,
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(message.clone()),
            },
        };
        let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) =
            writer.reducer().plan(&command)?;
        let publication = operations::IndexedPublication::new(
            &authority(),
            &event,
            encode_event(&authority(), &event, &mut None)?,
         &mut None)?;
        *provider
            .message_head_race
            .lock()
            .map_err(|_| Error::Storage("head race lock".into()))? = Some(
            publication
                .request(&client, StreamIdempotencyKey::new("head-racing-commit")?)
                .await?,
        );
        assert!(reader.latest_conversation_message(65_536).await?.is_none());
        assert_eq!(
            reader.latest_conversation_message(65_536).await?,
            Some(message)
        );
        drop(writer);
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert_eq!(
            reopened
                .reducer()
                .conversation()
                .map(|state| state.messages().len()),
            Some(1)
        );
        for fault in [1, 2, 3, 4] {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(reader.latest_conversation_message(65_536).await.is_err());
        }
        let head = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), "harness/v2/conversation-heads/conversations/conversation-1")?;
        let records = head.read(1, 1).await?.try_collect::<Vec<_>>().await?;
        let mut forged: Value = crate::executor::decode_json(
            &records
                .first()
                .ok_or_else(|| Error::Storage("head record missing".into()))?
                .value,
        )?;
        forged["sequence"] = json!(2);
        head.append(crate::contract::canonical_json_bytes(&forged)?)
            .await?;
        assert!(matches!(reader.latest_conversation_message(65_536).await,
            Err(Error::Storage(detail)) if detail.contains("atomic canonical event")));

        let incomplete = Arc::new(MemoryStream::default());
        let reducer = Reducer::new(authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
        let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) =
            reducer.plan(&bind)?;
        acyclic_stream::Stream::new(std::sync::Arc::clone(&incomplete), authority().stream_path(&mut None)?)?
            .append(encode_event(&authority(), &event, &mut None)?)
            .await?;
        let incomplete_reader = HistoryReader::new(&incomplete, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?;
        assert!(matches!(
            incomplete_reader.latest_conversation_message(65_536).await,
            Err(Error::Storage(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn indexed_conversation_lookup_is_atomic_bounded_and_pinned_after_eviction() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?
                .with_resident_event_limit(1)?,
        );
        let agent = crate::AgentId::from_bytes([8; 16]);
        let file = content()?;
        let scope = issuer().issue("message-writer", &Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "conversation:select_context".to_owned(),
                file.read_capability()?,
            ]), Some(agent), None, &mut None).expect("scope proof");
        writer
            .execute(Command {
                operation_id: OperationId::from_bytes([101; 16]),
                idempotency_key: IdempotencyKey::new("message-index-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        let reader = writer.history_reader()?;
        let empty = reader.pin(0, None).await?;
        assert!(reader.latest_conversation_message(65_536).await?.is_none());
        let message = |sequence| crate::conversation::ConversationMessage {
            id: uuid::Uuid::from_u128(u128::from(sequence)),
            sequence,
            kind: crate::conversation::MessageKind::User,
            content: file.clone(),
            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        let mut first_command = None;
        for sequence in 1..=10_000u64 {
            let next = Command {
                operation_id: OperationId::from_bytes(
                    uuid::Uuid::from_u128(u128::from(sequence) + 100_000).into_bytes(),
                ),
                idempotency_key: IdempotencyKey::new(format!("message-index-{sequence}"))?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(message(sequence)),
                },
            };
            if sequence == 1 {
                first_command = Some(next.clone());
            }
            writer.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
            if matches!(sequence, 1 | 1_000 | 10_000) {
                let cold = HistoryReader::new(&client, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?;
                provider.forbid_writes.store(true, Ordering::SeqCst);
                provider.observation_reads.store(0, Ordering::SeqCst);
                assert_eq!(
                    cold.latest_conversation_message(65_536).await?,
                    Some(message(sequence))
                );
                provider.forbid_writes.store(false, Ordering::SeqCst);
            }
        }
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let restored =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert_eq!(restored.reducer().revision(), writer.reducer().revision());
        let restored_history = restored
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("restored conversation missing".into()))?;
        let live_history = writer
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("live conversation missing".into()))?;
        assert_eq!(restored_history.logical_revision(), 10_000);
        assert_eq!(
            restored_history.history_prefix_digest(10_000, &mut None)?,
            live_history.history_prefix_digest(10_000, &mut None)?
        );
        assert!(
            restored
                .reducer()
                .events_after(restored.reducer().archived_through_revision(), 64)?
                .len()
                <= 64
        );
        assert!(provider.observation_maximum.load(Ordering::SeqCst) <= 64);
        provider.observation_maximum.store(0, Ordering::SeqCst);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        let archived = writer.reducer().archived_through_revision();
        assert_eq!(archived, writer.reducer().revision() - 1);
        assert_eq!(writer.reducer().events_after(archived, 2)?.len(), 1);
        assert!(matches!(
            writer.reducer().events_after(0, 2),
            Err(Error::Unsupported(_))
        ));
        let selection_operation = OperationId::from_bytes([98; 16]);
        let selection = crate::conversation::ModelContextSelection {
            conversation_revision: 10_000,
            message_ids: [1, 5_000, 10_000]
                .into_iter()
                .map(|sequence| message(sequence).id)
                .collect(),
            checkpoint: None,
        };
        writer
            .execute(Command {
                operation_id: selection_operation,
                idempotency_key: IdempotencyKey::new("indexed-context-selection")?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::SelectModelContext {
                    selection: selection.clone(),
                },
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        let pinned = reader.pin(0, None).await?;
        let mut future = message(10_001);
        future.kind = crate::conversation::MessageKind::Interaction;
        writer
            .execute(Command {
                operation_id: OperationId::from_bytes([99; 16]),
                idempotency_key: IdempotencyKey::new("message-after-pin")?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(future.clone()),
                },
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        assert!(matches!(
            writer
                .execute(
                    first_command.ok_or_else(|| Error::Invalid("missing first command".into()))?, #[cfg(feature = "filesystem")] None, &mut None)
                .await?,
            ApplyResult::Replayed { .. }
        ));
        let first_path = format!(
            "harness/v2/conversation-messages/conversations/conversation-1/{}",
            message(1).id
        );
        let records = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), &first_path)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut forged: Value = crate::executor::decode_json(
            &records
                .first()
                .ok_or_else(|| Error::Storage("message index missing".into()))?
                .value,
        )?;
        let forged_id = uuid::Uuid::from_u128(50_000);
        forged["message_id"] = json!(forged_id);
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), format!(
                "harness/v2/conversation-messages/conversations/conversation-1/{forged_id}"
            ))?
            .append(crate::contract::canonical_json_bytes(&forged)?)
            .await?;
        let mut forged_sequence: Value = crate::executor::decode_json(
            &records
                .first()
                .ok_or_else(|| Error::Storage("message index missing".into()))?
                .value,
        )?;
        forged_sequence["sequence"] = json!(20_001);
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), "harness/v2/conversation-sequences/conversations/conversation-1/20001")?
            .append(crate::contract::canonical_json_bytes(&forged_sequence)?)
            .await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader.latest_conversation_message(65_536).await?,
            Some(future.clone())
        );
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader.latest_conversation_message(0).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        assert!(matches!(
            reader.latest_conversation_message(1).await,
            Err(Error::Invalid(_))
        ));
        let range_limits = HistoryReadLimits {
            maximum_events: 3,
            maximum_bytes: 65_536,
        };
        provider.observation_reads.store(0, Ordering::SeqCst);
        let selected = reader
            .selected_conversation(&pinned, &selection, range_limits)
            .await?;
        assert_eq!(
            selected
                .messages()
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            selection.message_ids
        );
        assert!(selected.agent.is_none());
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .selected_conversation(
                    &pinned,
                    &selection,
                    HistoryReadLimits {
                        maximum_events: 2,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader
                .conversation_range(&pinned, 9_997, 10_000, range_limits, &mut None)
                .await?,
            [9_998, 9_999, 10_000]
                .into_iter()
                .map(message)
                .collect::<Vec<_>>()
        );
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(
                    &pinned,
                    9_997,
                    10_000,
                    HistoryReadLimits {
                        maximum_events: 2,
                        ..range_limits
                    }
                , &mut None)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        assert!(matches!(
            reader
                .conversation_range(&pinned, 10_000, 10_001, range_limits, &mut None)
                .await,
            Err(Error::Storage(_))
        ));
        let mut two_record_bytes = 0u64;
        for sequence in [9_998u64, 9_999] {
            let index = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), format!(
                    "harness/v2/conversation-sequences/conversations/conversation-1/{sequence}"
                ))?
                .read(0, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let event = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
                .read(sequence, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            two_record_bytes += index
                .first()
                .ok_or_else(|| Error::Storage("sequence index missing".into()))?
                .value
                .len() as u64
                + event
                    .first()
                    .ok_or_else(|| Error::Storage("message event missing".into()))?
                    .value
                    .len() as u64;
        }
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(
                    &pinned,
                    9_997,
                    10_000,
                    HistoryReadLimits {
                        maximum_bytes: two_record_bytes,
                        ..range_limits
                    }
                , &mut None)
                .await,
            Err(Error::Invalid(_))
        ));
        let last_selection = crate::conversation::ModelContextSelection {
            conversation_revision: 10_000,
            message_ids: [9_998, 9_999, 10_000]
                .into_iter()
                .map(|sequence| message(sequence).id)
                .collect(),
            checkpoint: None,
        };
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .selected_conversation(
                    &pinned,
                    &last_selection,
                    HistoryReadLimits {
                        maximum_bytes: two_record_bytes,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(&pinned, 19_999, 20_000, range_limits, &mut None)
                .await,
            Err(Error::Storage(_))
        ));
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(&pinned, 20_000, 20_001, range_limits, &mut None)
                .await,
            Err(Error::Storage(_))
        ));
        let mixed_cut = reader.pin(0, None).await?;
        assert_eq!(
            reader
                .conversation_range(&mixed_cut, 10_000, 10_001, range_limits, &mut None)
                .await?,
            vec![future.clone()]
        );
        assert!(matches!(
            reader
                .conversation_range(
                    &mixed_cut,
                    10_000,
                    10_001,
                    HistoryReadLimits {
                        maximum_events: 0,
                        ..range_limits
                    }
                , &mut None)
                .await,
            Err(Error::Invalid(_))
        ));
        for sequence in [1, 5_000, 10_000] {
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert_eq!(
                reader
                    .conversation_message(&pinned, message(sequence).id, 65_536)
                    .await?,
                Some(message(sequence))
            );
        }
        assert!(
            reader
                .conversation_message(&pinned, future.id, 65_536)
                .await?
                .is_none()
        );
        assert!(
            reader
                .conversation_message(&empty, message(1).id, 65_536)
                .await?
                .is_none()
        );
        let current = reader.pin(0, None).await?;
        assert_eq!(
            reader
                .conversation_message(&current, future.id, 65_536)
                .await?,
            Some(future)
        );
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader.conversation_message(&pinned, message(1).id, 1).await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            reader
                .conversation_message(&pinned, forged_id, 65_536)
                .await,
            Err(Error::Storage(_))
        ));
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::ExecutionJournal as _;
            let filesystem_provider =
                crate::resources::ProviderRef::new("cold-selected-context".into(), "filesystem".into(), "2".into(), &mut None)?;
            let host = Arc::new(crate::filesystem::FilesystemHost::new(
                acyclic_fs::Fs::memory(),
                filesystem_provider.clone(),
            )?);
            let private = crate::conversation::VolumeRef::new(
                filesystem_provider,
                "journal",
                crate::conversation::VolumeClass::AgentPrivate,
                crate::conversation::VolumeOwner::Agent(agent),
            )?;
            host.create_volume(&private).await?;
            let journal_scope = issuer().issue("cold-selection-journal", &Capabilities::new([
                    private.capability(crate::conversation::VolumeOperation::Read)?,
                    private.capability(crate::conversation::VolumeOperation::Write)?,
                ]), Some(agent), None, &mut None).expect("scope proof");
            let journal = crate::filesystem::FilesystemExecutionJournal::new(
                client.clone(),
                host,
                &private,
                &issuer().verifier(&mut None).expect("verifier copy"),
                &schemas(),
                &journal_scope,
                65_536, None,
            )?
            .with_input_verifier(Arc::new(TestContent));
            let sparse = crate::conversation::ConversationState::from_wire(crate::conversation::ConversationStateWire { agent: None, messages: [1, 5_000, 10_000].into_iter().map(message).collect(), archived_prefix: None }, acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).map(|(state, _)| state)?;
            assert_eq!(sparse.messages().len(), 3);
            assert!(sparse.agent.is_none());
            let selected = crate::projection::select_model_context(
                &sparse,
                selection.conversation_revision,
                selection.clone(),
                &TestContent,
                3,
                8,
                65_536,
                8,
                None,
            )
            .await?;
            provider.observation_reads.store(0, Ordering::SeqCst);
            journal
                .verify_selected_context(selection_operation, &selected)
                .await?;
            let mut altered = selected.clone();
            altered.messages.pop();
            assert!(matches!(
                journal
                    .verify_selected_context(selection_operation, &altered)
                    .await,
                Err(Error::Conflict(_))
            ));
            let mut wrong_selection = selected;
            wrong_selection.selection.message_ids.pop();
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert!(matches!(
                journal
                    .verify_selected_context(selection_operation, &wrong_selection)
                    .await,
                Err(Error::Conflict(_))
            ));
        }
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_resident_suffix_preserves_cold_retries_and_recovery() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?
                .with_resident_event_limit(2)?,
        );
        let original = command(1)?;
        let first = writer.execute(original.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
        for identity in 2..=5 {
            let mut next = command(identity)?;
            next.expected_revision = writer.reducer().revision();
            writer.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        assert_eq!(writer.reducer().archived_through_revision(), 3);
        assert!(matches!(
            writer.reducer().events_after(0, 10),
            Err(Error::Unsupported(_))
        ));
        assert_eq!(writer.reducer().events_after(3, 10)?.len(), 2);
        let snapshot = writer.reducer().snapshot(usize::MAX, &mut None)?.0;
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(writer.read_event_at(1, &mut None).await?.0.revision, 1);
        let mut reused = original.clone();
        reused.expected_revision = writer.reducer().revision();
        assert!(matches!(
            writer.reducer().plan(&reused),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            writer.execute(original.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?,
            ApplyResult::Replayed { .. }
        ));
        let mut conflicting = original.clone();
        conflicting.causal_parent = Some(EventReference {
            authority: authority(),
            revision: 1,
        });
        assert!(matches!(
            writer.execute(conflicting, #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Conflict(_))
        ));
        let mut restored = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(), Some(snapshot), crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?
            .with_resident_event_limit(2)?,
        );
        assert_eq!(restored.reducer().archived_through_revision(), 3);
        let ApplyResult::Replayed { event } = restored.execute(original, #[cfg(feature = "filesystem")] None, &mut None).await? else {
            return Err(Error::Invalid("cold retry was not replayed".into()));
        };
        let ApplyResult::Applied {
            event: original_event,
        } = first
        else {
            return Err(Error::Invalid("fixture first event was not applied".into()));
        };
        assert_eq!(event, original_event);
        assert_eq!(restored.reducer().revision(), 5);
        let mut next = command(6)?;
        next.expected_revision = 5;
        restored.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
        assert_eq!(restored.reducer().archived_through_revision(), 4);
        assert_eq!(restored.reducer().snapshot(usize::MAX, &mut None)?.0.events.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn cold_history_reader_bounds_ten_thousand_events_without_reducer_restore() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        for identity in 1_u64..=10_000 {
            let mut next = command(1)?;
            let mut bytes = [0; 16];
            bytes
                .get_mut(..8)
                .ok_or_else(|| Error::Invalid("fixture identity missing".into()))?
                .copy_from_slice(&identity.to_le_bytes());
            next.operation_id = OperationId::from_bytes(bytes);
            next.idempotency_key = IdempotencyKey::new(format!("history-{identity}"))?;
            next.expected_revision = writer.reducer().revision();
            writer.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        let last = writer.read_event_at(10_000, &mut None).await?.0;
        let mut later = command(2)?;
        later.expected_revision = writer.reducer().revision();
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let reader = HistoryReader::new(&client, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?;
        let cursor = reader.pin(9_997, None).await?;
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        writer.execute(later, #[cfg(feature = "filesystem")] None, &mut None).await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let limits = HistoryReadLimits {
            maximum_events: 2,
            maximum_bytes: 131_072,
        };
        let first = reader.read_page(&cursor, limits).await?;
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.cursor.after_revision, 9_999);
        let second = reader.read_page(&first.cursor, limits).await?;
        assert_eq!(second.events, vec![last.clone()]);
        assert_eq!(second.cursor.after_revision, 10_000);
        assert!(
            reader
                .read_page(&second.cursor, limits)
                .await?
                .events
                .is_empty()
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert_eq!(cursor.after_revision, 9_997);
        #[cfg(feature = "filesystem")]
        assert_measured_history_receipts(&provider, &reader, &cursor, limits, &first).await?;
        let encoded =
            serde_json::to_vec(&first.cursor).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            serde_json::from_slice::<HistoryCursor>(&encoded)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            first.cursor
        );
        for fault in [1, 2, 3, 4] {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(reader.read_page(&cursor, limits).await.is_err());
            assert_eq!(reader.read_page(&cursor, limits).await?, first);
        }
        assert!(
            reader
                .read_page(
                    &cursor,
                    HistoryReadLimits {
                        maximum_bytes: 1,
                        ..limits
                    }
                )
                .await
                .is_err()
        );
        assert!(
            reader
                .read_page(
                    &cursor,
                    HistoryReadLimits {
                        maximum_events: 0,
                        ..limits
                    }
                )
                .await
                .is_err()
        );
        let mut invalid = cursor.clone();
        invalid.through_revision = 10_002;
        assert!(reader.read_page(&invalid, limits).await.is_err());
        invalid = cursor;
        invalid.authority.id = "other".into();
        assert!(matches!(
            reader.read_page(&invalid, limits).await,
            Err(Error::Unauthorized(_))
        ));
        assert_cold_operation_lookup(&reader, last).await?;
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    async fn assert_measured_history_receipts(
        provider: &crate::test_stream::LostSessionAck<MemoryStream>,
        reader: &HistoryReader<crate::test_stream::LostSessionAck<MemoryStream>>,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        expected: &HistoryPage,
    ) -> Result<()> {
        use acyclic_fs::{CancellationToken, WorkBudget};
        use std::sync::atomic::Ordering;
        let bytes = acyclic_stream::MAX_RECORD_BYTES as u64 * 4;
        let budget = WorkBudget {
            authority_records_read: 8, authority_bytes_read: bytes,
            backend_read_operations: 8, source_bytes_read: bytes,
            bytes_copied: bytes, output_bytes: bytes,
            items_examined: 8, items_returned: 8,
            allocation_operations: 8, peak_allocation_bytes: bytes,
            ..WorkBudget::default()
        };
        let cancellation = CancellationToken::new();
        let page = reader.read_page_measured(cursor, limits, budget, &cancellation).await
            .map_err(|failure| failure.error)?;
        assert_eq!(&page.value, expected);
        for fault in 1..=6 {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(reader.read_page_measured(cursor,
                HistoryReadLimits { maximum_bytes: bytes, ..limits }, budget, &cancellation,
            ).await.is_err());
            assert_eq!(reader.read_page_measured(cursor, limits, budget, &cancellation).await
                .map_err(|failure| failure.error)?.value, *expected);
        }
        for narrow in [
            WorkBudget { backend_read_operations: 0, ..budget },
            WorkBudget { authority_bytes_read: 0, ..budget },
            WorkBudget { output_bytes: 0, ..budget },
            WorkBudget { peak_allocation_bytes: 0, ..budget },
        ] {
            assert!(reader.read_page_measured(cursor, limits, narrow, &cancellation).await.is_err());
        }
        let invalid = HistoryCursor {
            authority: Authority { id: "other".into(), ..cursor.authority.clone() }, ..cursor.clone()
        };
        assert!(matches!(reader.read_page_measured(&invalid, limits, budget, &cancellation).await,
            Err(acyclic_fs::OperationFailure { error: Error::Unauthorized(_), .. })));
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(reader.pin_measured(0, budget, &cancelled).await.is_err());
        assert!(reader.read_page_measured(cursor, limits, budget, &cancelled).await.is_err());
        assert!(reader.pin_measured(0, WorkBudget::UNBOUNDED, &cancellation).await.is_err());
        let terminal = HistoryCursor { after_revision: cursor.through_revision, ..cursor.clone() };
        assert!(reader.read_page_measured(&terminal, limits, budget, &cancellation).await
            .map_err(|failure| failure.error)?.value.events.is_empty());
        let during_tail = CancellationToken::new();
        *provider.history_cancel_after_tail.lock()
            .map_err(|_| Error::Storage("history cancellation fixture poisoned".into()))? =
            Some(during_tail.clone());
        assert!(reader.read_page_measured(cursor, limits, budget, &during_tail).await.is_err());
        Ok(())
    }

    async fn assert_cold_operation_lookup(
        reader: &HistoryReader<crate::test_stream::LostSessionAck<MemoryStream>>,
        expected: crate::core::Event,
    ) -> Result<()> {
        assert_eq!(reader.operation_event(expected.operation_id, 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await.map(|(event, _)| event)?, Some(expected));
        assert!(reader.operation_event(OperationId::new(), 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await.map(|(event, _)| event)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn operation_index_is_atomic_and_recovers_without_duplicate_publication() -> Result<()> {
        use std::sync::atomic::Ordering;
        for fault in 1..=5 {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let client = provider.clone();
            let mut aggregate = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
            );
            let submitted = command(1)?;
            provider
                .aggregate_commit_fault
                .store(fault, Ordering::SeqCst);
            let result = aggregate.execute(submitted.clone(), #[cfg(feature = "filesystem")] None, &mut None).await;
            if fault == 2 {
                assert!(matches!(result, Ok(ApplyResult::Applied { .. })));
            } else {
                assert!(
                    matches!(result, Err(Error::Indeterminate(id)) if id == submitted.operation_id)
                );
            }
            assert_eq!(provider.aggregate_commits.load(Ordering::SeqCst), 1);
            let indexed = aggregate.operation_event(submitted.operation_id).await?;
            assert_eq!(indexed.is_some(), fault != 1);
            let canonical = match acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?.read(0, 1).await {
                Ok(records) => records.try_collect::<Vec<_>>().await?,
                Err(StreamError::NotFound) => Vec::new(),
                Err(error) => return Err(error.into()),
            };
            assert_eq!(canonical.len(), usize::from(fault != 1));
            aggregate.execute(submitted.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
            let expected_commits = if fault == 1 { 2 } else { 1 };
            assert_eq!(
                provider.aggregate_commits.load(Ordering::SeqCst),
                expected_commits
            );
            assert_eq!(aggregate.reducer().revision(), 1);
            let mut later = command(2)?;
            later.expected_revision = 1;
            aggregate.execute(later, #[cfg(feature = "filesystem")] None, &mut None).await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            assert!(matches!(
                aggregate.execute(submitted.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?,
                ApplyResult::Replayed { .. }
            ));
            let mut changed = submitted.clone();
            changed.idempotency_key = IdempotencyKey::new("changed-retry")?;
            assert!(matches!(
                aggregate.execute(changed, #[cfg(feature = "filesystem")] None, &mut None).await,
                Err(Error::Conflict(_))
            ));
            for read_fault in [1, 3, 4] {
                provider
                    .history_read_fault
                    .store(read_fault, Ordering::SeqCst);
                assert!(aggregate.execute(submitted.clone(), #[cfg(feature = "filesystem")] None, &mut None).await.is_err());
            }
            assert!(matches!(
                aggregate.reconcile(&submitted).await?,
                Some(ApplyResult::Replayed { .. })
            ));
            assert_eq!(
                provider.aggregate_commits.load(Ordering::SeqCst),
                expected_commits + 1
            );
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            assert_eq!(aggregate.reducer().revision(), 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn operation_location_must_share_the_canonical_events_atomic_commit() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
        let ApplyResult::Applied { event } = reducer.apply(command(1)?)? else {
            return Err(Error::Invalid("fixture needs admitted event".into()));
        };
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
            .append_batch(
                vec![OwnedBytes::from(encode_event(&authority(), &event, &mut None)?)],
                Some(0),
                None,
            )
            .await?;
        let location = json!({"authority": authority(), "operation_id": event.operation_id,
            "revision": event.revision, "intent_digest": event.intent_digest});
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), operations::operation_path(&authority(), event.operation_id)?.as_str())?
            .append_batch(
                vec![OwnedBytes::from(crate::contract::canonical_json_bytes(
                    &location,
                )?)],
                Some(0),
                None,
            )
            .await?;
        let reader = HistoryReader::new(&client, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?;
        assert!(matches!(reader.operation_event(event.operation_id, 2 * acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).await.map(|(event, _)| event),
            Err(Error::Storage(message)) if message.contains("atomic canonical event")));
        Ok(())
    }

    #[tokio::test]
    async fn history_reader_rejects_unattested_events_and_wrong_authority() -> Result<()> {
        for wrong_authority in [false, true] {
            let client = Arc::new(MemoryStream::default());
            let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
            let ApplyResult::Applied { mut event } = reducer.apply(command(1)?)? else {
                return Err(Error::Invalid("fixture needs fresh event".into()));
            };
            let mut wire_authority = authority();
            if wrong_authority {
                wire_authority.id = "other".into();
            } else {
                event.intent_digest = [0; 32];
            }
            acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
                .append_batch(
                    vec![OwnedBytes::from(encode_event(&wire_authority, &event, &mut None)?)],
                    Some(0),
                    None,
                )
                .await?;
            let reader = HistoryReader::new(&client, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?;
            let cursor = reader.pin(0, None).await?;
            assert!(
                reader
                    .read_page(
                        &cursor,
                        HistoryReadLimits {
                            maximum_events: 1,
                            maximum_bytes: 65_536,
                        }
                    )
                    .await
                    .is_err()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn incremental_refresh_pins_tail_and_advances_only_bounded_pages() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let mut reader =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        for identity in 1..=5 {
            let mut next = command(identity)?;
            next.expected_revision = writer.reducer().revision();
            writer.execute(next, #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        let through = reader.tail_revision().await?;
        assert_eq!(through, 5);
        let mut later = command(6)?;
        later.expected_revision = writer.reducer().revision();
        writer.execute(later, #[cfg(feature = "filesystem")] None, &mut None).await?;
        assert!(reader.refresh_through(through, 0).await.is_err());
        assert!(!reader.refresh_through(through, 2).await?);
        assert_eq!(reader.reducer().revision(), 2);
        assert!(!reader.refresh_through(through, 2).await?);
        assert_eq!(reader.reducer().revision(), 4);
        assert!(reader.refresh_through(through, 2).await?);
        assert_eq!(reader.reducer().revision(), 5);
        assert!(reader.refresh_through(through, 2).await?);
        assert!(reader.refresh_through(4, 2).await.is_err());
        assert!(reader.refresh_through(7, 2).await.is_err());
        assert!(reader.refresh_through(6, 2).await?);
        assert_eq!(reader.reducer().revision(), 6);
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert_eq!(
            reader.reducer().events_after(0, 6)?,
            reopened.reducer().events_after(0, 6)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn extension_bytes_are_validated_before_ref_only_publication() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = provider;
        let mut unbound =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert!(matches!(
            unbound.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Unsupported(_))
        ));
        let mut wrong_schemas = SchemaRegistry::new();
        wrong_schemas.register(
            "example.message",
            1,
            json!({
                "type": "object", "properties": {"text": {"type": "integer"}},
                "required": ["text"]
            }),
            [9; 32],
            crate::core::ExtensionForkPolicy::Inherit, [], None,
        )?;
        let mut invalid = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), wrong_schemas, None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        assert!(matches!(
            invalid.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Invalid(_))
        ));
        let mut valid = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        valid.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let records = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(records.len(), 1);
        assert!(
            !records[0]
                .value
                .windows(b"hello".len())
                .any(|window| window == b"hello")
        );
        assert!(matches!(
            decode_event(&records[0].value, &mut None)?.0.1.payload,
            crate::core::EventPayload::Custom { record } if record.content == content()?
        ));
        Ok(())
    }

    #[tokio::test]
    async fn migrated_extension_state_is_validated_before_stream_publication() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let mut valid_schemas = schemas();
        valid_schemas.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset, [], None,
        )?;
        let mut migrations = NativeExtensionMigrations::default();
        migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|value| Ok(value.clone())),
        )?;
        assert!(matches!(
            migrations.register(
                "example.message",
                1,
                2,
                [10; 32],
                Arc::new(|value| Ok(value.clone()))
            ),
            Err(Error::Conflict(_))
        ));
        let migrations = Arc::new(migrations);
        let mut original = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                valid_schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?,
        );
        original.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let migration = Command {
            operation_id: OperationId::from_bytes([2; 16]),
            idempotency_key: IdempotencyKey::new("migrate-extension-state")?,
            expected_revision: 1,
            scope: issuer().issue("owner", &Capabilities::new([
                    "extension:migrate".to_owned(),
                    content()?
                        .volume()
                        .capability(crate::conversation::VolumeOperation::Write)?,
                ]), Some(crate::AgentId::from_bytes([8; 16])), None, &mut None).expect("scope proof"),
            causal_parent: None,
            action: Action::MigrateExtensionState {
                name: "example.message".into(),
                previous: crate::core::EventReference {
                    authority: authority(),
                    revision: 1,
                },
                to_version: 2,
                content: content()?,
            },
        };
        let mut invalid_schemas = schemas();
        invalid_schemas.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"integer"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset, [], None,
        )?;
        let mut invalid = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), invalid_schemas, None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?,
        )
        .with_extension_migrations(migrations.clone());
        assert!(matches!(
            invalid.execute(migration.clone(), #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(invalid.reducer().revision(), 1);
        let mut missing_executor = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                valid_schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?,
        );
        assert!(matches!(
            missing_executor.execute(migration.clone(), #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(missing_executor.reducer().revision(), 1);
        let mut wrong_migrations = NativeExtensionMigrations::default();
        wrong_migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|_| Ok(json!({"text":"substituted"}))),
        )?;
        let mut substituted = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                valid_schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?,
        )
        .with_extension_migrations(Arc::new(wrong_migrations));
        assert!(matches!(
            substituted.execute(migration.clone(), #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(substituted.reducer().revision(), 1);
        let mut valid = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                valid_schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?,
        )
        .with_extension_migrations(migrations);
        valid.execute(migration.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
        assert!(matches!(
            valid.execute(migration, #[cfg(feature = "filesystem")] None, &mut None).await?,
            ApplyResult::Replayed { .. }
        ));
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), valid_schemas, None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert_eq!(
            reopened
                .reducer()
                .extension_state("example.message")
                .map(|(_, state)| state.version),
            Some(2)
        );
        Ok(())
    }

    #[tokio::test]
    async fn extension_migration_retries_without_republishing_content() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let mut schemas = schemas();
        schemas.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset, [], None,
        )?;
        let mut migrations = NativeExtensionMigrations::default();
        migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|value| Ok(value.clone())),
        )?;
        let files = MigrationContentStore::default();
        files.insert(&content()?, EXTENSION_BYTES)?;
        let mut aggregate =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas, None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?
                .with_content_verifier(Arc::new(files.clone()))
                .with_extension_migrations(Arc::new(migrations));
        aggregate.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let request = ExtensionMigrationRequest {
            operation_id: OperationId::from_bytes([3; 16]),
            expected_revision: 1,
            previous: EventReference {
                authority: authority(),
                revision: 1,
            },
            name: "example.message".into(),
            to_version: 2,
        };
        let publisher = TestPublisher {
            volume: VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "extension-child",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
            )?,
            stages: std::sync::atomic::AtomicUsize::new(0),
            files: files.clone(),
        };
        let scope = issuer().issue("child-owner", &Capabilities::new([
                "extension:migrate".to_owned(),
                publisher
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write)?,
            ]), Some(crate::AgentId::from_bytes([9; 16])), None, &mut None).expect("scope proof");
        let unauthorized = issuer().issue("child-unauthorized", &Capabilities::new([publisher
                .volume
                .capability(crate::conversation::VolumeOperation::Write)?]), Some(crate::AgentId::from_bytes([9; 16])), None, &mut None).expect("scope proof");
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), unauthorized, &publisher)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let no_destination_write = issuer().issue("child-no-write", &Capabilities::new(["extension:migrate"]), Some(crate::AgentId::from_bytes([9; 16])), None, &mut None).expect("scope proof");
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), no_destination_write, &publisher)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let foreign_writer = issuer().issue("foreign-writer", &Capabilities::new([
                "extension:migrate".to_owned(),
                publisher
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write)?,
            ]), Some(crate::AgentId::from_bytes([8; 16])), None, &mut None).expect("scope proof");
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), foreign_writer, &publisher)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut stale = request.clone();
        stale.expected_revision = 2;
        assert!(matches!(
            aggregate
                .migrate_extension_state(stale, scope.clone(), &publisher)
                .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            publisher.stages.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), scope.clone(), &publisher)
                .await?,
            ApplyResult::Applied { .. }
        ));
        assert!(matches!(
            aggregate
                .migrate_extension_state(request, scope, &publisher)
                .await?,
            ApplyResult::Replayed { .. }
        ));
        assert_eq!(
            publisher.stages.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let (_, migrated) = aggregate
            .reducer()
            .extension_state("example.message")
            .ok_or_else(|| Error::NotFound("migrated extension state".into()))?;
        assert_eq!(migrated.content.volume(), &publisher.volume);
        assert_eq!(
            files.read(&migrated.content).await?.as_slice(),
            EXTENSION_BYTES
        );
        Ok(())
    }

    #[tokio::test]
    async fn configured_extension_cannot_publish_a_dangling_or_invalid_reference() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "configured-agent".into(),
        };
        let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
        let extension = ExtensionDependency {
            name: "example.configured".into(),
            version: 1,
        };
        let mut schemas = SchemaRegistry::new();
        schemas.register(
            extension.name.clone(),
            extension.version,
            json!({"type":"object"}),
            [9; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"}}})),
        )?;
        let bytes = br#"{"enabled":true}"#;
        let reference = interaction_file("extensions/configuration.json", bytes)?;
        let command = Command {
            operation_id: OperationId::from_bytes([32; 16]),
            idempotency_key: IdempotencyKey("configure-extension".into()),
            expected_revision: 0,
            scope: issuer.issue("owner", &Capabilities::new(["extension:configure"]), None, None, &mut None).expect("scope proof"),
            causal_parent: None,
            action: Action::ConfigureExtension {
                extension,
                content: reference.clone(),
            },
        };
        let mut aggregate = StreamAggregate::open(
            &client,
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            schemas.clone(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
        .await?;
        assert!(matches!(
            aggregate.execute(command.clone(), #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Unsupported(_))
        ));
        let mut invalid = std::collections::BTreeMap::new();
        invalid.insert(
            reference.path().to_owned(),
            br#"{"enabled":"wrong"}"#.to_vec(),
        );
        let mut aggregate = aggregate.with_content_verifier(Arc::new(InteractionContent(invalid)));
        assert!(aggregate.execute(command.clone(), #[cfg(feature = "filesystem")] None, &mut None).await.is_err());
        assert_eq!(aggregate.reducer().revision(), 0);
        let mut valid = std::collections::BTreeMap::new();
        valid.insert(reference.path().to_owned(), bytes.to_vec());
        let mut aggregate = aggregate.with_content_verifier(Arc::new(InteractionContent(valid)));
        aggregate.execute(command, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let reopened =
            StreamAggregate::open(&client, authority, issuer.verifier(&mut None).expect("verifier copy"), schemas, None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert_eq!(reopened.reducer().revision(), 1);
        assert!(matches!(
            &reopened.reducer().events_after(0, 1)?[0].payload,
            crate::core::EventPayload::ExtensionConfigured { record, .. }
                if record.content == reference
        ));
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn admission_content_keeps_original_published_bytes_until_last_alias() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkBudget, WorkError};
        use crate::conversation::VolumeOperation;
        use crate::filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost};

        let agent = crate::AgentId::from_bytes([94; 16]);
        let provider = ProviderRef::new("admission-bytes".into(), "filesystem".into(), "2".into(), &mut None)?;
        let volume = VolumeRef::new(provider.clone(), "admission-bytes",
            VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::memory(), provider)?);
        host.create_volume(&volume).await?;
        let issuer = issuer();
        let scope = issuer.issue("content-owner", &Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
        let publisher = FilesystemContentPublisher::new(
            host.clone(), &volume, &issuer.verifier(&mut None).expect("verifier copy"), &scope, 65_536, None)?;
        let reader = Arc::new(FilesystemContentVerifier::new(
            host, &issuer.verifier(&mut None).expect("verifier copy"), &scope, 65_536, None)?);
        let request = Interaction::question("original published prompt", json!({"type": "string"}))?;
        let published = crate::contract::canonical_json_bytes(&request)?;
        let reference = publisher.stage_once(OperationId::from_bytes([95; 16]),
            "interactions/request.json", &published, "application/json", "request.json").await?;
        let ticket = InteractionTicket {
            id: uuid::Uuid::from_bytes([96; 16]), kind: InteractionKind::Question,
            request: reference.clone(), deadline_unix_ms: None, approval: None,
        };
        let budget = WorkBudget {
            authority_records_read: 4_096, authority_bytes_read: 16_777_216,
            backend_read_operations: 4_096, source_bytes_read: 16_777_216,
            bytes_copied: 16_777_216, bytes_encoded: 16_777_216, bytes_hashed: 16_777_216,
            output_bytes: 16_777_216, items_examined: 1_000_000, items_returned: 4_096,
            allocation_operations: 1_000_000, peak_allocation_bytes: 16_777_216,
            ..WorkBudget::default()
        };
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original content ledger");
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        let mut io = history::native_work_io(&native).expect("original content observer");
        let (bytes, claim) = StreamAggregate::<MemoryStream>::read_admission_content_with_work(
            reader.as_ref(), &reference, &mut Some(&mut io)).await?;
        assert_eq!(bytes.as_ref(), published.as_slice());
        assert_eq!(claim, 0, "native carrier owns its claim, not the observer");
        let alias = bytes.clone();
        let empty_alias = alias.slice(0..0);
        ticket.validate_request_bytes(&alias, &mut Some(&mut io))?;
        drop(bytes);
        drop(alias);
        assert!(empty_alias.is_empty());
        assert!(matches!(native.admit_retained(available),
            Err(NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }))), "even an empty final view keeps the original whole allocation guarded");
        drop(empty_alias);
        native.admit_retained(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;

        let client = Arc::new(MemoryStream::default());
        let aggregate = StreamAggregate::open(&client, authority(), issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?.with_content_verifier(reader.clone());
        aggregate.validate_task_admission_content_with_work(
            &Action::OpenInteraction { ticket: ticket.clone() }, &mut Some(&mut io)).await?;
        native.admit_retained(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let missing = FileRef::new(volume, "interactions/missing.json", reference.version(),
            FileDescriptor::from_bytes(br#"{"type":"question","value":"yes"}"#, "application/json")?,
            "missing.json")?;
        let error = aggregate.validate_interaction_part_with_work(
            &ticket, &missing, None, &mut Some(&mut io)).await
            .expect_err("real publisher has no second response file");
        assert!(native.receipt().source_bytes_read > 0);
        drop(error);
        native.admit_retained(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert_eq!(io.into_custody().bytes(), 0);

        let mut ordinary = history::HistoryWork::new(budget, &cancellation);
        let error = aggregate.validate_interaction_part_with_work(
            &ticket, &missing, None, &mut Some(&mut ordinary)).await
            .expect_err("ordinary second read preserves the missing-file denial");
        drop(error);
        history::HistoryIo::admit_owned(&ordinary, budget.peak_allocation_bytes)?;
        assert!(ordinary.receipt().source_bytes_read > 0);
        Ok(())
    }

    #[tokio::test]
    async fn interaction_request_and_answer_commit_only_after_exact_content_validation()
    -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let request =
            Interaction::question("secret prompt", json!({"type": "string", "minLength": 2}))?;
        let request_bytes =
            serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        let answer_bytes = br#"{"type":"question","value":"yes"}"#;
        let ticket = InteractionTicket {
            id: uuid::Uuid::from_bytes([3; 16]),
            kind: InteractionKind::Question,
            request: interaction_file("interactions/request.json", &request_bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        let answer = interaction_file("interactions/answer.json", answer_bytes)?;
        let mut bytes = std::collections::BTreeMap::new();
        bytes.insert(ticket.request.path().to_owned(), request_bytes.clone());
        bytes.insert(answer.path().to_owned(), answer_bytes.to_vec());
        let mut aggregate =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await?
                .with_content_verifier(Arc::new(InteractionContent(bytes)));
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([9; 16]),
                idempotency_key: IdempotencyKey("bind-interaction-owner".into()),
                expected_revision: 0,
                scope: issuer().issue("owner", &Capabilities::new(["conversation:bind"]), None, None, &mut None).expect("scope proof"),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: crate::AgentId::from_bytes([8; 16]),
                },
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        let open = Command {
            operation_id: OperationId::from_bytes([10; 16]),
            idempotency_key: IdempotencyKey("open-interaction".into()),
            expected_revision: 1,
            scope: issuer().issue("opener", &Capabilities::new(["interaction:open"]), None, None, &mut None).expect("scope proof"),
            causal_parent: None,
            action: Action::OpenInteraction {
                ticket: ticket.clone(),
            },
        };
        aggregate.execute(open, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version: 1,
            outcome: InteractionOutcome::Answered {
                answer: Box::new(answer),
            },
            detail: None,
        };
        let resolve = Command {
            operation_id: OperationId::from_bytes([11; 16]),
            idempotency_key: IdempotencyKey("resolve-interaction".into()),
            expected_revision: 2,
            scope: issuer().issue("responder", &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]), None, None, &mut None).expect("scope proof"),
            causal_parent: None,
            action: Action::ResolveInteraction {
                resolution: resolution.clone(),
            },
        };
        aggregate.execute(resolve, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let records = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
            .read(0, 3)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|record| {
            !record
                .value
                .windows(b"secret prompt".len())
                .any(|window| window == b"secret prompt")
        }));
        assert!(records.iter().all(|record| {
            !record
                .value
                .windows(answer_bytes.len())
                .any(|window| window == answer_bytes)
        }));
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        assert_eq!(
            reopened
                .reducer()
                .interaction(&ticket.id)
                .and_then(|(_, outcome)| outcome.as_ref()),
            Some(&resolution)
        );
        let future = InteractionTicket {
            id: uuid::Uuid::from_bytes([4; 16]),
            deadline_unix_ms: Some(u64::MAX),
            ..ticket
        };
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([12; 16]),
                idempotency_key: IdempotencyKey("open-future-interaction".into()),
                expected_revision: 3,
                scope: issuer().issue("opener", &Capabilities::new(["interaction:open"]), None, None, &mut None).expect("scope proof"),
                causal_parent: None,
                action: Action::OpenInteraction {
                    ticket: future.clone(),
                },
            }, #[cfg(feature = "filesystem")] None, &mut None)
            .await?;
        let premature = Command {
            operation_id: OperationId::from_bytes([13; 16]),
            idempotency_key: IdempotencyKey("premature-expiry".into()),
            expected_revision: 4,
            scope: issuer().issue("responder", &Capabilities::new(["interaction:resolve", future.responder_grant().as_str()]), None, None, &mut None).expect("scope proof"),
            causal_parent: None,
            action: Action::ResolveInteraction {
                resolution: InteractionResolution {
                    id: future.id,
                    expected_version: 1,
                    outcome: InteractionOutcome::Expired,
                    detail: None,
                },
            },
        };
        assert!(matches!(
            aggregate.execute(premature, #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(aggregate.reducer().revision(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn verifier_must_match_the_exact_aggregate_audience() -> Result<()> {
        let client = Arc::new(MemoryStream::default());
        let foreign = AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Task,
                id: authority().id,
            },
        );
        assert!(matches!(
            StreamAggregate::open(&client, authority(), foreign.verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stale_writer_does_not_advance_its_projection() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = provider;
        let mut first = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        let mut stale = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        first.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let rejected = command(2)?;
        assert!(matches!(
            stale.execute(rejected.clone(), #[cfg(feature = "filesystem")] None, &mut None).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(stale.reducer().revision(), 0);
        let mut reopened = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        assert!(matches!(
            reopened.reconcile(&rejected).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_observes_a_commit_without_redispatch() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = provider;
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        let mut uncertain = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        let submitted = command(1)?;
        writer.execute(submitted.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
        assert!(matches!(
            uncertain.reconcile(&submitted).await?,
            Some(ApplyResult::Applied { .. })
        ));
        assert!(matches!(
            uncertain.reconcile(&submitted).await?,
            Some(ApplyResult::Replayed { .. })
        ));
        assert_eq!(uncertain.reducer().revision(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn canonical_paths_bind_aggregate_authority() -> Result<()> {
        assert_eq!(
            authority().stream_path(&mut None)?,
            "harness/v2/conversations/conversation-1"
        );
        assert_ne!(
            authority().stream_path(&mut None)?,
            Authority {
                kind: AggregateKind::Task,
                id: "conversation-1".into(),
            }
            .stream_path(&mut None)?
        );
        assert!(
            Authority {
                kind: AggregateKind::Task,
                id: "../escape".into(),
            }
            .stream_path(&mut None)
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_reopens_with_full_stream_history() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = provider;
        let mut aggregate = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        aggregate.execute(command(1)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let mut second = command(2)?;
        second.expected_revision = 1;
        aggregate.execute(second, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let snapshot = aggregate.reducer().snapshot(usize::MAX, &mut None)?.0;
        let mut third = command(3)?;
        third.expected_revision = 2;
        aggregate.execute(third, #[cfg(feature = "filesystem")] None, &mut None).await?;
        let history = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?;
        assert_eq!(history.bounds().await?.tail, 3);
        assert_eq!(
            history
                .read(0, 3)
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            3
        );

        let reopened = StreamAggregate::open(
            &client,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(), Some(snapshot), crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
        .await?;
        assert_eq!(reopened.reducer().revision(), 3);
        Ok(())
    }

    fn projection_command(identity: u128, revision: u64) -> Result<Command> {
        let mut command = command(1)?;
        command.operation_id = OperationId::from_bytes(identity.to_le_bytes());
        command.idempotency_key = IdempotencyKey::new(format!("projection-{identity}"))?;
        command.expected_revision = revision;
        Ok(command)
    }

    #[tokio::test]
    async fn checkpoint_open_pins_canonical_cut_before_selecting_checkpoint() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        for cut in [64_u64, 128] {
            while writer.reducer().revision() < cut {
                let revision = writer.reducer().revision();
                writer
                    .execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None)
                    .await?;
            }
            provider
                .stale_projection_head_tail
                .store(cut / 64, Ordering::SeqCst);
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let cold =
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
            assert_eq!(cold.reducer().revision(), cut);
            assert_eq!(cold.reducer().snapshot(usize::MAX, &mut None)?.0.events.len(), 1);
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            provider
                .stale_projection_head_tail
                .store(0, Ordering::SeqCst);
            provider.forbid_writes.store(false, Ordering::SeqCst);
        }
        Ok(())
    }


    #[tokio::test]
    async fn default_cold_projection_enforces_bounds_and_recovers_failed_observations() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        for revision in 0..69 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
        }
        drop(writer);
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let defaults = default_projection_read_limits();
        let loaded =
            checkpoints::load_with_work(&client, &authority(), &issuer().verifier(&mut None).expect("verifier copy"), defaults, None, &mut None).await?;
        let suffix = HistoryReader::new(&client, &authority(), &(issuer().verifier(&mut None).expect("verifier copy")), &mut None)?
            .read_page(
                &HistoryCursor {
                    authority: authority(),
                    after_revision: 64,
                    through_revision: 69,
                },
                defaults,
            )
            .await?;
        let suffix_bytes = suffix.events.iter().try_fold(0_u64, |sum, event| {
            Ok::<_, Error>(sum + encode_event(&authority(), event, &mut None)?.len() as u64)
        })?;
        let exact = HistoryReadLimits {
            maximum_events: 6,
            maximum_bytes: loaded.consumed_bytes + suffix_bytes,
        };
        assert_eq!(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(), None, exact, crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?
            .reducer()
            .revision(),
            69
        );
        for limits in [
            HistoryReadLimits {
                maximum_events: 5,
                ..exact
            },
            HistoryReadLimits {
                maximum_bytes: exact.maximum_bytes - 1,
                ..exact
            },
            HistoryReadLimits {
                maximum_events: 0,
                ..exact
            },
        ] {
            assert!(
                StreamAggregate::open(
                    &client,
                    authority(),
                    issuer().verifier(&mut None).expect("verifier copy"),
                    schemas(), None, limits, crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await
                .is_err()
            );
        }
        for fault in [1, 2, 3, 4] {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                    .await
                    .is_err()
            );
            assert_eq!(
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                    .await?
                    .reducer()
                    .revision(),
                69
            );
        }
        assert!(
            StreamAggregate::open(
                &client,
                authority(),
                AuthorityIssuer::new("test", [8; 32], authority()).verifier(&mut None).expect("verifier copy"),
                schemas()
            , None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await
            .is_err()
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn projection_checkpoint_commit_loss_preserves_original_admission() -> Result<()> {
        use std::sync::atomic::Ordering;
        for fault in [1, 2, 3, 4, 5] {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let client = provider.clone();
            let mut writer = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
            );
            for revision in 0..63 {
                writer
                    .execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None)
                    .await?;
            }
            let original = projection_command(64, 63)?;
            provider
                .aggregate_commit_fault
                .store(fault, Ordering::SeqCst);
            let _uncertain = writer.execute(original.clone(), #[cfg(feature = "filesystem")] None, &mut None).await;
            drop(writer);
            let mut cold = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
            );
            cold.execute(original.clone(), #[cfg(feature = "filesystem")] None, &mut None).await?;
            assert_eq!(cold.reducer().revision(), 64);
            assert!(matches!(
                cold.execute(original, #[cfg(feature = "filesystem")] None, &mut None).await?,
                ApplyResult::Replayed { .. }
            ));
            assert_eq!(
                acyclic_stream::Stream::new(std::sync::Arc::clone(&client), "harness/v3/projection-checkpoints/conversations/conversation-1")?
                    .tail()
                    .await?,
                1
            );
        }
        Ok(())
    }

    fn forge_projection_payload(
        location: &mut Value,
        payload: &mut Vec<u8>,
        mode: u8,
    ) -> Result<()> {
        if mode == 1 {
            let mut forged: Value = crate::contract::json_from_slice(payload)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            forged["projection"]["lifecycle"] = json!("completed");
            let digest = crate::contract::canonical_json_digest(&(
                &forged["format_version"],
                &forged["authority"],
                &forged["revision"],
                &forged["events"],
                &forged["projection"],
            ))?;
            forged["state_digest"] = json!(digest);
            *payload = crate::contract::canonical_json_bytes(&forged)?;
            location["digest"] = json!(*blake3::hash(payload).as_bytes());
            location["bytes"] = json!(payload.len());
            location["chunks"] = json!([{
                "digest": *blake3::hash(payload).as_bytes(), "bytes": payload.len()
            }]);
        } else if mode == 4 {
            location["bytes"] = json!(u64::MAX);
        } else if mode == 5 {
            payload[0] ^= 1;
        } else if mode == 6 {
            location["chunks"] = json!([]);
        } else if mode == 7 {
            location["chunks"][0]["bytes"] = json!(0);
        } else if mode == 8 {
            location["chunks"][0]["bytes"] = json!(1);
        }
        Ok(())
    }

    async fn publish_copied_projection(
        client: &std::sync::Arc<MemoryStream>,
        canonical: &[acyclic_stream::Record],
        head_path: acyclic_stream::StreamPath,
        head_bytes: OwnedBytes,
        non_atomic: bool,
    ) -> Result<()> {
        use acyclic_stream::{CommitCondition, CommitMutation, CommitRequest};
        let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?;
        stream
            .append_batch(
                canonical[..63]
                    .iter()
                    .map(|record| record.value.clone())
                    .collect(),
                Some(0),
                None,
            )
            .await?;
        if non_atomic {
            stream.append(canonical[63].value.clone()).await?;
            acyclic_stream::Stream::new(std::sync::Arc::clone(&client), head_path.as_str())?
                .append(head_bytes)
                .await?;
        } else {
            let outcome = client.commit(CommitRequest {
                    conditions: vec![
                        CommitCondition::Tail {
                            path: stream.path().clone(),
                            expected: 63,
                        },
                        CommitCondition::Absent {
                            path: head_path.clone(),
                        },
                    ],
                    mutations: vec![
                        CommitMutation::Append {
                            path: stream.path().clone(),
                            records: vec![canonical[63].value.clone()],
                        },
                        CommitMutation::Append {
                            path: head_path,
                            records: vec![head_bytes],
                        },
                    ],
                    idempotency_key: StreamIdempotencyKey::new("copied-projection")?,
                })
                .await?;
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
        }
        Ok(())
    }

    async fn copied_projection_client(
        source: &std::sync::Arc<crate::test_stream::LostSessionAck<MemoryStream>>,
        mode: u8,
    ) -> Result<std::sync::Arc<MemoryStream>> {
        use acyclic_stream::StreamPath;
        let head_path =
            StreamPath::new("harness/v3/projection-checkpoints/conversations/conversation-1")?;
        let index = acyclic_stream::Stream::new(std::sync::Arc::clone(&source), head_path.as_str())?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut location: Value = crate::contract::json_from_slice(&index[0].value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let digest: [u8; 32] = serde_json::from_value(location["chunks"][0]["digest"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let chunk_path = format!(
            "harness/v3/projection-chunks/conversations/conversation-1/{}",
            blake3::Hash::from_bytes(digest).to_hex()
        );
        let chunk = acyclic_stream::Stream::new(std::sync::Arc::clone(&source), &chunk_path)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut payload = chunk[0].value.to_vec();
        forge_projection_payload(&mut location, &mut payload, mode)?;
        let canonical = acyclic_stream::Stream::new(std::sync::Arc::clone(&source), authority().stream_path(&mut None)?)?
            .read(0, 64)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let client = Arc::new(MemoryStream::default());
        if mode != 0 {
            let digest: [u8; 32] = serde_json::from_value(if mode == 6 {
                json!(*blake3::hash(&payload).as_bytes())
            } else {
                location["chunks"][0]["digest"].clone()
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
            acyclic_stream::Stream::new(std::sync::Arc::clone(&client), format!(
                    "harness/v3/projection-chunks/conversations/conversation-1/{}",
                    blake3::Hash::from_bytes(digest).to_hex()
                ))?
                .append(OwnedBytes::from(payload))
                .await?;
        }
        publish_copied_projection(
            &client,
            &canonical,
            head_path,
            OwnedBytes::from(crate::contract::canonical_json_bytes(&location)?),
            mode == 3,
        )
        .await?;
        Ok(client)
    }

    #[tokio::test]
    async fn cold_projection_rejects_forged_state_and_non_atomic_heads() -> Result<()> {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let source = provider;
        let mut writer = with_content(
            StreamAggregate::open(&source, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        for revision in 0..64 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
        }
        for mode in 0..=8 {
            let client = copied_projection_client(&source, mode).await?;
            let result =
                StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await;
            match mode {
                1 => assert!(matches!(result, Err(Error::Unauthorized(_)))),
                2 => assert_eq!(result?.reducer().revision(), 64),
                _ => assert!(result.is_err()),
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn supplied_projection_charges_snapshot_and_suffix_before_io() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(&mut None).expect("verifier copy"), schemas(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?,
        );
        for revision in 0..64 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
        }
        let snapshot = writer.reducer().snapshot(1, &mut None).map(|(snapshot, _)| snapshot)?;
        let snapshot_bytes = crate::contract::canonical_json_bytes(&snapshot)?.len() as u64;
        for revision in 64..69 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
        }
        let suffix = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), authority().stream_path(&mut None)?)?
            .read(64, 5)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let bytes = snapshot_bytes
            + suffix
                .iter()
                .map(|record| record.value.len() as u64)
                .sum::<u64>();
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let restored = StreamAggregate::open(
            &client,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(), Some(snapshot.copy(&mut None)?), HistoryReadLimits {
                maximum_events: 6,
                maximum_bytes: bytes,
            }, crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
        .await?;
        assert_eq!(restored.reducer().revision(), 69);
        for limits in [
            HistoryReadLimits {
                maximum_events: 5,
                maximum_bytes: bytes,
            },
            HistoryReadLimits {
                maximum_events: 6,
                maximum_bytes: bytes - 1,
            },
        ] {
            assert!(
                StreamAggregate::open(
                    &client,
                    authority(),
                    issuer().verifier(&mut None).expect("verifier copy"),
                    schemas(), Some(snapshot.copy(&mut None)?), limits, crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                .await
                .is_err()
            );
        }
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(), Some(snapshot), HistoryReadLimits {
                    maximum_events: 6,
                    maximum_bytes: snapshot_bytes - 1
                }, crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await
            .is_err()
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        Ok(())
    }
}
