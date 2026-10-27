//! G29: one operation owner over the caller-selected canonical StreamProvider.
//! Backends must never reconstruct an absent operation by redispatching it.
use crate::*;
pub use crate::{OperationSettlement, SettledMutation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
mod backend;
#[cfg(not(target_arch = "wasm32"))]
pub use backend::{Backend, BackendCore, RawUsage};
mod bindings;
mod custody;
mod journal;
mod limits;
mod pages;
pub use bindings::NativeBinding;
pub use limits::RegistryLimits;
pub use pages::{NativePage, collect_pages};
mod raw;
pub use raw::{ForkSlot, NativeResource, NativeResources, RawCreated, RawMachines, RawObservation};
#[cfg(not(target_arch = "wasm32"))]
mod checkpoint;
#[cfg(not(target_arch = "wasm32"))]
mod destruction;
mod fork;
#[cfg(not(target_arch = "wasm32"))]
mod lifecycle;
#[cfg(not(target_arch = "wasm32"))]
pub use fork::retry_refused;
pub use fork::{Dispatch, fork_rounds, fork_tree};
#[cfg(not(target_arch = "wasm32"))]
mod settle;
#[cfg(not(target_arch = "wasm32"))]
pub use settle::settle;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
use journal::{Journal, storage};

#[derive(Serialize, Deserialize)]
struct StoredMachine {
    observation: MachineObservation,
    event_sequence: u64,
    #[serde(default)]
    native: Option<NativeBinding>,
}

#[derive(Serialize, Deserialize)]
struct StoredCheckpoint {
    observation: CheckpointObservation,
    native: Option<NativeBinding>,
}

/// Native resource ownership and the admitted intent, retained across restarts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: OperationId,
    pub key: IdempotencyKey,
    pub intent_digest: [u8; 32],
    pub provider: Option<String>,
    pub created: Vec<NativeResource>,
    pub targets: Vec<String>,
    pub phase: OperationPhase,
    pub outcome: Option<MutationOutcome>,
}

impl OperationRecord {
    #[must_use]
    pub const fn observation(&self) -> OperationObservation {
        OperationObservation {
            id: self.id,
            phase: self.phase,
        }
    }
}

/// The point projection stays bounded even when a fork result spans many records.
#[derive(Clone, Serialize, Deserialize)]
struct StoredOperation {
    id: OperationId,
    key: IdempotencyKey,
    intent_digest: [u8; 32],
    #[serde(default)]
    provider: Option<String>,
    created: Vec<NativeResource>,
    targets: Vec<String>,
    phase: OperationPhase,
    outcome: Option<journal::Blob>,
}

/// Immutable terminal publication. Later cleanup acknowledgments may change the
/// custody point, but cannot change the operation's settled result or CommitId.
#[derive(Serialize, Deserialize)]
struct StoredSettlement {
    id: OperationId,
    key: IdempotencyKey,
    phase: OperationPhase,
    outcome: Option<journal::Blob>,
}

fn terminal(phase: OperationPhase) -> bool {
    matches!(
        phase,
        OperationPhase::Succeeded | OperationPhase::Failed | OperationPhase::Cancelled
    )
}

impl StoredOperation {
    const fn observation(&self) -> OperationObservation {
        OperationObservation {
            id: self.id,
            phase: self.phase,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Admission {
    Replay(Box<SettledMutation>),
    Fresh(OperationId),
}

/// Stable operation addressing preserves the original Daytona retry identity.
#[must_use]
pub fn operation_id(key: IdempotencyKey) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"acyclic-machines-daytona/operation");
    hash.update(key.as_bytes());
    OperationId::from_digest(hash.finalize().into())
}

/// A view of the canonical Streams authority; it contains no outcome cache.
#[derive(Clone)]
pub struct OperationRegistry {
    journal: Journal,
    limits: RegistryLimits,
}

impl std::fmt::Debug for OperationRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationRegistry")
            .field("root", &self.journal.root)
            .finish_non_exhaustive()
    }
}

impl OperationRegistry {
    /// The account belongs to the supplied authenticated Streams session.
    /// Passing a different account string conveys no additional authority.
    pub fn new(
        provider: Arc<dyn acyclic_stream::StreamProvider>,
        account: &str,
    ) -> Result<Self, ProviderError> {
        Self::with_limits(provider, account, RegistryLimits::default())
    }

    /// Set the account admission profile when composing its canonical registry.
    /// Reopen the same namespace with the same limits; this grants no authority.
    pub fn with_limits(
        provider: Arc<dyn acyclic_stream::StreamProvider>,
        account: &str,
        limits: RegistryLimits,
    ) -> Result<Self, ProviderError> {
        Ok(Self {
            journal: Journal::new(provider, account)?,
            limits,
        })
    }

    /// Explicit process-local composition for the deterministic simulator. The
    /// fixed namespace still passes through canonical path validation at every IO.
    pub(crate) fn simulated() -> Self {
        Self {
            journal: Journal {
                provider: Arc::new(acyclic_stream::MemoryStream::new(Default::default())),
                root: "machines/simulation".into(),
            },
            limits: RegistryLimits {
                operations: Some(4096),
                machine_events: Some(4096),
                machines: Some(1024),
            },
        }
    }

    pub async fn admit<T: Serialize + Sync>(
        &self,
        key: IdempotencyKey,
        intent: &T,
    ) -> Result<Admission, ProviderError> {
        self.admit_in(key, intent, None).await
    }

    async fn admit_in<T: Serialize + Sync>(
        &self,
        key: IdempotencyKey,
        intent: &T,
        provider: Option<&str>,
    ) -> Result<Admission, ProviderError> {
        let digest = intent_digest(intent)?;
        let id = operation_id(key);
        loop {
            let path = format!("ops/{id}");
            let row = self.journal.load::<StoredOperation>(&path).await?;
            let condition = row.condition();
            if let Some(record) = row.value {
                if let Some(provider) = provider {
                    bindings::check_driver(&record, provider)?;
                }
                if record.intent_digest != digest {
                    return Err(ProviderError::Conflict("Machines key is bound to another intent; recovery: inspect the original key or use a fresh key for a different intent".into()));
                }
                return match (record.phase, record.outcome) {
                    (OperationPhase::Succeeded, Some(_)) => {
                        Ok(Admission::Replay(Box::new(self.settled(key).await?)))
                    }
                    (OperationPhase::Failed, _) => Err(ProviderError::Failed),
                    (OperationPhase::Cancelled, _) => Err(ProviderError::Cancelled),
                    _ => Err(ProviderError::Indeterminate(key)),
                };
            }
            let record = Some(StoredOperation {
                id,
                key,
                intent_digest: digest,
                provider: provider.map(str::to_owned),
                created: Vec::new(),
                targets: Vec::new(),
                phase: OperationPhase::Pending,
                outcome: None,
            });
            let mut conditions = vec![condition];
            let mut mutations = vec![
                self.journal.append(&path, &record)?,
                self.journal.operation_log(&path, &record)?,
            ];
            if let Some(limit) = self.limits.operations {
                let quota = self.journal.load::<u64>("limits/operations").await?;
                conditions.push(quota.condition());
                let used = quota.value.unwrap_or(0);
                if used >= limit {
                    return Err(ProviderError::Rejected("Machines operation limit reached; recovery: inspect the configured operation admission profile before admitting more operations".into()));
                }
                mutations.push(self.journal.append("limits/operations", &(used + 1))?);
            }
            if self
                .journal
                .publish(conditions, mutations, row.path, row.tail + 1)
                .await?
            {
                return Ok(Admission::Fresh(id));
            }
        }
    }

    async fn change<R>(
        &self,
        id: OperationId,
        mut f: impl FnMut(&mut StoredOperation) -> R + acyclic_stream::ProviderTask,
    ) -> Result<R, ProviderError> {
        self.journal
            .update_with(
                &format!("ops/{id}"),
                |entry: &mut Option<StoredOperation>| {
                    let record = entry
                        .as_mut()
                        .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
                    let was_terminal = terminal(record.phase);
                    let result = f(record);
                    let mutations = if !was_terminal && terminal(record.phase) {
                        vec![self.journal.append(
                            &format!("ops/{id}/settlement"),
                            &StoredSettlement {
                                id: record.id,
                                key: record.key,
                                phase: record.phase,
                                outcome: record.outcome.clone(),
                            },
                        )?]
                    } else {
                        Vec::new()
                    };
                    Ok((result, mutations))
                },
            )
            .await
    }

    pub async fn bind_created(
        &self,
        id: OperationId,
        resource: &NativeResource,
    ) -> Result<bool, ProviderError> {
        self.change(id, |record| {
            if record.phase == OperationPhase::Succeeded {
                return false;
            }
            if !record.created.iter().any(|id| id == resource) {
                record.created.push(resource.clone());
            }
            record.phase == OperationPhase::Pending
        })
        .await
    }

    pub async fn bind_adopted(
        &self,
        id: OperationId,
        resource: &str,
    ) -> Result<bool, ProviderError> {
        self.change(id, |record| {
            if record.phase != OperationPhase::Pending {
                return false;
            }
            bindings::target(record, resource);
            true
        })
        .await
    }

    pub async fn bind_target(&self, id: OperationId, resource: &str) -> Result<(), ProviderError> {
        let pending = self.bind_adopted(id, resource).await?;
        if pending {
            Ok(())
        } else {
            Err(ProviderError::Cancelled)
        }
    }

    pub async fn complete(
        &self,
        id: OperationId,
        outcome: MutationOutcome,
    ) -> Result<(), ProviderError> {
        let current = self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?
            .value
            .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
        if current.phase != OperationPhase::Pending {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&outcome)
            .map_err(|_| storage(acyclic_stream::StreamError::InvalidArgument))?;
        let outcome = self.journal.store_outcome(id, &bytes).await?;
        self.change(id, |record| {
            if record.phase == OperationPhase::Pending {
                record.phase = OperationPhase::Succeeded;
                record.outcome = Some(outcome.clone());
                record.created.clear();
            }
        })
        .await
    }

    pub async fn fail(&self, id: OperationId, error: &ProviderError) -> Result<(), ProviderError> {
        let phase = match error {
            ProviderError::Indeterminate(_) | ProviderError::Unavailable => {
                OperationPhase::Indeterminate
            }
            ProviderError::Cancelled => OperationPhase::Cancelled,
            _ => OperationPhase::Failed,
        };
        self.change(id, |record| {
            if record.phase == OperationPhase::Pending {
                record.phase = phase;
            }
        })
        .await
    }

    async fn forget(&self, id: OperationId) -> Result<(), ProviderError> {
        loop {
            let path = format!("ops/{id}");
            let row = self.journal.load::<StoredOperation>(&path).await?;
            let condition = row.condition();
            let Some(record) = row.value else {
                return Ok(());
            };
            if record.phase != OperationPhase::Pending
                || !record.created.is_empty()
                || !record.targets.is_empty()
            {
                return Err(ProviderError::Indeterminate(record.key));
            }
            let mut conditions = vec![condition];
            let empty: Option<StoredOperation> = None;
            let mut mutations = vec![
                self.journal.append(&path, &empty)?,
                self.journal.operation_log(&path, &empty)?,
            ];
            if self.limits.operations.is_some() {
                let quota = self.journal.load::<u64>("limits/operations").await?;
                conditions.push(quota.condition());
                let used = quota.value.and_then(|used| used.checked_sub(1)).ok_or_else(||
                    ProviderError::Rejected("Machines operation admission count is inconsistent; recovery: reopen the original registry profile and inspect its canonical admission records".into()))?;
                mutations.push(self.journal.append("limits/operations", &used)?);
            }
            if self
                .journal
                .publish(conditions, mutations, row.path, row.tail + 1)
                .await?
            {
                return Ok(());
            }
        }
    }

    pub async fn record(&self, id: OperationId) -> Result<Option<OperationRecord>, ProviderError> {
        let Some(record) = self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?
            .value
        else {
            return Ok(None);
        };
        let outcome = match &record.outcome {
            Some(blob) => Some(self.journal.outcome(id, blob).await?),
            None => None,
        };
        Ok(Some(OperationRecord {
            id: record.id,
            key: record.key,
            intent_digest: record.intent_digest,
            provider: record.provider,
            created: record.created,
            targets: record.targets,
            phase: record.phase,
            outcome,
        }))
    }

    pub async fn record_for_key(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<OperationRecord>, ProviderError> {
        self.record(operation_id(key)).await
    }

    pub async fn inspect(
        &self,
        id: OperationId,
    ) -> Result<Option<OperationObservation>, ProviderError> {
        Ok(self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?
            .value
            .map(|record| record.observation()))
    }

    /// Cleanup ownership is retained until a successful delete is acknowledged.
    /// A process dying during cleanup therefore cannot lose an orphan list.
    pub async fn cancel(
        &self,
        id: OperationId,
    ) -> Result<Option<(OperationObservation, Vec<NativeResource>)>, ProviderError> {
        if self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?
            .value
            .is_none()
        {
            return Ok(None);
        }
        self.change(id, |record| {
            if matches!(
                record.phase,
                OperationPhase::Pending | OperationPhase::Indeterminate
            ) {
                record.phase = OperationPhase::Cancelled;
            }
            let created = if record.phase == OperationPhase::Succeeded {
                Vec::new()
            } else {
                record.created.clone()
            };
            Some((record.observation(), created))
        })
        .await
    }

    pub async fn created(&self, id: OperationId) -> Result<Vec<NativeResource>, ProviderError> {
        Ok(self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?
            .value
            .map(|record| record.created)
            .unwrap_or_default())
    }

    /// Release only this acknowledged deletion, preserving concurrent late creates.
    pub async fn deleted(
        &self,
        id: OperationId,
        resource: &NativeResource,
    ) -> Result<(), ProviderError> {
        self.acknowledge_deletion(id, resource, None).await
    }

    /// Delete newest descendants first. An unacknowledged native deletion remains
    /// owned, including when the process exits between the delete and its receipt.
    pub async fn cleanup(
        &self,
        driver: &(impl NativeResources + ?Sized),
        operation: OperationId,
    ) -> Result<(), ProviderError> {
        self.check_driver(operation, driver.name()).await?;
        for resource in self.created(operation).await?.into_iter().rev() {
            match driver.raw_destroy(&resource).await {
                Ok(at) => {
                    self.acknowledge_deletion(operation, &resource, Some(at))
                        .await?
                }
                Err(ProviderError::NotFound(_)) => self.deleted(operation, &resource).await?,
                Err(error) => tracing::warn!(%operation, resource = resource.handle(), %error,
                    "Machines native deletion failed; recovery: call MachinesProvider::cancel with this operation identity again"),
            }
        }
        Ok(())
    }

    /// The durable registry owns cancel and its cleanup; an adapter only deletes.
    pub async fn cancel_with(
        &self,
        driver: &(impl NativeResources + ?Sized),
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.check_driver(operation, driver.name()).await?;
        let (observation, _) = self
            .cancel(operation)
            .await?
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        self.cleanup(driver, operation).await?;
        Ok(observation)
    }

    /// Record a native result before the adapter proceeds. A cancelled operation
    /// cleans up its late create; failed deletion keeps ownership for later cancel.
    /// Adopted resources are targets and can never be reclaimed by this attempt.
    pub async fn claim(
        &self,
        driver: &(impl NativeResources + ?Sized),
        operation: OperationId,
        resource: &NativeResource,
        adopted: bool,
    ) -> Result<(), ProviderError> {
        let pending = self
            .change(operation, |record| {
                bindings::own(record, driver.name(), resource, adopted)
            })
            .await??;
        if pending {
            return Ok(());
        }
        match driver.raw_destroy(resource).await {
            Ok(at) => {
                self.acknowledge_deletion(operation, resource, Some(at))
                    .await?
            }
            Err(ProviderError::NotFound(_)) => self.deleted(operation, resource).await?,
            Err(error) => tracing::warn!(%operation, resource = resource.handle(), %error,
                "Machines late-create deletion failed; recovery: call MachinesProvider::cancel with this operation identity again"),
        }
        Err(ProviderError::Cancelled)
    }

    /// Read a terminal canonical publication, including explicit failed/cancelled state.
    pub async fn settlement(
        &self,
        key: IdempotencyKey,
    ) -> Result<OperationSettlement, ProviderError> {
        let id = operation_id(key);
        let row = self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?;
        let record = row
            .value
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))?;
        if !terminal(record.phase) {
            return Err(ProviderError::Indeterminate(key));
        }
        let row = self
            .journal
            .load::<StoredSettlement>(&format!("ops/{id}/settlement"))
            .await?;
        let record = row
            .value
            .ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?;
        if record.id != id || record.key != key || !terminal(record.phase) {
            return Err(storage(acyclic_stream::StreamError::InvalidResponse));
        }
        let settlement = row
            .anchor
            .ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?;
        let observation = OperationObservation {
            id: record.id,
            phase: record.phase,
        };
        let outcome = match record.outcome {
            Some(outcome) if record.phase == OperationPhase::Succeeded => {
                Some(self.journal.outcome(id, &outcome).await?)
            }
            None if record.phase != OperationPhase::Succeeded => None,
            _ => return Err(storage(acyclic_stream::StreamError::InvalidResponse)),
        };
        Ok(OperationSettlement {
            operation: observation,
            settlement,
            outcome,
        })
    }

    pub async fn settled(&self, key: IdempotencyKey) -> Result<SettledMutation, ProviderError> {
        self.settlement(key).await?.result(key)
    }

    /// Join through the durable follow. Lost observations never authorize dispatch.
    pub async fn join_settlement(
        &self,
        key: IdempotencyKey,
        operation: OperationId,
    ) -> Result<OperationSettlement, ProviderError> {
        use futures::TryStreamExt;
        if operation != operation_id(key) {
            return Err(ProviderError::Rejected("join operation does not match its admission key; recovery: retain the original key and operation returned by admission".into()));
        }
        let mut observations = self.watch(operation).await?;
        while let Some(observation) = observations.try_next().await? {
            if observation.phase != OperationPhase::Pending {
                return self.settlement(key).await;
            }
        }
        Err(ProviderError::Indeterminate(key))
    }

    pub async fn join(&self, key: IdempotencyKey) -> Result<SettledMutation, ProviderError> {
        self.join_settlement(key, operation_id(key))
            .await?
            .result(key)
    }

    /// Replay and live changes share the Streams follow cursor, with no poll timer.
    pub async fn watch(&self, id: OperationId) -> Result<OperationStream, ProviderError> {
        use futures::{StreamExt, TryStreamExt};
        let row = self
            .journal
            .load::<StoredOperation>(&format!("ops/{id}"))
            .await?;
        let current = row
            .value
            .ok_or_else(|| ProviderError::NotFound(id.to_string()))?
            .observation();
        let first = futures::stream::once(async move { Ok(current) });
        if current.phase != OperationPhase::Pending {
            return Ok(Box::pin(first));
        }
        let records = self
            .journal
            .provider
            .follow_checked(
                row.path,
                row.tail,
                row.anchor
                    .ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?,
                acyclic_stream::FollowFilter::default(),
            )
            .await
            .map_err(storage)?;
        let changes =
            futures::stream::try_unfold((records, current), |(mut records, previous)| async move {
                if previous.phase != OperationPhase::Pending {
                    return Ok(None);
                }
                while let Some(record) = records.try_next().await.map_err(storage)? {
                    let value: Option<StoredOperation> = serde_json::from_slice(&record.value)
                        .map_err(|_| storage(acyclic_stream::StreamError::InvalidResponse))?;
                    let current = value
                        .ok_or_else(|| ProviderError::NotFound(previous.id.to_string()))?
                        .observation();
                    if current != previous {
                        return Ok(Some((current, (records, current))));
                    }
                }
                Err(storage(acyclic_stream::StreamError::Unavailable))
            });
        Ok(Box::pin(first.chain(changes)))
    }

    /// The observation and its public state event have the same commit identity.
    pub async fn observe_machine(
        &self,
        observation: MachineObservation,
        at: u64,
    ) -> Result<(), ProviderError> {
        let suffix = observation.id.to_string();
        let event_path = self.journal.path(&format!("{suffix}/events"))?;
        self.journal
            .update_with(&suffix, |entry: &mut Option<StoredMachine>| {
                let mut mutations = Vec::new();
                if let Some(event) =
                    bindings::project(entry, &observation, at, self.limits.machine_events)?
                {
                    mutations.push(acyclic_stream::CommitMutation::Append {
                        path: event_path.clone(),
                        records: vec![
                            serde_json::to_vec(&event)
                                .map_err(|_| storage(acyclic_stream::StreamError::InvalidArgument))?
                                .into(),
                        ],
                    });
                }
                Ok(((), mutations))
            })
            .await
    }

    pub async fn machine(
        &self,
        id: MachineId,
    ) -> Result<Option<MachineObservation>, ProviderError> {
        Ok(self
            .journal
            .load::<StoredMachine>(&id.to_string())
            .await?
            .value
            .map(|entry| entry.observation))
    }

    pub async fn mark_destroyed(&self, id: MachineId, at: u64) -> Result<(), ProviderError> {
        if let Some(mut observation) = self.machine(id).await? {
            observation.state = MachineState::Destroyed;
            observation.changed_at_unix_ms = at;
            self.observe_machine(observation, at).await?;
        }
        Ok(())
    }

    pub async fn machines(&self) -> Result<Vec<MachineObservation>, ProviderError> {
        let mut values = Vec::new();
        let mut after = None;
        let mut hierarchy_version = None;
        loop {
            let page = self
                .journal
                .provider
                .children_page(acyclic_stream::ChildrenPageRequest {
                    parent: Some(self.journal.path("")?),
                    after,
                    hierarchy_version,
                    tail_stream: None,
                    limit: acyclic_stream::MAX_ITEMS as u32,
                })
                .await
                .map_err(storage)?;
            for child in page.children {
                if let Some(name) = child.path.as_str().rsplit('/').next()
                    && let Ok(id) = MachineId::parse(name)
                    && let Some(machine) = self.machine(id).await?
                {
                    values.push(machine);
                }
            }
            hierarchy_version = Some(page.hierarchy_version);
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
        values.sort_by_key(|value| value.id);
        Ok(values)
    }

    pub async fn events(
        &self,
        id: MachineId,
        after: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<MachineEvent>, Option<u64>), ProviderError> {
        use futures::TryStreamExt;
        if limit == 0 || limit > acyclic_stream::MAX_ITEMS {
            return Err(ProviderError::Invalid(
                "Machines event page admission failed; recovery: request 1..1024 events".into(),
            ));
        }
        let path = self.journal.path(&format!("{id}/events"))?;
        let tail = match self.journal.provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(acyclic_stream::StreamError::NotFound) => return Ok((Vec::new(), None)),
            Err(error) => return Err(storage(error)),
        };
        let mut records = match self
            .journal
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from: after.unwrap_or(0),
                limit: limit as u32,
            })
            .await
        {
            Ok(records) => records,
            Err(acyclic_stream::StreamError::NotFound) => return Ok((Vec::new(), None)),
            Err(error) => return Err(storage(error)),
        };
        let mut events: Vec<MachineEvent> = Vec::new();
        while let Some(record) = records.try_next().await.map_err(storage)? {
            events.push(
                serde_json::from_slice(&record.value)
                    .map_err(|_| storage(acyclic_stream::StreamError::InvalidResponse))?,
            );
        }
        let next = events
            .last()
            .filter(|event| event.sequence < tail)
            .map(|event| event.sequence);
        Ok((events, next))
    }

    pub async fn remember_checkpoint(
        &self,
        observation: CheckpointObservation,
    ) -> Result<(), ProviderError> {
        self.remember_bound_checkpoint(observation, None).await
    }

    pub async fn checkpoint(
        &self,
        id: CheckpointId,
    ) -> Result<Option<CheckpointObservation>, ProviderError> {
        Ok(self
            .journal
            .load::<StoredCheckpoint>(&format!("checkpoints/{id}"))
            .await?
            .value
            .map(|entry| entry.observation))
    }

    pub async fn mark_checkpoint_destroyed(&self, id: CheckpointId) -> Result<(), ProviderError> {
        self.journal
            .update(
                &format!("checkpoints/{id}"),
                |entry: &mut Option<StoredCheckpoint>| {
                    if let Some(entry) = entry {
                        entry.observation.forkable = false;
                    }
                    Ok(())
                },
            )
            .await
    }

    /// Shared dispatch ownership; adapters supply one native action and cleanup.
    /// Returns the exact terminal publication on first execution and replay.
    pub async fn apply<T, F, R>(
        &self,
        key: IdempotencyKey,
        intent: &T,
        action: F,
        rollback: R,
    ) -> Result<SettledMutation, ProviderError>
    where
        T: Serialize + Sync,
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
        R: AsyncFnOnce(OperationId) -> Result<(), ProviderError>,
    {
        self.apply_in(None, key, intent, action, rollback).await
    }

    /// The driver identity is part of canonical admission, before native IO.
    /// A different composed backend cannot clean up or reinterpret these handles.
    pub async fn apply_with<D, T, F>(
        &self,
        driver: &D,
        key: IdempotencyKey,
        intent: &T,
        action: F,
    ) -> Result<SettledMutation, ProviderError>
    where
        D: NativeResources + ?Sized,
        T: Serialize + Sync,
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
    {
        self.apply_in(
            Some(driver.name()),
            key,
            intent,
            action,
            async |operation| self.cleanup(driver, operation).await,
        )
        .await
    }

    async fn apply_in<T, F, R>(
        &self,
        provider: Option<&str>,
        key: IdempotencyKey,
        intent: &T,
        action: F,
        rollback: R,
    ) -> Result<SettledMutation, ProviderError>
    where
        T: Serialize + Sync,
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
        R: AsyncFnOnce(OperationId) -> Result<(), ProviderError>,
    {
        let operation = match self.admit_in(key, intent, provider).await? {
            Admission::Replay(outcome) => return Ok(*outcome),
            Admission::Fresh(operation) => operation,
        };
        match action(operation).await {
            Ok(outcome) => {
                self.complete(operation, outcome).await?;
                self.settled(key).await
            }
            Err(error) => {
                let touched = self
                    .journal
                    .load::<StoredOperation>(&format!("ops/{operation}"))
                    .await?
                    .value
                    .is_some_and(|record| !record.created.is_empty() || !record.targets.is_empty());
                let before_dispatch = matches!(
                    error,
                    ProviderError::Invalid(_)
                        | ProviderError::Unsupported(_)
                        | ProviderError::NotFound(_)
                        | ProviderError::Conflict(_)
                        | ProviderError::Rejected(_)
                );
                if !touched && before_dispatch {
                    self.forget(operation).await?;
                } else {
                    self.fail(operation, &error).await?;
                    if !matches!(
                        error,
                        ProviderError::Indeterminate(_)
                            | ProviderError::Unavailable
                            | ProviderError::Cancelled
                    ) {
                        rollback(operation).await?;
                    }
                }
                Err(error)
            }
        }
    }
}
