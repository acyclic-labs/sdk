//! Cleanup links are projections of the same canonical operation custody.
//! They address a known public observation without scanning the account or
//! inflating its bounded operation record with another copy of every child.
//! The operation owns ordered compact native-record references; acknowledgment
//! removes one reference in the same commit as the public deletion projection.
use super::*;
use acyclic_stream::{CommitCondition, CommitMutation};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum CleanupProjection {
    Machine(MachineId),
    Checkpoint(CheckpointId),
}

pub(super) fn projection_path(
    operation: OperationId,
    resource: &NativeResource,
) -> Result<String, ProviderError> {
    let encoded = serde_json::to_vec(resource)
        .map_err(|_| storage(acyclic_stream::StreamError::InvalidArgument))?;
    Ok(format!(
        "ops/{operation}/created/{}",
        hex::encode(Sha256::digest(encoded))
    ))
}

fn matches_binding(
    native: Option<&NativeBinding>,
    record: &StoredOperation,
    resource: &NativeResource,
) -> Result<(), ProviderError> {
    if native.is_none_or(|native| {
        Some(&native.provider) != record.provider.as_ref() || native.handle != resource.handle()
    }) {
        return Err(ProviderError::Rejected("cleanup projection contradicts its native custody; recovery: reconcile the original operation and native binding before acknowledging deletion".into()));
    }
    Ok(())
}

impl OperationRegistry {
    /// Native deletion and public destruction cannot diverge after reopening.
    /// Release custody in the same commit as the known projection and its event.
    pub(super) async fn acknowledge_deletion(
        &self,
        id: OperationId,
        resource: &NativeResource,
        at: Option<u64>,
    ) -> Result<(), ProviderError> {
        loop {
            let suffix = format!("ops/{id}");
            let op = self.journal.load::<StoredOperation>(&suffix).await?;
            let mut conditions = vec![op.condition()];
            let mut record = op
                .value
                .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
            let Some(resource_index) = self.resource_id(&record, resource).await? else {
                return Ok(());
            };
            if !record.created.contains(&resource_index) {
                return Ok(());
            }
            let link = self
                .journal
                .load::<CleanupProjection>(&projection_path(id, resource)?)
                .await?;
            conditions.push(link.condition());
            let mut mutations = Vec::new();
            if let Some(projection) = link.value {
                let (condition, changes) = self
                    .deletion_projection(&record, resource, projection, at)
                    .await?;
                conditions.push(condition);
                mutations.extend(changes);
            }
            // Unknown metadata has no public observation to project. Its typed
            // native address still remains owned until this acknowledgment.
            record.created.retain(|owned| *owned != resource_index);
            mutations.push(self.journal.append(&suffix, &Some(record.clone()))?);
            mutations.push(self.journal.operation_log(&suffix, &Some(record))?);
            if self
                .journal
                .publish(conditions, mutations, op.path, op.tail + 1)
                .await?
            {
                return Ok(());
            }
        }
    }

    async fn deletion_projection(
        &self,
        record: &StoredOperation,
        resource: &NativeResource,
        projection: CleanupProjection,
        at: Option<u64>,
    ) -> Result<(CommitCondition, Vec<CommitMutation>), ProviderError> {
        match (resource, projection) {
            (NativeResource::Machine(_), CleanupProjection::Machine(id)) => {
                let suffix = id.to_string();
                let row = self.journal.load::<StoredMachine>(&suffix).await?;
                let condition = row.condition();
                let state = row.value.ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?;
                matches_binding(state.native.as_ref(), record, resource)?;
                let mut observation = state.observation.clone();
                observation.state = MachineState::Destroyed;
                observation.changed_at_unix_ms = at.unwrap_or(observation.changed_at_unix_ms).max(observation.changed_at_unix_ms);
                let at = observation.changed_at_unix_ms;
                let mut state = Some(state);
                let mut mutations = Vec::new();
                if let Some(event) = bindings::project(&mut state, &observation, at, self.limits.machine_events)? {
                    mutations.push(self.journal.append(&format!("{suffix}/events"), &event)?);
                }
                mutations.push(self.journal.append(&suffix, &state)?);
                Ok((condition, mutations))
            }
            (NativeResource::Checkpoint(_), CleanupProjection::Checkpoint(id)) => {
                let suffix = format!("checkpoints/{id}");
                let row = self.journal.load::<StoredCheckpoint>(&suffix).await?;
                let condition = row.condition();
                let mut state = row.value.ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?;
                matches_binding(state.native.as_ref(), record, resource)?;
                state.observation.forkable = false;
                Ok((condition, vec![self.journal.append(&suffix, &Some(state))?]))
            }
            _ => Err(ProviderError::Rejected("cleanup projection has the wrong native resource kind; recovery: repair the original canonical custody mapping before acknowledging deletion".into())),
        }
    }

    /// Once an accepted checkpoint has valid metadata, publish its public
    /// identity, native binding and cleanup link in one operation commit.
    pub async fn claim_checkpoint<D: NativeResources + ?Sized>(
        &self,
        driver: &D,
        operation: OperationId,
        observation: CheckpointObservation,
        native: NativeBinding,
    ) -> Result<(), ProviderError> {
        if native.provider != driver.name() || native.handle.is_empty() {
            return Err(ProviderError::Invalid("native checkpoint binding is invalid; recovery: repair the raw adapter's provider and address mapping".into()));
        }
        let resource = NativeResource::Checkpoint(native.handle.clone());
        let link_path = projection_path(operation, &resource)?;
        let projection = CleanupProjection::Checkpoint(observation.id);
        let pending = loop {
            let suffix = format!("ops/{operation}");
            let op = self.journal.load::<StoredOperation>(&suffix).await?;
            let checkpoint_path = format!("checkpoints/{}", observation.id);
            let checkpoint = self
                .journal
                .load::<StoredCheckpoint>(&checkpoint_path)
                .await?;
            let link = self.journal.load::<CleanupProjection>(&link_path).await?;
            let mut conditions = vec![op.condition(), checkpoint.condition(), link.condition()];
            let mut record = op
                .value
                .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
            if checkpoint.value.as_ref().is_some_and(|known| {
                known.native.as_ref().is_some_and(|known| known != &native)
                    || known.observation.source != observation.source
                    || known.observation.contract != observation.contract
                    || !known.observation.forkable
            }) || link
                .value
                .as_ref()
                .is_some_and(|known| known != &projection)
            {
                self.claim(driver, operation, &resource, false).await?;
                return Err(ProviderError::Conflict("checkpoint identity or native address has a different canonical binding; recovery: reconcile the original checkpoint before retrying".into()));
            }
            let (pending, mut changes) = self
                .ownership(&mut record, Some(driver.name()), &resource, false)
                .await?;
            conditions.append(&mut changes.conditions);
            let state = if let Some(mut known) = checkpoint.value {
                known.native = Some(native.clone());
                known
            } else {
                StoredCheckpoint {
                    observation: observation.clone(),
                    native: Some(native.clone()),
                }
            };
            let mut mutations = changes.mutations;
            mutations.extend([
                self.journal.append(&checkpoint_path, &Some(state))?,
                self.journal.append(&link_path, &projection)?,
                self.journal.append(&suffix, &Some(record.clone()))?,
                self.journal.operation_log(&suffix, &Some(record))?,
            ]);
            if self
                .journal
                .publish(conditions, mutations, op.path, op.tail + 1)
                .await?
            {
                break pending;
            }
        };
        if pending {
            return Ok(());
        }
        match driver.raw_destroy(&resource).await {
            Ok(at) => {
                self.acknowledge_deletion(operation, &resource, Some(at))
                    .await?
            }
            Err(ProviderError::NotFound(_)) => self.deleted(operation, &resource).await?,
            Err(error) => tracing::warn!(%operation, %error,
                "Machines late-checkpoint deletion failed; recovery: call MachinesProvider::cancel with the original operation identity again"),
        }
        Err(ProviderError::Cancelled)
    }
}
