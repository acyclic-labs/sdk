//! Native addresses live in bounded canonical records, not inline in the operation point.
use super::*;
use acyclic_stream::{CommitCondition, CommitMutation, ReadRequest};
use futures::TryStreamExt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Resource {
    Created(NativeResource),
    Target(String),
}

#[derive(Default)]
pub(super) struct ResourceChanges {
    pub conditions: Vec<CommitCondition>,
    pub mutations: Vec<CommitMutation>,
}

impl OperationRegistry {
    async fn resource_index(
        &self,
        operation: OperationId,
        value: &Resource,
    ) -> Result<(String, journal::Row<u32>), ProviderError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|_| storage(acyclic_stream::StreamError::InvalidArgument))?;
        let suffix = format!(
            "ops/{operation}/native/{}",
            hex::encode(Sha256::digest(bytes))
        );
        let row = self.journal.load(&suffix).await?;
        Ok((suffix, row))
    }

    async fn resource_records(
        &self,
        record: &StoredOperation,
        from: u64,
        limit: u32,
    ) -> Result<Vec<Resource>, ProviderError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let path = self.journal.path(&format!("ops/{}/native", record.id))?;
        let mut values = Vec::new();
        while values.len() < limit as usize {
            let count = (limit as usize - values.len()).min(acyclic_stream::MAX_ITEMS) as u32;
            let start = from + values.len() as u64;
            let end = start + u64::from(count);
            let mut records = self
                .journal
                .provider
                .read(ReadRequest {
                    path: path.clone(),
                    from: start,
                    limit: count,
                })
                .await
                .map_err(storage)?;
            while let Some(row) = records.try_next().await.map_err(storage)? {
                if row.sequence != from + values.len() as u64 || row.sequence >= end {
                    return Err(storage(acyclic_stream::StreamError::InvalidResponse));
                }
                values.push(
                    serde_json::from_slice(&row.value)
                        .map_err(|_| storage(acyclic_stream::StreamError::InvalidResponse))?,
                );
            }
            if from + values.len() as u64 != end {
                return Err(storage(acyclic_stream::StreamError::InvalidResponse));
            }
        }
        Ok(values)
    }

    /// Intern one address in the same conditional commit as its operation custody.
    /// The hash path is a projection; its ledger record supplies the exact typed address.
    async fn intern_resource(
        &self,
        record: &mut StoredOperation,
        value: Resource,
        changes: &mut ResourceChanges,
    ) -> Result<u32, ProviderError> {
        let (index_path, row) = self.resource_index(record.id, &value).await?;
        if let Some(index) = row.value {
            // An index may have appeared after the operation read. Its immutable
            // value can be checked now; the caller's operation CAS then conflicts
            // and recomputes ownership without repeating native dispatch.
            if self.resource_records(record, u64::from(index), 1).await? != [value] {
                return Err(storage(acyclic_stream::StreamError::InvalidResponse));
            }
            return Ok(index);
        }
        let index = record.native_resources;
        record.native_resources = index.checked_add(1).ok_or_else(|| ProviderError::Rejected("native custody sequence exhausted; recovery: cancel this operation before creating a new one".into()))?;
        let suffix = format!("ops/{}/native", record.id);
        changes.conditions.push(row.condition());
        changes.conditions.push(CommitCondition::Tail {
            path: self.journal.path(&suffix)?,
            expected: u64::from(index),
        });
        changes
            .mutations
            .push(self.journal.append(&suffix, &value)?);
        changes
            .mutations
            .push(self.journal.append(&index_path, &index)?);
        Ok(index)
    }

    pub(super) async fn resource_id(
        &self,
        record: &StoredOperation,
        value: &NativeResource,
    ) -> Result<Option<u32>, ProviderError> {
        let resource = Resource::Created(value.clone());
        let (_, row) = self.resource_index(record.id, &resource).await?;
        match row.value {
            Some(index) => {
                if self.resource_records(record, u64::from(index), 1).await? != [resource] {
                    return Err(storage(acyclic_stream::StreamError::InvalidResponse));
                }
                Ok(Some(index))
            }
            None => Ok(None),
        }
    }

    async fn target_resource(
        &self,
        record: &mut StoredOperation,
        handle: &str,
        changes: &mut ResourceChanges,
    ) -> Result<(), ProviderError> {
        for value in [
            NativeResource::Machine(handle.into()),
            NativeResource::Checkpoint(handle.into()),
        ] {
            if self
                .resource_id(record, &value)
                .await?
                .is_some_and(|index| record.created.contains(&index))
            {
                return Ok(());
            }
        }
        let index = self
            .intern_resource(record, Resource::Target(handle.into()), changes)
            .await?;
        if !record.targets.contains(&index) {
            record.targets.push(index);
        }
        Ok(())
    }

    /// One ownership decision shared by machine/checkpoint claim and metadata publication.
    pub(super) async fn ownership(
        &self,
        record: &mut StoredOperation,
        provider: Option<&str>,
        resource: &NativeResource,
        adopted: bool,
    ) -> Result<(bool, ResourceChanges), ProviderError> {
        if let Some(provider) = provider {
            bindings::check_driver(record, provider)?;
        }
        if record.phase == OperationPhase::Succeeded {
            return Err(ProviderError::Conflict("native result arrived after settlement; recovery: inspect the settled operation before reclaiming any resource".into()));
        }
        let pending = record.phase == OperationPhase::Pending;
        if adopted && !pending {
            return Err(ProviderError::Cancelled);
        }
        if let Some(provider) = provider {
            record.provider = Some(provider.into());
        }
        let mut changes = ResourceChanges::default();
        if adopted {
            self.target_resource(record, resource.handle(), &mut changes)
                .await?;
        } else {
            let index = self
                .intern_resource(record, Resource::Created(resource.clone()), &mut changes)
                .await?;
            if !record.created.contains(&index) {
                record.created.push(index);
            }
        }
        Ok((pending, changes))
    }

    pub(super) async fn bind_resource(
        &self,
        id: OperationId,
        provider: Option<&str>,
        resource: &NativeResource,
        adopted: bool,
    ) -> Result<bool, ProviderError> {
        loop {
            let suffix = format!("ops/{id}");
            let row = self.journal.load::<StoredOperation>(&suffix).await?;
            let condition = row.condition();
            let mut record = row
                .value
                .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
            if provider.is_none()
                && (record.phase == OperationPhase::Succeeded
                    || (adopted && record.phase != OperationPhase::Pending))
            {
                return Ok(false);
            }
            let before = serde_json::to_vec(&record)
                .map_err(|_| storage(acyclic_stream::StreamError::InvalidArgument))?;
            let (pending, mut changes) = self
                .ownership(&mut record, provider, resource, adopted)
                .await?;
            if changes.mutations.is_empty()
                && before
                    == serde_json::to_vec(&record)
                        .map_err(|_| storage(acyclic_stream::StreamError::InvalidArgument))?
            {
                return Ok(pending);
            }
            changes.conditions.push(condition);
            changes
                .mutations
                .push(self.journal.append(&suffix, &Some(record.clone()))?);
            changes
                .mutations
                .push(self.journal.operation_log(&suffix, &Some(record))?);
            if self
                .journal
                .publish(
                    changes.conditions,
                    changes.mutations,
                    row.path,
                    row.tail + 1,
                )
                .await?
            {
                return Ok(pending);
            }
        }
    }

    pub(super) async fn custody(
        &self,
        record: &StoredOperation,
    ) -> Result<(Vec<NativeResource>, Vec<String>), ProviderError> {
        if record.created.is_empty() && record.targets.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let first = record
            .created
            .iter()
            .chain(&record.targets)
            .copied()
            .min()
            .ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?;
        let last = record
            .created
            .iter()
            .chain(&record.targets)
            .copied()
            .max()
            .ok_or_else(|| storage(acyclic_stream::StreamError::InvalidResponse))?;
        if last >= record.native_resources {
            return Err(storage(acyclic_stream::StreamError::InvalidResponse));
        }
        let resources = self
            .resource_records(record, u64::from(first), last - first + 1)
            .await?;
        let created = record
            .created
            .iter()
            .map(|index| match resources.get((*index - first) as usize) {
                Some(Resource::Created(value)) => Ok(value.clone()),
                _ => Err(storage(acyclic_stream::StreamError::InvalidResponse)),
            })
            .collect::<Result<_, _>>()?;
        let targets = record
            .targets
            .iter()
            .map(|index| match resources.get((*index - first) as usize) {
                Some(Resource::Target(value)) => Ok(value.clone()),
                _ => Err(storage(acyclic_stream::StreamError::InvalidResponse)),
            })
            .collect::<Result<_, _>>()?;
        Ok((created, targets))
    }
}
