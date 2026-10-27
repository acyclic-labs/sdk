//! One native snapshot lifecycle. Adapters only capture, restore and release
//! opaque handles; no provider may substitute a polling copy for this boundary.
use super::super::operations::NativeCheckpoint;
use super::*;

impl<D: SandboxDriver> RemoteMachines<D> {
    /// Releases every native checkpoint owned by this provider instance,
    /// including one left behind by a failed qualification vector. Provider
    /// retention is reported by its release primitive, never called deletion.
    pub async fn retire_checkpoints(&self) -> Result<(), ProviderError> {
        let mut failure = None;
        for id in self.ops.unreleased_checkpoints() {
            if let Err(error) = self.release_checkpoint(id).await {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(super) fn require_snapshots(&self) -> Result<(), ProviderError> {
        if self.driver.supports_disk_snapshots() {
            Ok(())
        } else {
            Err(snapshot_unavailable())
        }
    }

    pub(super) fn checkpoint_observation(
        &self,
        id: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.require_snapshots()?;
        let mut observation = self
            .ops
            .checkpoint(id)
            .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
        if self.ops.native_checkpoint(id).is_some_and(|native| {
            native
                .snapshot
                .expires_at_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms())
        }) {
            observation.forkable = false;
            self.ops.mark_checkpoint_destroyed(id);
        }
        Ok(observation)
    }

    pub(super) fn checkpoint_boot(&self, id: CheckpointId) -> Result<Boot, ProviderError> {
        if !self.checkpoint_observation(id)?.forkable {
            return Err(ProviderError::Conflict(
                "checkpoint no longer accepts forks; recovery: capture a fresh native checkpoint"
                    .into(),
            ));
        }
        let native = self
            .ops
            .native_checkpoint(id)
            .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
        Ok(Boot {
            image: native.image,
            snapshot: Some(native.snapshot),
        })
    }

    pub(super) async fn capture_checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
        operation: OperationId,
        publish_to_source: bool,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.require_snapshots()?;
        let mut source = self.inspect_machine(machine).await?;
        if source.state != MachineState::Running
            || !source.contract.capabilities.contains(&Capability::DiskFork)
        {
            return Err(ProviderError::Conflict("native disk checkpoint requires a running admitted machine; recovery: inspect its state and contract".into()));
        }
        let binding = self.binding(machine)?;
        self.ops.bind_target(operation, &binding.instance.id);
        let request_id = uuid::Uuid::new_v4().to_string();
        self.ops.bind_target(operation, &request_id);
        let started = now_unix_ms();
        let handle = self.driver.snapshot(&binding.instance, &request_id).await?;
        if handle.is_empty() {
            return Err(ProviderError::Unavailable);
        }
        let snapshot = super::super::ProviderSnapshot {
            handle,
            expires_at_unix_ms: self.driver.snapshot_retention().map(|retention| {
                started.saturating_add(u64::try_from(retention.as_millis()).unwrap_or(u64::MAX))
            }),
        };
        let now = now_unix_ms();
        let id = CheckpointId::from_digest(
            Sha256::digest(format!(
                "acyclic-machines-native-checkpoint/{}/{key}",
                self.config.namespace
            ))
            .into(),
        );
        let checkpoint = CheckpointObservation {
            id,
            source: machine,
            contract: source.contract.clone(),
            forkable: true,
            created_at_unix_ms: now,
        };
        if !self.ops.bind_native_checkpoint(
            operation,
            checkpoint.clone(),
            NativeCheckpoint {
                snapshot,
                image: binding.image,
            },
        ) {
            self.rollback(operation).await;
            return Err(ProviderError::Cancelled);
        }
        if !self.checkpoint_observation(id)?.forkable {
            return Err(ProviderError::Rejected("native checkpoint expired during capture; recovery: capture a fresh checkpoint with sufficient retention".into()));
        }
        if publish_to_source {
            source.last_checkpoint = Some(id);
            source.changed_at_unix_ms = now;
            self.ops.observe_machine(source, now);
        }
        Ok(checkpoint)
    }

    pub(super) fn validate_fork_count(count: NonZeroU32) -> Result<(), ProviderError> {
        if count.get() > MAX_FORK_CHILDREN {
            Err(ProviderError::Invalid(format!(
                "fork child count exceeds {MAX_FORK_CHILDREN}; recovery: request fewer children"
            )))
        } else {
            Ok(())
        }
    }

    pub(super) async fn fork_checkpoint(
        &self,
        checkpoint: CheckpointId,
        contract: MachineContract,
        count: NonZeroU32,
        key: IdempotencyKey,
        operation: OperationId,
    ) -> Result<Vec<MachineObservation>, ProviderError> {
        Self::validate_fork_count(count)?;
        let mut children = Vec::with_capacity(count.get() as usize);
        for index in 0..count.get() {
            let boot = self.checkpoint_boot(checkpoint)?;
            children.push(
                self.create_one(operation, key, index, contract.clone(), boot)
                    .await?,
            );
        }
        Ok(children)
    }

    pub(super) async fn release_checkpoint(&self, id: CheckpointId) -> Result<(), ProviderError> {
        if self.ops.checkpoint_released(id) {
            return Ok(());
        }
        let native = self
            .ops
            .native_checkpoint(id)
            .ok_or_else(|| ProviderError::NotFound(id.to_string()))?;
        // Revoke SDK admission before any release await. A failed physical
        // release cannot make a checkpoint forkable again.
        self.ops.mark_checkpoint_destroyed(id);
        match self.driver.release_snapshot(&native.snapshot).await {
            Ok(super::super::SnapshotRelease::Deleted) | Err(ProviderError::NotFound(_)) => Ok(()),
            Ok(super::super::SnapshotRelease::ProviderRetained) => {
                tracing::debug!(checkpoint = %id, "native snapshot reference revoked; provider data follows its retention policy");
                Ok(())
            }
            Err(error) => Err(error),
        }?;
        self.ops.mark_checkpoint_released(id);
        Ok(())
    }
}
