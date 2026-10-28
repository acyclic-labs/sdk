//! Native identity and cleanup ownership publish at the same canonical boundary.
use super::*;

/// Only native addressing, never credentials or a second operation journal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeBinding {
    pub provider: String,
    pub handle: String,
    pub endpoint: Option<String>,
}

pub(super) fn check_driver(record: &StoredOperation, provider: &str) -> Result<(), ProviderError> {
    if record
        .provider
        .as_ref()
        .is_some_and(|owner| owner != provider)
    {
        return Err(ProviderError::Unsupported(
            "operation belongs to another native provider; recovery: compose the original provider before recovery or cleanup".into(),
        ));
    }
    Ok(())
}

pub(super) fn project(
    entry: &mut Option<StoredMachine>,
    observation: &MachineObservation,
    at: u64,
    event_limit: Option<u64>,
) -> Result<Option<MachineEvent>, ProviderError> {
    if entry
        .as_ref()
        .is_some_and(|entry| entry.observation.state == MachineState::Destroyed)
    {
        return Ok(None);
    }
    let changed = entry
        .as_ref()
        .is_none_or(|entry| entry.observation.state != observation.state);
    let sequence = entry
        .as_ref()
        .map_or(0, |entry| entry.event_sequence)
        .checked_add(u64::from(changed))
        .ok_or_else(|| {
            ProviderError::Rejected(
                "machine event sequence exhausted; recovery: create a new machine identity".into(),
            )
        })?;
    if event_limit.is_some_and(|limit| sequence > limit) {
        return Err(ProviderError::Rejected("Machines event limit reached; recovery: create a new bounded simulation before publishing further state transitions".into()));
    }
    let native = entry.as_ref().and_then(|entry| entry.native.clone());
    *entry = Some(StoredMachine {
        observation: observation.clone(),
        event_sequence: sequence,
        native,
    });
    Ok(changed.then_some(MachineEvent {
        machine: observation.id,
        sequence,
        observed_at_unix_ms: at,
        fact: EventFact::State(observation.state),
    }))
}

impl OperationRegistry {
    pub(super) async fn check_driver(
        &self,
        operation: OperationId,
        provider: &str,
    ) -> Result<(), ProviderError> {
        let record = self
            .journal
            .load::<StoredOperation>(&format!("ops/{operation}"))
            .await?
            .value
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        check_driver(&record, provider)
    }

    /// For an already admitted native result, including malformed metadata.
    /// Normal native actions bind their driver in the initial admission commit.
    pub async fn bind_driver(
        &self,
        operation: OperationId,
        provider: &str,
    ) -> Result<(), ProviderError> {
        self.journal
            .update(
                &format!("ops/{operation}"),
                |entry: &mut Option<StoredOperation>| {
                    let record = entry
                        .as_mut()
                        .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
                    check_driver(record, provider)?;
                    record.provider = Some(provider.to_owned());
                    Ok(())
                },
            )
            .await
    }
    /// A different driver cannot interpret a native handle in this account.
    /// This check precedes the raw observation or mutation, including after reopen.
    pub async fn native_binding(
        &self,
        machine: MachineId,
        provider: &str,
    ) -> Result<Option<NativeBinding>, ProviderError> {
        Ok(self.bound_machine(machine, provider).await?.1)
    }

    pub(super) async fn bound_machine(
        &self,
        machine: MachineId,
        provider: &str,
    ) -> Result<(Option<MachineObservation>, Option<NativeBinding>), ProviderError> {
        let row = self
            .journal
            .load::<StoredMachine>(&machine.to_string())
            .await?
            .value;
        let (known, value) = match row {
            Some(row) => (Some(row.observation), row.native),
            None => (None, None),
        };
        if value
            .as_ref()
            .is_some_and(|value| value.provider != provider)
        {
            return Err(ProviderError::Unsupported(
                "native binding belongs to another provider; recovery: compose the original provider for this machine identity".into(),
            ));
        }
        Ok((known, value))
    }

    /// The same canonical native addressing rule applies to snapshots. A logical
    /// checkpoint UUID need not be a provider's accepted snapshot name or handle.
    pub async fn native_checkpoint_binding(
        &self,
        checkpoint: CheckpointId,
        provider: &str,
    ) -> Result<Option<NativeBinding>, ProviderError> {
        let native = self
            .journal
            .load::<StoredCheckpoint>(&format!("checkpoints/{checkpoint}"))
            .await?
            .value
            .and_then(|row| row.native);
        if native
            .as_ref()
            .is_some_and(|native| native.provider != provider)
        {
            return Err(ProviderError::Unsupported(
                "checkpoint binding belongs to another provider; recovery: compose the original provider for this checkpoint identity".into(),
            ));
        }
        Ok(native)
    }

    pub(crate) async fn remember_bound_checkpoint(
        &self,
        observation: CheckpointObservation,
        native: Option<NativeBinding>,
    ) -> Result<(), ProviderError> {
        self.journal.update(&format!("checkpoints/{}", observation.id), |entry: &mut Option<StoredCheckpoint>| {
            if let Some(entry) = entry {
                if native.as_ref().is_some_and(|native| entry.native.as_ref().is_some_and(|known| known != native)) {
                    return Err(ProviderError::Conflict(
                        "checkpoint identity has a different native binding; recovery: reconcile the original provider snapshot before retrying".into(),
                    ));
                }
                if !entry.observation.forkable {
                    return Ok(());
                }
                entry.observation = observation.clone();
                if native.is_some() { entry.native = native.clone(); }
            } else {
                *entry = Some(StoredCheckpoint { observation: observation.clone(), native: native.clone() });
            }
            Ok(())
        }).await
    }

    /// Publish handle, initial observation/event, and created/adopted ownership in
    /// one conditional commit, before readiness or any further native mutation.
    pub async fn claim_machine<D: NativeResources + ?Sized>(
        &self,
        driver: &D,
        operation: OperationId,
        observation: MachineObservation,
        native: NativeBinding,
        adopted: bool,
    ) -> Result<(), ProviderError> {
        self.claim_projection(
            driver,
            operation,
            super::custody::CleanupProjection::Machine(observation.id),
            &native,
            adopted,
            |state: &mut Option<StoredMachine>| {
                if state.as_ref().and_then(|state| state.native.as_ref()).is_some_and(|binding| binding != &native) {
                    return Err(ProviderError::Conflict(
                        "machine identity has a different native binding; recovery: reconcile the original provider resource before retrying".into(),
                    ));
                }
                let event = project(state, &observation, observation.changed_at_unix_ms, self.limits.machine_events)?;
                if let Some(state) = state { state.native = Some(native.clone()); }
                Ok(event)
            },
        )
        .await
    }
}
