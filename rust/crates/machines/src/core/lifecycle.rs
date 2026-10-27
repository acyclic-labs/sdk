//! Lifecycle decisions over single native dispatches, shared by every adapter.
use super::{OperationRegistry, RawMachines, RawObservation, settle};
use crate::*;

impl OperationRegistry {
    pub async fn machine_page(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "machine page admission failed; recovery: request 1..256 machines".into(),
            ));
        }
        let mut machines = self
            .machines()
            .await?
            .into_iter()
            .filter(|value| after.is_none_or(|cursor| value.id > cursor))
            .take(limit as usize + 1)
            .collect::<Vec<_>>();
        let next = if machines.len() > limit as usize {
            machines.pop();
            machines.last().map(|value| value.id)
        } else {
            None
        };
        Ok(MachinePage { machines, next })
    }

    pub async fn event_page<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        after: Option<u64>,
        limit: u32,
        at: u64,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "event page admission failed; recovery: request 1..1024 events".into(),
            ));
        }
        self.inspect_machine(driver, machine, at).await?;
        let (events, next_sequence) = self.events(machine, after, limit as usize).await?;
        Ok(EventPage {
            events,
            next_sequence,
        })
    }

    pub async fn inspect_machine<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        at: u64,
    ) -> Result<MachineObservation, ProviderError> {
        let known = self.machine(machine).await?;
        match driver.raw_observe(machine, known.as_ref()).await {
            Ok(RawObservation { machine: value, .. }) => {
                self.observe_machine(value, at).await?;
                self.machine(machine)
                    .await?
                    .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
            }
            Err(ProviderError::NotFound(_)) => {
                known.ok_or_else(|| ProviderError::NotFound(machine.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    /// Repeats native reads only. Public Running does not imply native readiness.
    pub async fn wait_machine<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        key: IdempotencyKey,
        interval: Duration,
        timeout: Duration,
    ) -> Result<MachineObservation, ProviderError> {
        let value = settle(key, interval, timeout, || async {
            let known = self.machine(machine).await?;
            let observed = driver.raw_observe(machine, known.as_ref()).await?;
            if !observed.settled {
                return Ok(None);
            }
            if observed.machine.state == MachineState::Failed {
                return Err(ProviderError::Failed);
            }
            Ok(Some(observed.machine))
        })
        .await?;
        self.observe_machine(value.clone(), value.changed_at_unix_ms)
            .await?;
        self.machine(machine)
            .await?
            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
    }

    /// Suspend/wake are one canonical state transition with one native dispatch.
    pub async fn transition<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        key: IdempotencyKey,
        wake: bool,
        interval: Duration,
        timeout: Duration,
        at: u64,
    ) -> Result<MutationOutcome, ProviderError> {
        let intent = if wake {
            MutationIntent::Wake(machine)
        } else {
            MutationIntent::Suspend(machine)
        };
        self.apply(key, &intent, async |operation| {
            let current = self.inspect_machine(driver, machine, at).await?;
            require_capability(&current, Capability::SuspendResume)?;
            let (from, to, outcome) = if wake {
                (MachineState::Suspended, MachineState::Running, MutationOutcome::Woken(machine))
            } else {
                (MachineState::Running, MachineState::Suspended, MutationOutcome::Suspended(machine))
            };
            if current.state == to {
                return Ok(outcome);
            }
            if current.state != from {
                return Err(ProviderError::Conflict(format!(
                    "machine transition requires {from:?}; recovery: inspect the machine before retrying"
                )));
            }
            self.bind_target(operation, &machine.to_string()).await?;
            if wake {
                driver.raw_wake(machine).await?;
            } else {
                driver.raw_suspend(machine).await?;
            }
            if self.wait_machine(driver, machine, key, interval, timeout).await?.state != to {
                return Err(ProviderError::Failed);
            }
            Ok(outcome)
        }, async |operation| self.cleanup(driver, operation).await).await
    }

    pub async fn set_suspension_policy<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
        at: u64,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::Policy(machine, policy),
            async |operation| {
                let mut current = self.inspect_machine(driver, machine, at).await?;
                require_capability(&current, Capability::SuspendResume)?;
                if current.state == MachineState::Destroyed {
                    return Err(ProviderError::Conflict(
                        "destroyed machine cannot change policy; recovery: create a new machine"
                            .into(),
                    ));
                }
                self.bind_target(operation, &machine.to_string()).await?;
                driver.raw_set_suspension_policy(machine, policy).await?;
                current.contract.suspension = policy;
                current.changed_at_unix_ms = at;
                self.observe_machine(current, at).await?;
                Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
            },
            async |operation| self.cleanup(driver, operation).await,
        )
        .await
    }
}

pub(super) fn require_capability(
    machine: &MachineObservation,
    capability: Capability,
) -> Result<(), ProviderError> {
    if machine.contract.capabilities.contains(&capability) {
        Ok(())
    } else {
        Err(ProviderError::Unsupported(format!(
            "machine contract does not declare {capability:?}; recovery: select a qualified image"
        )))
    }
}
