//! Lifecycle decisions over single native dispatches, shared by every adapter.
use super::{
    NativeBinding, NativeResource, OperationRegistry, RawCreated, RawMachines, RawObservation,
    settle,
};
use crate::*;

impl OperationRegistry {
    /// Creation has the same admission, native ownership and readiness boundary
    /// as every other lifecycle mutation. Successful retries perform no native IO.
    pub async fn create_machine<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        request: CreateMachine,
        interval: Duration,
        timeout: Duration,
    ) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        self.apply_with(driver, key, &MutationIntent::Create(request.clone()), async |operation| {
            if interval.is_zero() || timeout.is_zero() {
                return Err(ProviderError::Invalid(
                    "create readiness admission failed; recovery: configure positive poll_interval and ready_timeout".into(),
                ));
            }
            let created = driver.raw_create(&request).await?.checked(|initial| {
                if initial.contract.image != request.image
                    || initial.contract.compatibility != request.compatibility
                    || initial.contract.expiration != request.expiration
                    || initial.contract.network_policy_digest != request.network_policy_digest
                    || initial.contract.budgets != request.budgets
                    || (initial.contract.suspension != request.suspension
                        && (initial.contract.capabilities.contains(&Capability::SuspendResume)
                            || initial.contract.suspension != SuspensionPolicy::Manual))
                {
                    return Err(ProviderError::Rejected(
                        "native create contract contradicts admission; recovery: inspect this operation's cleanup before creating a new machine".into(),
                    ));
                }
                Ok(())
            });
            let initial = self.accept_created(driver, operation, created, key).await?;
            let ready = self.wait_machine(driver, initial.id, key, interval, timeout).await?;
            if ready.state != MachineState::Running || ready.contract != initial.contract {
                return Err(ProviderError::Failed);
            }
            Ok(MutationOutcome::Created(ready))
        }).await
    }

    /// Claim accepted native output before reporting a metadata failure. Used by
    /// creation and forks so their malformed-response cleanup cannot diverge.
    pub async fn accept_created<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        operation: OperationId,
        created: RawCreated,
        key: IdempotencyKey,
    ) -> Result<MachineObservation, ProviderError> {
        if created.handle.is_empty() {
            return Err(ProviderError::Indeterminate(key));
        }
        let native = NativeBinding {
            provider: driver.name().into(),
            handle: created.handle,
            endpoint: created.endpoint,
        };
        let observation = match created.observation {
            Ok(observation) => observation,
            Err(error) => {
                self.claim(
                    driver,
                    operation,
                    &NativeResource::Machine(native.handle),
                    created.adopted,
                )
                .await?;
                tracing::error!(%operation, %error,
                    "native create metadata mapping failed; recovery: cancel this operation before dispatching a new create");
                return Err(ProviderError::Indeterminate(key));
            }
        };
        self.claim_machine(
            driver,
            operation,
            observation.clone(),
            native,
            created.adopted,
        )
        .await?;
        Ok(observation)
    }

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
        let (known, native) = self.bound_machine(machine, driver.name()).await?;
        match driver
            .raw_observe(machine, native.as_ref(), known.as_ref())
            .await
        {
            Ok(RawObservation { machine: value, .. }) => {
                same_machine(machine, value.id)?;
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
            let (known, native) = self.bound_machine(machine, driver.name()).await?;
            let observed = driver
                .raw_observe(machine, native.as_ref(), known.as_ref())
                .await?;
            same_machine(machine, observed.machine.id)?;
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
        self.apply_with(driver, key, &intent, async |operation| {
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
            let native = self.native_binding(machine, driver.name()).await?;
            let handle = native.as_ref().map_or_else(|| machine.to_string(), |value| value.handle.clone());
            self.bind_target(operation, &handle).await?;
            if wake {
                driver.raw_wake(machine, native.as_ref()).await?;
            } else {
                driver.raw_suspend(machine, native.as_ref()).await?;
            }
            if self.wait_machine(driver, machine, key, interval, timeout).await?.state != to {
                return Err(ProviderError::Failed);
            }
            Ok(outcome)
        }).await
    }

    pub async fn set_suspension_policy<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
        at: u64,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply_with(
            driver,
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
                let native = self.native_binding(machine, driver.name()).await?;
                let handle = native
                    .as_ref()
                    .map_or_else(|| machine.to_string(), |value| value.handle.clone());
                self.bind_target(operation, &handle).await?;
                driver
                    .raw_set_suspension_policy(machine, native.as_ref(), policy)
                    .await?;
                current.contract.suspension = policy;
                current.changed_at_unix_ms = at;
                self.observe_machine(current, at).await?;
                Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
            },
        )
        .await
    }
}

fn same_machine(expected: MachineId, actual: MachineId) -> Result<(), ProviderError> {
    if expected != actual {
        return Err(ProviderError::Rejected(
            "native observation identity mismatch; recovery: repair the raw adapter binding before retrying".into(),
        ));
    }
    Ok(())
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
