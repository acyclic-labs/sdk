//! Shared fork ordering and definite-refusal handling. Unknown dispatch is never retried.
use crate::{IdempotencyKey, ProviderError};
use std::{future::Future, time::Duration};

/// Every existing node forks at most once per round. Node zero is the parent;
/// node n+1 is child n. All starts in a round are joined before reporting failure.
pub async fn fork_tree<T, F, O>(parent: T, count: u32, fork: F) -> Result<Vec<T>, ProviderError>
where
    T: Clone + Send,
    F: Fn(u32, T) -> O + Send + Sync,
    O: Future<Output = Result<T, ProviderError>> + Send,
{
    let mut nodes = vec![parent];
    for round in fork_rounds(count) {
        let starts = round.into_iter().map(|(index, source)| {
            nodes.get(source).cloned().map(|source| fork(index, source)).ok_or_else(||
                ProviderError::Rejected("fork tree source is absent; recovery: report the invariant failure before retrying".into()))
        }).collect::<Result<Vec<_>, _>>()?;
        let mut failure = None;
        let mut children = Vec::new();
        for result in futures::future::join_all(starts).await {
            match result {
                Ok(child) => children.push(child),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        nodes.extend(children);
    }
    Ok(nodes.into_iter().skip(1).collect())
}

#[must_use]
pub fn fork_rounds(count: u32) -> Vec<Vec<(u32, usize)>> {
    let mut rounds = Vec::new();
    let mut produced: u32 = 0;
    while produced < count {
        let batch = produced.saturating_add(1).min(count - produced);
        rounds.push(
            (0..batch)
                .map(|source| (produced + source, source as usize))
                .collect(),
        );
        produced += batch;
    }
    rounds
}

/// Refused means the native service definitively did not start a mutation.
/// Transport failure, conflict and a lost reply must remain errors instead.
pub enum Dispatch<T> {
    Accepted(T),
    Refused,
}

pub async fn retry_refused<T, F, O>(
    key: IdempotencyKey,
    interval: Duration,
    timeout: Duration,
    mut dispatch: F,
) -> Result<T, ProviderError>
where
    T: Send,
    F: FnMut() -> O + Send,
    O: Future<Output = Result<Dispatch<T>, ProviderError>> + Send,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| {
            ProviderError::Invalid(
                "fork deadline overflow; recovery: choose a finite readiness timeout".into(),
            )
        })?;
    if interval.is_zero() || timeout.is_zero() {
        return Err(ProviderError::Invalid(
            "fork timing admission failed; recovery: set nonzero readiness interval and timeout"
                .into(),
        ));
    }
    let mut backoff = interval;
    loop {
        match tokio::time::timeout_at(deadline, dispatch()).await {
            Ok(Ok(Dispatch::Accepted(value))) => return Ok(value),
            Ok(Ok(Dispatch::Refused)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(ProviderError::Indeterminate(key)),
        }
        if tokio::time::timeout_at(deadline, tokio::time::sleep(backoff))
            .await
            .is_err()
        {
            return Err(ProviderError::Indeterminate(key));
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
    }
}

use super::{ForkSlot, OperationRegistry, RawMachines};
use crate::{
    ForkFidelity, MAX_FORK_CHILDREN, MachineId, MachineState, MutationIntent, MutationOutcome,
    NonZeroU32,
};

impl OperationRegistry {
    /// One shared tree owns native fork admission, refused-only retry, custody,
    /// metadata sequencing, readiness and the exact ordered terminal outcome.
    pub async fn fork_machine<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
        interval: Duration,
        timeout: Duration,
        at: u64,
    ) -> Result<MutationOutcome, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply_with(driver, key, &MutationIntent::ForkMachine(machine, count), async |operation| {
            if interval.is_zero() || timeout.is_zero() {
                return Err(ProviderError::Invalid("fork readiness admission failed; recovery: configure positive poll_interval and ready_timeout".into()));
            }
            let parent = self.inspect_machine(driver, machine, at).await?;
            let fidelity = parent.contract.fork_fidelity().ok_or_else(|| {
                ProviderError::Unsupported("machine contract does not declare live fork; recovery: select a qualified atomic snapshot image".into())
            })?;
            if parent.state != MachineState::Running {
                return Err(ProviderError::Conflict("only a running machine can be forked; recovery: wake the source before forking".into()));
            }
            // Disk-only drivers must bind an admitted immutable generation; an
            // execution-time tree copy is never an implementation of this primitive.
            if fidelity != ForkFidelity::MemoryAndDisk {
                return Err(ProviderError::Unsupported("atomic container snapshot is unavailable; recovery: select a VM snapshot with native fork or an admitted immutable filesystem generation".into()));
            }
            let parent_ref = &parent;
            let ids = fork_tree(machine, count, |index, source| async move {
                let slot = ForkSlot { index, count };
                let native = self.native_binding(source, driver.name()).await?;
                let handle = native.as_ref().map_or_else(|| source.to_string(), |native| native.handle.clone());
                self.bind_target(operation, &handle).await?;
                let created = retry_refused(key, interval, timeout, || async {
                    let ready = self.wait_machine(driver, source, key, interval, timeout).await?;
                    if ready.state != MachineState::Running {
                        return Err(ProviderError::Conflict("native fork source is not running; recovery: inspect its state before retrying".into()));
                    }
                    driver.raw_fork_machine(parent_ref, source, native.as_ref(), slot, key).await
                }).await?.checked(|child| {
                    if child.id == machine || child.id == source || child.contract != parent_ref.contract {
                        return Err(ProviderError::Rejected("native live-fork child contradicts admission; recovery: inspect the operation's cleanup before creating a new fork".into()));
                    }
                    Ok(())
                });
                let child = self.accept_created(driver, operation, created, key).await?;
                let child_native = self.native_binding(child.id, driver.name()).await?;
                driver.raw_finish_fork(child.id, child_native.as_ref(), parent_ref, &handle, slot, key).await?;
                let ready = self.wait_machine(driver, child.id, key, interval, timeout).await?;
                if ready.state != MachineState::Running || ready.contract != parent_ref.contract {
                    return Err(ProviderError::Failed);
                }
                Ok(child.id)
            }).await?;
            let mut seen = std::collections::BTreeSet::new();
            let mut children = Vec::with_capacity(ids.len());
            for id in ids {
                if !seen.insert(id) {
                    return Err(ProviderError::Rejected("native live fork repeated a child identity; recovery: inspect the operation's cleanup before creating a new fork".into()));
                }
                let child = self.wait_machine(driver, id, key, interval, timeout).await?;
                if child.state != MachineState::Running || child.contract != parent.contract {
                    return Err(ProviderError::Failed);
                }
                children.push(child);
            }
            Ok(MutationOutcome::MachineForked { source: machine, fidelity, children })
        }).await
    }
}
