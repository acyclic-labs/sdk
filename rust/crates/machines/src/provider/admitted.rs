//! Common public provider for an already admitted native workspace incarnation.
use super::deadline::GuestDeadline;
use super::journal::{Admission, Journal};
use super::*;
use acyclic_stream::{StreamPath, StreamProvider};
use std::sync::Weak;
use tokio::sync::{MutexGuard, watch};

/// Physical primitives. Authentication, selection, keyed settlement and
/// retained observations belong to the shared provider above this boundary.
#[async_trait]
pub(crate) trait NativeMachine: Send + Sync + 'static {
    async fn exec(
        &self,
        command: ExecCommand,
        until: std::time::Instant,
    ) -> Result<ExecStream, ProviderError>;
    async fn read(
        &self,
        access: FileAccess,
        limit: u32,
        deadline: &GuestDeadline,
    ) -> Result<FileRead, ProviderError>;
    async fn write(
        &self,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
        deadline: &GuestDeadline,
    ) -> Result<FileWrite, ProviderError>;
    async fn stop(&self) -> Result<(), ProviderError>;
    async fn wake(&self) -> Result<(), ProviderError>;
}

pub(crate) struct AdmittedMachine<B> {
    backend: B,
    observation: Mutex<(MachineObservation, Option<IdempotencyKey>)>,
    journal: Journal,
    /// Present only under an AfterIdle policy, which enrollment fixes.
    idle: Option<(Duration, watch::Sender<Activity>)>,
}

type Admitted<'a> = MutexGuard<'a, (MachineObservation, Option<IdempotencyKey>)>;

/// Guest work in flight, and a counter that changes whenever work begins or ends.
#[derive(Clone, Copy, Default)]
struct Activity {
    busy: usize,
    epoch: u64,
}

/// Holds a Machine busy, so its idle deadline cannot pass, until dropped.
struct Busy(watch::Sender<Activity>);
impl Busy {
    fn new(activity: &watch::Sender<Activity>) -> Self {
        activity.send_modify(|value| {
            value.busy += 1;
            value.epoch = value.epoch.wrapping_add(1);
        });
        Self(activity.clone())
    }
}
impl Drop for Busy {
    fn drop(&mut self) {
        self.0.send_modify(|value| {
            value.busy -= 1;
            value.epoch = value.epoch.wrapping_add(1);
        });
    }
}

#[derive(Serialize)]
enum Intent<'a> {
    Create(&'a CreateMachine),
    Attach(MachineId, &'a wire::selection::VolumeRef),
    Suspend(MachineId),
    Wake(MachineId),
    Destroy(MachineId),
    Policy(MachineId, SuspensionPolicy),
}

impl<B: NativeMachine> AdmittedMachine<B> {
    pub(crate) async fn open(
        backend: B,
        observation: MachineObservation,
        streams: Arc<dyn StreamProvider>,
        root: StreamPath,
    ) -> Result<Self, ProviderError> {
        let journal = Journal::new(streams, root, observation.id);
        let observation = journal.enroll(observation).await?;
        if observation.state != MachineState::Running {
            backend.stop().await?;
        }
        let idle = match observation.contract.suspension {
            SuspensionPolicy::AfterIdle(idle) if observation.contract.wakes_on_demand() => {
                Some((idle, watch::Sender::new(Activity::default())))
            }
            _ => None,
        };
        Ok(Self {
            backend,
            observation: Mutex::new((observation, None)),
            journal,
            idle,
        })
    }

    /// Shares the provider and starts its AfterIdle producer: one parked task
    /// per Machine, woken only by its own activity or idle deadline.
    pub(crate) fn start(self) -> Arc<Self> {
        let machine = Arc::new(self);
        if let Some((idle, activity)) = &machine.idle {
            let id = machine.journal.machine_id();
            tokio::spawn(suspend_when_idle(
                Arc::downgrade(&machine),
                id,
                *idle,
                activity.subscribe(),
            ));
        }
        machine
    }

    fn busy(&self) -> Option<Busy> {
        self.idle.as_ref().map(|(_, activity)| Busy::new(activity))
    }

    /// Admits guest work on a running incarnation. An idle-suspended Machine is
    /// woken first, under the caller's lock wait and deadline; nothing else is.
    async fn running(&self, machine: MachineId) -> Result<(), ProviderError> {
        let admitted = self.observation.lock().await;
        Self::select(&admitted.0, machine)?;
        let (state, wakes) = (admitted.0.state, admitted.0.contract.wakes_on_demand());
        match state {
            MachineState::Running => return Ok(()),
            MachineState::Suspended if wakes => {
                let key = self.journal.implicit_key(b"wake").await?;
                self.change_locked(admitted, machine, key, MachineState::Running)
                    .await?;
                if self.observation.lock().await.0.state == MachineState::Running {
                    return Ok(());
                }
            }
            _ => {}
        }
        Err(ProviderError::Conflict(
            "Machine is not running; recovery: wake the original incarnation first".into(),
        ))
    }

    /// Suspends after idle without guest work, unless work began since quiet.
    async fn suspend_idle(&self, machine: MachineId, quiet: u64) -> Result<(), ProviderError> {
        let admitted = self.observation.lock().await;
        let current = self.idle.as_ref().map(|(_, activity)| *activity.borrow());
        if admitted.0.state != MachineState::Running
            || current.is_none_or(|value| value.busy != 0 || value.epoch != quiet)
        {
            return Ok(());
        }
        let key = self.journal.implicit_key(b"suspend").await?;
        self.change_locked(admitted, machine, key, MachineState::Suspended)
            .await
            .map(drop)
    }

    fn select(value: &MachineObservation, machine: MachineId) -> Result<(), ProviderError> {
        if value.id == machine {
            Ok(())
        } else {
            Err(ProviderError::NotFound(machine.to_string()))
        }
    }

    async fn change(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
        target: MachineState,
    ) -> Result<MutationOutcome, ProviderError> {
        let admission = self.observation.lock().await;
        if target != MachineState::Destroyed {
            admission.0.contract.provisioned()?;
        }
        let outcome = self.change_locked(admission, machine, key, target).await?;
        // An explicit wake restarts the idle deadline, exactly like guest work.
        if let (MachineState::Running, Some((_, activity))) = (target, &self.idle) {
            activity.send_modify(|value| value.epoch = value.epoch.wrapping_add(1));
        }
        Ok(outcome)
    }

    async fn change_locked(
        &self,
        mut admission: Admitted<'_>,
        machine: MachineId,
        key: IdempotencyKey,
        target: MachineState,
    ) -> Result<MutationOutcome, ProviderError> {
        let (value, pending_key) = &mut *admission;
        Self::select(value, machine)?;
        let (intent, outcome) = match target {
            MachineState::Suspended => (
                Intent::Suspend(machine),
                MutationOutcome::Suspended(machine),
            ),
            MachineState::Running => (Intent::Wake(machine), MutationOutcome::Woken(machine)),
            MachineState::Destroyed => (
                Intent::Destroy(machine),
                MutationOutcome::MachineDestroyed(machine),
            ),
            _ => {
                return Err(ProviderError::Invalid(
                    "unknown native transition; recovery: use suspend, wake or destroy".into(),
                ));
            }
        };
        let digest = intent_digest(&(value.id, &intent))?;
        let mut pending = value.clone();
        pending.state = match target {
            MachineState::Running => MachineState::Waking,
            MachineState::Suspended => MachineState::Suspending,
            _ => MachineState::Destroying,
        };
        let physical = (value.state != target).then_some(&pending);
        let pending_state = pending.state;
        let state = &mut value.state;
        let closing_key = &mut *pending_key;
        let admitted = match self
            .journal
            .begin(key, digest, physical, move || {
                if matches!(
                    *state,
                    MachineState::Suspending
                        | MachineState::Waking
                        | MachineState::Destroying
                        | MachineState::Indeterminate
                        | MachineState::Failed
                ) {
                    return Err(ProviderError::Indeterminate(key));
                }
                if *state == MachineState::Destroyed && target != MachineState::Destroyed {
                    return Err(ProviderError::Conflict(
                "Machine incarnation is destroyed; recovery: retain its terminal destroy receipt"
                    .into(),
            ));
                }
                Ok(move || {
                    if *state != target {
                        *state = pending_state;
                        *closing_key = Some(key);
                    }
                })
            })
            .await?
        {
            Admission::Replay(original) => {
                return self.reconcile(key, original, admission).await;
            }
            Admission::Original(head) => head,
        };
        if value.state != target {
            let physical = if target == MachineState::Running {
                self.backend.wake().await
            } else {
                self.backend.stop().await
            };
            physical.map_err(|_| ProviderError::Indeterminate(key))?;
            pending.changed_at_unix_ms = now()?;
        }
        pending.state = target;
        let result = self
            .journal
            .finish(key, digest, admitted, &pending, outcome)
            .await?;
        *value = pending;
        *pending_key = None;
        Ok(result)
    }

    async fn reconcile(
        &self,
        key: IdempotencyKey,
        outcome: MutationOutcome,
        admission: Admitted<'_>,
    ) -> Result<MutationOutcome, ProviderError> {
        if admission.1 != Some(key) {
            return Ok(outcome);
        }
        drop(admission);
        // A retained terminal result proves that its atomic Machine publication
        // committed, even when the original reply or observer was lost. Read it
        // without holding admission, then join only that same pending key.
        let published = self
            .journal
            .machine()
            .await
            .map_err(|_| ProviderError::Indeterminate(key))?;
        let (value, pending_key) = &mut *self.observation.lock().await;
        if *pending_key == Some(key) {
            *value = published;
            *pending_key = None;
        }
        Ok(outcome)
    }
}

#[async_trait]
impl<B: NativeMachine> MachinesProvider for AdmittedMachine<B> {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }
    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        let deadline = GuestDeadline::new(
            Duration::from_millis(command.timeout_ms),
            command.expiry_unix_ms,
        )?;
        let busy = self.busy();
        let output = deadline
            .run(async {
                self.running(machine).await?;
                self.backend.exec(command, deadline.until()).await
            })
            .await
            .unwrap_or(Err(ProviderError::CommandIndeterminate(machine)))?;
        Ok(match busy {
            None => output,
            // The Machine stays busy for as long as its output is observable.
            Some(busy) => Box::pin(output.map(move |frame| {
                let _busy = &busy;
                frame
            })) as ExecStream,
        })
    }
    async fn read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError> {
        let deadline = GuestDeadline::absolute(access.deadline_unix_ms)?;
        let _busy = self.busy();
        deadline
            .run(self.running(machine))
            .await
            .ok_or_else(|| ProviderError::Rejected("file deadline expired before native admission; recovery: obtain a fresh original deadline".into()))??;
        self.backend.read(access, limit, &deadline).await
    }
    async fn write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        let deadline = GuestDeadline::absolute(access.deadline_unix_ms)?;
        let _busy = self.busy();
        deadline
            .run(self.running(machine))
            .await
            .ok_or_else(|| ProviderError::Rejected("file deadline expired before native admission; recovery: obtain a fresh original deadline".into()))??;
        self.backend.write(access, data, truncate, &deadline).await
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        let (value, _) = &*self.observation.lock().await;
        Self::select(value, machine)?;
        Ok(value.clone())
    }
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "empty Machine page credit; recovery: supply positive credit".into(),
            ));
        }
        let (value, _) = &*self.observation.lock().await;
        Ok(MachinePage {
            machines: if after.is_none_or(|after| after < value.id) {
                vec![value.clone()]
            } else {
                Vec::new()
            },
            next: None,
        })
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        let (value, _) = &*self.observation.lock().await;
        if image != value.contract.image {
            return Err(unsupported("replace the admitted host image"));
        }
        Ok(ImageQualification {
            image,
            capabilities: value.contract.capabilities.clone(),
            compatibility_revision: value.contract.compatibility_revision,
        })
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let (value, _) = &*self.observation.lock().await;
        let digest = intent_digest(&(value.id, &Intent::Create(&request)))?;
        // The local factory enrolls an existing workspace. It never fabricates
        // another filesystem view or an immutable image capture.
        if request
            .image
            .as_ref()
            .is_some_and(|image| image != &value.contract.image)
            || request.selected_volume_ref != value.contract.selected_volume_ref
            || request.network_policy_digest != value.contract.network_policy_digest
            || request.suspension != value.contract.suspension
            || request.expiration != value.contract.expiration
            || request.budgets != value.contract.budgets
            || request.compatibility != value.contract.compatibility
        {
            return Err(unsupported("change the enrolled workspace contract"));
        }
        match self
            .journal
            .begin(request.idempotency_key, digest, None, || {
                settled(value, request.idempotency_key).map(|()| || {})
            })
            .await?
        {
            Admission::Replay(original) => Ok(original),
            Admission::Original(head) => {
                self.journal
                    .finish(
                        request.idempotency_key,
                        digest,
                        head,
                        value,
                        MutationOutcome::Created(value.clone()),
                    )
                    .await
            }
        }
    }
    async fn attach_volume(
        &self,
        machine: MachineId,
        volume: wire::selection::VolumeRef,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let (value, _) = &*self.observation.lock().await;
        Self::select(value, machine)?;
        if value.contract.selected_volume_ref.as_ref() != Some(&volume) {
            return Err(unsupported("replace the admitted selected volume"));
        }
        let digest = intent_digest(&(value.id, &Intent::Attach(machine, &volume)))?;
        match self
            .journal
            .begin(key, digest, None, || settled(value, key).map(|()| || {}))
            .await?
        {
            Admission::Replay(original) => Ok(original),
            Admission::Original(head) => {
                self.journal
                    .finish(
                        key,
                        digest,
                        head,
                        value,
                        MutationOutcome::VolumeAttached(value.clone()),
                    )
                    .await
            }
        }
    }
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.change(machine, key, MachineState::Suspended).await
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.change(machine, key, MachineState::Running).await
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.change(machine, key, MachineState::Destroyed).await
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let (value, _) = &*self.observation.lock().await;
        Self::select(value, machine)?;
        if policy != value.contract.suspension {
            return Err(unsupported("install an automatic suspension producer"));
        }
        let digest = intent_digest(&(value.id, &Intent::Policy(machine, policy)))?;
        match self
            .journal
            .begin(key, digest, None, || settled(value, key).map(|()| || {}))
            .await?
        {
            Admission::Replay(original) => Ok(original),
            Admission::Original(head) => {
                self.journal
                    .finish(
                        key,
                        digest,
                        head,
                        value,
                        MutationOutcome::SuspensionPolicySet(machine, policy),
                    )
                    .await
            }
        }
    }
    async fn events(
        &self,
        machine: MachineId,
        after: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        Self::select(&self.observation.lock().await.0, machine)?;
        self.journal.events(after, limit).await
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        let outcome = self.journal.recover(key).await?;
        self.reconcile(key, outcome, self.observation.lock().await)
            .await
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        let operation = OperationId(key.0);
        self.journal.inspect(operation).await?;
        Ok(operation)
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.journal.inspect(operation).await
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.journal.inspect(operation).await
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.journal.watch(operation).await
    }
    async fn read_output(
        &self,
        _: MachineId,
        _: u64,
        _: u32,
    ) -> Result<MachineOutput, ProviderError> {
        Err(unsupported("replay an unretained console"))
    }
    async fn checkpoint(
        &self,
        _: MachineId,
        _: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(unsupported("snapshot a mutable host workspace"))
    }
    async fn inspect_checkpoint(
        &self,
        _: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        Err(unsupported("inspect an uncreated snapshot"))
    }
    async fn fork(
        &self,
        _: CheckpointId,
        _: NonZeroU32,
        _: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(unsupported(
            "fork without an immutable filesystem generation",
        ))
    }
    async fn destroy_checkpoint(
        &self,
        _: CheckpointId,
        _: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(unsupported("destroy an uncreated snapshot"))
    }
    async fn usage(&self, _: MachineId, _: u64, _: u64) -> Result<UsageReceipt, ProviderError> {
        Err(unsupported("attest unmeasured host usage"))
    }
}

fn settled(value: &MachineObservation, key: IdempotencyKey) -> Result<(), ProviderError> {
    match value.state {
        MachineState::Running | MachineState::Suspended => Ok(()),
        MachineState::Destroyed => Err(ProviderError::Conflict(
            "Machine incarnation is destroyed; recovery: retain its original terminal receipt"
                .into(),
        )),
        _ => Err(ProviderError::Indeterminate(key)),
    }
}
fn unsupported(action: &str) -> ProviderError {
    ProviderError::Unsupported(format!(
        "local Machine cannot {action}; recovery: use the original admitted workspace or a provider with that primitive"
    ))
}

fn now() -> Result<u64, ProviderError> {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ProviderError::Unavailable)?
            .as_millis(),
    )
    .map_err(|_| ProviderError::Unavailable)
}

/// The AfterIdle producer. It waits for quiescence, then for idle without a
/// new begin or end, then suspends once; a suspended Machine has no deadline
/// until it is used again. It ends with its Machine.
async fn suspend_when_idle<B: NativeMachine>(
    owner: Weak<AdmittedMachine<B>>,
    machine: MachineId,
    idle: Duration,
    mut activity: watch::Receiver<Activity>,
) {
    loop {
        let Ok(quiet) = activity
            .wait_for(|value| value.busy == 0)
            .await
            .map(|value| value.epoch)
        else {
            return;
        };
        match tokio::time::timeout(idle, activity.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => return,
            Err(_) => {}
        }
        let Some(provider) = owner.upgrade() else {
            return;
        };
        // An uncertain suspension stays pending in the journal under its own
        // key, exactly like a caller's; this producer never repeats it.
        if provider.suspend_idle(machine, quiet).await.is_err() {
            return;
        }
        drop(provider);
        if activity.changed().await.is_err() {
            return;
        }
    }
}
