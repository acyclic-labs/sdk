//! In-process hosting over the public Streams transaction boundary.

use super::*;
use crate::{
    Error,
    workflow::{DurableWorkflowHost, MachineRegistry, WorkflowAdmission, WorkflowJournal},
};
use acyclic_stream::{StreamPath, StreamProvider};
use std::sync::Mutex;

mod journal;
#[cfg(test)]
mod tests;
use journal::{Bootstrap, Entry, Journal};

/// Host operation being authorized, independent of caller-controlled input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionAction {
    /// Install the pinned package and resolve its resources.
    Install,
    /// Create or transfer an actor placement.
    Place,
    /// Admit an ordinary input.
    Deliver,
    /// Read retained state and reconcile its pending work.
    Recover,
    /// Stop and drain an actor's effects.
    Cancel,
}

/// Runtime-supplied authorization and binding resolution. Implementations own
/// the workload identity; input JSON never supplies authority.
pub trait AdmissionPolicy: ProviderPlatform {
    /// Checks the calling workload, placement and complete admitted input.
    /// `None` identifies deployment, placement or recovery rather than delivery.
    fn authorize<'a>(
        &'a self,
        action: AdmissionAction,
        manifest: &'a Manifest,
        placement: Option<&'a Placement>,
        event: Option<&'a Event>,
    ) -> BoxProviderFuture<'a, Result<()>>;
}

#[derive(Clone)]
struct Installed {
    manifest: Manifest,
    machine: Arc<dyn ResumableMachine>,
    registry: MachineRegistry,
}

/// The package host uses the same deterministic workflow engine as task workers.
/// Its durable state is entirely in the selected Streams provider. Hosts supply
/// resolved resources, authenticated admission and fenced effect execution.
pub struct InProcessRuntime<P: StreamProvider> {
    stream: Arc<P>,
    bindings: Bindings,
    authority: Arc<dyn AdmissionPolicy>,
    effects: Arc<dyn EffectHost>,
    installed: Mutex<Option<Installed>>,
}

impl<P: StreamProvider> InProcessRuntime<P> {
    /// Constructs a host with explicit resource bindings. No package is installed
    /// and no actor can execute until `install` succeeds.
    pub fn new(
        stream: Arc<P>,
        bindings: Bindings,
        authority: Arc<dyn AdmissionPolicy>,
        effects: Arc<dyn EffectHost>,
    ) -> Self {
        Self {
            stream,
            bindings,
            authority,
            effects,
            installed: Mutex::new(None),
        }
    }

    fn package(&self) -> Result<Installed> {
        self.installed
            .lock()
            .map_err(|_| Error::Storage("package installation lock poisoned".into()))?
            .clone()
            .ok_or_else(|| Error::NotFound("no Actors package installed".into()))
    }

    fn journal(&self, package: &Installed, placement: &Placement) -> Result<Arc<Journal<P>>> {
        if !crate::is_valid_component_label(&placement.node) || placement.epoch == 0 {
            return Err(Error::Invalid("invalid actor placement".into()));
        }
        if let VolumePlacement::NodeLocal(owner) = &package.manifest.bindings.placement
            && owner != &placement.node
        {
            return Err(Error::Conflict(
                "node-local actor volume is unavailable on this node".into(),
            ));
        }
        Journal::new(
            self.stream.clone(),
            package.manifest.clone(),
            placement.clone(),
        )
        .map(Arc::new)
    }

    async fn checked(
        &self,
        action: AdmissionAction,
        placement: &Placement,
        event: Option<&Event>,
    ) -> Result<(Installed, Arc<Journal<P>>)> {
        let package = self.package()?;
        self.authority
            .authorize(action, &package.manifest, Some(placement), event)
            .await?;
        let journal = self.journal(&package, placement)?;
        Ok((package, journal))
    }

    async fn finish_cancel(&self, journal: &Journal<P>) -> Result<Snapshot> {
        let current = journal.load().await?;
        if current.last.snapshot.cancelled {
            return Ok(current.last.snapshot);
        }
        if !current.last.cancelling {
            return Err(Error::Conflict("actor has no cancellation request".into()));
        }
        journal.require_durable(current.tail).await?;
        // A fence permanently closes this epoch to new dispatch and waits for
        // all previously started effects. Settlement is published afterwards.
        self.effects
            .fence(current.last.snapshot.placement.clone())
            .await?;
        let mut settled = current.last;
        settled.event = None;
        settled.workflow = None;
        settled.bootstrap = None;
        settled.snapshot.cancelled = true;
        settled.snapshot.pending.clear();
        settled.snapshot.status = MachineStatus::Failed {
            message: "actor cancelled".into(),
        };
        journal
            .publish(&current.head, current.tail, settled.clone(), None)
            .await?;
        Ok(settled.snapshot)
    }

    async fn finish_move(&self, journal: &Journal<P>, target: &Placement) -> Result<Snapshot> {
        let current = journal.load().await?;
        if current.last.moving.is_none() && current.last.snapshot.placement == *target {
            return Ok(current.last.snapshot);
        }
        if current.last.moving.as_ref() != Some(target) {
            return Err(Error::Conflict("another actor placement is pending".into()));
        }
        // Persist the intent before irreversibly closing the old effect epoch.
        // A dropped fence can then be resumed after reopening the same package.
        journal.require_durable(current.tail).await?;
        self.effects
            .fence(current.last.snapshot.placement.clone())
            .await?;
        let mut moved = current.last;
        moved.snapshot.placement = target.clone();
        moved.moving = None;
        journal
            .publish(&current.head, current.tail, moved.clone(), None)
            .await?;
        Ok(moved.snapshot)
    }
    fn deliver_event(
        &self,
        placement: Placement,
        event: Event,
        trusted: bool,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let (package, journal) = self
                .checked(AdmissionAction::Deliver, &placement, Some(&event))
                .await?;
            let _claim = journal.claim_handler().await?;
            let current = journal.load().await?;
            journal.require_owner(&current)?;
            if !trusted {
                journal.reject_result_identity(&event).await?;
            }
            if let Some(original) = journal.retry(&event).await? {
                if original.cancelling {
                    return Err(Error::Conflict(
                        "delivery reuses a cancellation event".into(),
                    ));
                }
                return Ok(original.snapshot);
            }
            if current.last.cancelling {
                return Err(Error::Conflict(
                    "actor cancellation has fenced delivery".into(),
                ));
            }
            if !matches!(current.last.snapshot.status, MachineStatus::Suspended) {
                return Err(Error::Conflict("actor is terminal".into()));
            }
            if let Some(next) = journal.next_input(current.last.snapshot.consumed).await?
                && (next.event != event || next.cancellation)
            {
                return Err(Error::Conflict(
                    "an earlier admitted input requires recovery".into(),
                ));
            }
            // Admission rejects malformed or undeclared work before accepting
            // an immutable inbox input. The authoritative step still runs under
            // the journal CAS and is verified on every recovery.
            let (_, proposed, _, _) = package.registry.step(
                &current.last.snapshot.checkpoint,
                std::borrow::Cow::Borrowed(&event.input),
                None,
            )?;
            package.manifest.validate_transition(&proposed)?;
            crate::workflow::WorkflowRecord {
                operation_id: event.operation_id,
                idempotency_key: event.idempotency_key.clone(),
                prior: current.last.snapshot.checkpoint.clone(),
                input: event.input.clone(),
                transition: proposed.clone(),
            }
            .validate(None)?;
            journal
                .check_commands(&proposed, event.operation_id)
                .await?;
            if !matches!(proposed.status, MachineStatus::Suspended)
                && current.last.snapshot.pending.iter().any(|command| {
                    effect_result_operation(command.operation_id) != event.operation_id
                })
            {
                return Err(Error::Conflict(
                    "terminal actor still has pending effects".into(),
                ));
            }
            let mut host = DurableWorkflowHost::open(
                package.registry,
                current.bootstrap.admission.initial,
                journal.clone(),
            )
            .await?;
            journal.admit_event(event.clone(), false).await?;
            host.step(
                event.operation_id,
                event.idempotency_key,
                event.input,
                |transition| package.manifest.validate_transition(transition),
            )
            .await?;
            Ok(journal.load().await?.last.snapshot)
        })
    }
}

impl<P: StreamProvider> ActorRuntime for InProcessRuntime<P> {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let machine = validate_package(package.as_ref())?;
            let manifest = package.manifest().clone();
            if manifest.bindings != self.bindings {
                return Err(Error::Conflict(
                    "package resources differ from resolved runtime bindings".into(),
                ));
            }
            StreamPath::new(&manifest.bindings.streams)
                .map_err(|error| journal::stream_error(&error))?;
            self.authority
                .authorize(AdmissionAction::Install, &manifest, None, None)
                .await?;
            let mut registry = MachineRegistry::default();
            registry.register(machine.clone(), &mut None)?;
            let mut installed = self
                .installed
                .lock()
                .map_err(|_| Error::Storage("package installation lock poisoned".into()))?;
            if let Some(existing) = installed.as_ref() {
                if existing.manifest != manifest {
                    return Err(Error::Conflict(
                        "a different package is already installed".into(),
                    ));
                }
            } else {
                *installed = Some(Installed {
                    manifest,
                    machine,
                    registry,
                });
            }
            Ok(())
        })
    }

    fn place(&self, placement: Placement, input: Value) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let (package, journal) = self
                .checked(AdmissionAction::Place, &placement, None)
                .await?;
            let _claim = journal.claim_handler().await?;
            if let Some(current) = journal.load_optional().await? {
                if current.bootstrap.input != input
                    || current.bootstrap.manifest != package.manifest
                {
                    return Err(Error::Conflict("actor admission is already bound".into()));
                }
                if current.last.moving.is_some() {
                    return self.finish_move(&journal, &placement).await;
                }
                let old = &current.last.snapshot.placement;
                if old == &placement {
                    return Ok(current.last.snapshot);
                }
                if placement.epoch <= old.epoch || current.last.cancelling {
                    return Err(Error::Conflict("stale or cancelled actor placement".into()));
                }
                let mut moved = current.last;
                moved.moving = Some(placement.clone());
                moved.event = None;
                moved.workflow = None;
                moved.bootstrap = None;
                journal
                    .publish(&current.head, current.tail, moved, None)
                    .await?;
                return self.finish_move(&journal, &placement).await;
            }
            let initial = MachineCheckpoint {
                machine: package.manifest.agent.clone(),
                revision: 0,
                state: package.machine.initialize(&input)?,
            };
            if !jsonschema::validator_for(package.machine.state_schema())
                .map_err(|error| Error::Invalid(error.to_string()))?
                .is_valid(&initial.state)
            {
                return Err(Error::Invalid(
                    "initial actor state violates the package schema".into(),
                ));
            }
            let admission = WorkflowAdmission {
                operation_id: OperationId::from_bytes(placement.actor.into_bytes()),
                request_digest: *blake3::hash(
                    &serde_json::to_vec(&(&package.manifest, &input))
                        .map_err(|error| journal::codec_error(&error))?,
                )
                .as_bytes(),
                initial: initial.clone(),
            };
            admission.validate(None)?;
            let snapshot = Snapshot {
                placement,
                checkpoint: initial,
                consumed: 0,
                status: MachineStatus::Suspended,
                pending: Vec::new(),
                cancelled: false,
            };
            journal
                .create(Entry {
                    snapshot: snapshot.clone(),
                    bootstrap: Some(Bootstrap {
                        manifest: package.manifest,
                        input,
                        admission,
                    }),
                    workflow: None,
                    event: None,
                    cancelling: false,
                    moving: None,
                })
                .await?;
            Ok(snapshot)
        })
    }

    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.deliver_event(placement, event, false)
    }
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let (package, journal) = self
                .checked(AdmissionAction::Recover, &placement, None)
                .await?;
            let current = journal.load().await?;
            if current.last.moving.as_ref() == Some(&placement) {
                let _claim = journal.claim_handler().await?;
                return self.finish_move(&journal, &placement).await;
            }
            journal.require_owner(&current)?;
            // Recovery validates all retained workflow transitions against the
            // pinned implementation even when there is no pending work.
            DurableWorkflowHost::open(
                package.registry,
                current.bootstrap.admission.initial,
                journal.clone(),
            )
            .await?;
            if current.last.cancelling {
                let _claim = journal.claim_handler().await?;
                return self.finish_cancel(&journal).await;
            }
            if let Some(admitted) = journal.next_input(current.last.snapshot.consumed).await? {
                return if admitted.cancellation {
                    self.cancel(placement, admitted.event).await
                } else {
                    self.deliver_event(placement, admitted.event, true).await
                };
            }
            let Some(command) = current.last.snapshot.pending.first().cloned() else {
                return Ok(current.last.snapshot);
            };
            journal.require_durable(current.tail).await?;
            let _claim = journal.claim_effects().await?;
            // Cancellation and placement can advance while a dispatch claim is
            // acquired. Recheck before entering the host's permanently fenced
            // epoch; the host closes the final check/start race itself.
            let latest = journal.load().await?;
            journal.require_owner(&latest)?;
            if latest.last.cancelling || latest.head != current.head {
                return Ok(latest.last.snapshot);
            }
            let event = self
                .effects
                .execute(placement.clone(), command.clone())
                .await?;
            if event.operation_id != effect_result_operation(command.operation_id) {
                return Err(Error::Conflict(
                    "effect result operation differs from committed command".into(),
                ));
            }
            self.deliver_event(placement, event, true).await
        })
    }

    fn cancel(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let (_, journal) = self
                .checked(AdmissionAction::Cancel, &placement, Some(&event))
                .await?;
            let _claim = journal.claim_handler().await?;
            let current = journal.load().await?;
            journal.require_owner(&current)?;
            journal.reject_result_identity(&event).await?;
            if let Some(original) = journal.retry(&event).await? {
                if !original.cancelling {
                    return Err(Error::Conflict(
                        "cancellation reuses an ordinary event".into(),
                    ));
                }
            } else {
                if current.last.cancelling {
                    return Err(Error::Conflict(
                        "actor cancellation is already admitted".into(),
                    ));
                }
                // Admission and the durable stop intent are one transaction.
                journal.admit_event(event, true).await?;
            }
            self.finish_cancel(&journal).await
        })
    }
}

/// Stable identity for the inbox result of a committed effect. It differs from
/// the command's identity, which the workflow reserves against reuse as a step.
#[must_use]
pub fn effect_result_operation(command: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.actor.effect-result/v1\0");
    hash.update(&command.into_bytes());
    let mut bytes = [0; 16];
    for (slot, byte) in bytes.iter_mut().zip(hash.finalize().as_bytes()) {
        *slot = *byte;
    }
    OperationId::from_bytes(bytes)
}
