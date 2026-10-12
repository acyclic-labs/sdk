use super::*;
use crate::{
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    core::AggregateKind,
    resources::ProviderRef,
};
mod lifecycle;
mod model_ordering;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
mod orchestration;
mod property;
mod session;
fn id(value: u8) -> OperationId {
    OperationId::from_bytes([value; 16])
}
fn state_ref() -> Result<FileRef> {
    let volume = VolumeRef::new(
        ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    FileRef::new(
        volume,
        "state/initial.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(b"null", "application/json", &mut None)?,
        "initial.json".into(),
        &mut None,
    )
}
fn result_ref(bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        state_ref()?.volume().clone(),
        "results/value.json".into(),
        "generation-2".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
        "value.json".into(),
        &mut None,
    )
}
fn spec(operation_id: OperationId, orchestration: Orchestration) -> Result<OperationSpec> {
    Ok(OperationSpec {
        operation_id,
        parent: None,
        owner: DurableOwner::Detached {
            authority: Authority {
                kind: AggregateKind::Task,
                id: operation_id.to_string(),
            },
        },
        owner_scope: crate::core::AuthorityIssuer::new(
            "scheduler-test",
            [9; 32],
            Authority {
                kind: AggregateKind::Task,
                id: operation_id.to_string(),
            },
        )
        .issue(
            "original-declare",
            &crate::Capabilities::new(["operation:declare"]),
            None,
            None,
            &mut None,
        )?,
        entrypoint: EntrypointRef {
            name: "example.task".into(),
            version: "1".into(),
            digest: [1; 32],
            result_schema: Value::Object(Default::default()),
        },
        dependencies: BTreeSet::new(),
        resources: ResourceRequest::default(),
        placement: BTreeMap::new(),
        orchestration,
        state: state_ref()?,
    })
}
fn reservation(lease: u8, amount: u64) -> Reservation {
    Reservation {
        id: format!("lease-{lease}"),
        placement: "worker".into(),
        admitted: cpu(amount),
    }
}
fn fence(lease: u8) -> LeaseFence {
    LeaseFence::from(&reservation(lease, 0))
}
fn two_slot_parent(orchestration: Orchestration) -> Result<Scheduler> {
    let mut scheduler = Scheduler::new();
    scheduler.apply(&scheduler.declare(spec(id(1), orchestration)?)?, &mut None)?;
    start(&mut scheduler, id(1), reservation(0, 0))?;
    scheduler.apply(
        &SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: fence(0),
        },
        &mut None,
    )?;
    for (op, slot) in [(2, "a"), (3, "b")] {
        let mut child = spec(id(op), Orchestration::Leaf)?;
        child.parent = Some(ParentLink {
            operation_id: id(1),
            slot: slot.into(),
        });
        scheduler.apply(&scheduler.declare(child)?, &mut None)?;
        start(&mut scheduler, id(op), reservation(0, 0))?;
    }
    Ok(scheduler)
}

fn start(
    scheduler: &mut Scheduler,
    operation_id: OperationId,
    reservation: Reservation,
) -> Result<()> {
    let fence = (&reservation).into();
    scheduler.apply(
        &SchedulerEvent::Admitted {
            operation_id,
            reservation,
        },
        &mut None,
    )?;
    scheduler.apply(
        &SchedulerEvent::Started {
            operation_id,
            fence,
        },
        &mut None,
    )
}
fn complete(
    scheduler: &mut Scheduler,
    operation_id: OperationId,
    outcome: Outcome<FileRef>,
    fence: Option<LeaseFence>,
) -> Result<()> {
    scheduler.apply(
        &SchedulerEvent::Completed {
            operation_id,
            outcome,
            fence,
            execution_duration_ns: None,
        },
        &mut None,
    )
}

fn child(operation: u8, parent: u8, slot: &str) -> Result<OperationSpec> {
    let mut child = spec(id(operation), Orchestration::Leaf)?;
    child.parent = Some(ParentLink {
        operation_id: id(parent),
        slot: slot.into(),
    });
    Ok(child)
}
fn cpu(amount: u64) -> ResourceRequest {
    ResourceRequest(BTreeMap::from([("cpu".into(), amount)]))
}

fn claim(op: u8, lease: u8, attempt: u8, step: u8, digest: u8) -> SchedulerEvent {
    SchedulerEvent::ModelDispatchClaimed {
        operation_id: id(op),
        attempt_id: id(attempt),
        step: u32::from(step),
        request_digest: [digest; 32],
        fence: fence(lease),
        ceiling: 2,
    }
}

fn claims_preserved(before: &Scheduler, after: &Scheduler) -> bool {
    before.model_claims.iter().all(|(op, prior)| {
        after.model_claims.get(op).is_some_and(|current| {
            current.ceiling == prior.ceiling
                && prior.attempts.iter().all(|(attempt, steps)| {
                    current.attempts.get(attempt).is_some_and(|retained| {
                        steps
                            .iter()
                            .all(|(step, digest)| retained.get(step) == Some(digest))
                    })
                })
        })
    })
}
