use super::*;
#[derive(Clone, Copy, Debug)]
enum Step {
    Declare {
        op: u8,
        parent: Option<u8>,
        dependency: Option<u8>,
        orchestration: u8,
        resources: u64,
    },
    WaitForCapacity(u8),
    Admit(u8, u8, u64),
    PartiallyAdmit(u8, u8, u64),
    Reject(u8),
    Start(u8, u8),
    Checkpoint(u8, u8),
    WaitForChildren(u8, u8),
    ReleaseLease(u8, u8),
    Cancel(u8, bool),
    ModelClaim(u8, u8, u8, u8, u8),
    Complete(u8, u8, Option<u8>),
    Orchestrate(u8),
    /// The next lifecycle event for the operation's current phase.
    Advance(u8, u8),
}
fn step() -> impl proptest::strategy::Strategy<Value = Step> {
    use proptest::prelude::*;
    let op = || 1..6_u8;
    let lease = || prop_oneof![9 => Just(0_u8), 1 => Just(1_u8)];
    let declare = (
        4..6_u8,
        proptest::option::of(op()),
        proptest::option::weighted(0.2, op()),
    );
    prop_oneof![
        1 => (declare, 0..3_u8, 0..2_u64).prop_map(
            |((op, parent, dependency), orchestration, resources)| Step::Declare {
                op,
                parent,
                dependency,
                orchestration,
                resources,
            },
        ),
        2 => op().prop_map(Step::WaitForCapacity),
        4 => (op(), lease(), 0..3_u64)
            .prop_map(|(op, lease, amount)| Step::Admit(op, lease, amount)),
        1 => (op(), lease(), 0..3_u64)
            .prop_map(|(op, lease, amount)| Step::PartiallyAdmit(op, lease, amount)),
        1 => op().prop_map(Step::Reject),
        4 => (op(), lease()).prop_map(|(op, lease)| Step::Start(op, lease)),
        1 => (op(), lease()).prop_map(|(op, lease)| Step::Checkpoint(op, lease)),
        3 => (op(), lease()).prop_map(|(op, lease)| Step::WaitForChildren(op, lease)),
        1 => (op(), lease()).prop_map(|(op, lease)| Step::ReleaseLease(op, lease)),
        1 => (op(), any::<bool>()).prop_map(|(op, recursive)| Step::Cancel(op, recursive)),
        4 => (op(), lease(), 0..2_u8, 0..3_u8, 0..4_u8)
            .prop_map(|(op, lease, attempt, step, digest)|
                Step::ModelClaim(op, lease, attempt, step, digest)),
        4 => (op(), 0..4_u8, proptest::option::weighted(0.8, lease()))
            .prop_map(|(op, outcome, fence)| Step::Complete(op, outcome, fence)),
        4 => op().prop_map(Step::Orchestrate),
        12 => (op(), any::<u8>()).prop_map(|(op, choice)| Step::Advance(op, choice)),
    ]
}
fn event(scheduler: &Scheduler, step: Step) -> Result<Option<SchedulerEvent>> {
    Ok(Some(match step {
        Step::Declare {
            op,
            parent,
            dependency,
            orchestration,
            resources,
        } => {
            let orchestration = match orchestration {
                0 => Orchestration::Leaf,
                1 => Orchestration::Join,
                _ => Orchestration::Race,
            };
            let mut declared = spec(id(op), orchestration)?;
            declared.dependencies.extend(dependency.map(id));
            declared.resources = ResourceRequest(BTreeMap::from([("cpu".into(), resources)]));
            if let Some(parent) = parent {
                declared.owner = spec(id(parent), Orchestration::Leaf)?.owner;
                declared.parent = Some(ParentLink {
                    operation_id: id(parent),
                    slot: format!("slot-{op}"),
                });
            }
            SchedulerEvent::Declared {
                spec: Box::new(declared),
            }
        }
        Step::WaitForCapacity(op) => SchedulerEvent::WaitingForCapacity {
            operation_id: id(op),
        },
        Step::Admit(op, lease, amount) => SchedulerEvent::Admitted {
            operation_id: id(op),
            reservation: reservation(lease, amount),
        },
        Step::PartiallyAdmit(op, lease, amount) => SchedulerEvent::PartiallyAdmitted {
            operation_id: id(op),
            reservation: reservation(lease, amount),
        },
        Step::Reject(op) => SchedulerEvent::Rejected {
            operation_id: id(op),
            reason: "rejected".into(),
        },
        Step::Start(op, lease) => SchedulerEvent::Started {
            operation_id: id(op),
            fence: fence(lease),
        },
        Step::Checkpoint(op, lease) => SchedulerEvent::Checkpointed {
            operation_id: id(op),
            checkpoint: crate::resources::CheckpointRef::new(
                ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?,
                vec![1],
                None,
            )?,
            fence: fence(lease),
        },
        Step::WaitForChildren(op, lease) => SchedulerEvent::WaitingForChildren {
            operation_id: id(op),
            fence: fence(lease),
        },
        Step::ReleaseLease(op, lease) => SchedulerEvent::LeaseReleased {
            operation_id: id(op),
            fence: fence(lease),
        },
        Step::Cancel(op, recursive) => SchedulerEvent::CancellationRequested {
            operation_id: id(op),
            recursive,
        },
        Step::ModelClaim(op, lease, attempt, step, digest) => {
            SchedulerEvent::ModelDispatchClaimed {
                operation_id: id(op),
                attempt_id: id(attempt),
                step: u32::from(step),
                request_digest: [digest; 32],
                fence: fence(lease),
                ceiling: 2,
            }
        }
        Step::Complete(op, outcome, lease) => SchedulerEvent::Completed {
            operation_id: id(op),
            outcome: match outcome {
                0 => Outcome::Succeeded(result_ref(b"2")?),
                1 => Outcome::Failed {
                    message: "failed".into(),
                },
                2 => Outcome::Cancelled,
                _ => Outcome::Indeterminate {
                    operation_id: id(op),
                },
            },
            fence: lease.map(fence),
            execution_duration_ns: None,
        },
        Step::Advance(op, choice) => {
            let Some(state) = scheduler.operation(id(op)) else {
                return Ok(None);
            };
            let lease = u8::from(
                state
                    .reservation
                    .as_ref()
                    .is_some_and(|held| held.id == "lease-1"),
            );
            let next = match state.phase {
                OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity => {
                    Step::Admit(op, lease, 1)
                }
                OperationPhase::Admitted => Step::Start(op, lease),
                OperationPhase::Running if state.spec.orchestration != Orchestration::Leaf => {
                    Step::WaitForChildren(op, lease)
                }
                OperationPhase::Running => Step::Complete(op, choice % 4, Some(lease)),
                OperationPhase::WaitingForChildren => Step::Orchestrate(op),
                OperationPhase::Reconciling => Step::Complete(op, choice % 3, None),
                OperationPhase::Suspended | OperationPhase::Terminal => return Ok(None),
            };
            return event(scheduler, next);
        }
        Step::Orchestrate(op) => {
            let Some(parent) = scheduler.operation(id(op)) else {
                return Ok(None);
            };
            let (outcome, cancel, reduction_digest) =
                match scheduler.orchestration(id(op), &mut None)? {
                    OrchestrationDecision::Complete { outcome, cancel } => (outcome, cancel, None),
                    OrchestrationDecision::Assemble {
                        assembly,
                        values,
                        cancel,
                    } => (
                        Outcome::Succeeded(result_ref(b"3")?),
                        cancel,
                        Some(assembly_invocation_digest(assembly, &values)?),
                    ),
                    OrchestrationDecision::Wait | OrchestrationDecision::Reduce { .. } => {
                        return Ok(None);
                    }
                };
            SchedulerEvent::Orchestrated {
                operation_id: id(op),
                expected_revision: parent.revision,
                outcome,
                cancel,
                reducer: None,
                reduction_digest,
            }
        }
    }))
}
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(128))]

    #[test]
    fn scheduler_apply_preserves_lifecycle_invariants(
        orchestration in 0..3_u8,
        steps in proptest::collection::vec(step(), 1..64),
    ) {
        use proptest::prelude::*;
        let fail = |error: Error| TestCaseError::fail(error.to_string());
        let mut scheduler = Scheduler::new();

        for (op, parent) in [(1, None), (2, Some(1)), (3, Some(1))] {
            let declared = Step::Declare {
                op,
                parent,
                dependency: None,
                orchestration: if parent.is_none() { orchestration } else { 0 },
                resources: 1,
            };
            if let Some(event) = event(&scheduler, declared).map_err(fail)? {
                scheduler.apply(&event, &mut None).map_err(fail)?;
            }
            if parent.is_none() {
                scheduler.apply(&SchedulerEvent::SessionConfigured {
                    operation_id: id(op),
                    limits: SessionLimits {
                        active_tasks: 2,
                        total_tasks: 5,
                        depth: 3,
                        model_steps: 2,
                    },
                }, &mut None).map_err(fail)?;
            }
        }
        for step in steps {
            let Some(event) = event(&scheduler, step).map_err(fail)? else {
                continue;
            };
            let before = scheduler.clone();
            if scheduler.apply(&event, &mut None).is_err() {
                prop_assert_eq!(&scheduler, &before);
                continue;
            }
            for (id, prior) in &before.operations {
                let current = scheduler.operations.get(id);
                prop_assert!(current.is_some_and(|current| current.revision >= prior.revision));
                if prior.phase == OperationPhase::Terminal {
                    prop_assert_eq!(current, Some(prior), "terminal is absorbing");
                }
            }
            for operation in scheduler.operations.values() {

                prop_assert!(
                    !operation.cancellation_requested
                        || operation.phase != OperationPhase::WaitingForChildren
                );
                prop_assert_eq!(
                    operation.outcome.is_some(),
                    matches!(
                        operation.phase,
                        OperationPhase::Terminal | OperationPhase::Reconciling
                    )
                );

                prop_assert!(operation.reservation.is_none() || matches!(
                    operation.phase,
                    OperationPhase::WaitingForCapacity
                        | OperationPhase::Admitted
                        | OperationPhase::Running
                        | OperationPhase::Reconciling
                ));
            }

            for (op, prior) in &before.model_claims {
                let current = scheduler.model_claims.get(op);
                prop_assert!(current.is_some_and(|current|
                    current.ceiling == prior.ceiling
                        && prior.attempts.iter().all(|(attempt, steps)|
                            current.attempts.get(attempt).is_some_and(|retained|
                                steps.iter().all(|(step, digest)|
                                    retained.get(step) == Some(digest))))));
            }
            for (root, limits) in &scheduler.sessions {
                let used = scheduler.model_claims.iter()
                    .filter(|(op, _)| scheduler.session_root(**op, &mut None)
                        .is_ok_and(|(candidate, _)| candidate == *root))
                    .map(|(_, claims)| claims.used())
                    .sum::<u64>();
                prop_assert!(used <= limits.model_steps);
            }
            for claims in scheduler.model_claims.values() {
                prop_assert!(claims.used() <= claims.ceiling);
            }
            let completed = scheduler.completion_order.iter().collect::<BTreeSet<_>>();
            prop_assert_eq!(completed.len(), scheduler.completion_order.len());
            let terminal = scheduler
                .operations
                .values()
                .filter(|operation| operation.phase == OperationPhase::Terminal)
                .map(|operation| &operation.spec.operation_id)
                .collect::<BTreeSet<_>>();
            prop_assert_eq!(completed, terminal);
        }
    }
}

fn check_model_claim_orderings(scheduler: &Scheduler, remaining: &[SchedulerEvent]) -> u64 {
    if remaining.is_empty() {
        return 1;
    }
    let used = |state: &Scheduler| {
        state
            .model_claims
            .values()
            .map(TaskModelClaims::used)
            .sum::<u64>()
    };
    let mut leaves = 0;
    for (index, event) in remaining.iter().enumerate() {
        let mut next = scheduler.clone();
        let accepted = next.apply(event, &mut None).is_ok();
        if !accepted {
            assert_eq!(&next, scheduler, "rejected event mutated state: {event:?}");
        }
        if matches!(event, SchedulerEvent::LeaseReleased { .. }) {
            assert!(!accepted, "stale fence released ownership: {event:?}");
        }
        if let SchedulerEvent::ModelDispatchClaimed {
            operation_id,
            attempt_id,
            step,
            request_digest,
            ..
        } = event
        {
            if scheduler
                .operation(*operation_id)
                .is_some_and(|op| op.cancellation_requested)
            {
                assert!(!accepted, "cancelled owner charged work: {event:?}");
            }
            if let Some(retained) = scheduler
                .model_claims
                .get(operation_id)
                .and_then(|claims| claims.attempts.get(attempt_id))
                .and_then(|steps| steps.get(step))
                && retained != request_digest
            {
                assert!(!accepted, "changed request accepted as retry: {event:?}");
            }
            if accepted {
                let existing = scheduler
                    .model_claims
                    .get(operation_id)
                    .and_then(|claims| claims.attempts.get(attempt_id))
                    .is_some_and(|steps| steps.contains_key(step));
                assert_eq!(
                    used(&next),
                    used(scheduler) + u64::from(!existing),
                    "exact retry changed accounting: {event:?}"
                );
            }
        }
        assert!(used(&next) <= 2, "shared budget exceeded: {event:?}");
        for (op, prior) in &scheduler.model_claims {
            let current = next.model_claims.get(op).expect("claim refunded");
            assert_eq!(current.ceiling, prior.ceiling);
            for (attempt, steps) in &prior.attempts {
                for (step, digest) in steps {
                    assert_eq!(
                        current
                            .attempts
                            .get(attempt)
                            .and_then(|retained| retained.get(step)),
                        Some(digest),
                        "retained request changed: {event:?}"
                    );
                }
            }
        }

        for op in [id(2), id(3)] {
            assert_eq!(
                next.operation(op).and_then(|op| op.reservation.as_ref()),
                scheduler
                    .operation(op)
                    .and_then(|op| op.reservation.as_ref()),
                "uncertainty released ownership: {event:?}"
            );
        }
        let mut pending = remaining.to_vec();
        pending.remove(index);
        leaves += check_model_claim_orderings(&next, &pending);
    }
    leaves
}
#[test]
fn all_model_claim_cancellation_and_uncertainty_orders_preserve_accounting() -> Result<()> {
    let mut scheduler = Scheduler::new();
    scheduler.apply(
        &scheduler.declare(spec(id(1), Orchestration::Join)?)?,
        &mut None,
    )?;
    scheduler.apply(
        &SchedulerEvent::SessionConfigured {
            operation_id: id(1),
            limits: SessionLimits {
                active_tasks: 2,
                total_tasks: 3,
                depth: 1,
                model_steps: 2,
            },
        },
        &mut None,
    )?;
    for (op, lease) in [(2, 0), (3, 1)] {
        let mut child = spec(id(op), Orchestration::Leaf)?;
        child.parent = Some(ParentLink {
            operation_id: id(1),
            slot: op.to_string(),
        });
        scheduler.apply(&scheduler.declare(child)?, &mut None)?;
        scheduler.apply(
            &event(&scheduler, Step::Admit(op, lease, 0))?
                .ok_or_else(|| Error::Invalid("model admission missing".into()))?,
            &mut None,
        )?;
        scheduler.apply(
            &event(&scheduler, Step::Start(op, lease))?
                .ok_or_else(|| Error::Invalid("model start missing".into()))?,
            &mut None,
        )?;
    }
    let actions = [
        Step::ModelClaim(2, 0, 0, 0, 1),
        Step::ModelClaim(2, 0, 0, 0, 1), // exact retry
        Step::ModelClaim(2, 0, 0, 0, 2), // changed request negative control
        Step::ModelClaim(3, 1, 0, 0, 1),
        Step::ModelClaim(2, 0, 1, 0, 1), // competing fresh attempt
        Step::Cancel(1, true),
        Step::Complete(2, 3, Some(0)), // uncertainty retains the lease
        Step::ReleaseLease(2, 1),      // another task's fence negative control
    ]
    .into_iter()
    .map(|step| {
        event(&scheduler, step)?.ok_or_else(|| Error::Invalid("model action missing".into()))
    })
    .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        check_model_claim_orderings(&scheduler, &actions),
        (1..=actions.len() as u64).product::<u64>()
    );
    Ok(())
}
