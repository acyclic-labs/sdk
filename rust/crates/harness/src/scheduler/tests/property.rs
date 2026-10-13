use super::*;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Exercise {
    Event(SchedulerEvent),
    Orchestrate(u8),
    Advance(u8, u8),
}

fn declaration(
    op: u8,
    parent: Option<u8>,
    dependency: Option<u8>,
    kind: u8,
    resources: u64,
) -> Result<SchedulerEvent> {
    let mut declared = spec(
        id(op),
        match kind {
            0 => Orchestration::Leaf,
            1 => Orchestration::Join,
            _ => Orchestration::Race,
        },
    )?;
    declared.dependencies.extend(dependency.map(id));
    declared.resources = cpu(resources);
    if let Some(parent) = parent {
        declared.owner = spec(id(parent), Orchestration::Leaf)?.owner;
        declared.parent = Some(ParentLink {
            operation_id: id(parent),
            slot: format!("slot-{op}"),
        });
    }
    Ok(SchedulerEvent::Declared {
        spec: Box::new(declared),
    })
}

fn completion(op: u8, kind: u8, lease: Option<u8>) -> Result<SchedulerEvent> {
    Ok(SchedulerEvent::Completed {
        operation_id: id(op),
        outcome: match kind {
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
    })
}

fn exercise() -> impl Strategy<Value = Exercise> {
    let op = || 1..6_u8;
    let lease = || prop_oneof![9 => Just(0_u8), 1 => Just(1_u8)];
    let checkpoint = CheckpointRef::new(
        ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)
            .expect("provider"),
        vec![1],
        None,
    )
    .expect("checkpoint");
    let events = prop_oneof![
        1 => ((4..6_u8, proptest::option::of(op()), proptest::option::weighted(0.2, op())), 0..3_u8, 0..2_u64)
            .prop_map(|((op, parent, dependency), kind, resources)|
                declaration(op, parent, dependency, kind, resources).expect("declaration")),
        2 => op().prop_map(|op| SchedulerEvent::WaitingForCapacity { operation_id: id(op) }),
        4 => (op(), lease(), 0..3_u64).prop_map(|(op, lease, amount)| SchedulerEvent::Admitted {
            operation_id: id(op),
            reservation: reservation(lease, amount),
        }),
        1 => (op(), lease(), 0..3_u64).prop_map(|(op, lease, amount)| SchedulerEvent::PartiallyAdmitted {
            operation_id: id(op),
            reservation: reservation(lease, amount),
        }),
        1 => op().prop_map(|op| SchedulerEvent::Rejected {
            operation_id: id(op),
            reason: "rejected".into(),
        }),
        4 => (op(), lease()).prop_map(|(op, lease)| SchedulerEvent::Started {
            operation_id: id(op),
            fence: fence(lease),
        }),
        1 => (op(), lease()).prop_map(move |(op, lease)| SchedulerEvent::Checkpointed {
            operation_id: id(op),
            checkpoint: checkpoint.clone(),
            fence: fence(lease),
        }),
        3 => (op(), lease()).prop_map(|(op, lease)| SchedulerEvent::WaitingForChildren {
            operation_id: id(op),
            fence: fence(lease),
        }),
        1 => (op(), lease()).prop_map(|(op, lease)| SchedulerEvent::LeaseReleased {
            operation_id: id(op),
            fence: fence(lease),
        }),
        1 => (op(), any::<bool>()).prop_map(|(op, recursive)| SchedulerEvent::CancellationRequested {
            operation_id: id(op),
            recursive,
        }),
        4 => (op(), lease(), 0..2_u8, 0..3_u8, 0..4_u8)
            .prop_map(|(op, lease, attempt, step, digest)| claim(op, lease, attempt, step, digest)),
        4 => (op(), 0..4_u8, proptest::option::weighted(0.8, lease()))
            .prop_map(|(op, kind, lease)| completion(op, kind, lease).expect("completion")),
    ].prop_map(Exercise::Event);
    prop_oneof![
        27 => events,
        4 => op().prop_map(Exercise::Orchestrate),
        12 => (op(), any::<u8>()).prop_map(|(op, choice)| Exercise::Advance(op, choice)),
    ]
}

fn event(scheduler: &Scheduler, exercise: Exercise) -> Result<Option<SchedulerEvent>> {
    match exercise {
        Exercise::Event(event) => Ok(Some(event)),
        Exercise::Advance(op, choice) => {
            let Some(state) = scheduler.operation(id(op)) else {
                return Ok(None);
            };
            let lease = u8::from(
                state
                    .reservation
                    .as_ref()
                    .is_some_and(|held| held.id == "lease-1"),
            );
            Ok(Some(match state.phase {
                OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity => {
                    SchedulerEvent::Admitted {
                        operation_id: id(op),
                        reservation: reservation(lease, 1),
                    }
                }
                OperationPhase::Admitted => SchedulerEvent::Started {
                    operation_id: id(op),
                    fence: fence(lease),
                },
                OperationPhase::Running if state.spec.orchestration != Orchestration::Leaf => {
                    SchedulerEvent::WaitingForChildren {
                        operation_id: id(op),
                        fence: fence(lease),
                    }
                }
                OperationPhase::Running => completion(op, choice % 4, Some(lease))?,
                OperationPhase::WaitingForChildren => {
                    return event(scheduler, Exercise::Orchestrate(op));
                }
                OperationPhase::Reconciling => completion(op, choice % 3, None)?,
                OperationPhase::Suspended | OperationPhase::Terminal => return Ok(None),
            }))
        }
        Exercise::Orchestrate(op) => {
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
            Ok(Some(SchedulerEvent::Orchestrated {
                operation_id: id(op),
                expected_revision: parent.revision,
                outcome,
                cancel,
                reducer: None,
                reduction_digest,
            }))
        }
    }
}

fn check_lifecycle(
    orchestration: u8,
    steps: Vec<Exercise>,
) -> proptest::test_runner::TestCaseResult {
    let fail = |error: Error| TestCaseError::fail(error.to_string());
    let mut scheduler = Scheduler::new();
    for (op, parent) in [(1, None), (2, Some(1)), (3, Some(1))] {
        let declared = declaration(
            op,
            parent,
            None,
            if parent.is_none() { orchestration } else { 0 },
            1,
        )
        .map_err(fail)?;
        scheduler.apply(&declared, &mut None).map_err(fail)?;
        if parent.is_none() {
            scheduler
                .apply(
                    &SchedulerEvent::SessionConfigured {
                        operation_id: id(op),
                        limits: SessionLimits {
                            active_tasks: 2,
                            total_tasks: 5,
                            depth: 3,
                            model_steps: 2,
                        },
                    },
                    &mut None,
                )
                .map_err(fail)?;
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
            prop_assert!(
                operation.reservation.is_none()
                    || matches!(
                        operation.phase,
                        OperationPhase::WaitingForCapacity
                            | OperationPhase::Admitted
                            | OperationPhase::Running
                            | OperationPhase::Reconciling
                    )
            );
        }
        prop_assert!(claims_preserved(&before, &scheduler));
        for (root, limits) in &scheduler.sessions {
            let used = scheduler
                .model_claims
                .iter()
                .filter(|(op, _)| {
                    scheduler
                        .session_root(**op, &mut None)
                        .is_ok_and(|(candidate, _)| candidate == *root)
                })
                .map(|(_, claims)| claims.used())
                .sum::<u64>();
            prop_assert!(used <= limits.model_steps);
        }
        prop_assert!(
            scheduler
                .model_claims
                .values()
                .all(|claims| claims.used() <= claims.ceiling)
        );
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
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn scheduler_apply_preserves_lifecycle_invariants(
        orchestration in 0..3_u8,
        steps in proptest::collection::vec(exercise(), 1..64),
    ) {
        check_lifecycle(orchestration, steps)?;
    }
}
