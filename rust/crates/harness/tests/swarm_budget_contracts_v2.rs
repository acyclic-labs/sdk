use acyclic_harness::{
    Error, IdempotencyKey, OperationId,
    swarm_budget::{
        SwarmBudget, SwarmBudgetEvent, SwarmBudgetLimits, SwarmForkRequest, SwarmOwnerFence,
        SwarmReservationState, SwarmResourceRequest, SwarmUsage,
    },
};
use std::{
    sync::{Arc, Barrier},
    thread,
};

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn owner(generation: u64) -> SwarmOwnerFence {
    SwarmOwnerFence::new(format!("budget-worker-{generation}"), generation).expect("owner")
}

fn limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents: 8,
        max_total_agents: 16,
        max_recursion_depth: 3,
        max_model_steps: 64,
        max_output_bytes: 640,
        max_execution_time_ms: 640,
    }
}

fn request(operation_id: OperationId, key: &str, depth: u32) -> SwarmForkRequest {
    SwarmForkRequest {
        operation_id,
        idempotency_key: IdempotencyKey::new(key).expect("idempotency key"),
        parent_operation_id: None,
        depth,
        resources: SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 40,
            execution_time_ms: 40,
        },
        admission_digest: None,
    }
}

fn assert_conflict<T>(result: acyclic_harness::Result<T>) {
    assert!(
        matches!(result, Err(Error::Conflict(_))),
        "expected typed conflict"
    );
}

#[test]
fn public_budget_enforces_each_configurable_ceiling() -> acyclic_harness::Result<()> {
    let mut constrained = limits();
    constrained.max_active_agents = 1;
    constrained.max_total_agents = 1;
    let budget = SwarmBudget::new(operation(1), owner(0), constrained)?;
    assert_conflict(budget.reserve_child(request(operation(2), "active-limit", 1)));

    let mut constrained = limits();
    constrained.max_active_agents = 2;
    constrained.max_total_agents = 2;
    let budget = SwarmBudget::new(operation(3), owner(0), constrained)?;
    budget.reserve_child(request(operation(4), "total-limit", 1))?;
    assert_conflict(budget.reserve_child(request(operation(5), "total-limit-2", 1)));

    let mut constrained = limits();
    constrained.max_recursion_depth = 1;
    let budget = SwarmBudget::new(operation(6), owner(0), constrained)?;
    let parent = budget.reserve_child(request(operation(7), "depth-parent", 1))?;
    let mut child = request(operation(8), "depth-child", 2);
    child.parent_operation_id = Some(parent.reservation.operation_id);
    assert_conflict(budget.reserve_child(child));

    for (field, key) in [
        ("max_model_steps", "model-step-limit"),
        ("max_output_bytes", "output-limit"),
        ("max_execution_time_ms", "execution-limit"),
    ] {
        let mut constrained = limits();
        let request = match field {
            "max_model_steps" => {
                constrained.max_model_steps = 3;
                SwarmForkRequest {
                    resources: SwarmResourceRequest {
                        model_steps: 4,
                        output_bytes: 40,
                        execution_time_ms: 40,
                    },
                    ..request(operation(9), key, 1)
                }
            }
            "max_output_bytes" => {
                constrained.max_output_bytes = 39;
                SwarmForkRequest {
                    resources: SwarmResourceRequest {
                        model_steps: 4,
                        output_bytes: 40,
                        execution_time_ms: 40,
                    },
                    ..request(operation(10), key, 1)
                }
            }
            _ => {
                constrained.max_execution_time_ms = 39;
                SwarmForkRequest {
                    resources: SwarmResourceRequest {
                        model_steps: 4,
                        output_bytes: 40,
                        execution_time_ms: 40,
                    },
                    ..request(operation(11), key, 1)
                }
            }
        };
        let budget = SwarmBudget::new(operation(12), owner(0), constrained)?;
        assert_conflict(budget.reserve_child(request));
    }
    Ok(())
}

#[test]
fn public_admission_is_atomic_under_concurrent_capacity_race() {
    let budget = Arc::new(
        SwarmBudget::new(
            operation(20),
            owner(0),
            SwarmBudgetLimits {
                max_active_agents: 4,
                max_total_agents: 4,
                ..limits()
            },
        )
        .expect("budget"),
    );
    let start = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|index| {
            let budget = Arc::clone(&budget);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                budget.reserve_child(request(operation(21 + index), &format!("race-{index}"), 1))
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().expect("admission worker"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 3);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 5);
    assert_eq!(budget.usage().expect("usage").active_agents, 4);
    assert_eq!(budget.usage().expect("usage").total_agents, 4);
}

#[test]
fn public_replay_preserves_live_capacity_and_takeover_fences_stale_owner() {
    let session = operation(30);
    let old_owner = owner(0);
    let budget = SwarmBudget::new(session, old_owner.clone(), limits()).expect("budget");
    let admission = budget
        .reserve_child(request(operation(31), "restart-child", 1))
        .expect("admission");
    let replayed = SwarmBudget::replay([
        SwarmBudgetEvent::Started {
            session_id: session,
            owner: old_owner.clone(),
            limits: limits(),
            root_dispatch_id: None,
        },
        admission.durable_event(),
    ])
    .expect("replay");
    let usage = replayed.usage().expect("replayed usage");
    assert_eq!(usage.active_agents, 2);
    assert_eq!(usage.total_agents, 2);
    assert_eq!(
        replayed
            .reservation(operation(31))
            .expect("reservation")
            .expect("live reservation")
            .state,
        SwarmReservationState::Reserved
    );

    let new_owner = replayed
        .takeover(&old_owner, "budget-worker-restarted")
        .expect("takeover");
    assert_eq!(new_owner.generation, 1);
    assert_conflict(replayed.cancel(operation(31), &old_owner));
    replayed
        .cancel(operation(31), &new_owner)
        .expect("cancel after takeover");
    let final_usage = replayed.usage().expect("final usage");
    assert_eq!(final_usage.active_agents, 1);
    assert_eq!(final_usage.total_agents, 2);
    assert_eq!(final_usage.reserved, SwarmUsage::default());
}
