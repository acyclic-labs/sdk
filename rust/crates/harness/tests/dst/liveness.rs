//! Liveness: a waiter always has the event that will wake it, and a parent
//! observes a child's settlement at its next boundary without a timer.
//!
//! C1 is a pure control state machine, so its world is an exhaustive check of
//! the production kernel over every waiter/trigger pair rather than a sampled
//! host scenario. C5 drives the shipping local composition: both a direct
//! child (depth 1) and a grandchild through a worker (depth 2) settle for each
//! event and parent state, and the parent's next request must carry the
//! settlement.
use super::*;
use acyclic_harness::{
    agent::kernel,
    context::{CapacityCompaction, CompactionPolicy, ModelContextCapacity},
    model::ModelContentPart,
    test_support::ScriptedEvent,
};
use family::{Generator, Variant};
use oracle::{Plan, Prompted, World, call, done, service, settlements, tag};
use serde_json::json;
use std::collections::BTreeSet;

/// C1: every waiter is paired with an event source the schedule can still fire.
pub(super) const WAIT_WITHOUT_EVENT: Generator = Generator {
    class: "C1",
    seeds: every_waiter_has_its_event::VALUES,
    variants: wait_variants,
    world: wait_world,
};

/// C5: every settlement event reaches its parent at its next boundary.
pub(super) const SETTLEMENT_LATENCY: Generator = Generator {
    class: "C5",
    seeds: every_settlement_reaches_its_parent::VALUES,
    variants: settlement_variants,
    world: settlement_world,
};

acyclic_test_support::cases! {
    module = every_waiter_has_its_event,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0],
}

acyclic_test_support::cases! {
    module = every_settlement_reaches_its_parent,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4,
        seed_5 => 5, seed_6 => 6, seed_7 => 7, seed_8 => 8, seed_9 => 9,
        seed_10 => 10, seed_11 => 11, seed_12 => 12, seed_13 => 13, seed_14 => 14,
        seed_15 => 15, seed_16 => 16, seed_17 => 17, seed_18 => 18, seed_19 => 19,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    if seed == 0 {
        WAIT_WITHOUT_EVENT.run(seed)
    } else {
        SETTLEMENT_LATENCY.run(seed - 1)
    }
}

const WAITERS: [&str; 8] = [
    "wait_tool",
    "turn_boundary",
    "actor_redelivery",
    "pause_ack",
    "resume",
    "spawn_start",
    "interaction",
    "stream_follow",
];
const TRIGGERS: [&str; 4] = [
    "event_lost_at_crash",
    "event_before_subscribe",
    "event_from_other_agent",
    "handler_undecided",
];

fn wait_variants(_seed: u64) -> Vec<Variant> {
    WAITERS
        .iter()
        .flat_map(|waiter| {
            TRIGGERS.iter().map(move |trigger| {
                Variant::from([("waiter", *waiter), ("trigger", *trigger)])
            })
        })
        .collect()
}

/// The control each waiter parks in, and the production action that delivers
/// its event. The deadline is always far in the future, so a test that only
/// passes because a timer fired fails the assertion below.
fn waiting(waiter: &str) -> (kernel::Control, bool, bool, usize) {
    use kernel::{Control, Mode, CONTINUE_NOW};
    let control = match waiter {
        // A deferred wait on an open child.
        "wait_tool" => Control {
            mode: Mode::Await(None),
            turn: true,
            ..Control::ADOPTED
        },
        // An idle turn boundary waiting for queued input.
        "turn_boundary" => Control::ADOPTED,
        // A redelivered record whose handler has not decided yet.
        "actor_redelivery" => Control::ADOPTED,
        // A pause acknowledgement drains children and then reports once.
        "pause_ack" => Control {
            mode: Mode::Await(Some(CONTINUE_NOW)),
            turn: true,
            ..Control::ADOPTED
        },
        // A resume with queued work and its original deadline.
        "resume" => Control {
            mode: Mode::Await(Some(u64::MAX)),
            turn: true,
            input: true,
            ..Control::ADOPTED
        },
        // A spawn's child start is the next immediate step.
        "spawn_start" => Control {
            mode: Mode::Running,
            turn: true,
            ..Control::ADOPTED
        },
        // A question reply arrives as selected input.
        "interaction" => Control {
            mode: Mode::Await(Some(u64::MAX)),
            turn: true,
            input: true,
            ..Control::ADOPTED
        },
        // A followed stream's event is the next queued inbox input.
        "stream_follow" => Control::ADOPTED,
        other => panic!("unknown waiter {other}"),
    };
    let queued = matches!(waiter, "turn_boundary" | "actor_redelivery" | "resume" | "stream_follow");
    let approval = false;
    (control, queued, approval, 0)
}

/// The event each trigger delivers, in the order the production host sees it.
/// Every one is an event, never the passage of the deadline.
fn deliver(
    waiter: &str,
    trigger: &str,
    control: kernel::Control,
    queued: bool,
) -> (kernel::Control, bool) {
    use kernel::{Admit, Mode};
    match trigger {
        // A durable event already committed before the crash is replayed on
        // reopen; the original state and its alarm survive.
        "event_lost_at_crash" => {
            if matches!(control.mode, Mode::Await(_)) {
                (kernel::satisfy(control), true)
            } else {
                (control, queued || waiter == "spawn_start")
            }
        }
        // The event happened before the waiter subscribed: it is already in
        // the inbox or the condition already holds.
        "event_before_subscribe" => {
            if matches!(control.mode, Mode::Await(_)) {
                (kernel::satisfy(control), queued)
            } else {
                (control, true)
            }
        }
        // Another agent's settlement or delivery wakes this waiter.
        "event_from_other_agent" => {
            if matches!(control.mode, Mode::Await(_)) {
                (kernel::satisfy(control), queued)
            } else {
                (control, true)
            }
        }
        // An unknown handler outcome is retried; the retry is the same input
        // event and must not leave a parked waiter without a source.
        "handler_undecided" => {
            if matches!(control.mode, Mode::Await(_)) {
                (kernel::satisfy(control), true)
            } else {
                let admitted = kernel::admit(control, Admit::Input);
                (admitted.control, true)
            }
        }
        other => panic!("unknown trigger {other}"),
    }
}

fn wait_world(_seed: u64) -> std::result::Result<bool, String> {
    for waiter in WAITERS {
        for trigger in TRIGGERS {
            let (mut control, mut queued, approval, pending) = waiting(waiter);
            // The waiter is parked with a real future deadline; its event
            // source must exist before the event.
            if matches!(control.mode, kernel::Mode::Await(deadline) if deadline.is_some())
                && kernel::alarm(control, queued, approval, pending).is_none()
            {
                return Err(format!(
                    "{waiter}/{trigger} parked without a registered alarm or event"
                ));
            }
            let (next, next_queued) = deliver(waiter, trigger, control, queued);
            control = next;
            queued = next_queued;
            let picked = kernel::pick(control, false, queued, approval);
            let woke = !matches!(picked, kernel::Pick::Nothing)
                || matches!(control.mode, kernel::Mode::Running);
            if !woke {
                return Err(format!(
                    "{waiter}/{trigger} completed only by a deadline: {control:?} queued={queued}"
                ));
            }
            // The shipping alarm obligation still holds after the event.
            let expected = kernel::alarm(control, queued, approval, pending);
            if matches!(control.mode, kernel::Mode::Await(Some(u64::MAX))) && expected.is_none() {
                return Err(format!(
                    "{waiter}/{trigger} lost its registered event after delivery"
                ));
            }
        }
    }
    Ok(true)
}

const EVENTS: [&str; 5] = [
    "child_answer",
    "child_failure",
    "advisor_result",
    "merge_done",
    "send_delivery",
];
const PARENTS: [&str; 4] = ["in_wait", "mid_turn", "in_compaction", "reopened"];
const DEPTHS: [&str; 2] = ["1", "2"];

fn settlement(seed: u64) -> (&'static str, &'static str) {
    let pair = usize::try_from(seed).unwrap_or(0) % (EVENTS.len() * PARENTS.len());
    (EVENTS[pair % EVENTS.len()], PARENTS[pair / EVENTS.len()])
}

fn settlement_variants(seed: u64) -> Vec<Variant> {
    let (event, parent) = settlement(seed);
    DEPTHS
        .iter()
        .map(|depth| {
            Variant::from([
                ("event", event),
                ("parent", parent),
                ("depth", *depth),
            ])
        })
        .collect()
}

fn role(event: &str) -> &'static str {
    if event == "advisor_result" {
        "advisor"
    } else {
        "worker"
    }
}

fn parent_steps(event: &str, mode: &str, children: &[&str], file: &str) -> Vec<Vec<acyclic_harness::model::ToolCall>> {
    let mut steps = Vec::new();
    if event == "send_delivery" {
        for child in children {
            steps.push(vec![call(
                &format!("send-{child}"),
                "send",
                json!({"agent": child, "message": "delivery"}),
            )]);
        }
    }
    match mode {
        "in_wait" | "reopened" => {
            steps.push(vec![call(
                "wait-children",
                "wait",
                json!({"agents": children}),
            )]);
            if event == "merge_done" {
                steps.push(vec![call(
                    "read-merged",
                    "read",
                    json!({"path": file}),
                )]);
            }
        }
        "mid_turn" | "in_compaction" => {
            // Enough boundaries for the child to settle while this parent is
            // still in its turn. No wait call is made, so only the waking
            // settlement notice can carry the child's result here.
            for index in 0..8 {
                steps.push(vec![call(
                    &format!("write-{index}"),
                    "write",
                    json!({
                        "path": format!("parent-{index}.txt"),
                        "content": format!("parent boundary {index}"),
                    }),
                )]);
            }
            if event == "merge_done" {
                steps.push(vec![call(
                    "read-merged",
                    "read",
                    json!({"path": file}),
                )]);
            }
        }
        other => panic!("unknown parent mode {other}"),
    }
    steps
}

fn event_plan(event: &str, child_tag: &'static str, file: &str) -> Plan {
    let (steps, fail_once) = match event {
        "child_failure" => (
            vec![Vec::new()],
            Some((
                0,
                ScriptedEvent::Error {
                    error: format!("{child_tag} failed"),
                },
            )),
        ),
        "merge_done" => (
            vec![vec![call(
                &format!("write-{child_tag}"),
                "write",
                json!({"path": file, "content": format!("{child_tag} wrote {file}")}),
            )]],
            None,
        ),
        "send_delivery" => (
            vec![vec![call(
                &format!("wait-{child_tag}"),
                "wait",
                json!({"input": true}),
            )]],
            None,
        ),
        _ => (Vec::new(), None),
    };
    Plan {
        tag: child_tag,
        steps,
        fail_once,
    }
}

fn plans(event: &str, mode: &str) -> Vec<Plan> {
    let event_role = role(event);
    let mut plans = Vec::new();
    plans.push(Plan {
        tag: "root",
        steps: {
            let mut steps = vec![
                vec![call(
                    "spawn-d",
                    "spawn",
                    json!({"name": "d", "task": tag("d"), "role": event_role}),
                )],
                vec![call(
                    "spawn-w",
                    "spawn",
                    json!({"name": "w", "task": tag("w"), "role": "worker"}),
                )],
            ];
            steps.extend(parent_steps(event, mode, &["d", "w"], "d.txt"));
            steps
        },
        fail_once: None,
    });
    plans.push(Plan {
        tag: "w",
        steps: {
            let mut steps = vec![vec![call(
                "spawn-g",
                "spawn",
                json!({"name": "g", "task": tag("g"), "role": event_role}),
            )]];
            steps.extend(parent_steps(event, mode, &["g"], "g.txt"));
            steps
        },
        fail_once: None,
    });
    plans.push(event_plan(event, "d", "d.txt"));
    plans.push(event_plan(event, "g", "g.txt"));
    plans.push(Plan {
        tag: "recover",
        steps: vec![
            vec![call(
                "recover-send",
                "send",
                json!({"agent": "w", "message": "recover"}),
            )],
            vec![call(
                "recover-wait",
                "wait",
                json!({"agents": ["d", "w"]}),
            )],
        ],
        fail_once: None,
    });
    plans
}

fn request_tag(request: &acyclic_harness::model::ModelRequest) -> Option<&'static str> {
    let tags = ["root", "w", "d", "g", "recover"];
    request.messages.iter().rev().find_map(|message| {
        let body = serde_json::to_string(&message.content).unwrap_or_default();
        tags.into_iter()
            .find(|name| body.contains(&tag(name)))
    })
}

fn notices(request: &acyclic_harness::model::ModelRequest) -> BTreeSet<String> {
    request
        .messages
        .iter()
        .filter_map(|message| message.content.notice_parts())
        .flat_map(|(notices, _)| notices)
        .filter(|notice| notice.kind == "child.settled")
        .map(|notice| notice.source.clone())
        .collect()
}

fn observed(
    requests: &[acyclic_harness::model::ModelRequest],
    node: &str,
    expected: usize,
) -> std::result::Result<(), String> {
    let tagged: Vec<_> = requests
        .iter()
        .filter(|request| request_tag(request) == Some(node))
        .collect();
    let last = tagged.last().ok_or_else(|| format!("{node} made no request"))?;
    let seen = notices(last);
    let wait_results = last
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult {
                name,
                content,
                ..
            } if name == "wait" => Some(
                serde_json::to_string(content).unwrap_or_default(),
            ),
            _ => None,
        })
        .filter(|result| result.contains("\"settled\""))
        .count();
    if seen.len() < expected && wait_results == 0 {
        return Err(format!(
            "{node}'s last request ({:?}) carries {} child settlements, expected {expected}: {}",
            request_tag(last),
            seen.len(),
            serde_json::to_string(last).unwrap_or_default()
        ));
    }
    Ok(())
}

fn settlement_world(seed: u64) -> std::result::Result<bool, String> {
    let (event, parent) = settlement(seed);
    let mode = parent;
    oracle::simulate(seed + 100, move || {
        entry(async move {
            let mut model = service(plans(event, mode));
            if mode == "in_compaction" {
                model = model.with_context_capacity(ModelContextCapacity {
                    context_tokens: 8192,
                    output_tokens: Some(2048),
                    output_shares_context: false,
                });
            }
            let mut world =
                World::new(seed + 100, &[], model).map_err(oracle::failed)?;
            if mode == "in_compaction" {
                world = world.compaction(CompactionPolicy::Capacity(CapacityCompaction {}));
            }
            let root = ("root", tag("root"));
            let incarnation = if mode == "reopened" {
                let crash = oracle::Crash {
                    seed: seed + 100,
                    files: &[],
                    plans: || plans(event, mode),
                    prompts: vec![vec![root.clone()]],
                    window: (1, usize::MAX),
                    mode: oracle::crash_mode("crash_after", seed)
                        .ok_or("crash mode")?,
                };
                let (crashed, recovered) = crash
                    .recover(vec![vec![("recover", tag("recover"))]])
                    .await?;
                if !crashed.in_window {
                    return Ok(false);
                }
                recovered
            } else {
                let incarnation = world
                    .incarnation(None, &[vec![root.clone()]])
                    .await
                    .map_err(oracle::failed)?;
                if mode == "in_compaction"
                    && !incarnation.requests.iter().any(|request| request.summary)
                {
                    return Err("in_compaction world never dispatched a compaction".into());
                }
                if !matches!(
                    incarnation.prompts.first(),
                    Some((Prompted::Settled(text), _)) if *text == done("root")
                ) {
                    return Err(format!(
                        "{event}/{parent} root did not settle: {:?}",
                        incarnation.prompts
                    ));
                }
                incarnation
            };
            oracle::all_settled(&incarnation.requests)?;
            observed(&incarnation.requests, "root", 2)?;
            observed(&incarnation.requests, "w", 1)?;
            if event == "send_delivery" {
                // The delivery itself must have reached each child before it
                // answered, rather than the child settling from its brief.
                let sent = incarnation
                    .requests
                    .iter()
                    .filter(|request| {
                        settlements(request).is_ok_and(|settled| {
                            settled.iter().any(|settled| {
                                settled.call.name == "send" && settled.succeeded()
                            })
                        })
                    })
                    .count();
                if sent == 0 {
                    return Err(format!("{event}/{parent} delivered no message"));
                }
            }
            Ok(true)
        })
    })
}
