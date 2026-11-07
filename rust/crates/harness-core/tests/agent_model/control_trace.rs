//! Trace conformance added to the original runtime and quorum controls.
//! The provider/driver remain those controls' original implementations. This
//! qualifies kernel decisions, not native storage or a real model provider.

use super::{Result, id};
use acyclic_harness_core::agent::kernel as production;
use acyclic_harness_core::agent::{
    AgentId, AgentPath,
    control_trace::{Decision, Event},
    kernel::{CONTINUE_NOW, Pick},
};
use acyclic_test_support::{differential::observe_async, trace};
use std::collections::BTreeSet;

#[path = "../formal/agent_control_contract.rs"]
mod contract;

struct KernelModel {
    agents: BTreeSet<AgentPath>,
}

impl trace::Model for KernelModel {
    type Event = Event;
    type Error = &'static str;

    fn action(event: &Event) -> &str {
        event.decision.action()
    }

    fn transition(&mut self, event: &Event) -> std::result::Result<(), Self::Error> {
        if !self.agents.contains(&event.agent) {
            return Err("foreign agent decision");
        }
        if let Decision::Alarm(control, queued, approval, pending, actual) = event.decision
            && actual.is_some() != contract::runnable(control, queued, approval, pending)
        {
            return Err("shipping alarm violates the formal runnable obligation");
        }
        if !event.decision.conforms() {
            return Err("shipping decision differs from the proof kernel");
        }
        Ok(())
    }

    fn finish(&mut self) -> std::result::Result<(), Self::Error> {
        // These captures are finite decision prefixes. The original tests
        // separately assert committed state/effects; no terminal is inferred.
        Ok(())
    }
}

fn check(events: &[Event], agents: &[AgentPath], required: &[&str]) -> Result {
    let (inventory, mapping) = crate::agent_control_mapping::original()?;
    let model = || KernelModel {
        agents: agents.iter().cloned().collect(),
    };
    let coverage = trace::validate(&inventory, &mapping, events, &mut model())
        .map_err(|error| error.to_string())?;
    assert_eq!(coverage.events, events.len());
    for action in required {
        assert!(
            coverage.actions.contains(*action),
            "missing original {action} decision"
        );
    }
    for agent in agents {
        assert!(
            events.iter().any(|event| &event.agent == agent),
            "missing agent {agent:?}"
        );
    }

    // Independently corrupt one actual result for every observed action. No
    // fabricated execution supplies the positive or the negative input.
    let mut changed_actions = BTreeSet::new();
    for (index, event) in events.iter().enumerate() {
        if !changed_actions.insert(event.decision.action()) {
            continue;
        }
        let mut changed = events.to_vec();
        match &mut changed[index].decision {
            Decision::Admit(_, _, result) => result.enqueue = !result.enqueue,
            Decision::Pick(_, _, _, _, result) => {
                *result = if *result == Pick::Nothing {
                    Pick::Front
                } else {
                    Pick::Nothing
                };
            }
            Decision::Settle(_, _, result) | Decision::Report(_, result) => result.1 = !result.1,
            Decision::Cascade(_, _, result) => result.report = !result.report,
            Decision::Alarm(_, _, _, _, result) => {
                *result = if result.is_some() {
                    None
                } else {
                    Some(CONTINUE_NOW)
                };
            }
            Decision::Satisfy(_, result) => result.closed = !result.closed,
        }
        assert_eq!(
            trace::validate(&inventory, &mapping, &changed, &mut model()),
            Err(trace::Error::Transition {
                index,
                source: if matches!(event.decision, Decision::Alarm(..)) {
                    "shipping alarm violates the formal runnable obligation"
                } else {
                    "shipping decision differs from the proof kernel"
                },
            })
        );
    }
    let mut changed = events.to_vec();
    changed[0].agent = AgentPath::root(&id(9));
    assert_eq!(
        trace::validate(&inventory, &mapping, &changed, &mut model()),
        Err(trace::Error::Transition {
            index: 0,
            source: "foreign agent decision"
        })
    );
    assert_eq!(
        trace::validate(&inventory, &mapping, std::iter::empty(), &mut model()),
        Err(trace::Error::Empty)
    );
    Ok(())
}

pub(super) async fn runtime(future: impl std::future::Future<Output = Result>) -> Result {
    let (result, events) = observe_async::<_, Event>(future).await;
    result?;
    let root = AgentPath::root(&id(1));
    let child = root.child(&AgentId::derive(&root.id(), "c"))?;
    check(
        &events,
        &[root, child],
        &["admit", "pick", "settle", "cascade", "alarm"],
    )
}

pub(super) async fn quorum(future: impl std::future::Future<Output = Result>) -> Result {
    let (result, events) = observe_async::<_, Event>(future).await;
    result?;
    check(
        &events,
        &[AgentPath::root(&id(1))],
        &["admit", "pick", "settle", "satisfy", "alarm"],
    )
}

pub(super) async fn checkpoint(
    future: impl std::future::Future<Output = Result<Vec<AgentPath>>>,
) -> Result {
    let (agents, events) = observe_async::<_, Event>(future).await;
    check(
        &events,
        &agents?,
        &["admit", "pick", "settle", "report", "alarm"],
    )
}
