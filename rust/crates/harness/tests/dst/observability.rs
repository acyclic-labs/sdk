//! Run records carry the effective prompt, timed tool spans and model usage.
use super::*;
use acyclic_harness::executor::ExecutionEvent;

pub(super) const SEEDS: &[u64] = &[0];

pub(super) const GENERATOR: Generator = Generator {
    class: "O1",
    seeds: SEEDS,
    variants,
    world,
};

fn variants(_: u64) -> Vec<Variant> {
    vec![BTreeMap::from([
        ("span", "tool_call"),
        ("fault", "none"),
    ])]
}

pub(super) fn world(seed: u64) -> std::result::Result<bool, String> {
    oracle::simulate(seed, || {
        entry(async move {
            let plans = vec![Plan {
                tag: "observe",
                steps: vec![vec![oracle::call(
                    "tool",
                    "write",
                    serde_json::json!({"path": "observed.txt", "content": "observed"}),
                )]],
                fail_once: None,
            }];
            let mut world = World::new(seed, &[], oracle::service(plans)).map_err(failed)?;
            let incarnation = world
                .incarnation(None, &[vec![("observe", oracle::tag("observe"))]])
                .await
                .map_err(failed)?;
            if incarnation.crashed.is_some() {
                return Ok(false);
            }
            let (mut prompt, mut start, mut complete, mut usage) = (false, false, false, false);
            for record in &incarnation.records {
                match &record.event {
                    ExecutionEvent::Prompt { step: 0, .. } => prompt = true,
                    ExecutionEvent::ToolStarted { call_id, .. } if call_id == "tool" => {
                        start = record.committed_at_micros.is_some();
                    }
                    ExecutionEvent::ToolCompleted { call_id, .. } if call_id == "tool" => {
                        complete = record.committed_at_micros.is_some();
                    }
                    ExecutionEvent::Model { usage: Some(_), .. }
                    | ExecutionEvent::ModelInline { usage: Some(_), .. } => usage = true,
                    _ => {}
                }
            }
            if prompt && start && complete && usage {
                Ok(true)
            } else {
                Err(format!(
                    "run record lacks prompt={prompt}, tool start time={start}, tool completion time={complete}, or usage={usage}"
                ))
            }
        })
    })
}
