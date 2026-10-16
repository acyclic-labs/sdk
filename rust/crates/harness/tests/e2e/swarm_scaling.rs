use super::{TestResult, scenario, swarm, swarm_case::SwarmCase, swarm_wire::Fault};
use proptest::prelude::*;
use std::{collections::BTreeMap, path::Path, time::Instant};

type Work = BTreeMap<&'static str, u64>;

fn case(agents: usize, attention_every: usize) -> TestResult<SwarmCase> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut value = scenario::load_value(&root, &root.join("swarm/load/one-thousand.json"))?;
    value.as_object_mut().ok_or("case")?.remove("driver");
    let mut case: SwarmCase = serde_json::from_value(value)?;
    case.agents = agents;
    // Exact doubling of attention work, including small shrinking examples.
    case.attention_every = attention_every;
    case.expect.wire = serde_json::json!({});
    Ok(case)
}

fn measured(case: SwarmCase, fault: Fault) -> TestResult<Work> {
    let report = futures::executor::block_on(swarm::run_with_fault(case, fault))?;
    let mut work = Work::new();
    for key in [
        "allocations",
        "allocated_bytes",
        "delivered_records",
        "discovered_children",
        "server_follows",
        "server_records",
        "attention",
        "full_records",
        "fold_items",
    ] {
        work.insert(key, report[key].as_u64().ok_or("missing work counter")?);
    }
    Ok(work)
}

fn units(work: &Work) -> u64 {
    [
        "server_follows",
        "server_records",
        "delivered_records",
        "discovered_children",
    ]
    .iter()
    .map(|key| work[key])
    .sum()
}

fn linear(small: &Work, doubled: &Work) -> TestResult {
    for name in [
        "server_follows",
        "server_records",
        "discovered_children",
        "attention",
        "full_records",
        "fold_items",
    ] {
        let (before, after) = (small[name], doubled[name]);
        if before == 0 || after < before || after > 2 * before {
            return Err(format!("nonlinear {name}: {before} -> {after}").into());
        }
    }
    // Latest-value conflation can change the number of forwarded records when
    // discovery crosses the server's 1,024-frame window. Each input record may
    // be delivered at most once. Allocation work per actual processed item must
    // not grow; these items are bounded by follows + 2*input records + children.
    for work in [small, doubled] {
        if work["delivered_records"] > work["server_records"] {
            return Err("wire delivered more records than the provider produced".into());
        }
    }
    for name in ["allocations", "allocated_bytes"] {
        if small[name] == 0 || doubled[name] * units(small) > small[name] * units(doubled) {
            return Err(
                format!("nonlinear {name} per processed item: {small:?} -> {doubled:?}").into(),
            );
        }
    }
    if doubled["attention"] != 2 * small["attention"]
        || doubled["discovered_children"] != 2 * small["discovered_children"]
        || doubled["full_records"] != small["full_records"]
        || doubled["fold_items"] != small["fold_items"]
    {
        return Err("swarm work changed fidelity or selected workload".into());
    }
    Ok(())
}

#[test]
fn one_and_two_thousand_agents_have_linear_work_and_reject_quadratic_copies() -> TestResult {
    let started = Instant::now();
    let small = measured(case(1_000, 100)?, Fault::None)?;
    let doubled = measured(case(2_000, 100)?, Fault::None)?;
    linear(&small, &doubled)?;
    assert!(small["server_follows"] >= 1_000);
    assert!(doubled["server_follows"] >= 2_000);
    let broken_small = measured(case(256, 1)?, Fault::CloneSwarmPerStatus)?;
    let broken_doubled = measured(case(512, 1)?, Fault::CloneSwarmPerStatus)?;
    let error =
        linear(&broken_small, &broken_doubled).expect_err("accepted quadratic status copies");
    assert!(error.to_string().contains("allocated_bytes"), "{error}");
    println!(
        "swarm scaling: 1k={small:?} 2k={doubled:?}; quadratic 256={} 512={} allocated bytes; elapsed={:?}",
        broken_small["allocated_bytes"],
        broken_doubled["allocated_bytes"],
        started.elapsed()
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Doubling agents with identical history and viewport cannot more than
    // double admitted follows/records/attention or increase heap work per item.
    #[test]
    fn doubling_swarm_size_preserves_the_linear_work_bound(agents in 24usize..65) {
        let small = case(agents, 1).and_then(|case| measured(case, Fault::None))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let doubled = case(2 * agents, 1).and_then(|case| measured(case, Fault::None))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        linear(&small, &doubled).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
