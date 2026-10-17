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
    let agents = case.agents as u64;
    let report = futures::executor::block_on(swarm::run_with_fault(case, fault, true))?;
    let mut work = Work::new();
    work.insert("agents", agents);
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

fn linear_counts(small: &Work, doubled: &Work) -> TestResult {
    for name in [
        "server_follows",
        "server_records",
        "delivered_records",
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
    for work in [small, doubled] {
        if work["delivered_records"] != work["server_records"] {
            return Err("paced delivery conflated or duplicated a provider record".into());
        }
    }
    // The parent membership volume is one persistent root child, independent
    // of agent count. Assert that exact overhead before comparing swarm work.
    for work in [small, doubled] {
        if work["discovered_children"] != work["agents"] + 1 {
            return Err("discovery must visit every agent and exactly one parent volume".into());
        }
    }
    if doubled["agents"] != 2 * small["agents"]
        || doubled["attention"] != 2 * small["attention"]
        || doubled["full_records"] != small["full_records"]
        || doubled["fold_items"] != small["fold_items"]
    {
        return Err("swarm work changed fidelity or selected workload".into());
    }
    Ok(())
}

fn linear(small: &Work, doubled: &Work) -> TestResult {
    linear_counts(small, doubled)?;
    // Same delivered history per agent: compare raw heap work, with no
    // normalization that could admit n*log(n) growth across this size change.
    for name in ["allocations", "allocated_bytes"] {
        if small[name] == 0 || doubled[name] > 2 * small[name] {
            return Err(format!("nonlinear {name}: {small:?} -> {doubled:?}").into());
        }
    }
    Ok(())
}

#[test]
fn one_and_two_thousand_agents_have_linear_work() -> TestResult {
    let started = Instant::now();
    let small = measured(case(1_000, 100)?, Fault::None)?;
    let doubled = measured(case(2_000, 100)?, Fault::None)?;
    linear(&small, &doubled)?;
    assert!(small["server_follows"] >= 1_000);
    assert!(doubled["server_follows"] >= 2_000);
    println!(
        "swarm scaling: 1k={small:?} 2k={doubled:?}; elapsed={:?}",
        started.elapsed()
    );
    Ok(())
}

#[test]
fn provider_n_log_n_and_quadratic_work_require_the_heap_assertion() -> TestResult {
    for fault in [Fault::LogarithmicProviderWork, Fault::QuadraticProviderWork] {
        let small = measured(case(128, 1)?, fault)?;
        let doubled = measured(case(256, 1)?, fault)?;
        // Mutation control: deleting the heap assertion leaves every protocol,
        // fidelity and persisted-byte assertion green on these same real runs.
        linear_counts(&small, &doubled)?;
        let error = linear(&small, &doubled).expect_err("accepted nonlinear provider work");
        assert!(
            error.to_string().starts_with("nonlinear allocations:"),
            "{fault:?}: {error}"
        );
        println!("{fault:?}: 128={small:?} 256={doubled:?}");
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Doubling agents with identical history and viewport cannot more than
    // double actual follows, records, attention or raw heap work.
    #[test]
    fn doubling_swarm_size_preserves_the_linear_work_bound(agents in 24usize..65) {
        let small = case(agents, 1).and_then(|case| measured(case, Fault::None))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let doubled = case(2 * agents, 1).and_then(|case| measured(case, Fault::None))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        linear(&small, &doubled).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
