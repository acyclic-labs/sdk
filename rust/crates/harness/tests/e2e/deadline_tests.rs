//! Scenario outcomes may depend only on readiness: a scripted model turn (an exhausted script is an error),
//! a follow up to a cut that is already published (each change must strictly advance toward it), or a journal
//! replay that ends at an empty page. A wall-clock deadline turns host load into scenario failures, so the
//! scenario sources must not contain one. The separate shared progress guard observes native I/O; its idle
//! budget is infrastructure, never an expected scenario outcome or a per-scenario deadline.
use std::path::Path;

/// Wall-clock deadlines and sleeps, assembled so this file does not match itself.
fn forbidden() -> Vec<String> {
    let time = ["tokio", "::time::"].concat();
    vec![
        format!("{time}timeout("),
        format!("{time}timeout_at("),
        format!("{time}sleep("),
        format!("{time}sleep_until("),
        format!("{time}interval("),
        ["std::thread", "::sleep("].concat(),
        ["Duration", "::from_secs("].concat(),
        ["Duration", "::from_millis("].concat(),
    ]
}

fn deadlines(source: &str) -> Vec<String> {
    let forbidden = forbidden();
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| forbidden.iter().any(|token| line.contains(token.as_str())))
        .map(|(number, line)| format!("{}: {}", number + 1, line.trim()))
        .collect()
}

#[test]
fn scenario_sources_contain_no_wall_clock_deadline() -> crate::TestResult {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(tests.join("e2e"))? {
        sources.push(entry?.path());
    }
    // The shared support modules main.rs includes by path.
    // The single infrastructure guard and its virtual-clock controls live in
    // progress_guard{,_tests}.rs. Scenario semantics still cannot use a clock.
    for name in [
        "quiescent.rs",
        "scenario_data.rs",
        "swarm_session.rs",
        "progress_registry.rs",
    ] {
        sources.push(tests.join("support").join(name));
    }
    let mut found = Vec::new();
    for path in sources
        .iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
    {
        for hit in deadlines(&std::fs::read_to_string(path)?) {
            found.push(format!("{}:{hit}", path.display()));
        }
    }
    assert!(sources.len() > 40, "scanned only {} sources", sources.len());
    assert!(
        found.is_empty(),
        "wall-clock deadlines in e2e sources:\n{}",
        found.join("\n")
    );
    Ok(())
}

#[test]
fn the_scan_rejects_each_former_deadline_shape() {
    let time = ["tokio", "::time::"].concat();
    let former = format!(
        "{time}timeout(Duration{}5), drivers::run(case)).await??;",
        "::from_secs("
    );
    assert_eq!(deadlines(&former).len(), 1);
    let wrapped = format!(
        "    {time}timeout(\n        std::time::Duration{}5),\n",
        "::from_secs("
    );
    assert_eq!(deadlines(&wrapped).len(), 2);
    assert!(deadlines("drivers::run(case).await?;").is_empty());
}
