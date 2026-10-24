use super::*;
use std::cell::Cell;

thread_local! {
    static OMIT_INITIALIZATION: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn skip_initialization() -> bool {
    OMIT_INITIALIZATION.get()
}

const PROCESS_CASE: &str = "ACYCLIC_TEST_COLD_SOURCE_CASE";

#[tokio::test]
async fn cold_source_process_probe() -> Result<()> {
    let Ok(case) = std::env::var(PROCESS_CASE) else {
        return Ok(());
    };
    assert!(matches!(case.as_str(), "ordinary" | "omit"));
    OMIT_INITIALIZATION.set(case == "omit");
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted(std::iter::empty::<ScriptedTurn>());
    let result = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model()?, provider))?
            .without_default_tools()
            .goal_continuations(None),
    )
    .await;
    OMIT_INITIALIZATION.set(false);
    result?.close().await
}

#[test]
fn cold_checkout_startup_initializes_source_services_with_omission_control() {
    for (case, expected_success) in [("ordinary", true), ("omit", false)] {
        // A separate process keeps CoreServices cold for both implementations;
        // running the control after a successful in-process open proves nothing.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "local::tests::cold_source::cold_source_process_probe",
                "--nocapture",
            ])
            .env(PROCESS_CASE, case)
            .output()
            .unwrap();
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.success(),
            expected_success,
            "{case}: {diagnostic}"
        );
        if !expected_success {
            assert!(
                diagnostic.contains("source does not support original native work custody"),
                "control failed for another reason: {diagnostic}"
            );
        }
    }
}
