//! Native workspace admission through the installed binary and shared boundary.
use super::binary;

#[test]
fn execution_reopen_keeps_the_named_agent_without_ambient_selection()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_harness::{
        agent::{AgentId, AgentPath},
        local::host_owner::HostAction,
    };
    let root = AgentPath::root(&AgentId::from_bytes([1; 16]));
    for expected in [root.clone(), root.child(&AgentId::from_bytes([2; 16]))?] {
        let arguments = ["reopen".into(), expected.to_string()];
        let action = acyclic_plugin::input::workspace_action(&arguments, || {
            panic!("explicit reopen consulted the ambient workspace")
        })?;
        let HostAction::Reopen { task } = action else {
            panic!("explicit reopen changed its operation")
        };
        assert_eq!(task, expected);
    }
    // A current-agent command must still use the original resolver and retain
    // its refusal. This distinguishes bypassing selection from dropping it.
    let cause = std::io::Error::other("selected agent refused");
    let error =
        acyclic_plugin::input::workspace_action(&["agents".into()], || Err(Box::new(cause)))
            .err()
            .ok_or("current-agent command dropped the original refusal")?;
    assert_eq!(
        error
            .downcast_ref::<std::io::Error>()
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::Other)
    );
    assert_eq!(error.to_string(), "selected agent refused");
    let invalid =
        acyclic_plugin::input::workspace_action(&["reopen".into(), "invalid".into()], || {
            panic!("invalid reopen consulted the ambient workspace")
        })
        .err()
        .ok_or("invalid explicit identity was admitted")?;
    assert!(invalid.source().is_some_and(|cause| {
        cause
            .downcast_ref::<acyclic_harness::agent::Error>()
            .is_some()
    }));
    Ok(())
}

#[test]
fn execution_native_admission_precedes_agent_resolution() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    for args in [
        vec!["unknown-command"],
        vec!["agents", "unexpected"],
        vec!["log", "unexpected"],
        vec!["capture", "unexpected"],
        vec!["fs"],
        vec!["discard"],
        vec!["reopen"],
        vec!["reopen", "invalid"],
        vec!["reopen", "invalid", "unexpected"],
        vec!["--task"],
        vec!["--task", "invalid"],
        vec!["--task", "invalid", "unknown-command"],
    ] {
        let output = std::process::Command::new(binary::acyclic())
            .args(&args)
            .env_clear()
            .env("ACYCLIC_TASK", "invalid")
            .current_dir(directory.path())
            .output()?;
        assert!(!output.status.success(), "accepted {args:?}");
        assert!(output.stdout.is_empty(), "stdout for {args:?}");
        let error = String::from_utf8(output.stderr)?;
        assert!(
            error.contains("step=admit native command:"),
            "{args:?}: {error}"
        );
        assert!(error.contains("recovery: run acyclic"), "{args:?}: {error}");
        if args == ["reopen", "invalid"] {
            assert!(error.contains("invalid agent path:"), "{args:?}: {error}");
        }
    }
    // A valid operation must reach the original selection path. The invalid
    // environment value is a negative control against refusing every command.
    let output = std::process::Command::new(binary::acyclic())
        .arg("agents")
        .env_clear()
        .env("ACYCLIC_TASK", "invalid")
        .current_dir(directory.path())
        .output()?;
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr)?;
    assert!(!error.contains("step=admit native command:"), "{error}");
    assert!(error.contains("invalid agent path:"), "{error}");
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}
