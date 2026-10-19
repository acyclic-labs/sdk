//! The OAuth HTTP client in a fresh process, before any rustls provider is
//! installed. Each case re-executes this binary so nothing earlier in the
//! process can have installed one.
use std::process::{Command, Output};

const MODE: &str = "ACYCLIC_OAUTH_PROVIDER_CHILD";
const BUILT: &str = "built OAuth client in a fresh process";

fn child(mode: &str) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fresh_process_child", "--nocapture"])
        .env(MODE, mode)
        .output()
        .unwrap()
}

#[test]
fn fresh_process_child() {
    // Only meaningful when re-executed by the test below.
    #[expect(
        clippy::disallowed_methods,
        reason = "fixed child-mode control set by this test's own parent; this target has no test-support seam"
    )]
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    match mode.as_str() {
        "harness" => drop(acyclic_harness::mcp::oauth::default_oauth_http_client().unwrap()),
        // The former re-export path: rmcp's constructor builds its own client.
        "rmcp" => drop(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(rmcp::transport::auth::AuthorizationManager::new(
                    "http://127.0.0.1:9/",
                ))
                .unwrap(),
        ),
        _ => unreachable!("unknown child mode"),
    }
    println!("{BUILT}");
}

#[test]
fn oauth_client_needs_no_preinstalled_provider() {
    let harness = child("harness");
    let stdout = String::from_utf8_lossy(&harness.stdout);
    assert!(
        harness.status.success() && stdout.contains(BUILT) && stdout.contains("1 passed"),
        "{stdout}\n{}",
        String::from_utf8_lossy(&harness.stderr)
    );
    // Negative control: the provider-unaware constructor fails in the same
    // fresh process for the missing provider, so the case above observes the
    // provider installation.
    let rmcp = child("rmcp");
    let (stdout, stderr) = (
        String::from_utf8_lossy(&rmcp.stdout),
        String::from_utf8_lossy(&rmcp.stderr),
    );
    assert!(
        !rmcp.status.success()
            && !stdout.contains(BUILT)
            && format!("{stdout}{stderr}").contains("No rustls crypto provider is configured"),
        "{stdout}
{stderr}"
    );
}
