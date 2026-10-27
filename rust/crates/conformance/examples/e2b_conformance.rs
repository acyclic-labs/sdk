//! Runs the unchanged canonical suite against E2B or an account-free Embed host.
mod support;
use acyclic_machines::provider_core::e2b::E2b;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("E2B_API_URL").unwrap_or_else(|_| "https://api.e2b.app".into());
    let domain = std::env::var("E2B_GUEST_DOMAIN").unwrap_or_else(|_| "e2b.app".into());
    let proxy = std::env::var("E2B_ENVD_PROXY").ok();
    let driver = E2b::new(
        &endpoint,
        &support::required("E2B_API_KEY")?,
        &domain,
        proxy.as_deref(),
        Duration::from_secs(3600),
    )?;
    // Exact template_id:build_uuid; shared core refuses mutable defaults/tags.
    let image = support::required("E2B_TEMPLATE_ID")?;
    let workspace = std::env::var("E2B_WORKSPACE").unwrap_or_else(|_| "/workspace".into());
    support::run(
        driver,
        image,
        workspace,
        "cargo run --locked -p acyclic-conformance --features e2b,machines-shell --example e2b-conformance",
    )
    .await
}
