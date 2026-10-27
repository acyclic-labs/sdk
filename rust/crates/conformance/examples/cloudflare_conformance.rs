//! Runs the canonical Machines suite against a disposable Containers Worker.
mod support;
use acyclic_machines::provider_core::cloudflare::Cloudflare;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:15440".into());
    let token = std::env::var_os("CLOUDFLARE_MACHINES_TRANSPORT_TOKEN")
        .map(|_| support::required("CLOUDFLARE_MACHINES_TRANSPORT_TOKEN"))
        .transpose()?;
    support::run(
        Cloudflare::new(&endpoint, token.as_deref())?,
        "workspace".into(),
        "/workspace".into(),
        "bash scripts/qualify-machines-cloudflare.sh",
    )
    .await
}
