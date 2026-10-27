//! Runs the unchanged canonical suite against a Modal Sandbox V2 deployment.
mod support;
use acyclic_machines::provider_core::modal::Modal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("MODAL_API_URL").unwrap_or_else(|_| "https://api.modal.com".into());
    let driver = Modal::new(
        &endpoint,
        &support::required("MODAL_TOKEN_ID")?,
        &support::required("MODAL_TOKEN_SECRET")?,
        &support::required("MODAL_APP_ID")?,
    )?;
    support::run(
        driver,
        support::required("MODAL_IMAGE_ID")?,
        "/workspace".into(),
        "cargo run --locked -p acyclic-conformance --features modal,machines-shell --example modal-conformance",
    )
    .await
}
