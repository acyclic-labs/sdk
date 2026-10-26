//! Real PostgreSQL conformance and bounded resource checks; invoke on bb2.
#[path = "postgres_conformance/checks.rs"]
mod checks;
#[path = "postgres_conformance/scale.rs"]
mod scale;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("STORAGE_POSTGRES_URL").map_err(|_| "step=config: STORAGE_POSTGRES_URL missing; recovery: run bash scripts/backends/storage-conformance.sh")?;
    if std::env::args().any(|argument| argument == "--scale") {
        scale::verify(&database).await
    } else {
        checks::verify(&database).await
    }
}
