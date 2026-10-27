//! Runs the public provider suite against an authority with a fresh conformance prefix.
use acyclic_stream::{conformance, http::HttpStream};
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ACYCLIC_CONFORMANCE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_CONFORMANCE_TOKEN")?;
    let ca = std::env::var("ACYCLIC_CONFORMANCE_CA")
        .ok()
        .map(std::fs::read)
        .transpose()?;
    let provider =
        HttpStream::with_ca_certificate(&endpoint, &token, 16 * 1024 * 1024, ca.as_deref())?;
    tokio::time::timeout(
        std::time::Duration::from_secs(9),
        conformance::verify(&acyclic_stream::conformance::Fixture::new(provider.clone())),
    )
    .await??;
    println!("STREAM_CONFORMANCE passed");
    Ok(())
}
