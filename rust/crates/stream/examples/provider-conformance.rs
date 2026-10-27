//! Runs the public provider suite against an authority with a fresh conformance prefix.
use acyclic_stream::{conformance, http::HttpStream};
#[path = "provider_conformance/http_fixture.rs"]
mod http_fixture;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!(
            "STEP_FAILED stream-provider-conformance: {error}; recovery: set ACYCLIC_CONFORMANCE_ENDPOINT and ACYCLIC_CONFORMANCE_TOKEN, then cargo run --locked -p acyclic-stream --features http,conformance --example provider-conformance; for the local HTTPS fixture run node typescript/packages/stream/test/http-provider-conformance.mjs"
        );
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let (endpoint, token, fixture) = match arguments.as_slice() {
        [] => (
            std::env::var("ACYCLIC_CONFORMANCE_ENDPOINT")?,
            std::env::var("ACYCLIC_CONFORMANCE_TOKEN")?,
            false,
        ),
        [flag, endpoint] if flag == "--fixture" => (endpoint.clone(), "conformance".into(), true),
        _ => return Err("usage: provider-conformance [--fixture HTTPS_ENDPOINT]".into()),
    };
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
    if fixture {
        http_fixture::verify(&endpoint).await?;
    }
    println!("STREAM_CONFORMANCE passed");
    Ok(())
}
