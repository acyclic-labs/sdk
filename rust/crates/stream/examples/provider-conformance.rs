//! Runs the public provider suite against an authority with a fresh conformance prefix.
#[cfg(feature = "http")]
use acyclic_stream::{conformance, http::HttpStream};
#[cfg(feature = "http")]
#[path = "provider_conformance/http_fixture.rs"]
mod http_fixture;
#[cfg(feature = "postgres")]
#[path = "postgres_conformance/checks.rs"]
mod postgres_checks;
#[cfg(feature = "postgres")]
#[path = "postgres_conformance/scale.rs"]
mod postgres_scale;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        let recovery = if std::env::args().nth(1).as_deref() == Some("--postgres") {
            "pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh"
        } else if std::env::args().nth(1).as_deref() == Some("--fixture") {
            "node typescript/packages/stream/test/http-provider-conformance.mjs"
        } else {
            "set ACYCLIC_CONFORMANCE_ENDPOINT and ACYCLIC_CONFORMANCE_TOKEN, then cargo run --locked -p acyclic-stream --features http,conformance --example provider-conformance"
        };
        eprintln!("STEP_FAILED stream-provider-conformance: {error}; recovery: {recovery}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [flag] if flag == "--postgres" => postgres(false).await,
        [flag, scale] if flag == "--postgres" && scale == "--scale" => postgres(true).await,
        _ => http(&arguments).await,
    }
}

#[cfg(feature = "postgres")]
async fn postgres(scale: bool) -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    if scale {
        postgres_scale::verify(&database).await
    } else {
        postgres_checks::verify(&database).await
    }
}

#[cfg(not(feature = "postgres"))]
async fn postgres(_: bool) -> Result<(), Box<dyn std::error::Error>> {
    Err("Postgres mode requires --features postgres,conformance; run bash scripts/backends/storage-conformance.sh".into())
}

#[cfg(not(feature = "http"))]
async fn http(_: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    Err("HTTP mode requires --features http,conformance".into())
}

#[cfg(feature = "http")]
async fn http(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, token, fixture) = match arguments {
        [] => (
            std::env::var("ACYCLIC_CONFORMANCE_ENDPOINT")?,
            std::env::var("ACYCLIC_CONFORMANCE_TOKEN")?,
            false,
        ),
        [flag, endpoint] if flag == "--fixture" => (endpoint.clone(), "conformance".into(), true),
        _ => {
            return Err(
                "usage: provider-conformance [--fixture HTTPS_ENDPOINT | --postgres [--scale]]"
                    .into(),
            );
        }
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
