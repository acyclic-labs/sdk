//! Local real-issuer qualification; no production account or credential discovery.
use acyclic_agent_host_conformance::{Backend, Result, Storage};
use acyclic_harness_core::oidc;
use acyclic_stream::{
    BoxProviderFuture, LocalStream, LocalStreamLimits, StreamProvider, UnixMillisClock as _,
};
use std::{path::PathBuf, sync::Arc};

struct Local;
struct Directory(tempfile::TempDir);
impl Storage for Directory {
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(
                Arc::new(LocalStream::open(self.0.path(), LocalStreamLimits::default()).await?)
                    as Arc<dyn StreamProvider>,
            )
        })
    }
}
impl Backend for Local {
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async { Ok(Box::new(Directory(tempfile::tempdir()?)) as Box<dyn Storage>) })
    }
    fn handler(
        &self,
        handler: Arc<dyn acyclic_actors::guest::Handler>,
    ) -> Result<Arc<dyn acyclic_actors::guest::Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

async fn run(directory: PathBuf) -> Result {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("manifest.json"))?)?;
    let field = |name: &str| {
        manifest
            .get(name)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("missing captured OIDC {name}"))
    };
    use sha2::Digest as _;
    for name in ["discovery", "jwks", "token", "auth"] {
        let expected = manifest
            .get("captures")
            .and_then(|captures| captures.get(name))
            .and_then(|capture| capture.get("sha256"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("missing OIDC body digest: {name}"))?;
        let bytes = std::fs::read(directory.join(format!("{name}.body")))?;
        if hex::encode(sha2::Sha256::digest(&bytes)) != expected {
            return Err(format!("captured OIDC body digest differs: {name}").into());
        }
    }
    let response: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("token.body"))?)?;
    let token = response
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .ok_or("Dex response has no ID token")?;
    let source = oidc::HttpSource::new()?;
    let mut verifier = oidc::Verifier::new(oidc::Config::loopback(
        field("issuer")?.into(),
        field("audience")?.into(),
        field("jwks_uri")?.into(),
        oidc::SigningAlgorithm::Rs256,
    )?);
    let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000;
    verifier.refresh(&source, now).await?;
    let started = std::time::Instant::now();
    acyclic_agent_host_conformance::verify(&Local).await?;
    acyclic_agent_host_conformance::verify_oidc(&Local, &verifier, token, field("subject")?, now)
        .await?;
    println!(
        "OIDC_LIVE_PASS complete-agent-host-suite,real-Dex-admission,scope-deadline elapsed_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

fn main() -> std::process::ExitCode {
    let directory = std::env::args_os().nth(1).map(PathBuf::from);
    let result = acyclic_native_runtime::coordination_runtime()
        .map_err(|error| error.to_string())
        .and_then(|runtime| {
            runtime
                .block_on(async {
                    run(directory.ok_or("usage: oidc-qualify CAPTURE_DIRECTORY")?).await
                })
                .map_err(|error| error.to_string())
        });
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "STEP_FAILED oidc-qualification: {error}; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/be-edge-oidc.sh"
            );
            std::process::ExitCode::FAILURE
        }
    }
}
