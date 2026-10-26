//! Runs the canonical wave-1 histories against a caller-selected HTTP endpoint.
use acyclic_stream::{
    StreamPath,
    conformance::wave1::{self, Case},
    http::HttpStream,
};
use std::time::{Duration, Instant};

fn required(name: &'static str) -> Result<String, String> {
    std::env::var(name).map_err(|_| {
        format!(
            "step=read_environment missing={name}; recovery: set {name} and rerun {}",
            wave1::RECOVERY
        )
    })
}

async fn run() -> Result<(), String> {
    let endpoint = required("ACYCLIC_CONFORMANCE_ENDPOINT")?;
    let token = required("ACYCLIC_CONFORMANCE_TOKEN")?;
    let namespace = StreamPath::new(required("ACYCLIC_CONFORMANCE_NAMESPACE")?)
        .map_err(|error| format!("step=parse_namespace error={error}; recovery: set a fresh valid ACYCLIC_CONFORMANCE_NAMESPACE and rerun {}", wave1::RECOVERY))?;
    let provider = HttpStream::new(&endpoint, &token, 1024 * 1024)
        .map_err(|_| format!("step=configure_provider; recovery: correct ACYCLIC_CONFORMANCE_ENDPOINT and ACYCLIC_CONFORMANCE_TOKEN, then rerun {}", wave1::RECOVERY))?;
    let selection = std::env::var("ACYCLIC_CONFORMANCE_CASE").ok();
    let cases: Vec<_> = Case::ALL
        .into_iter()
        .filter(|case| {
            selection
                .as_ref()
                .is_none_or(|selected| selected == case.name())
        })
        .collect();
    if cases.is_empty() {
        return Err(format!(
            "step=select_case no_matching_history; recovery: unset ACYCLIC_CONFORMANCE_CASE and rerun {}",
            wave1::RECOVERY
        ));
    }
    for case in cases {
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(9), wave1::verify_case(&provider, &namespace, case))
            .await.map_err(|_| format!("step=history_deadline case={} timeout_seconds=9; recovery: use a fresh namespace and rerun {}", case.name(), wave1::RECOVERY))?
            .map_err(|error| error.to_string())?;
        println!(
            "{}",
            serde_json::json!({ "suite": "wave1_streams", "case": case.name(), "status": "passed", "provider": "http", "elapsed_micros": start.elapsed().as_micros() })
        );
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("WAVE1_CONFORMANCE_FAILURE {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
