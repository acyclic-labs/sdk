//! Qualify complete captured-real inference families or a disposable local endpoint.
use acyclic_conformance::inference::replay;

#[tokio::main]
async fn main() {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if arguments.is_empty() {
        eprintln!(
            "STEP_FAILED arguments: manifest paths required; recovery: cargo run -p acyclic-conformance --features inference --bin inference-qualify -- PATH/manifest.json"
        );
        std::process::exit(2);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "--local-live")
    {
        if arguments.len() != 4 {
            eprintln!(
                "STEP_FAILED arguments; recovery: inference-qualify --local-live PATH/manifest.json http://127.0.0.1:PORT/v1 http://127.0.0.1:RATE_PORT/v1"
            );
            std::process::exit(2);
        }
        let started = std::time::Instant::now();
        let result = replay::local_live(
            std::path::Path::new(&arguments[1]),
            &arguments[2].to_string_lossy(),
            &arguments[3].to_string_lossy(),
        )
        .await;
        if let Err(cause) = result {
            eprintln!(
                "STEP_FAILED local-live: {cause}; recovery: BE_EDGE_RUN_SUITE=1 bash scripts/be-edge-local.sh"
            );
            std::process::exit(1);
        }
        println!(
            "INFERENCE_LIVE_PASS {} elapsed_ms={}",
            acyclic_conformance::inference::SUITE.join(","),
            started.elapsed().as_millis()
        );
        return;
    }
    for path in &arguments {
        let started = std::time::Instant::now();
        match replay::qualify(std::path::Path::new(path)).await {
            Ok(cases) => println!(
                "INFERENCE_PASS {:?}: {} cases {} elapsed_ms={}",
                path,
                cases.len(),
                cases.join(","),
                started.elapsed().as_millis()
            ),
            Err(cause) => {
                eprintln!(
                    "STEP_FAILED inference {:?}: {cause}; recovery: cargo run -p acyclic-conformance --features inference --bin inference-qualify -- {:?}",
                    path, path
                );
                std::process::exit(1);
            }
        }
    }
}
