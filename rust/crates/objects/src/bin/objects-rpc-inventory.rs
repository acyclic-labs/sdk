//! Emits the compiler-owned client methods and canonical HTTP routes.
#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let inventory = serde_json::json!({
        "grpc": acyclic_objects::grpc::GrpcObjects::RPC_METHODS,
        "http": acyclic_objects::http::HttpObjects::RPC_METHODS,
        "routes": acyclic_objects::HTTP_ROUTES.iter().chain(acyclic_objects::HTTP_STREAM_ROUTES.iter()).collect::<Vec<_>>(),
    });
    if let Err(error) = serde_json::to_writer(std::io::stdout(), &inventory) {
        eprintln!(
            "STEP_FAILED objects RPC inventory: {error}; recovery: cargo run --locked -p acyclic-objects --features grpc,http --bin objects-rpc-inventory"
        );
        std::process::exit(1);
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    eprintln!(
        "STEP_FAILED objects RPC inventory requires native adapters; recovery: cargo run --locked -p acyclic-objects --features grpc,http --bin objects-rpc-inventory"
    );
    std::process::exit(1);
}
