//! Emits the compiler-owned client methods and canonical HTTP routes.
#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let inventory = serde_json::json!({
        "grpc": acyclic_stream::grpc::Client::RPC_METHODS,
        "http": acyclic_stream::http::HttpStream::RPC_METHODS,
        "routes": acyclic_stream::HTTP_RESPONSE_CONTRACT,
    });
    if let Err(error) = serde_json::to_writer(std::io::stdout(), &inventory) {
        eprintln!(
            "STEP_FAILED stream RPC inventory: {error}; recovery: cargo run --locked -p acyclic-stream --features http --bin stream-rpc-inventory"
        );
        std::process::exit(1);
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    eprintln!(
        "STEP_FAILED stream RPC inventory requires native adapters; recovery: cargo run --locked -p acyclic-stream --features grpc,http --bin stream-rpc-inventory"
    );
    std::process::exit(1);
}
