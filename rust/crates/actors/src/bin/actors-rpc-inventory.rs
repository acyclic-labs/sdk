//! Emits the compiler-owned client methods and canonical HTTP routes.
#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let inventory = serde_json::json!({
        "grpc": acyclic_actors::client::Client::RPC_METHODS,
        "http": acyclic_actors::http::Client::RPC_METHODS,
        "routes": acyclic_actors::HTTP_ROUTES,
    });
    if let Err(error) = serde_json::to_writer(std::io::stdout(), &inventory) {
        eprintln!(
            "STEP_FAILED actors RPC inventory: {error}; recovery: cargo run --locked -p acyclic-actors --bin actors-rpc-inventory"
        );
        std::process::exit(1);
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    eprintln!(
        "STEP_FAILED actors RPC inventory requires native adapters; recovery: cargo run --locked -p acyclic-actors --bin actors-rpc-inventory"
    );
    std::process::exit(1);
}
