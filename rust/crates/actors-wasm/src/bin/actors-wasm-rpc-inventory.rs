//! Emits methods from the compiler-checked wasm-bindgen operation implementation.
fn main() {
    // Inventory values are Rust identifiers, which need no JSON escaping.
    println!(
        "[{}]",
        acyclic_actors_wasm::ActorsClient::RPC_METHODS
            .iter()
            .map(|method| format!("\"{method}\""))
            .collect::<Vec<_>>()
            .join(",")
    );
}
