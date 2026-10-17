//! Emit the descriptor identity used by the native and WASM transports.
fn main() {
    println!("{}", blake3::hash(acyclic_fs::FILE_DESCRIPTOR_SET).to_hex());
}
