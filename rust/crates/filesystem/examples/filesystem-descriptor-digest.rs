//! Emit the descriptor identity used by the native and WASM transports.
fn main() {
    println!("{}", blake3::hash(acyclic_fs_wire::FILE_DESCRIPTOR_SET).to_hex());
}
