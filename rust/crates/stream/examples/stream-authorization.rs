//! Emit the actual client admission table for TypeScript and wasm consumers.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!(
        "{}",
        acyclic_proto_build::typescript_authorization(
            acyclic_stream_wire::authorization::AUTHORIZATION,
            "StreamMethod"
        )?
    );
    Ok(())
}
