//! Emit the actual public admission table for TypeScript consumers.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!(
        "{}",
        acyclic_proto_build::typescript_authorization(
            acyclic_objects::AUTHORIZATION,
            "ObjectsMethod"
        )?
    );
    Ok(())
}
