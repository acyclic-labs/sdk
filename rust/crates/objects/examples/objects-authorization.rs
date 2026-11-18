//! Emit the actual public admission table for TypeScript consumers.
/// Compiler-generated Objects method admission.
pub mod contract {
    use acyclic_auth_action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!(
        "{}",
        acyclic_proto_build::typescript_authorization(contract::AUTHORIZATION, "ObjectsMethod")?
    );
    Ok(())
}
