//! Generate an ephemeral TLS identity for local transport conformance.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    println!(
        "{}",
        serde_json::json!({"certificate": identity.cert.pem(), "key": identity.signing_key.serialize_pem()})
    );
    Ok(())
}
