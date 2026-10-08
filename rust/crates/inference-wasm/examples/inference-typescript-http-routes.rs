//! Emits the canonical Rust-validated Inference HTTP routes for TypeScript.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let routes = acyclic_inference::http_codec::routes()?;
    let entries = routes
        .iter()
        .map(|route| {
            (
                route.method.full_name(),
                (
                    &route.path,
                    if route.method.is_server_streaming() {
                        "server_streaming"
                    } else {
                        "unary"
                    },
                ),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    println!("{}", serde_json::to_string(&entries)?);
    Ok(())
}
