use std::path::Path;

fn capture() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("workspace root is missing")?;
    let output = root.join("target/compatibility-descriptors");
    std::fs::create_dir_all(&output)
        .map_err(|error| format!("create descriptor output directory: {error}"))?;
    for (family, baseline, protos) in acyclic_proto_build::FAMILIES {
        let destination = output.join(baseline);
        acyclic_proto_build::descriptor(
            &root.join("rust/crates").join(family),
            protos,
            &destination,
        )
        .map_err(|error| format!("compile {family} descriptor: {error}"))?;
        println!("{family}: {}", destination.display());
    }
    Ok(())
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "descriptor capture failed: {error}\nrecovery: cargo run --locked -p acyclic-proto-build --example capture_descriptors"
        );
        std::process::exit(1);
    }
}
