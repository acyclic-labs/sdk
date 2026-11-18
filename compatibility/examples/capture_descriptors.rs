//! Capture current wire identities with the same compiler used by crate builds.

fn capture() -> Result<(), Box<dyn std::error::Error>> {
    let root = acyclic_test_support::source_path("");
    let output = root.join("target/compatibility-descriptors");
    std::fs::create_dir_all(&output)
        .map_err(|error| format!("create descriptor output directory: {error}"))?;
    let compiler = acyclic_proto_build::Compiler::new()?;
    for (family, baseline, protos) in acyclic_proto_build::FAMILIES {
        let destination = output.join(baseline);
        compiler
            .descriptor(&root.join("rust/crates").join(family), protos, &destination)
            .map_err(|error| format!("compile {family} descriptor: {error}"))?;
        println!("{family}: {}", destination.display());
    }
    Ok(())
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "descriptor capture failed: {error}\nrecovery: bazel run //compatibility:capture_descriptors_example"
        );
        std::process::exit(1);
    }
}
