//! Generate the Rust-owned deployment declarations and optional selected-entry env type.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_manifest as manifest;
    let mut args = std::env::args().skip(1);
    let output = std::path::PathBuf::from(args.next().ok_or("output directory required")?);
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("manifest.ts"), manifest::typescript()?)?;
    std::fs::write(
        output.join("manifest.schema.json"),
        manifest::json_schema()?,
    )?;
    if let Some(input) = args.next() {
        let environment = args.next().ok_or("environment required")?;
        let entry = args.next().ok_or("entry required")?;
        let parsed = manifest::parse(&std::fs::read(input)?)?;
        std::fs::write(
            output.join("env.ts"),
            manifest::environment_typescript(&parsed, &environment, &entry)?,
        )?;
    }
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    Ok(())
}
