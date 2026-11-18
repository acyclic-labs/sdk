//! Renders the canonical Actors protobuf source for maintainers.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let output = arguments
        .next()
        .ok_or("expected a proto output directory")?;
    if arguments.next().is_some() {
        return Err("expected exactly one proto output directory".into());
    }
    acyclic_actors::contract::render_proto_files(output)?;
    Ok(())
}
