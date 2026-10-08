//! Emit Workers routes, Rust-owned Proto and semantic TypeScript.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    if let Some(proto) = args.next() {
        let typescript = args
            .next()
            .ok_or("expected Proto root and TypeScript root")?;
        if args.next().is_some() {
            return Err("unexpected Workers generation argument".into());
        }
        acyclic_workers::contract::render_proto_files(proto)?;
        acyclic_workers::domain::export_typescript(typescript)?;
    } else {
        println!("{}", serde_json::to_string(acyclic_workers::HTTP_ROUTES)?);
    }
    Ok(())
}
