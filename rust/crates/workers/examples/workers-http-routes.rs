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
        let root = std::path::Path::new(&typescript);
        let additional = acyclic_workers::export_client_typescript(root)?;
        let metadata = acyclic_workers::domain::export_typescript_with_metadata(root, additional)?;
        let parent = root.parent().ok_or("semantic output requires a parent")?;
        std::fs::write(parent.join("Workers.service.bin"), acyclic_workers::FILE_DESCRIPTOR_SET)?;
        std::fs::write(parent.join("Workers.semantic-names.json"), serde_json::to_vec(&metadata)?)?;
        std::fs::write(parent.join("workers-binding.ts"), format!(
            "// Generated from Rust package identity and canonical failure DTO. Do not edit.\nimport type {{ WorkersFailure }} from \"./semantic/workers/WorkersFailure.js\";\nexport const WORKERS_BINDING_VERSION = {};\nexport const MAX_MESSAGE_BYTES = {};\nexport function workersCancelledFailure(): WorkersFailure {{\n  const metadata = {};\n  return {{ ...metadata, rawDetails: Uint8Array.from(metadata.rawDetails) }};\n}}\n",
            serde_json::to_string(env!("CARGO_PKG_VERSION"))?, acyclic_workers::MAX_MESSAGE_BYTES,
            serde_json::to_string(&acyclic_workers::Failure::cancelled())?,
        ))?;
    } else {
        println!("{}", serde_json::to_string(acyclic_workers::HTTP_ROUTES)?);
    }
    Ok(())
}
