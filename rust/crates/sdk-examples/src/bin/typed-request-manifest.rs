//! Emit the Rust-owned typed request manifest used by SDK qualification.

fn main() {
    let result = std::env::args_os().nth(1).map_or_else(
        || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("build fixture runtime: {error}"))?
                .block_on(acyclic_sdk_examples::fixtures::typed_request_manifest::actual_manifest_json())
                .and_then(|manifest| serde_json::to_string_pretty(&manifest).map_err(|error| format!("serialize manifest: {error}")))
                .map(|json| format!("{json}\n"))
        },
        |path| {
            let bytes = std::fs::read(path).map_err(|error| format!("read observations: {error}"))?;
            let observations: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|error| format!("decode observations: {error}"))?;
            let observations = observations
                .as_array()
                .ok_or_else(|| "observations must be a JSON array".to_owned())?;
            let manifest = acyclic_sdk_examples::fixtures::typed_request_manifest::manifest_json_from_observations(observations)?;
            serde_json::to_string_pretty(&manifest)
                .map(|json| format!("{json}\n"))
                .map_err(|error| format!("serialize manifest: {error}"))
        },
    );
    match result {
        Ok(manifest) => print!("{manifest}"),
        Err(error) => {
            eprintln!("typed request manifest generation failed: {error}");
            std::process::exit(1);
        }
    }
}
