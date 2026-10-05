//! Emit the Rust-owned typed request manifest used by SDK qualification.

fn main() {
    let args = std::env::args_os().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--haskell-output") {
        let output = args
            .iter()
            .position(|arg| arg == "--haskell-output")
            .and_then(|index| args.get(index + 1))
            .cloned()
            .ok_or_else(|| "--haskell-output requires a path".to_owned())
            .and_then(|path| {
                let manifest_path = args
                    .iter()
                    .position(|arg| arg == "--manifest")
                    .and_then(|index| args.get(index + 1));
                let manifest = if let Some(manifest_path) = manifest_path {
                    let bytes = std::fs::read(manifest_path)
                        .map_err(|error| format!("read Rust manifest: {error}"))?;
                    serde_json::from_slice(&bytes)
                        .map_err(|error| format!("decode Rust manifest: {error}"))?
                } else {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| format!("build fixture runtime: {error}"))?
                        .block_on(acyclic_sdk_examples::fixtures::typed_request_manifest::actual_manifest_json())?
                };
                let source = acyclic_sdk_examples::fixtures::typed_request_manifest::haskell_replay_source(&manifest)?;
                std::fs::write(&path, source)
                    .map_err(|error| format!("write Haskell replay: {error}"))?;
                Ok::<_, String>(format!("generated Haskell replay {}\n", path.to_string_lossy()))
            });
        match output {
            Ok(message) => print!("{message}"),
            Err(error) => {
                eprintln!("typed request Haskell generation failed: {error}");
                std::process::exit(1);
            }
        }
        return;
    }
    if args.iter().any(|arg| arg == "--haskell-semantics-output") {
        let output = args
            .iter()
            .position(|arg| arg == "--haskell-semantics-output")
            .and_then(|index| args.get(index + 1))
            .cloned()
            .ok_or_else(|| "--haskell-semantics-output requires a path".to_owned())
            .and_then(|path| {
                let source = acyclic_sdk_examples::fixtures::typed_request_manifest::haskell_semantic_types_source()?;
                std::fs::write(&path, source)
                    .map_err(|error| format!("write Haskell semantic types: {error}"))?;
                Ok::<_, String>(format!("generated Haskell semantic types {}\n", path.to_string_lossy()))
            });
        match output {
            Ok(message) => print!("{message}"),
            Err(error) => {
                eprintln!("typed request Haskell semantic generation failed: {error}");
                std::process::exit(1);
            }
        }
        return;
    }
    let result = args.get(1).map_or_else(
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
