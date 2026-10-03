use std::{env, fs, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let first = args.next();
    if first.as_deref() == Some(std::ffi::OsStr::new("--powershell-adaptation")) {
        let Some(path) = args.next().map(PathBuf::from) else {
            eprintln!("usage: sdk-openapi --powershell-adaptation <generated-adaptation.ps1>");
            return ExitCode::FAILURE;
        };
        let script = match sdk_openapi_prototype::powershell_workers_adaptation_script(
            &acyclic_sdk_contract_wire::workers::WORKERS,
        ) {
            Ok(script) => script,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = fs::write(&path, script) {
            eprintln!("failed writing {}: {error}", path.display());
            return ExitCode::FAILURE;
        }
        println!(
            "wrote Rust-owned PowerShell adaptation to {}",
            path.display()
        );
        return ExitCode::SUCCESS;
    }
    if first.as_deref() == Some(std::ffi::OsStr::new("--c-adaptation")) {
        let Some(path) = args.next().map(PathBuf::from) else {
            eprintln!("usage: sdk-openapi --c-adaptation <generated-adaptation.ps1>");
            return ExitCode::FAILURE;
        };
        let script = match sdk_openapi_prototype::c_workers_adaptation_script(
            &acyclic_sdk_contract_wire::workers::WORKERS,
        ) {
            Ok(script) => script,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = fs::write(&path, script) {
            eprintln!("failed writing {}: {error}", path.display());
            return ExitCode::FAILURE;
        }
        println!("wrote Rust-owned C adaptation to {}", path.display());
        return ExitCode::SUCCESS;
    }
    if first.as_deref() == Some(std::ffi::OsStr::new("--dart-adaptation")) {
        let Some(path) = args.next().map(PathBuf::from) else {
            eprintln!("usage: sdk-openapi --dart-adaptation <generated-adaptation.ps1>");
            return ExitCode::FAILURE;
        };
        let script = match sdk_openapi_prototype::dart_workers_adaptation_script(
            &acyclic_sdk_contract_wire::workers::WORKERS,
        ) {
            Ok(script) => script,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = fs::write(&path, script) {
            eprintln!("failed writing {}: {error}", path.display());
            return ExitCode::FAILURE;
        }
        println!("wrote Rust-owned Dart adaptation to {}", path.display());
        return ExitCode::SUCCESS;
    }
    let check = first.as_deref() == Some(std::ffi::OsStr::new("--check"));
    let family = if check {
        "actors".to_owned()
    } else if first.as_deref() == Some(std::ffi::OsStr::new("--contract")) {
        args.next()
            .and_then(|value| value.into_string().ok())
            .unwrap_or_else(|| "actors".to_owned())
    } else {
        "actors".to_owned()
    };
    let output = args
        .next()
        .or_else(|| {
            (!check && first.as_deref() != Some(std::ffi::OsStr::new("--contract")))
                .then(|| first.unwrap())
        })
        .map(PathBuf::from);
    if check {
        let Some(path) = output else {
            eprintln!("usage: sdk-openapi --check <generated-openapi.json>");
            return ExitCode::FAILURE;
        };
        return match sdk_openapi_prototype::check_json_file(&path) {
            Ok(true) => {
                println!("generated OpenAPI is current: {}", path.display());
                ExitCode::SUCCESS
            }
            Ok(false) => {
                eprintln!("generated OpenAPI is stale: {}", path.display());
                ExitCode::FAILURE
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }
    let document = match family.as_str() {
        "actors" => sdk_openapi_prototype::json_document(),
        "workers" => sdk_openapi_prototype::json_document_from_contract(
            &acyclic_sdk_contract_wire::workers::WORKERS,
        ),
        "stream" => sdk_openapi_prototype::json_document_stream_polling(),
        "objects" => sdk_openapi_prototype::json_document_from_contract(
            &acyclic_sdk_contract_wire::objects::OBJECTS_V2,
        ),
        "inference" => sdk_openapi_prototype::json_document_inference(),
        other => Err(sdk_openapi_prototype::Error::MissingContract(format!(
            "unknown contract family {other}; expected actors, workers, objects, inference, or stream"
        ))),
    };
    match document {
        Ok(document) => {
            if let Some(path) = output {
                if let Err(error) = fs::write(&path, document) {
                    eprintln!("failed writing {}: {error}", path.display());
                    return ExitCode::FAILURE;
                }
                println!("wrote derived OpenAPI projection to {}", path.display());
            } else {
                print!("{document}");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
