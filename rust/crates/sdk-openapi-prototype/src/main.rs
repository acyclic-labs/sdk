use std::{env, fs, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    let output = env::args_os().nth(1).map(PathBuf::from);
    match sdk_openapi_prototype::json_document() {
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
