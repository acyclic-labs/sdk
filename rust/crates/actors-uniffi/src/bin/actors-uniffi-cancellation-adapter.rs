use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let mut args = env::args_os().skip(1);
    let flag = args.next();
    let output = args.next().map(PathBuf::from);
    if flag.as_deref() != Some("--output".as_ref()) || output.is_none() || args.next().is_some() {
        eprintln!("usage: actors-uniffi-cancellation-adapter --output PATH");
        std::process::exit(2);
    }
    let output = output.expect("validated output path");
    if let Some(parent) = output.parent() {
        if let Err(error) = fs::create_dir_all(parent) {
            eprintln!("failed to create {}: {error}", parent.display());
            std::process::exit(1);
        }
    }
    let rendered = acyclic_actors_uniffi::cancellation_metadata::render_kotlin_adapter();
    if let Err(error) = fs::write(&output, rendered) {
        eprintln!("failed to write {}: {error}", output.display());
        std::process::exit(1);
    }
}