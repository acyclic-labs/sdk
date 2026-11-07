//! Capture nonsecret envelopes through the original native owner writer.
#[path = "../tests/owner_codec/oracle.rs"]
mod native_owner_record;

fn capture() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::args_os()
        .nth(1)
        .ok_or("missing corpus destination")?;
    std::fs::create_dir_all(&directory)?;
    for (name, bytes) in native_owner_record::seeds()? {
        native_owner_record::check_retained(&bytes)?;
        std::fs::write(std::path::Path::new(&directory).join(name), bytes)?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "owner-record capture: {error}; recovery: cargo run --locked -p acyclic-native-runtime --example capture_owner_record -- <corpus-directory>"
        );
        std::process::exit(1);
    }
}
