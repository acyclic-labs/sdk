//! Capture installed hook groups through the production manifest writer.
use acyclic_harness::host_hooks::claude_hooks;

fn capture() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected corpus directory")?,
    );
    std::fs::create_dir_all(&directory)?;
    for (event, groups) in claude_hooks("acyclic") {
        let bytes = serde_json::to_vec(&groups[0])?;
        std::fs::write(directory.join(event), &bytes)?;
        println!("captured {event}: {}", std::str::from_utf8(&bytes)?);
    }
    Ok(())
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "step=capture-native-hooks: {error}; recovery: cargo run --locked -p acyclic-sdk-fuzz --example capture_native_hooks -- rust/fuzz/corpus/native_hooks"
        );
        std::process::exit(1);
    }
}
