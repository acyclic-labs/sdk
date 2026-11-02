//! Emits the same configuration declarations served in the shared seed.
use std::io::Write;

fn main() -> std::process::ExitCode {
    match std::io::stdout()
        .lock()
        .write_all(acyclic_harness_core::config::typescript().as_bytes())
    {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "Writing configuration declarations failed: {error}\n\
                 Recovery: cargo run --locked -p acyclic-harness-core --example config-types"
            );
            std::process::ExitCode::FAILURE
        }
    }
}
