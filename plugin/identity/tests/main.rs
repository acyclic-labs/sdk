//! Existing integration scenarios share one executable entrypoint.

#[path = "command_entry.rs"]
mod command_entry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    command_entry::main()
}
