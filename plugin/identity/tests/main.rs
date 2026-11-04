//! Existing integration scenarios share one executable entrypoint.
//!
//! The scenario runs on the process main thread's default stack, so this binary has
//! no libtest harness and speaks libtest's list and filter protocol itself: test
//! runners that enumerate tests (`--list`) see one test instead of running it.

#[path = "command_entry.rs"]
mod command_entry;

const TEST: &str = "command_entry";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let ignored_only = arguments.iter().any(|argument| argument == "--ignored");
    if arguments.iter().any(|argument| argument == "--list") {
        if !ignored_only {
            println!("{TEST}: test");
        }
        return Ok(());
    }
    let filtered_out = arguments
        .iter()
        .filter(|argument| !argument.starts_with('-'))
        .any(|filter| !TEST.contains(filter.as_str()));
    if ignored_only || filtered_out {
        return Ok(());
    }
    command_entry::main()
}
