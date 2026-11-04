//! Existing integration scenarios share one executable entrypoint.
//!
//! The scenario runs on the process main thread's default stack, so this binary has
//! no libtest harness and speaks libtest's list and selection protocol itself: test
//! runners that enumerate tests (`--list`) see one test instead of running it, and
//! filters select it exactly as libtest would.

#[path = "command_entry.rs"]
mod command_entry;

const TEST: &str = "command_entry";
/// libtest options that consume the following argument as their value.
const VALUED: &[&str] = &[
    "--color",
    "--format",
    "--logfile",
    "--shuffle-seed",
    "--skip",
    "--test-threads",
    "-Z",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let (mut list, mut ignored_only, mut exact) = (false, false, false);
    let (mut filters, mut skips) = (Vec::new(), Vec::new());
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--list" => list = true,
            "--ignored" => ignored_only = true,
            "--exact" => exact = true,
            "--skip" => skips.extend(arguments.next()),
            option if VALUED.contains(&option) => {
                arguments.next();
            }
            option if option.starts_with('-') => {}
            _ => filters.push(argument),
        }
    }
    let matches = |pattern: &String| {
        if exact {
            TEST == pattern
        } else {
            TEST.contains(pattern.as_str())
        }
    };
    let selected = !ignored_only
        && (filters.is_empty() || filters.iter().any(matches))
        && !skips.iter().any(matches);
    if list {
        if selected {
            println!("{TEST}: test");
        }
        return Ok(());
    }
    if selected {
        command_entry::main()?;
    }
    Ok(())
}
