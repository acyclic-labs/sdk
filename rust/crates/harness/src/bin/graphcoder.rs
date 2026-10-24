//! Argv, the harness client, and shared TUI composition.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

#[path = "graphcoder/args.rs"]
mod args;
#[path = "graphcoder/entry.rs"]
mod entry;
#[path = "graphcoder/view.rs"]
mod view;
use clap::Parser;
use std::process::ExitCode;
fn main() -> ExitCode {
    match entry::serve(args::Args::parse(), None, None) {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("graphcoder: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
#[path = "graphcoder/tests.rs"]
mod tests;
