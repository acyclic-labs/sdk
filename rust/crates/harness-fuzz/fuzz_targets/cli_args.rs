#![no_main]

use clap::Parser as _;
use libfuzzer_sys::fuzz_target;

#[path = "../../../../plugin/src/runner/args.rs"]
mod args;

// Exercise the production parser and cross-command validation. No runtime,
// filesystem, provider, command execution or reconstructed parser is involved.
fuzz_target!(|arguments: Vec<String>| {
    if let Ok(mut args) =
        args::Args::try_parse_from(std::iter::once("acyclic".to_owned()).chain(arguments))
    {
        let _ = args.prepare_input();
    }
});
