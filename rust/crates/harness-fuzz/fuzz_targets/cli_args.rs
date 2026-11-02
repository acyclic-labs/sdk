#![no_main]

use clap::Parser as _;
use libfuzzer_sys::fuzz_target;

#[path = "../../../../plugin/src/runner/args.rs"]
mod args;
#[path = "../../../../plugin/src/ingress.rs"]
mod ingress;

// Exercise the production parser and cross-command validation. No runtime,
// filesystem, provider, command execution or reconstructed parser is involved.
fuzz_target!(|input: (Vec<String>, Vec<Vec<u8>>)| {
    let (arguments, raw) = input;
    let mut arguments = std::iter::once(std::ffi::OsString::from("acyclic"))
        .chain(arguments.into_iter().map(std::ffi::OsString::from))
        .collect::<Vec<_>>();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        arguments.extend(raw.into_iter().map(std::ffi::OsString::from_vec));
    }
    #[cfg(not(unix))]
    arguments.extend(
        raw.into_iter()
            .filter_map(|bytes| String::from_utf8(bytes).ok())
            .map(std::ffi::OsString::from),
    );
    match ingress::parse(arguments) {
        Ok(ingress::Input::Agent(arguments)) => {
            if let Ok(mut args) = args::Args::try_parse_from(arguments) {
                let _ = args.prepare_input();
            }
        }
        Ok(ingress::Input::Native(arguments)) => drop(arguments),
        Err(_) => {}
    }
});
