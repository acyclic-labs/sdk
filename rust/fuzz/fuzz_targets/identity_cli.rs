//! Exercise the owning CLI parser without admitting credential or endpoint IO.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let arguments = std::iter::once("acyclic").chain(text.split('\0'));
    if let Ok(matches) = acyclic_plugin_identity::command().try_get_matches_from(arguments)
        && let Some((command, _)) = matches.subcommand()
    {
        assert!(acyclic_plugin_identity::is_command(command));
    }
});
