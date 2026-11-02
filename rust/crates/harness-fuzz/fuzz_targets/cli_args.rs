#![no_main]

use clap::Parser as _;
use libfuzzer_sys::fuzz_target;

#[path = "../../../../plugin/src/runner/args.rs"]
mod args;
#[path = "../../../../plugin/src/ingress.rs"]
mod ingress;

// Exercise the production parser and cross-command validation. No runtime,
// filesystem, provider, command execution or reconstructed parser is involved.
fuzz_target!(|input: (Vec<String>, Vec<Vec<u8>>, bool, [u8; 16])| {
    let (arguments, raw, refuse, id) = input;
    // A generated canonical path goes through the shipping admission boundary.
    // Explicit selection must preserve that path and cannot consult ambient
    // state, even when ordinary argument mutations rarely spell a valid ID.
    let root =
        acyclic_harness::agent::AgentPath::root(&acyclic_harness::agent::AgentId::from_bytes(id));
    for expected in [
        root.clone(),
        root.child(&acyclic_harness::agent::AgentId::from_bytes(id))
            .unwrap(),
    ] {
        let action = ingress::workspace_action(&["reopen".into(), expected.to_string()], || {
            panic!("explicit reopen consulted ambient selection")
        })
        .unwrap();
        let acyclic_harness::local::host_owner::HostAction::Reopen { task } = action else {
            panic!("explicit reopen changed its operation")
        };
        assert_eq!(task, expected);
    }
    let mut arguments = std::iter::once(std::ffi::OsString::from("acyclic"))
        .chain(arguments.into_iter().map(std::ffi::OsString::from))
        .collect::<Vec<_>>();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        arguments.extend(raw.into_iter().map(std::ffi::OsString::from_vec));
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        arguments.extend(raw.into_iter().map(|bytes| {
            let wide = bytes
                .chunks(2)
                .map(|pair| u16::from(pair[0]) | (u16::from(*pair.get(1).unwrap_or(&0)) << 8))
                .collect::<Vec<_>>();
            std::ffi::OsString::from_wide(&wide)
        }));
    }
    #[cfg(not(any(unix, windows)))]
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
        Ok(ingress::Input::Native(arguments)) => {
            let _ = ingress::workspace_action(&arguments, || {
                if refuse {
                    Err("no selected agent in argument fuzzing".into())
                } else {
                    Ok(acyclic_harness::agent::AgentPath::root(
                        &acyclic_harness::agent::AgentId::from_bytes([0; 16]),
                    ))
                }
            });
        }
        Err(_) => {}
    }
});
