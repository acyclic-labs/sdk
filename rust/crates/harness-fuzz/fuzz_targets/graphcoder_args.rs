#![no_main]

use clap::Parser as _;
use std::ffi::OsString;

use acyclic_plugin::{ingress, input::workspace_action, test_support as args};

fn native(bytes: &[u8]) -> OsString {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return text.into();
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        OsString::from_vec(bytes.to_vec())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let units: Vec<_> = bytes
            .chunks(2)
            .map(|pair| u16::from_le_bytes([pair[0], *pair.get(1).unwrap_or(&0)]))
            .collect();
        OsString::from_wide(&units)
    }
    #[cfg(not(any(unix, windows)))]
    {
        String::from_utf8_lossy(bytes).into_owned().into()
    }
}

fn arguments(bytes: &[u8]) -> Vec<OsString> {
    #[cfg(windows)]
    if let Some(wide) = bytes.strip_prefix(b"\xff\0") {
        use std::os::windows::ffi::OsStringExt as _;
        let units: Vec<_> = wide
            .chunks(2)
            .map(|pair| u16::from_le_bytes([pair[0], *pair.get(1).unwrap_or(&0)]))
            .collect();
        // Split native units, preserving zero bytes within D800/DC00 units.
        return units
            .split(|unit| *unit == 0)
            .map(OsString::from_wide)
            .collect();
    }
    (!bytes.is_empty())
        .then(|| bytes.split(|byte| *byte == 0))
        .into_iter()
        .flatten()
        .map(native)
        .collect()
}

fn check(mut parsed: args::Args) {
    let original_prompt = parsed.prompt.clone();
    let execution = match &parsed.command {
        Some(args::Command::Exec { prompt }) => Some(prompt.clone()),
        _ => None,
    };
    if parsed.prepare_input().is_err() {
        return;
    }
    use args::{Command, InputFormat, OutputFormat};
    assert!(
        parsed.input_format != InputFormat::Jsonl || parsed.operation.is_some(),
        "JSONL requires an operation identity"
    );
    if let Some(prompt) = execution {
        assert!(original_prompt.is_none(), "execution has two prompt owners");
        assert_eq!(parsed.prompt, prompt, "execution changed its prompt");
        assert!(
            prompt.is_some() || parsed.input_format == InputFormat::Jsonl,
            "taskless execution must consume JSONL"
        );
    }
    let projection = parsed.output_format == OutputFormat::Jsonl
        || parsed.event_cursor.is_some()
        || parsed.output_schema.is_some()
        || parsed.output.is_some();
    assert!(
        !projection
            || (!parsed.host_workspace
                && matches!(
                    parsed.command,
                    None | Some(Command::Run | Command::Exec { .. } | Command::Doctor)
                )
                && parsed.output_format == OutputFormat::Jsonl),
        "output options require JSONL execution or configuration diagnosis"
    );
    assert!(
        parsed.output_format != OutputFormat::Jsonl
            || parsed.input_format == InputFormat::Jsonl
            || parsed.prompt.is_some()
            || parsed.command.is_some(),
        "event output requires an input producer"
    );
    assert!(
        parsed.output.as_deref() != Some(std::path::Path::new("-")),
        "artifact must not replace event stdout"
    );
    assert!(
        parsed.output_format != OutputFormat::Jsonl
            || parsed.usage_jsonl.as_deref() != Some(std::path::Path::new("-")),
        "usage must not replace event stdout"
    );
    let destinations: Vec<_> = [&parsed.output, &parsed.event_cursor, &parsed.usage_jsonl]
        .into_iter()
        .flatten()
        .filter(|path| path.as_os_str() != "-")
        .collect();
    for (index, path) in destinations.iter().enumerate() {
        assert!(
            !destinations[index + 1..].contains(path),
            "output writers require distinct destinations"
        );
    }
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let argv = std::iter::once(OsString::from("acyclic"))
        .chain(arguments(bytes))
        .collect();
    match ingress::parse(argv) {
        Ok(ingress::Input::Agent(argv)) => {
            if let Ok(parsed) = args::Args::try_parse_from(argv) {
                check(parsed);
            }
        }
        Ok(ingress::Input::Native(arguments)) => {
            let _ = workspace_action(&arguments, || {
                Err("no selected agent in argument fuzzing".into())
            });
        }
        Err(_) => {}
    }
    let mut id = [0; 16];
    for (output, input) in id.iter_mut().zip(bytes) {
        *output = *input;
    }
    let root =
        acyclic_harness::agent::AgentPath::root(&acyclic_harness::agent::AgentId::from_bytes(id));
    for expected in [
        root.clone(),
        root.child(&acyclic_harness::agent::AgentId::from_bytes(id))
            .unwrap(),
    ] {
        let action = workspace_action(&["reopen".into(), expected.to_string()], || {
            panic!("explicit reopen consulted ambient selection")
        })
        .unwrap();
        let acyclic_harness::local::host_owner::HostAction::Reopen { task } = action else {
            panic!("explicit reopen changed its operation");
        };
        assert_eq!(task, expected);
    }
    // Arbitrary rejection is not a valid implementation either. Construct one
    // explicitly identified argument shape, preserving arbitrary prompt text
    // without ever opening config, a runtime, a provider or an output file.
    let prompt = String::from_utf8_lossy(bytes).replace('\0', "\\0");
    let mut valid = args::Args::try_parse_from([
        "graphcoder",
        "--operation=oracle",
        "--output-format=jsonl",
        "exec",
        "--",
        &prompt,
    ])
    .expect("explicit execution arguments must parse");
    valid
        .prepare_input()
        .expect("explicit execution must be admitted");
    let prompt_option = format!("--prompt={prompt}");
    let path = root.to_string();
    let shape: &[&str] = match id[0] % 8 {
        0 => &[
            "--operation=oracle",
            "--output-format=jsonl",
            "exec",
            "--",
            &prompt,
        ],
        1 => &["--host-workspace", "--host-state-directory=state"],
        2 => &["serve", "127.0.0.1:0"],
        3 => &[
            "serve",
            "0.0.0.0:443",
            "--tls-certificate=certificate.pem",
            "--tls-key=key.pem",
            "--url=https://localhost",
        ],
        4 => &[&prompt_option],
        5 => &["--input-format=jsonl", "--operation=oracle"],
        6 => &["run", "--template=graphcoder", &prompt_option],
        _ => &["attach", &path],
    };
    let mut valid =
        args::Args::try_parse_from(std::iter::once("acyclic").chain(shape.iter().copied()))
            .expect("supported argument family must parse");
    valid
        .prepare_input()
        .expect("supported argument family must be admitted");
});
