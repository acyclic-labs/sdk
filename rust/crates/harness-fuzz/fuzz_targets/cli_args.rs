#![no_main]

use clap::Parser as _;
use libfuzzer_sys::fuzz_target;
use std::ffi::OsString;

#[path = "../../../../plugin/src/runner/args.rs"]
mod args;
#[path = "../../../../plugin/src/ingress.rs"]
mod ingress;

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

fn check(mut parsed: args::Args) -> Result<(), &'static str> {
    use args::{Command, InputFormat, OutputFormat};
    assert_eq!(
        parsed.host_workspace,
        parsed.host_state_directory.is_some(),
        "native workspace ownership requires explicit state and one owner"
    );
    if parsed.host_workspace {
        assert!(
            parsed.prompt.is_none()
                && parsed.model.is_none()
                && parsed.catalog.is_none()
                && parsed.credentials.is_none()
                && parsed.operation.is_none()
                && parsed.settings_root.is_none()
                && !parsed.default_config
                && parsed.usage_jsonl.is_none()
                && !parsed.resume
                && parsed.agent.is_none()
                && parsed.endpoint.is_none(),
            "native workspace ownership must exclude agent inputs"
        );
    }
    if let Some(Command::Serve(serve)) = &parsed.command {
        assert!(
            serve.tls_certificate.is_some() == serve.tls_key.is_some()
                && serve.tls_certificate.is_some() == serve.url.is_some(),
            "serving requires a complete TLS identity or none"
        );
    }
    assert!(
        parsed.prompt.is_none() || parsed.input_format != InputFormat::Jsonl,
        "text prompt and JSONL stdin require distinct input owners"
    );
    let original_prompt = parsed.prompt.clone();
    let execution = match &parsed.command {
        Some(Command::Exec { prompt }) => Some(prompt.clone()),
        _ => None,
    };
    let attached = match &parsed.command {
        Some(Command::Attach { path }) => Some(path.clone()),
        _ => None,
    };
    parsed.prepare_input()?;
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
    if let Some(path) = attached {
        assert_eq!(
            parsed.agent,
            Some(path),
            "attach changed its selected agent"
        );
    }
    if matches!(
        parsed.command,
        Some(Command::Attach { .. } | Command::Serve(_))
    ) {
        assert!(
            parsed.prompt.is_none()
                && parsed.operation.is_none()
                && parsed.input_format == InputFormat::Text
                && !parsed.host_workspace,
            "attach and serve cannot admit execution input"
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
                    None | Some(Command::Run | Command::Exec { .. })
                )
                && parsed.output_format == OutputFormat::Jsonl),
        "projection requires JSONL execution"
    );
    assert!(
        parsed.output_format != OutputFormat::Jsonl
            || parsed.input_format == InputFormat::Jsonl
            || parsed.prompt.is_some(),
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
    Ok(())
}

// Exercise the production parser and cross-command validation. No runtime,
// filesystem, provider, command execution or reconstructed parser is involved.
fuzz_target!(|bytes: &[u8]| {
    let mut id = [0; 16];
    for (output, input) in id.iter_mut().zip(bytes) {
        *output = *input;
    }
    let refuse = bytes.first().is_some_and(|byte| byte & 1 != 0);
    // A generated canonical path goes through the shipping admission boundary.
    // Explicit selection must preserve that path and cannot consult ambient
    // state, even when ordinary argument mutations rarely spell a valid ID.
    let root = acyclic_harness_core::agent::AgentPath::root(
        &acyclic_harness_core::agent::AgentId::from_bytes(id),
    );
    for expected in [
        root.clone(),
        root.child(&acyclic_harness_core::agent::AgentId::from_bytes(id))
            .unwrap(),
    ] {
        let action = ingress::workspace_action(&["reopen".into(), expected.to_string()], || {
            panic!("explicit reopen consulted ambient selection")
        })
        .unwrap();
        let acyclic_harness_core::native_host::HostAction::Reopen { task } = action else {
            panic!("explicit reopen changed its operation")
        };
        assert_eq!(task, expected);
    }
    // Successful admission is checked too: refusing every input cannot pass.
    let prompt = String::from_utf8_lossy(bytes).replace('\0', "\\0");
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
    let valid = args::Args::try_parse_from(std::iter::once("acyclic").chain(shape.iter().copied()))
        .expect("supported argument family must parse");
    check(valid).expect("supported argument family must be admitted");
    let arguments = std::iter::once(OsString::from("acyclic"))
        .chain(arguments(bytes))
        .collect();
    match ingress::parse(arguments) {
        Ok(ingress::Input::Agent(arguments)) => {
            if let Ok(args) = args::Args::try_parse_from(arguments) {
                let _ = check(args);
            }
        }
        Ok(ingress::Input::Native(arguments)) => {
            let _ = ingress::workspace_action(&arguments, || {
                if refuse {
                    Err("no selected agent in argument fuzzing".into())
                } else {
                    Ok(acyclic_harness_core::agent::AgentPath::root(
                        &acyclic_harness_core::agent::AgentId::from_bytes([0; 16]),
                    ))
                }
            });
        }
        Err(_) => {}
    }
});
