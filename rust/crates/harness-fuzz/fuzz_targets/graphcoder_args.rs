#![no_main]

use clap::Parser as _;
use std::ffi::OsString;

// This is the shipping parser and normalizer, not a model of its transitions.
#[path = "../../harness/src/bin/graphcoder/args.rs"]
mod args;

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
    if let Some(args::Command::Serve(serve)) = &parsed.command {
        assert!(
            serve.tls_certificate.is_some() == serve.tls_key.is_some()
                && serve.tls_certificate.is_some() == serve.url.is_some(),
            "serving requires a complete TLS identity or none"
        );
    }
    assert!(
        parsed.prompt.is_none() || parsed.input_format != args::InputFormat::Jsonl,
        "text prompt and JSONL stdin require distinct input owners"
    );
    let original_prompt = parsed.prompt.clone();
    let execution = match &parsed.command {
        Some(args::Command::Exec { prompt }) => Some(prompt.clone()),
        _ => None,
    };
    parsed.prepare_input()?;
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
                && matches!(parsed.command, None | Some(Command::Exec { .. }))
                && parsed.output_format == OutputFormat::Jsonl),
        "projection requires JSONL execution"
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
    Ok(())
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let argv = std::iter::once(OsString::from("graphcoder")).chain(arguments(bytes));
    if let Ok(parsed) = args::Args::try_parse_from(argv) {
        let _ = check(parsed);
    }
    // Arbitrary rejection is not a valid implementation either. Exercise one
    // accepted input family per invocation, without opening its resources.
    let prompt = String::from_utf8_lossy(bytes).replace('\0', "\\0");
    let prompt_option = format!("--prompt={prompt}");
    let shape: &[&str] = match bytes.first().copied().unwrap_or(0) % 6 {
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
        _ => &["--input-format=jsonl", "--operation=oracle"],
    };
    let valid =
        args::Args::try_parse_from(std::iter::once("graphcoder").chain(shape.iter().copied()))
            .expect("supported argument family must parse");
    check(valid).expect("supported argument family must be admitted");
});
