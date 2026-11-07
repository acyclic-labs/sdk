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
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let argv = std::iter::once(OsString::from("graphcoder")).chain(arguments(bytes));
    if let Ok(parsed) = args::Args::try_parse_from(argv) {
        check(parsed);
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
});
