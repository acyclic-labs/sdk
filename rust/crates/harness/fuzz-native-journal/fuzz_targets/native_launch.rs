//! Exercise native policy decoding, path anchoring and complete child commands.
#![no_main]

use acyclic_harness::{Error, local::host_process::launch};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

fn non_unicode_root(input: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let mut bytes = b"/native-root/".to_vec();
        bytes.extend_from_slice(input);
        bytes.push(0xff);
        OsString::from_vec(bytes).into()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        let units = "C:\\native-root\\"
            .encode_utf16()
            .chain(input.iter().copied().map(u16::from))
            .chain([0xd800])
            .collect::<Vec<_>>();
        OsString::from_wide(&units).into()
    }
}

fn paths(
    host: &str,
    flags: &[String],
    cwd: &Path,
    non_unicode: &Path,
    value: &str,
) -> Result<(), String> {
    let options: &[&str] = match host {
        "codex" => &["--add-dir"],
        "claude-code" => &["--plugin-dir", "--add-dir", "--settings"],
        _ => &["--session-dir", "--extension"],
    };
    let relative = format!("configuration/{value}");
    let absolute = cwd.join(&relative);
    for option in options {
        for value in [
            relative.as_str(),
            absolute
                .to_str()
                .ok_or_else(|| format!("generated absolute path is not Unicode: {absolute:?}"))?,
        ] {
            let mut supplied = flags.to_vec();
            if *option == "--settings" {
                // Replace the required inline settings, retaining its cardinality.
                *supplied
                    .get_mut(1)
                    .ok_or("required settings value absent")? = value.into();
            } else {
                supplied.extend([(*option).into(), value.into()]);
            }
            let inherited = launch::parent_flags(host, &supplied, cwd).map_err(|error| {
                format!("admitted path policy was refused: {host}, {supplied:?}: {error}")
            })?;
            let index = if *option == "--settings" {
                1
            } else {
                flags.len() + 1
            };
            assert_eq!(
                inherited.get(index).map(Path::new),
                Some(absolute.as_path()),
                "native path policy changed its root target"
            );
            if value == relative {
                assert!(
                    matches!(
                        launch::parent_flags(host, &supplied, non_unicode),
                        Err(Error::Invalid(_))
                    ),
                    "native launch accepted a non-Unicode policy root"
                );
            }
        }
    }
    if host == "pi" {
        resource_paths(flags, cwd, value, options)?;
    }
    Ok(())
}

fn resource_paths(
    flags: &[String],
    cwd: &Path,
    value: &str,
    options: &[&str],
) -> Result<(), String> {
    let host = "pi";
    for (option, prefix) in [
        ("--extension", "npm:"),
        ("--extension", "git:"),
        ("--extension", "github:"),
        ("--extension", "http:"),
        ("--extension", "https:"),
        ("--extension", "ssh:"),
        ("--extension", "builtin:"),
        ("--extension", " \tnpm:"),
        ("--extension", "file://"),
        ("--session-dir", "file://"),
        ("--extension", "~/"),
        ("--session-dir", "~/"),
    ] {
        let reference = format!("{prefix}{value}");
        let mut supplied = flags.to_vec();
        supplied.extend([option.into(), reference.clone()]);
        let inherited = launch::parent_flags(host, &supplied, cwd).map_err(|error| {
            format!("admitted Pi resource reference was refused: {supplied:?}: {error}")
        })?;
        assert_eq!(
            inherited.last(),
            Some(&reference),
            "native Pi resource reference was rebased as a local path"
        );
    }
    for option in options {
        for reference in std::iter::once("~").chain(cfg!(windows).then_some("~\\home")) {
            let mut supplied = flags.to_vec();
            supplied.extend([(*option).into(), reference.into()]);
            let inherited = launch::parent_flags(host, &supplied, cwd).map_err(|error| {
                format!("admitted Pi home reference was refused: {supplied:?}: {error}")
            })?;
            assert_eq!(
                inherited.last().map(String::as_str),
                Some(reference),
                "native Pi home reference was rebased as a local path"
            );
        }
    }
    Ok(())
}

fn policy(host: &str, flags: &[String], cwd: &Path, reference: &str) -> Result<(), String> {
    let inherited = launch::parent_flags(host, flags, cwd).map_err(|error| {
        format!("admitted root policy could not be inherited: {host}, {flags:?}: {error}")
    })?;
    let encoded = serde_json::to_string(&inherited)
        .map_err(|error| format!("root argument serialization failed: {inherited:?}: {error}"))?;
    assert_eq!(
        launch::inherited_arguments(&encoded)
            .map_err(|error| format!("serialized root policy was refused: {encoded:?}: {error}"))?,
        inherited,
        "native policy decoder changed root argument vector"
    );
    for session in [None, Some(reference)] {
        let arguments = launch::child_arguments(host, &inherited, cwd, session)
            .map_err(|error| format!("admitted inherited policy was refused: {host}, {inherited:?}, {session:?}: {error}"))?;
        let (prefix, suffix): (&[&str], Vec<OsString>) = match host {
            "codex" => (
                &[],
                vec!["exec".into(), "--json".into(), "--cd".into(), cwd.into()],
            ),
            "claude-code" => (
                &["-p", "--output-format", "stream-json", "--verbose"],
                vec![],
            ),
            _ => (&["--mode", "json", "-p"], vec![]),
        };
        let mut expected = prefix
            .iter()
            .copied()
            .map(OsString::from)
            .chain(inherited.iter().map(OsString::from))
            .chain(suffix)
            .collect::<Vec<_>>();
        if let Some(reference) = session {
            let option = match host {
                "codex" => "resume",
                "claude-code" => "--resume",
                _ => "--fork",
            };
            expected.extend([option, reference].into_iter().map(OsString::from));
        }
        if host == "codex" {
            expected.push("-".into());
        }
        assert_eq!(
            arguments, expected,
            "native child changed the complete command vector"
        );
    }
    assert!(
        matches!(
            launch::child_arguments(host, &inherited, cwd, Some("--untrusted-option")),
            Err(Error::Invalid(_))
        ),
        "native resume reference was admitted as an option"
    );
    Ok(())
}

libfuzzer_sys::fuzz_target!(|input: &[u8]| assert_eq!(check(input), Ok(())));

fn check(input: &[u8]) -> Result<(), String> {
    let Ok(encoded) = std::str::from_utf8(input) else {
        return Ok(());
    };
    let expected = serde_json::from_str::<serde_json::Value>(encoded)
        .ok()
        .and_then(|value| match value {
            serde_json::Value::Array(values) => values
                .into_iter()
                .map(|value| value.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>(),
            _ => None,
        });
    let decoded = match (launch::inherited_arguments(encoded), expected) {
        (Ok(actual), Some(expected)) => {
            assert_eq!(
                actual, expected,
                "native policy decoder changed root argument vector"
            );
            Some(actual)
        }
        (Err(Error::Storage(_)), None) => None,
        (actual, expected) => {
            return Err(format!(
                "native policy decoder changed string-vector admission: actual {actual:?}, expected {expected:?}"
            ));
        }
    };
    let cwd = Path::new(if cfg!(windows) {
        "C:\\native-root"
    } else {
        "/native-root"
    });
    let reference = format!("native-{encoded}");
    let non_unicode = non_unicode_root(input);
    for (host, required) in [
        (
            "codex",
            &[
                "--sandbox",
                "workspace-write",
                "--ask-for-approval",
                "never",
            ][..],
        ),
        (
            "claude-code",
            &["--settings", "{}", "--permission-mode", "default"][..],
        ),
        ("pi", &[][..]),
    ] {
        check_host(
            host,
            required,
            decoded.as_deref(),
            cwd,
            &non_unicode,
            encoded,
            &reference,
        )?;
    }
    assert!(matches!(
        launch::validate("unknown-host", &[]),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

fn check_host(
    host: &str,
    required: &[&str],
    decoded: Option<&[String]>,
    cwd: &Path,
    non_unicode: &Path,
    encoded: &str,
    reference: &str,
) -> Result<(), String> {
    if let Some(flags) = decoded {
        match launch::validate(host, flags) {
            Ok(()) => policy(host, flags, cwd, reference)?,
            Err(Error::Invalid(_)) => {
                assert!(matches!(
                    launch::parent_flags(host, flags, cwd),
                    Err(Error::Invalid(_))
                ));
                assert!(matches!(
                    launch::child_arguments(host, flags, cwd, None),
                    Err(Error::Invalid(_))
                ));
            }
            Err(error) => {
                return Err(format!(
                    "native policy refusal lost its invalid-input type: {host}, {flags:?}: {error}"
                ));
            }
        }
    }
    // Authored valid policy requests are a positive control, not a parser.
    let mut flags = required
        .iter()
        .copied()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    flags.extend(["--model".into(), encoded.into()]);
    if host == "claude-code" {
        let whitespace = ["", " ", "\t", "\n"]
            .get(encoded.len() % 4)
            .ok_or("inline settings whitespace selection absent")?;
        *flags
            .get_mut(1)
            .ok_or("required inline settings value absent")? =
            format!("{whitespace}{}", serde_json::json!({"policy": encoded}));
    }
    let inherited = launch::parent_flags(host, &flags, cwd).map_err(|error| {
        format!("admitted non-path policy was refused: {host}, {flags:?}: {error}")
    })?;
    assert!(
        inherited.starts_with(&flags),
        "native launch changed non-path policy bytes"
    );
    policy(host, &flags, cwd, reference)?;
    paths(host, &flags, cwd, non_unicode, encoded)?;
    if host != "pi" {
        assert!(
            matches!(
                launch::parent_flags(host, &flags, non_unicode),
                Err(Error::Invalid(_))
            ),
            "native launch accepted a non-Unicode workspace root"
        );
    }
    refused_policies(host, &mut flags, required, cwd, encoded, reference)
}

fn refused_policies(
    host: &str,
    flags: &mut Vec<String>,
    required: &[&str],
    cwd: &Path,
    encoded: &str,
    reference: &str,
) -> Result<(), String> {
    for (index, pair) in required.chunks_exact(2).enumerate() {
        let mut missing = flags.clone();
        missing.drain(2 * index..2 * index + 2);
        assert!(
            matches!(launch::validate(host, &missing), Err(Error::Invalid(_))),
            "native launch admitted a missing required policy"
        );
        let mut repeated = flags.clone();
        repeated.extend(pair.iter().copied().map(str::to_owned));
        assert!(
            matches!(launch::validate(host, &repeated), Err(Error::Invalid(_))),
            "native launch admitted duplicate required policy"
        );
    }
    flags.push("--model".into());
    assert!(
        matches!(launch::validate(host, &flags), Err(Error::Invalid(_))),
        "native launch admitted a missing policy value"
    );
    flags.pop();
    flags.push("--no-extensions".into());
    if host == "pi" {
        policy(host, &flags, cwd, reference)?;
    } else {
        assert!(
            matches!(launch::validate(host, &flags), Err(Error::Invalid(_))),
            "native launch admitted another host's policy switch"
        );
    }
    flags.pop();
    flags.extend(["--untrusted-option".into(), encoded.into()]);
    assert!(
        matches!(launch::validate(host, &flags), Err(Error::Invalid(_))),
        "native launch admitted an unknown policy option"
    );
    Ok(())
}
