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

fn paths(host: &str, flags: &[String], cwd: &Path, non_unicode: &Path, value: &str) {
    let options: &[&str] = match host {
        "codex" => &["--add-dir"],
        "claude-code" => &["--plugin-dir", "--add-dir", "--settings"],
        _ => &["--session-dir", "--extension"],
    };
    let relative = format!("configuration/{value}");
    let absolute = cwd.join(&relative);
    for option in options {
        for value in [relative.as_str(), absolute.to_str().expect("Unicode root")] {
            let mut supplied = flags.to_vec();
            if *option == "--settings" {
                // Replace the required inline settings, retaining its cardinality.
                supplied[1] = value.into();
            } else {
                supplied.extend([(*option).into(), value.into()]);
            }
            let inherited = launch::parent_flags(host, &supplied, cwd)
                .expect("admitted path policy was refused");
            let index = if *option == "--settings" {
                1
            } else {
                flags.len() + 1
            };
            assert_eq!(
                Path::new(&inherited[index]),
                absolute,
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
            let inherited = launch::parent_flags(host, &supplied, cwd)
                .expect("admitted Pi resource reference was refused");
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
                let inherited = launch::parent_flags(host, &supplied, cwd)
                    .expect("admitted Pi home reference was refused");
                assert_eq!(
                    inherited.last().map(String::as_str),
                    Some(reference),
                    "native Pi home reference was rebased as a local path"
                );
            }
        }
    }
}

fn policy(host: &str, flags: &[String], cwd: &Path, reference: &str) {
    let inherited = launch::parent_flags(host, flags, cwd)
        .expect("admitted root policy could not be inherited");
    let encoded = serde_json::to_string(&inherited).expect("root argument serialization failed");
    assert_eq!(
        launch::inherited_arguments(&encoded).expect("serialized root policy was refused"),
        inherited,
        "native policy decoder changed root argument vector"
    );
    let expected = inherited.iter().map(OsString::from).collect::<Vec<_>>();
    let offset = match host {
        "codex" => 0,
        "claude-code" => 4,
        _ => 3,
    };
    for session in [None, Some(reference)] {
        let arguments = launch::child_arguments(host, &inherited, cwd, session)
            .expect("admitted inherited policy was refused");
        assert_eq!(
            arguments.get(offset..offset + expected.len()),
            Some(expected.as_slice()),
            "native child changed the inherited root policy"
        );
        if let Some(reference) = session {
            let suffix = match host {
                "codex" => vec!["resume", reference, "-"],
                "claude-code" => vec!["--resume", reference],
                _ => vec!["--session", reference],
            }
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
            assert!(
                arguments.ends_with(&suffix),
                "native child changed the admitted resume reference"
            );
        }
    }
    assert!(
        matches!(
            launch::child_arguments(host, &inherited, cwd, Some("--untrusted-option")),
            Err(Error::Invalid(_))
        ),
        "native resume reference was admitted as an option"
    );
}

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let Ok(encoded) = std::str::from_utf8(input) else {
        return;
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
        _ => panic!("native policy decoder changed string-vector admission"),
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
        if let Some(flags) = &decoded {
            match launch::validate(host, flags) {
                Ok(()) => policy(host, flags, cwd, &reference),
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
                Err(error) => panic!("native policy refusal lost its invalid-input type: {error}"),
            }
        }
        // Authored valid policy requests are a positive control, not a parser.
        let mut flags = required
            .iter()
            .copied()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        flags.extend(["--model".into(), encoded.into()]);
        policy(host, &flags, cwd, &reference);
        paths(host, &flags, cwd, &non_unicode, encoded);
        if host != "pi" {
            assert!(
                matches!(
                    launch::parent_flags(host, &flags, &non_unicode),
                    Err(Error::Invalid(_))
                ),
                "native launch accepted a non-Unicode workspace root"
            );
        }
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
            policy(host, &flags, cwd, &reference);
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
    }
    assert!(matches!(
        launch::validate("unknown-host", &[]),
        Err(Error::Invalid(_))
    ));
});
