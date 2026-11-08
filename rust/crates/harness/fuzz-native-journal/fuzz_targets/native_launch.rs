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
        "agy" => &["--add-dir", "--settings"],
        "pi" => &["--session-dir", "--extension"],
        _ => &[],
    };
    let relative = format!("configuration/{value}");
    let absolute = cwd.join(&relative);
    for option in options {
        for value in [relative.as_str(), absolute.to_str().expect("Unicode root")] {
            let mut supplied = flags.to_vec();
            if host == "claude-code" && *option == "--settings" {
                // Replace the required inline settings, retaining its cardinality.
                supplied[1] = value.into();
            } else {
                supplied.extend([(*option).into(), value.into()]);
            }
            let inherited = launch::parent_flags(host, &supplied, cwd)
                .expect("admitted path policy was refused");
            let index = if host == "claude-code" && *option == "--settings" {
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
    // Arbitrary external settings filenames are not opened by this in-memory
    // fuzzer. Relative inline permissions have their semantic oracle below;
    // the real-file property covers the same production argument boundary.
    if host == "agy" {
        let mut values = flags.iter();
        while let Some(name) = values.next() {
            if name == "--sandbox" {
                continue;
            }
            let value = values.next().expect("validated option value");
            if name == "--settings" {
                if value.trim_start().starts_with('{') {
                    let result = launch::child_arguments(host, &inherited, cwd, None);
                    if serde_json::from_str::<serde_json::Value>(value).is_err() {
                        assert!(matches!(result, Err(Error::Invalid(_))));
                    } else {
                        assert!(result.is_ok());
                    }
                }
                return;
            }
        }
    }
    for session in [None, Some(reference)] {
        let arguments = launch::child_arguments(host, &inherited, cwd, session)
            .expect("admitted inherited policy was refused");
        let (prefix, suffix): (&[&str], Vec<OsString>) = match host {
            "codex" => (
                &[],
                vec!["exec".into(), "--json".into(), "--cd".into(), cwd.into()],
            ),
            "claude-code" => (
                &["-p", "--output-format", "stream-json", "--verbose"],
                vec![],
            ),
            "opencode" => (&["run", "--format", "json", "--dir"], vec![]),
            "agy" => (
                &[
                    "--input-format",
                    "stream-json",
                    "--output-format",
                    "stream-json",
                ],
                vec![],
            ),
            _ => (&["--mode", "json", "-p"], vec![]),
        };
        let mut expected = prefix
            .iter()
            .copied()
            .map(OsString::from)
            .chain((host == "opencode").then(|| OsString::from(cwd)))
            .chain(inherited.iter().map(OsString::from))
            .chain(suffix)
            .collect::<Vec<_>>();
        if let Some(reference) = session {
            let option = match host {
                "codex" => "resume",
                "claude-code" => "--resume",
                "pi" => "--fork",
                "agy" => "--conversation",
                _ => "--session",
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
}

fn relative_permissions(input: &str, cwd: &Path, non_unicode: &Path) {
    let target = format!("./{input}");
    let mut original = serde_json::json!({
        "permissions": {}, "provider": serde_json::from_str::<serde_json::Value>(input).ok(),
        "unrelated": input,
    });
    for list in ["allow", "ask", "deny"] {
        original["permissions"][list] = serde_json::json!([
            format!("read_file({target})"),
            format!("write_file({target})"),
            "write_file(*)",
            "read_file(~/.config)",
            "command(git diff)",
            format!("write_file({})", cwd.display()),
        ]);
    }
    let flags = [
        "--sandbox".into(),
        "--mode".into(),
        "plan".into(),
        "--settings".into(),
        original.to_string(),
    ];
    let args = launch::child_arguments("agy", &flags, cwd, None).expect("relative inline policy");
    let settings = args
        .windows(2)
        .find(|pair| pair[0] == "--settings")
        .expect("settings value");
    let actual: serde_json::Value =
        serde_json::from_str(settings[1].to_str().expect("Unicode JSON"))
            .expect("native settings JSON");
    let mut expected = original;
    for list in ["allow", "ask", "deny"] {
        for (index, action) in ["read_file", "write_file"].iter().enumerate() {
            let rule = actual["permissions"][list][index]
                .as_str()
                .expect("native file rule");
            let path = rule
                .strip_prefix(&format!("{action}("))
                .and_then(|rule| rule.strip_suffix(')'))
                .expect("native file rule syntax");
            assert_eq!(
                Path::new(path),
                cwd.join(&target),
                "relative permissions changed authority or workspace"
            );
            assert!(
                !path
                    .split(std::path::MAIN_SEPARATOR)
                    .any(|component| component == "."),
                "relative permissions retained a current-directory component"
            );
            expected["permissions"][list][index] = actual["permissions"][list][index].clone();
        }
    }
    assert_eq!(
        actual, expected,
        "relative permissions changed authority or workspace"
    );
    assert!(matches!(
        launch::child_arguments("agy", &flags, non_unicode, None),
        Err(Error::Invalid(_))
    ));
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
    relative_permissions(encoded, cwd, &non_unicode);
    for (host, required_groups) in [
        (
            "codex",
            &[
                &["--sandbox", "workspace-write"][..],
                &["--ask-for-approval", "never"][..],
            ][..],
        ),
        (
            "claude-code",
            &[
                &["--settings", "{}"][..],
                &["--permission-mode", "default"][..],
            ][..],
        ),
        ("pi", &[][..]),
        ("opencode", &[][..]),
        ("agy", &[&["--sandbox"][..], &["--mode", "auto"][..]][..]),
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
        let mut flags = required_groups
            .iter()
            .flat_map(|group| group.iter().copied())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        flags.extend(["--model".into(), encoded.into()]);
        if host == "claude-code" {
            let whitespace = ["", " ", "\t", "\n"][input.len() % 4];
            flags[1] = format!("{whitespace}{}", serde_json::json!({"policy": encoded}));
        }
        let inherited =
            launch::parent_flags(host, &flags, cwd).expect("admitted non-path policy was refused");
        assert!(
            inherited.starts_with(&flags),
            "native launch changed non-path policy bytes"
        );
        policy(host, &flags, cwd, &reference);
        paths(host, &flags, cwd, &non_unicode, encoded);
        if matches!(host, "codex" | "claude-code" | "agy") {
            assert!(
                matches!(
                    launch::parent_flags(host, &flags, &non_unicode),
                    Err(Error::Invalid(_))
                ),
                "native launch accepted a non-Unicode workspace root"
            );
        }
        let mut start = 0;
        for group in required_groups {
            let mut missing = flags.clone();
            missing.drain(start..start + group.len());
            assert!(
                matches!(launch::validate(host, &missing), Err(Error::Invalid(_))),
                "native launch admitted a missing required policy"
            );
            let mut repeated = flags.clone();
            repeated.extend(group.iter().copied().map(str::to_owned));
            assert!(
                matches!(launch::validate(host, &repeated), Err(Error::Invalid(_))),
                "native launch admitted duplicate required policy"
            );
            start += group.len();
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
