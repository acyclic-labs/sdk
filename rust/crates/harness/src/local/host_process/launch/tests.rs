//! Launch policy: children inherit the root's policy and keep its paths.
use super::*;
use std::path::PathBuf;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[test]
fn relative_parent_paths_keep_their_targets_when_child_cwd_changes() -> Result {
    let fixture = tempfile::tempdir()?;
    let workspace = fixture.path().join("workspace");
    let view = workspace.join(".volumes/agents/child");
    let shared = fixture.path().join("shared");
    std::fs::create_dir_all(&view)?;
    std::fs::create_dir(&shared)?;
    std::fs::write(shared.join("policy.json"), "{}")?;
    for (host, supplied) in [
        (
            "codex",
            vec![
                "--sandbox",
                "workspace-write",
                "--ask-for-approval",
                "never",
                "--add-dir",
                "../shared",
            ],
        ),
        (
            "claude-code",
            vec![
                "--settings",
                "../shared/policy.json",
                "--permission-mode",
                "default",
                "--plugin-dir",
                "../shared",
                "--add-dir",
                "../shared",
            ],
        ),
        (
            "pi",
            vec![
                "--session-dir",
                "../shared",
                "--extension",
                "../shared/policy.json",
            ],
        ),
        (
            "agy",
            vec![
                "--sandbox",
                "--mode",
                "default",
                "--settings",
                "../shared/policy.json",
            ],
        ),
    ] {
        let supplied = supplied.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let flags = parent_flags(host, &supplied, &workspace)?;
        let arguments = child_arguments(host, &flags, &view, None)?
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        for (option, expected) in [
            ("--add-dir", shared.clone()),
            ("--plugin-dir", shared.clone()),
            ("--settings", shared.join("policy.json")),
            ("--session-dir", shared.clone()),
            ("--extension", shared.join("policy.json")),
        ] {
            if supplied.iter().any(|arg| arg == option) {
                if host == "agy" && option == "--settings" {
                    let actual = arguments
                        .windows(2)
                        .find(|pair| pair[0] == option)
                        .ok_or("native settings absent")?;
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&actual[1])?,
                        serde_json::json!({})
                    );
                    continue;
                }
                let actual = arguments
                    .windows(2)
                    .find_map(|pair| match pair {
                        [name, value] if name == option => Some(PathBuf::from(value)),
                        _ => None,
                    })
                    .ok_or("native path option absent")?;
                assert!(
                    actual.is_absolute(),
                    "native path must not change with child cwd"
                );
                assert_eq!(view.join(actual).canonicalize()?, expected.canonicalize()?);
            }
        }
    }
    for relative in [
        "extensions/missing.ts",
        "./extensions/missing.ts",
        "../missing.ts",
    ] {
        let collision = view.join(relative);
        std::fs::create_dir_all(collision.parent().ok_or("extension parent absent")?)?;
        std::fs::write(&collision, "")?;
        assert!(!workspace.join(relative).exists());
        let flags = parent_flags("pi", &["--extension".into(), relative.into()], &workspace)?;
        let arguments = child_arguments("pi", &flags, &view, None)?;
        let inherited = arguments
            .windows(2)
            .find_map(|pair| match pair {
                [name, value] if name == "--extension" => Some(PathBuf::from(value)),
                _ => None,
            })
            .ok_or("inherited extension absent")?;
        assert_eq!(inherited, workspace.join(relative));
        assert!(
            !view.join(inherited).exists(),
            "a missing parent extension must not load the child's collision"
        );
    }
    Ok(())
}

#[test]
fn relative_file_permissions_preserve_native_policy_in_each_view() -> Result {
    use proptest::prelude::*;
    let fixture = tempfile::tempdir()?;
    let root = fixture.path().join("workspace");
    let child = root.join(".volumes/agents/child");
    let settings_file = fixture.path().join("policy.json");
    proptest!(|(target in prop_oneof!["[a-zA-Z0-9 _/-]{1,32}", Just("../linked/../marker".to_owned())], mode in prop_oneof![Just("accept-edits"), Just("plan")])| {
        let mut original = serde_json::json!({
            "modelProvider": "gemini",
            "provider": {"options": {"baseURL": "http://127.0.0.1:1234"}},
            "permissions": {},
            "unknown": {"untouched": [null, false, 4]}
        });
        let relative = Path::new(".").join(&target);
        let relative = relative.to_str().expect("generated Unicode path");
        for list in ["allow", "ask", "deny"] {
            original["permissions"][list] = serde_json::json!([
                format!("write_file({relative})"), format!("read_file({relative})"),
                "write_file(.)", "read_file(.)",
                format!("write_file({})", fixture.path().display()),
                "write_file(*)", "read_file(~/.config)", "command(git diff)", "mcp(server/tool)"
            ]);
        }
        std::fs::write(&settings_file, serde_json::to_vec(&original).expect("JSON settings")).expect("write selected settings");
        for supplied in [original.to_string(), settings_file.to_string_lossy().into_owned()] {
            let flags = parent_flags("agy", &["--sandbox".into(), "--mode".into(), mode.into(), "--settings".into(), supplied], &root).expect("admitted native settings");
            for cwd in [&root, &child] {
                let args = if cwd == &root {
                    policy_arguments("agy", &flags, cwd)
                } else {
                    child_arguments("agy", &flags, cwd, Some("native-session"))
                }.expect("relative policy resolution");
                let resolved = args.windows(2).find(|pair| pair[0] == "--settings").expect("settings option");
                let actual: serde_json::Value = serde_json::from_str(resolved[1].to_str().expect("Unicode settings")).expect("native JSON settings");
                let mut expected = original.clone();
                for list in ["allow", "ask", "deny"] {
                    for (index, action) in ["write_file", "read_file"].iter().enumerate() {
                        let rule = actual["permissions"][list][index].as_str().expect("native file rule");
                        let path = rule.strip_prefix(&format!("{action}(")).and_then(|rule| rule.strip_suffix(')')).expect("native rule syntax");
                        prop_assert_eq!(Path::new(path), cwd.join(relative));
                        prop_assert!(!path.split(std::path::MAIN_SEPARATOR).any(|component| component == "."));
                        expected["permissions"][list][index] = actual["permissions"][list][index].clone();
                        expected["permissions"][list][index + 2] = format!("{action}({})", cwd.display()).into();
                    }
                }
                prop_assert_eq!(actual, expected);
                prop_assert!(args.windows(2).any(|pair| pair[0] == "--mode" && pair[1] == mode));
            }
        }
    });
    for invalid in ["{", "{\"permissions\":"] {
        assert!(matches!(
            agy::settings(invalid, &root),
            Err(Error::Invalid(_))
        ));
    }
    Ok(())
}

#[test]
fn native_child_arguments_allocate_only_the_returned_vector_and_strings() -> Result {
    for (host, supplied) in [
        (
            "codex",
            vec![
                "--sandbox",
                "workspace-write",
                "--ask-for-approval",
                "never",
            ],
        ),
        (
            "claude-code",
            vec!["--settings", "policy.json", "--permission-mode", "default"],
        ),
        (
            "pi",
            vec![
                "--no-extensions",
                "--thinking",
                "high",
                "--session-dir",
                "native-sessions",
            ],
        ),
        ("pi", vec![]),
        ("opencode", vec![]),
        ("agy", vec!["--sandbox", "--mode", "default"]),
    ] {
        let supplied = supplied.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let flags = parent_flags(host, &supplied, Path::new("workspace"))?;
        for session in [None, Some("native-reference")] {
            let mut command = None;
            let allocations = acyclic_test_support::measure(|| {
                command = Some(child_arguments(host, &flags, Path::new("view"), session));
            });
            let command = command.ok_or("native argument result absent")??;
            assert_eq!(
                allocations.count_total,
                command.len() as u64 + 1,
                "native arguments must allocate only their nonempty strings and one result vector"
            );
        }
    }
    Ok(())
}

#[test]
fn native_lifetime_and_delegation_follow_the_admitted_root_policy() -> Result {
    let cwd = Path::new("workspace");
    for timeout in [None, Some("0"), Some("47m"), Some("12h30m")] {
        let mut supplied = vec![
            "--sandbox".into(),
            "--mode".into(),
            "accept-edits".into(),
            "--model".into(),
            "--print-timeout".into(),
        ];
        if let Some(value) = timeout {
            supplied.extend(["--print-timeout".into(), value.into()]);
        }
        let inherited = parent_flags("agy", &supplied, cwd)?;
        for session in [None, Some("original-session")] {
            let args = child_arguments("agy", &inherited, cwd, session)?;
            // --model's value resembles the lifetime switch. Parse option
            // positions, so values never acquire policy authority.
            let mut values = args.iter();
            let mut lifetimes = Vec::new();
            while let Some(option) = values.next() {
                if option == "--sandbox" {
                    continue;
                }
                let value = values.next().ok_or("missing native option value")?;
                if option == "--print-timeout" {
                    lifetimes.push(value.as_os_str());
                }
            }
            assert_eq!(
                lifetimes,
                vec![std::ffi::OsStr::new(timeout.unwrap_or("0"))]
            );
        }
    }
    assert!(
        validate(
            "agy",
            &[
                "--sandbox",
                "--mode",
                "accept-edits",
                "--print-timeout",
                "0",
                "--print-timeout",
                "5m"
            ]
            .map(String::from)
        )
        .is_err()
    );
    for (host, supplied, option, expected) in [
        (
            "codex",
            vec![
                "--sandbox",
                "workspace-write",
                "--ask-for-approval",
                "never",
                "--config",
                "agents.enabled=true",
            ],
            "--config",
            "agents.enabled=false",
        ),
        (
            "claude-code",
            vec![
                "--settings",
                "{}",
                "--permission-mode",
                "default",
                "--disallowedTools",
                "Bash,Read(secret)",
            ],
            "--disallowedTools",
            "Bash,Read(secret),Agent,Task",
        ),
    ] {
        let supplied = supplied.into_iter().map(String::from).collect::<Vec<_>>();
        let flags = parent_flags(host, &supplied, cwd)?;
        for args in [
            policy_arguments(host, &flags, cwd)?,
            child_arguments(host, &flags, cwd, None)?,
        ] {
            assert_eq!(
                args.windows(2)
                    .rfind(|pair| pair[0] == option)
                    .map(|pair| pair[1].as_os_str()),
                Some(std::ffi::OsStr::new(expected))
            );
        }
    }
    Ok(())
}
