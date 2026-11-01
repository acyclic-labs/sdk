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
    let inline = [
        "--settings",
        " {\"sandbox\":{\"enabled\":true}}",
        "--permission-mode",
        "default",
    ]
    .map(str::to_owned);
    let flags = parent_flags("claude-code", &inline, &workspace)?;
    assert_eq!(
        flags.first_chunk::<4>().ok_or("inline settings absent")?,
        &inline
    );
    Ok(())
}

#[test]
fn pi_package_sources_are_not_shadowed_by_local_filenames() -> Result {
    let fixture = tempfile::tempdir()?;
    let workspace = fixture.path();
    for source in [
        "npm:fixture-extension",
        "builtin:fixture",
        "git:example.com/fixture/extension",
        "github:fixture/extension",
        "http://example.com/fixture/extension",
        "https://example.com/fixture/extension",
        "ssh://example.com/fixture/extension",
    ] {
        #[cfg(unix)]
        {
            let collision = workspace.join(source);
            std::fs::create_dir_all(collision.parent().ok_or("fixture parent absent")?)?;
            std::fs::write(
                collision,
                "a local filename must not replace a package source",
            )?;
        }
        let supplied = ["--extension".into(), source.into()];
        assert_eq!(
            parent_flags("pi", &supplied, workspace)?,
            supplied,
            "native package source must not become a local extension"
        );
    }
    Ok(())
}

#[test]
fn native_launch_preserves_policy_cwd_and_resume_without_bypass() -> Result {
    let supplied = [
        "--sandbox",
        "workspace-write",
        "--ask-for-approval",
        "never",
    ]
    .map(str::to_owned);
    assert!(
        validate(
            "codex",
            supplied.first_chunk::<2>().ok_or("policy pair absent")?
        )
        .is_err()
    );
    let flags = parent_flags("codex", &supplied, Path::new("workspace"))?;
    let (policy, roots) = flags
        .split_at_checked(supplied.len())
        .ok_or("inherited policy absent")?;
    assert_eq!(policy, supplied);
    assert_eq!(roots, ["--add-dir", "workspace"]);
    let command = child_arguments("codex", &flags, Path::new("view"), Some("resume-id"))?;
    assert!(
        child_arguments("codex", &flags, Path::new("view"), Some("--unsafe")).is_err(),
        "resume references cannot change native options"
    );
    assert_eq!(
        command
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        [
            "--sandbox",
            "workspace-write",
            "--ask-for-approval",
            "never",
            "--add-dir",
            "workspace",
            "exec",
            "--json",
            "--cd",
            "view",
            "resume",
            "resume-id",
            "-"
        ]
    );
    Ok(())
}

#[test]
fn claude_and_pi_children_preserve_native_options_and_resume_references() -> Result {
    for (host, flags, resume_option) in [
        (
            "claude-code",
            vec!["--settings", "policy.json", "--permission-mode", "default"],
            "--resume",
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
            "--session",
        ),
        (
            "pi",
            vec![
                "--thinking",
                "high",
                "--no-extensions",
                "--session-dir",
                "native-sessions",
            ],
            "--session",
        ),
        (
            "pi",
            vec![
                "--thinking",
                "high",
                "--session-dir",
                "native-sessions",
                "--no-extensions",
            ],
            "--session",
        ),
    ] {
        let supplied = flags.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let mut foreign_switch = supplied.clone();
        foreign_switch.push("--unknown-policy".into());
        assert!(validate(host, &foreign_switch).is_err());
        let mut missing_value = supplied.clone();
        missing_value.push("--model".into());
        assert!(validate(host, &missing_value).is_err());
        if host != "pi" {
            let mut pi_switch = supplied.clone();
            pi_switch.push("--no-extensions".into());
            assert!(validate(host, &pi_switch).is_err());
        }
        let flags = parent_flags(host, &supplied, Path::new("workspace"))?;
        if host == "pi" {
            assert_eq!(
                flags
                    .windows(2)
                    .find(|pair| pair[0] == "--session-dir")
                    .ok_or("native session directory absent")?[1],
                Path::new("workspace")
                    .join("native-sessions")
                    .to_string_lossy()
                    .into_owned()
            );
            assert_eq!(
                flags.iter().position(|flag| flag == "--no-extensions"),
                supplied.iter().position(|flag| flag == "--no-extensions")
            );
        } else {
            assert_eq!(
                flags.get(supplied.len()..).ok_or("workspace root absent")?,
                ["--add-dir", "workspace"]
            );
        }
        let command = child_arguments(
            host,
            &flags,
            Path::new("admitted-view"),
            Some("native-reference"),
        )?;
        assert!(
            child_arguments(host, &flags, Path::new("admitted-view"), Some("--unsafe")).is_err(),
            "resume references cannot change native options"
        );
        let arguments = command
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(arguments.windows(flags.len()).any(|window| window == flags));
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == [resume_option, "native-reference"])
        );
    }
    assert!(
        validate(
            "claude-code",
            &["--permission-mode".into(), "default".into()]
        )
        .is_err()
    );
    Ok(())
}
