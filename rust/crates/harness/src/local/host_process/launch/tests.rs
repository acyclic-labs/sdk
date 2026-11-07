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
