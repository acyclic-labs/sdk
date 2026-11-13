use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    workspace::{VolumeView, compose},
};
use acyclic_fs::access::*;
use std::{collections::BTreeMap, path::PathBuf};
#[cfg(unix)]
use std::{fs::OpenOptions, io::Write};

fn workspace_hints(environment: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    serde_json::from_str(&environment["ACYCLIC_WORKSPACES"]).expect("native discovery is JSON")
}

#[cfg(unix)]
fn table(root: PathBuf) -> Result<MountTable<Arc<tempfile::TempDir>>> {
    table_with_home(root, false)
}

fn table_with_home(root: PathBuf, home: bool) -> Result<MountTable<Arc<tempfile::TempDir>>> {
    let native = Arc::new(tempfile::tempdir().map_err(Error::process_io)?);
    let resource = ResourcePattern {
        lineage_root: None,
        volume: None,
        generations: Generations::All,
        path: "**".into(),
    };
    let principal = Principal {
        kind: PrincipalKind::Agent,
        id: "owner".into(),
        lineage: vec!["owner".into()],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let actions = Action::ALL.into_iter().collect();
    let policy = Arc::new(Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![vec![Rule {
            id: "owner".into(),
            effect: Effect::Allow,
            principal: PrincipalPattern::default(),
            actions,
            resource: resource.clone(),
            conditions: Conditions::default(),
        }]],
    });
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource,
        }]],
        expires_at: None,
    };
    let volume = |name: &str| -> Result<_> {
        Ok(VolumeView {
            name: name.into(),
            agent: "owner".into(),
            volume: VolumeRef::new(
                ProviderRef::new(
                    "bash-test".into(),
                    "filesystem".into(),
                    "1".into(),
                    &mut None,
                )?,
                name.into(),
                VolumeClass::Project,
                VolumeOwner::Project("project".into()),
                &mut None,
            )?,
            lineage: principal.lineage.clone(),
            generation: 1,
            head: 1,
            config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Ephemeral),
            policy: policy.clone(),
            source: Arc::clone(&native),
        })
    };
    let mut volumes = vec![volume("data")?, volume("shared")?];
    if home {
        volumes.push(volume("home")?);
    }
    let mut mounts = compose(
        root,
        &principal,
        &capability,
        Context {
            now: 1,
            approvals: 0,
        },
        volume("project")?,
        volumes,
    )?;
    for (relative, mount) in mounts.mounts_mut() {
        if relative == "volume:///shared" {
            mount.native_root = crate::workspace::NativeRoot::Logical;
        } else if !relative.is_empty() {
            mount.native_root = crate::workspace::NativeRoot::Path(native.path().to_path_buf());
        }
    }
    Ok(mounts)
}

fn invocation(root: &Path) -> Invocation {
    Invocation {
        // Unix environment-isolation children deliberately cannot discover bash
        // through PATH. Windows still resolves its own native shell.
        #[cfg(unix)]
        executable: PathBuf::from("/bin/bash"),
        #[cfg(windows)]
        executable: acyclic_native_runtime::platform_shell()
            .expect("native platform shell")
            .to_path_buf(),
        argv: Vec::new(),
        cwd: root.into(),
        environment: BTreeMap::new(),
        inherit_environment: false,
        execution_grant: None,
        schedule: acyclic_native_runtime::CaptureSchedule::Unbounded,
        maximum_output_bytes: Some(32_768),
        process_custody: None,
        standard_input: None,
    }
}

#[cfg(any(target_os = "linux", windows))]
#[tokio::test]
async fn abandoned_preparation_retains_its_root_and_leaves_the_executor_available() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    struct Retired(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Retired {
        fn drop(&mut self) {
            if let Some(retired) = self.0.take() {
                let _ = retired.send(());
            }
        }
    }

    for refuse in [false, true] {
        let project = tempfile::tempdir().map_err(Error::process_io)?;
        let root = project.path().canonicalize().map_err(Error::process_io)?;
        let mounts = Arc::new(table_with_home(root.clone(), true)?);
        let home = mounts
            .native_mounts()
            .find(|(name, _, _)| *name == "volume:///home")
            .expect("private home projection")
            .1
            .to_path_buf();
        if refuse {
            std::fs::write(home.join("tmp"), b"occupied").map_err(Error::process_io)?;
        }
        let native = invocation(&root);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let (retired, retirement) = tokio::sync::oneshot::channel();
        let preparation_mounts = Arc::clone(&mounts);
        let (completed, completion) = tokio::sync::oneshot::channel();
        let mut preparation = Box::pin(super::super::prepare_with_custody(
            // Tuple fields retire in order: the event follows physical root cleanup.
            (project, Retired(Some(retired))),
            move |_| {
                let _ = entered.send(());
                released
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .map_err(|cause| {
                        Error::Storage(format!("preparation blocked its executor: {cause}"))
                    })?;
                let prepared = prepare(
                    ShellInput {
                        command: "echo prepared".into(),
                        cwd: None,
                        timeout_ms: None,
                        background: false,
                        target: None,
                    },
                    &preparation_mounts,
                    native,
                    1,
                    std::time::Instant::now(),
                );
                let _ = completed.send(prepared.is_err());
                prepared
            },
        ));
        {
            // Even a callback admitting inline I/O must dispatch this preparation.
            // An inline implementation reaches its bounded refusal before this
            // executor can observe the worker's entry event.
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            tokio::select! {
                biased;
                _ = &mut preparation => panic!("preparation ran on the executor"),
                started = started => started.map_err(|cause| Error::Storage(cause.to_string()))?,
            }
        }
        drop(preparation);
        assert!(
            root.is_dir(),
            "caller loss released the admitted root early"
        );
        release
            .send(())
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        retirement
            .await
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        assert!(!root.exists(), "completed preparation retained its root");
        assert_eq!(
            completion
                .await
                .map_err(|cause| Error::Storage(cause.to_string()))?,
            refuse,
            "preparation did not preserve the original filesystem outcome"
        );
        assert_eq!(home.join("tmp").is_dir(), !refuse);
    }
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
#[tokio::test]
async fn temporary_files_use_private_home_with_default_and_explicit_grants() -> Result<()> {
    let project = tempfile::tempdir().map_err(Error::process_io)?;
    let mut mounts = table_with_home(
        project.path().canonicalize().map_err(Error::process_io)?,
        true,
    )?;
    let home = mounts
        .native_mounts()
        .find(|(name, _, _)| *name == "volume:///home")
        .expect("private home")
        .1
        .to_path_buf();
    #[cfg(target_os = "linux")]
    let binding = tempfile::tempdir().map_err(Error::process_io)?;
    #[cfg(target_os = "linux")]
    let alias = binding.path().join("home");
    #[cfg(target_os = "linux")]
    std::os::unix::fs::symlink(&home, &alias).map_err(Error::process_io)?;
    #[cfg(windows)]
    let alias = home.clone();
    for (name, mount) in mounts.mounts_mut() {
        if name == "volume:///home" {
            mount.native_root = crate::workspace::NativeRoot::Path(alias.clone());
        }
    }
    #[cfg(target_os = "linux")]
    let command = r#"test "$HOME" = "$ACYCLIC_VOLUME_HOME" && test "$TMPDIR" = "$HOME/tmp" && printf scratch > "$TMPDIR/receipt""#;
    #[cfg(windows)]
    let command = r#"if ($HOME -ne $env:ACYCLIC_VOLUME_HOME -or $env:HOME -ne $env:ACYCLIC_VOLUME_HOME -or $env:USERPROFILE -ne $env:ACYCLIC_VOLUME_HOME -or $env:TMPDIR -ne (Join-Path $HOME 'tmp') -or $env:TEMP -ne $env:TMPDIR -or $env:TMP -ne $env:TMPDIR) { exit 2 }; [System.IO.File]::WriteAllText((Join-Path $env:TMPDIR 'receipt'), 'scratch')"#;
    for explicit in [false, true] {
        let mut native = invocation(&mounts.root);
        if explicit {
            native.execution_grant = workspace_grant(&mounts, &mut native.environment)?;
            // Grant selection must not be responsible for environment setup.
            native.environment.clear();
            #[cfg(windows)]
            {
                for name in ["home", "userprofile"] {
                    native.environment.insert(
                        name.into(),
                        home.to_str().expect("UTF-8 private home").into(),
                    );
                }
                for name in ["tmpdir", "temp", "tmp"] {
                    native.environment.insert(
                        name.into(),
                        project.path().to_str().expect("UTF-8 project").into(),
                    );
                }
            }
        }
        let result = run(
            ShellInput {
                command: command.into(),
                cwd: None,
                timeout_ms: None,
                background: false,
                target: None,
            },
            &mounts,
            native,
            1,
            None,
            || Ok(()),
        )
        .await?
        .expect("completed shell");
        assert!(result.success, "{result:?}");
        assert_eq!(
            std::fs::read(home.join("tmp/receipt")).map_err(Error::process_io)?,
            b"scratch"
        );
        assert!(!project.path().join("tmp").exists());
    }
    #[cfg(target_os = "linux")]
    {
        // Scratch creation must not follow an authored link outside the volume.
        std::fs::remove_file(home.join("tmp/receipt")).map_err(Error::process_io)?;
        std::fs::remove_dir(home.join("tmp")).map_err(Error::process_io)?;
        std::os::unix::fs::symlink(project.path(), home.join("tmp")).map_err(Error::process_io)?;
        assert!(workspace_environment(&mounts, &mut BTreeMap::new()).is_err());
        assert!(!project.path().join("receipt").exists());
    }
    Ok(())
}

// Admission failures must preserve their original error class.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_only_project_uses_its_writable_volume_for_temporary_files() -> Result<()> {
    let project = tempfile::tempdir().map_err(Error::process_io)?;
    let mut mounts = table(project.path().canonicalize().map_err(Error::process_io)?)?;
    for (name, mount) in mounts.mounts_mut() {
        if name.is_empty() {
            mount.mode = MountMode::default();
        }
    }
    let data = mounts
        .native_mounts()
        .find(|(name, _, _)| *name == "volume:///data")
        .expect("writable data projection")
        .1
        .to_path_buf();
    let result = run(
        ShellInput {
            command:
                r#"test "$TMPDIR" = "$ACYCLIC_VOLUME_DATA" && printf scratch > "$TMPDIR/receipt""#
                    .into(),
            cwd: None,
            timeout_ms: None,
            background: false,
            target: None,
        },
        &mounts,
        invocation(&mounts.root),
        1,
        None,
        || Ok(()),
    )
    .await?
    .expect("completed shell");
    assert!(result.success, "{result:?}");
    assert_eq!(
        std::fs::read(data.join("receipt")).map_err(Error::process_io)?,
        b"scratch"
    );
    assert!(!project.path().join("receipt").exists());
    Ok(())
}

// Admission failures must preserve their original error class.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn shell_admission_does_not_require_a_logical_volume_path() -> Result<()> {
    let temp = tempfile::tempdir().map_err(Error::process_io)?;
    let root = temp.path().join("view");
    std::fs::create_dir_all(&root).map_err(Error::process_io)?;
    let root = root.canonicalize().map_err(Error::process_io)?;
    for args in [
        vec!["init", "--quiet"],
        vec![
            "-c",
            "user.name=Regression",
            "-c",
            "user.email=regression@example.invalid",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    ] {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(&root)
            .status()
            .map_err(Error::process_io)?;
        assert!(status.success());
    }
    let mut mounts = table(root.clone())?;
    let missing = temp.path().join("missing-shared");
    assert!(!missing.try_exists().map_err(Error::process_io)?);
    const SAVED_COMMAND: &str = "pwd && git rev-parse HEAD && git status --short && ls -la && if [ -f inventory.json ]; then cat inventory.json; fi";
    let input = || ShellInput {
        command: crate::tools::ShellCommand::Source(SAVED_COMMAND.into()),
        cwd: Some(".".into()),
        timeout_ms: Some(20000),
        background: false,
        target: None,
    };
    // A claimed physical binding must fail with its real filesystem class;
    // omitting absent roots by existence would silently weaken authority.
    for (relative, mount) in mounts.mounts_mut() {
        if relative == "volume:///shared" {
            mount.native_root = crate::workspace::NativeRoot::Path(missing.clone());
        }
    }
    let error = match prepare(
        input(),
        &mounts,
        invocation(&root),
        1,
        std::time::Instant::now(),
    ) {
        Err(error) => error,
        Ok(_) => panic!("missing physical root admitted"),
    };
    assert!(
        matches!(&error, Error::Process(crate::ProcessError::NotFound { .. })),
        "{error:?}"
    );
    assert_eq!(
        crate::executor::ToolErrorClass::of(&error),
        crate::executor::ToolErrorClass::NotFound
    );
    assert!(
        error
            .to_string()
            .contains(missing.to_str().expect("fixture path")),
        "{error}"
    );
    for (relative, mount) in mounts.mounts_mut() {
        if relative == "volume:///shared" {
            mount.native_root = crate::workspace::NativeRoot::Logical;
        }
    }
    let mut native = invocation(&root);
    native
        .environment
        .insert("PATH".into(), std::env::var("PATH").expect("test PATH"));
    let output = run(input(), &mounts, native, 1, None, || Ok(()))
        .await?
        .expect("completed call");
    assert!(output.success, "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(".git"),
        "{output:?}"
    );
    assert!(!missing.try_exists().map_err(Error::process_io)?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn sandbox_denies_outside_write_including_descendants_and_volume_aliases() -> Result<()> {
    let temp = tempfile::tempdir().map_err(Error::process_io)?;
    let root = temp.path().join("view");
    let volume = temp.path().join("volume");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&root).map_err(Error::process_io)?;
    std::fs::create_dir(&volume).map_err(Error::process_io)?;
    std::fs::write(&outside, "original").map_err(Error::process_io)?;
    std::os::unix::fs::symlink(&outside, root.join("escape")).map_err(Error::process_io)?;
    let mut mounts = table(root.canonicalize().map_err(Error::process_io)?)?;
    for (relative, mount) in mounts.mounts_mut() {
        if !relative.is_empty() {
            mount.native_root = crate::workspace::NativeRoot::Logical;
        }
    }
    // A logical binding grants file-tool access without fabricating an OS
    // projection, even when a same-named native directory happens to exist.
    assert!(!workspace_hints(&mounts.environment()?).contains_key("data"));
    assert!(matches!(
        prepare(
            ShellInput {
                command: crate::tools::ShellCommand::Source("true".into()),
                cwd: Some("volume:///data".into()),
                timeout_ms: None,
                background: false,
                target: None,
            },
            &mounts,
            invocation(&mounts.root),
            1,
            std::time::Instant::now(),
        ),
        Err(Error::Unsupported(_))
    ));
    for (relative, mount) in mounts.mounts_mut() {
        if relative == "volume:///data" {
            mount.native_root = crate::workspace::NativeRoot::Path(volume.clone());
        }
    }
    assert!(!workspace_hints(&mounts.environment()?).contains_key("shared"));
    let logical = prepare(
        crate::tools::ShellInput {
            cwd: Some("volume:///shared".into()),
            command: crate::tools::ShellCommand::Source("true".into()),
            timeout_ms: None,
            background: false,
            target: None,
        },
        &mounts,
        invocation(&root),
        1,
        std::time::Instant::now(),
    );
    assert!(matches!(logical, Err(Error::Unsupported(_))));
    // Both source and literal argv use production preparation and the original
    // native capture owner. No optional-kernel skip: this is Linux qualification.
    for (cwd, command) in [
        (
            None,
            crate::tools::ShellCommand::Source(
                r#"printf inside > inside; printf volume > "$FIXTURE_DATA/written";
             printf temp > "$TMPDIR/tempfile";
             /bin/cat "$OUTSIDE" >/dev/null 2>&1 && exit 13;
             /bin/cat /proc/self/environ >/dev/null 2>&1 && exit 14;
             (printf escaped > escape) 2>/dev/null && exit 10;
             /bin/sh -c 'printf escaped > "$1"' sh "$OUTSIDE" 2>/dev/null && exit 11;
             /bin/cat "/proc/$OWNER/environ" >/dev/null 2>&1 && exit 12;
             printf confined"#
                    .into(),
            ),
        ),
        (
            Some("volume:///data".into()),
            crate::tools::ShellCommand::Argv(vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf mounted > cwd-write; printf mounted".into(),
            ]),
        ),
    ] {
        let mut native = invocation(&mounts.root);
        native.environment.insert(
            "FIXTURE_DATA".into(),
            workspace_hints(&mounts.environment()?)["data"].clone(),
        );
        native.environment.insert(
            "OUTSIDE".into(),
            outside.to_str().expect("fixture path").into(),
        );
        native
            .environment
            .insert("OWNER".into(), std::process::id().to_string());
        let output = run(
            ShellInput {
                command,
                cwd,
                timeout_ms: None,
                background: false,
                target: None,
            },
            &mounts,
            native,
            1,
            None,
            || Ok(()),
        )
        .await?
        .ok_or_else(|| Error::Storage("sandbox capture interrupted".into()))?;
        assert!(output.success, "{output:?}");
        assert!(!output.stdout.is_empty());
        assert_eq!(
            std::fs::read(&outside).map_err(Error::process_io)?,
            b"original"
        );
    }
    assert_eq!(
        std::fs::read(root.join("inside")).map_err(Error::process_io)?,
        b"inside"
    );
    assert_eq!(
        std::fs::read(root.join("tempfile")).map_err(Error::process_io)?,
        b"temp"
    );
    assert_eq!(
        std::fs::read(volume.join("written")).map_err(Error::process_io)?,
        b"volume"
    );
    assert_eq!(
        std::fs::read(volume.join("cwd-write")).map_err(Error::process_io)?,
        b"mounted"
    );
    Ok(())
}

// A declared native binding may leave the primary root, but resolving a
// symlink within it must never leave that binding's own authorized root.
#[cfg(unix)]
#[test]
fn bound_volume_cwd_keeps_its_authority() {
    use proptest::prelude::*;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("view");
    let native = temp.path().join("native");
    let outside = temp.path().join("outside");
    for directory in [&root, &native, &outside] {
        std::fs::create_dir(directory).unwrap();
    }
    std::os::unix::fs::symlink(&outside, native.join(".escape")).unwrap();
    std::os::unix::fs::symlink(temp.path(), root.join("escape")).unwrap();
    let native = native.canonicalize().unwrap();
    let mut mounts = table(root.canonicalize().unwrap()).unwrap();
    for (relative, mount) in mounts.mounts_mut() {
        if relative == "volume:///data" {
            mount.native_root = crate::workspace::NativeRoot::Path(native.clone());
        }
    }
    // The task root has no native binding; it must refuse the same escape at
    // production preparation, before a command can create anything outside it.
    let result = prepare(
        ShellInput {
            command: "printf escaped > should-not-exist".into(),
            cwd: Some("escape".into()),
            timeout_ms: None,
            background: false,
            target: None,
        },
        &mounts,
        invocation(&mounts.root),
        1,
        std::time::Instant::now(),
    );
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert!(!temp.path().join("should-not-exist").exists());
    // All cases use the same original authority. Only the requested component
    // and escape decision vary; no case publishes or changes this mount table.
    let mut config = ProptestConfig::with_cases(32);
    config.source_file = Some(file!());
    config.test_name = Some(concat!(
        module_path!(),
        "::bound_volume_cwd_keeps_its_authority"
    ));
    proptest::test_runner::TestRunner::new(config)
        .run(
            &(
                "[a-z]{1,12}",
                proptest::bool::ANY,
                proptest::option::of(300_001u32..=u32::MAX),
            ),
            |(component, escape, lifetime)| {
                std::fs::create_dir_all(native.join(&component)).unwrap();
                let suffix = if escape { ".escape" } else { &component };
                let before = std::time::Instant::now();
                let result = prepare(
                    ShellInput {
                        command: "pwd".into(),
                        cwd: Some(format!("volume:///data/{suffix}")),
                        timeout_ms: lifetime,
                        background: false,
                        target: None,
                    },
                    &mounts,
                    invocation(&mounts.root),
                    1,
                    before,
                );
                if escape {
                    prop_assert!(matches!(result, Err(Error::Invalid(_))));
                } else {
                    let prepared = result.unwrap();
                    match (lifetime, prepared.schedule) {
                        (None, acyclic_native_runtime::CaptureSchedule::Unbounded) => {}
                        (
                            Some(milliseconds),
                            acyclic_native_runtime::CaptureSchedule::Deadline(deadline),
                        ) => {
                            let duration =
                                std::time::Duration::from_millis(u64::from(milliseconds));
                            // The deadline runs from admission, not preparation.
                            prop_assert_eq!(deadline, before + duration);
                        }
                        other => {
                            return Err(TestCaseError::fail(format!(
                                "shell lifetime changed: {other:?}"
                            )));
                        }
                    }
                    prop_assert_eq!(prepared.cwd, native.join(&component));
                    prop_assert_eq!(
                        workspace_hints(&prepared.environment).get("data").unwrap(),
                        native.to_str().unwrap()
                    );
                }
                Ok(())
            },
        )
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn shell_preserves_explicit_environment_without_inheriting_host_secrets() -> Result<()> {
    use proptest::prelude::*;
    use std::os::unix::{ffi::OsStringExt, fs::PermissionsExt};
    const CHILD: &str = "ACYCLIC_TEST_BASH_HOST_ENVIRONMENT";
    if crate::native_environment::variable(CHILD).is_none() {
        let temp = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let bin = temp.path().join("bin");
        let home = temp.path().join("home");
        std::fs::create_dir(&bin).map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(&home).map_err(|error| Error::Storage(error.to_string()))?;
        let tool = bin.join("acyclic-explicit-path-only");
        std::fs::write(&tool, b"#!/bin/sh\nprintf 'ordinary: %s\\nhome: %s\\n' \"$ACYCLIC_TEST_ORDINARY\" \"$HOME\"\n")
            .map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| Error::Storage(error.to_string()))?;
        let startup = temp.path().join("startup");
        std::fs::write(&startup, b"printf ambient-startup >&2\n")
            .map_err(|error| Error::Storage(error.to_string()))?;
        // A separate real test process owns the hostile ambient environment;
        // never mutate environment shared by other tests or native workers.
        let test_name = format!(
            "{}::shell_preserves_explicit_environment_without_inheriting_host_secrets",
            module_path!()
                .split_once("::")
                .map_or(module_path!(), |(_, path)| path)
        );
        let executable =
            std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?;
        // The closed allowlist excludes each ambient key independently. Put all
        // 32 generated non-NUL native values in one isolated environment; the
        // same shell observes their absence and the original leak control.
        proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(1))
            .run(
                &prop::collection::vec(prop::collection::vec(1u8..=255, 0..32), 32),
                |values| {
                    let mut command = std::process::Command::new(&executable);
                    command
                        .args([test_name.as_str(), "--exact", "--nocapture"])
                        .env(CHILD, &home)
                        .env("PATH", &bin)
                        .env("HOME", "ambient-home")
                        .env("ANTHROPIC_API_KEY", "fixture-only-credential")
                        .env(
                            "ACYCLIC_TEST_RENAMED_SECRET",
                            "fixture-only-renamed-credential",
                        )
                        .env("ACYCLIC_WORKSPACES", "stale mount")
                        .env("ACYCLIC_VOLUME_DATA_MODE", "ro")
                        .env("BASH_ENV", &startup)
                        .env(
                            std::ffi::OsString::from_vec(b"ACYCLIC_TEST_NATIVE_\xff".to_vec()),
                            std::ffi::OsString::from_vec(b"fixture-native-\xff".to_vec()),
                        );
                    for (index, value) in values.into_iter().enumerate() {
                        let name = if index == 0 {
                            "ACYCLIC_TEST_NATIVE_BYTES".into()
                        } else {
                            format!("ACYCLIC_TEST_NATIVE_BYTES_{index}")
                        };
                        command.env(name, std::ffi::OsString::from_vec(value));
                    }
                    let result = command
                        .output()
                        .map_err(|error| TestCaseError::fail(error.to_string()))?;
                    prop_assert!(
                        result.status.success(),
                        "{}\n{}",
                        String::from_utf8_lossy(&result.stdout),
                        String::from_utf8_lossy(&result.stderr)
                    );
                    // A stale selector may succeed after running zero tests.
                    prop_assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
                    Ok(())
                },
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
        return Ok(());
    }
    let home = crate::native_environment::variable(CHILD).expect("fixture-owned child home");
    let home = home.to_str().expect("UTF-8 fixture path");
    // The PATH-only fixture tool is part of this fixture's workspace, rather
    // than an unrelated directory whose readability the default must deny.
    let workspace = Path::new(home).parent().expect("fixture home has a parent");
    let mounts = table(
        workspace
            .canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
    let input = ShellInput {
        command: r#"test -z "${ANTHROPIC_API_KEY+x}" && test -z "${ACYCLIC_TEST_RENAMED_SECRET+x}" && test -z "${ACYCLIC_TEST_NATIVE_BYTES+x}" && test -z "${!ACYCLIC_TEST_NATIVE_BYTES*}" && test -z "${ACYCLIC_TEST_BASH_HOST_ENVIRONMENT+x}" && test -z "${BASH_ENV+x}" && test -n "$ACYCLIC_WORKSPACES" && acyclic-explicit-path-only"#.into(),
        cwd: None,
        timeout_ms: Some(5000), background: false, target: None,
    };
    let configured = |inherit_environment| {
        let mut invocation = invocation(&mounts.root);
        invocation.inherit_environment = inherit_environment;
        invocation.environment =
            crate::native_environment::tool_environment(|| Ok(invocation.executable.as_path()))
                .unwrap();
        invocation.environment.insert("HOME".into(), home.into());
        invocation
            .environment
            .insert("ACYCLIC_TEST_ORDINARY".into(), "approved".into());
        invocation
    };
    for inherit_environment in [false, true] {
        let prepared = prepare(
            input.clone(),
            &mounts,
            configured(inherit_environment),
            1,
            std::time::Instant::now(),
        )?;
        assert!(!prepared.inherit_environment);
        assert_eq!(
            prepared.environment.get("ACYCLIC_WORKSPACES"),
            mounts.environment()?.get("ACYCLIC_WORKSPACES")
        );
        let output = capture::capture_with_custody(
            prepared,
            |(), _| Ok(()),
            || Ok(()),
            (),
            None,
            capture::OutputRetention::Collect,
        )
        .await?
        .0?
        .ok_or_else(|| Error::Storage("process capture was indeterminate".into()))?;
        assert!(output.success);
        assert_eq!(
            output.stdout,
            format!("ordinary: approved\nhome: {home}\n").as_bytes()
        );
        assert!(output.stderr.is_empty());
    }
    let mut collision = configured(false);
    collision
        .environment
        .insert("ACYCLIC_WORKSPACES".into(), "stale explicit mode".into());
    assert!(matches!(
        prepare(
            input.clone(),
            &mounts,
            collision,
            1,
            std::time::Instant::now()
        ),
        Err(Error::Invalid(_))
    ));
    // An empty production map loses a tool outside the shell's default path.
    let mut missing_path = prepare(
        input.clone(),
        &mounts,
        configured(false),
        1,
        std::time::Instant::now(),
    )?;
    missing_path.environment.remove("PATH");
    let output = capture::capture_with_custody(
        missing_path,
        |(), _| Ok(()),
        || Ok(()),
        (),
        None,
        capture::OutputRetention::Collect,
    )
    .await?
    .0?
    .ok_or_else(|| Error::Storage("tool search capture was indeterminate".into()))?;
    assert!(!output.success);
    assert!(output.stdout.is_empty());
    let mut invalid = configured(false);
    invalid
        .environment
        .insert("invalid=key".into(), "fixture-only-credential".into());
    let error = prepare(
        input.clone(),
        &mounts,
        invalid,
        1,
        std::time::Instant::now(),
    )
    .err()
    .expect("invalid settings refuse");
    assert!(matches!(error, Error::Invalid(_)));
    assert!(!error.to_string().contains("fixture-only-credential"));
    // The old inheritance choice physically exposes both named credentials and
    // the ambient shell startup hook to exactly the same command.
    let mut broken = prepare(
        input,
        &mounts,
        configured(false),
        1,
        std::time::Instant::now(),
    )?;
    // This is the deliberately unconfined old path, not an attempt to make
    // an authenticated grant accept an ambient environment.
    broken.execution_grant = None;
    broken.inherit_environment = true;
    let output = capture::capture_with_custody(
        broken,
        |(), _| Ok(()),
        || Ok(()),
        (),
        None,
        capture::OutputRetention::Collect,
    )
    .await?
    .0?
    .ok_or_else(|| Error::Storage("negative control was indeterminate".into()))?;
    assert!(!output.success);
    assert_eq!(output.stderr, b"ambient-startup");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn command_uses_the_composed_cwd_and_streams_before_returning_its_exit_code() -> Result<()> {
    let temp = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let root = temp.path().join("view");
    std::fs::create_dir_all(&root).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(root.join("src")).map_err(|error| Error::Storage(error.to_string()))?;
    let mounts = table(
        root.canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
    std::fs::write(
        mounts
            .native_root_for(mounts.named(Some("data"))?.1)
            .expect("native data binding")
            .join("input"),
        b"from data\n",
    )
    .map_err(Error::process_io)?;
    let stdout = temp.path().join("stdout");
    let stderr = temp.path().join("stderr");
    let paths = (stdout.clone(), stderr.clone());
    let observer: Arc<NativeProcessObserver> = Arc::new(move |chunk| {
        let (path, bytes) = match chunk {
            acyclic_native_runtime::ProcessOutput::Stdout(bytes) => (&paths.0, bytes),
            acyclic_native_runtime::ProcessOutput::Stderr(bytes) => (&paths.1, bytes),
        };
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?
            .write_all(bytes)
    });
    let mut native = invocation(&mounts.root);
    native.environment.insert(
        "FIXTURE_DATA".into(),
        workspace_hints(&mounts.environment()?)["data"].clone(),
    );
    let output = run(ShellInput { command: r#"test -n "$ACYCLIC_WORKSPACES"; IFS= read -r line < "$FIXTURE_DATA/input"; printf '%s' "$line" > result; printf streamed; printf diagnostic >&2; exit 7"#.into(), cwd: Some("src".into()), timeout_ms: Some(5000), background: false, target: None, },
        &mounts, native, 1, Some(observer), || Ok(())).await?
        .ok_or_else(|| Error::Storage("process capture was indeterminate".into()))?;
    assert_eq!(output.exit_code, Some(7));
    assert!(!output.success);
    assert!(output.stop.is_none());
    assert_eq!(output.stdout, b"streamed");
    assert_eq!(output.stderr, b"diagnostic");
    assert_eq!(
        std::fs::read(stdout).map_err(|error| Error::Storage(error.to_string()))?,
        output.stdout
    );
    assert_eq!(
        std::fs::read(stderr).map_err(|error| Error::Storage(error.to_string()))?,
        output.stderr
    );
    assert_eq!(
        std::fs::read(root.join("src/result"))
            .map_err(|error| Error::Storage(error.to_string()))?,
        b"from data"
    );
    // The same admitted view also runs literal argv without shell expansion.
    let literal = "$(touch injected); * $HOME ' ";
    let output = run(
        ShellInput {
            command: crate::tools::ShellCommand::Argv(vec![
                "/usr/bin/printf".into(),
                "%s\n".into(),
                literal.into(),
                "".into(),
            ]),
            cwd: None,
            timeout_ms: None,
            background: false,
            target: None,
        },
        &mounts,
        invocation(&mounts.root),
        1,
        None,
        || Ok(()),
    )
    .await?
    .ok_or_else(|| Error::Storage("literal capture was indeterminate".into()))?;
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout, format!("{literal}\n\n").as_bytes());
    assert!(!root.join("injected").exists());
    // Standard input is data even when it contains shell syntax or NUL bytes.
    let stdin = "$(touch injected); $HOME ' \" \0\n";
    let output = run(
        ShellInput {
            command: crate::tools::ShellCommand::Input {
                argv: vec!["/usr/bin/cat".into()],
                stdin: stdin.into(),
            },
            cwd: None,
            timeout_ms: None,
            background: false,
            target: None,
        },
        &mounts,
        invocation(&mounts.root),
        1,
        None,
        || Ok(()),
    )
    .await?
    .ok_or_else(|| Error::Storage("stdin capture was indeterminate".into()))?;
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout, stdin.as_bytes());
    assert!(!root.join("injected").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn missing_shell_inputs_are_preflight_results_with_paths() -> Result<()> {
    let directory = tempfile::tempdir().map_err(Error::process_io)?;
    let root = directory.path().join("view");
    std::fs::create_dir(&root).map_err(Error::process_io)?;
    let root = root.canonicalize().map_err(Error::process_io)?;
    std::fs::write(root.join("file-cwd"), b"file").map_err(Error::process_io)?;
    let mounts = table(root)?;
    for (cwd, command) in [
        (
            Some(".volumes/data".into()),
            crate::tools::ShellCommand::Source("pwd".into()),
        ),
        (
            Some("missing-cwd".into()),
            crate::tools::ShellCommand::Source("pwd".into()),
        ),
        (
            None,
            crate::tools::ShellCommand::Argv(vec!["/missing-executable".into()]),
        ),
    ] {
        let expected_cwd = cwd.clone();
        let error = prepare(
            ShellInput {
                command,
                cwd,
                timeout_ms: None,
                background: false,
                target: None,
            },
            &mounts,
            invocation(&mounts.root),
            1,
            std::time::Instant::now(),
        )
        .err()
        .ok_or_else(|| Error::Invalid("missing shell input was accepted".into()))?;
        assert!(
            matches!(error, Error::Process(crate::ProcessError::NotFound { .. })),
            "{error:?}"
        );
        assert!(error.is_tool_failure());
        if let Some(cwd) = expected_cwd {
            let Error::Process(crate::ProcessError::NotFound { message, .. }) = &error else {
                return Err(Error::Invalid(
                    "missing cwd lost its process classification".into(),
                ));
            };
            assert_eq!(message, &format!("cwd {cwd} does not exist"));
        }
    }
    for (cwd, command) in [
        (
            Some("file-cwd".into()),
            crate::tools::ShellCommand::Source("pwd".into()),
        ),
        (
            Some("file-cwd/child".into()),
            crate::tools::ShellCommand::Source("pwd".into()),
        ),
        (
            Some("../escape".into()),
            crate::tools::ShellCommand::Source("pwd".into()),
        ),
        (None, crate::tools::ShellCommand::Argv(Vec::new())),
        (
            None,
            crate::tools::ShellCommand::Source("nul\0command".into()),
        ),
    ] {
        let error = prepare(
            ShellInput {
                command,
                cwd,
                timeout_ms: None,
                background: false,
                target: None,
            },
            &mounts,
            invocation(&mounts.root),
            1,
            std::time::Instant::now(),
        )
        .err()
        .ok_or_else(|| Error::Invalid("invalid shell input was accepted".into()))?;
        assert!(error.is_tool_failure(), "{error:?}");
        assert!(!error.uncertain());
    }
    Ok(())
}
