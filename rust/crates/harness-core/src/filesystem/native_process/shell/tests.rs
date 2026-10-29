use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    workspace::{VolumeView, compose},
};
use acyclic_fs::access::*;
use std::{collections::BTreeMap, fs::OpenOptions, io::Write, path::PathBuf};

fn table(root: PathBuf) -> Result<MountTable<()>> {
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
            source: (),
        })
    };
    compose(
        root,
        &principal,
        &capability,
        Context {
            now: 1,
            approvals: 0,
        },
        volume("project")?,
        [volume("data")?],
    )
}

fn invocation(root: &Path) -> Invocation {
    Invocation {
        executable: PathBuf::from("/bin/bash"),
        argv: Vec::new(),
        cwd: root.into(),
        environment: BTreeMap::new(),
        inherit_environment: false,
        timeout_ms: 300_000,
        control_timeout_ms: 1000,
        maximum_output_bytes: 32_768,
        process_custody: None,
        standard_input: None,
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // A declared native binding may leave the primary root, but resolving a
    // symlink within it must never leave that binding's own authorized root.
    #[test]
    fn bound_volume_cwd_keeps_its_authority(component in "[a-z]{1,12}", escape in proptest::bool::ANY) {
        use proptest::prelude::*;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("view");
        let native = temp.path().join("native");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir_all(native.join(&component)).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, native.join(".escape")).unwrap();
        let native = native.canonicalize().unwrap();
        let mut mounts = table(root.canonicalize().unwrap()).unwrap();
        for (relative, mount) in mounts.mounts_mut() {
            if !relative.is_empty() {
                mount.native_root = Some(native.clone());
            }
        }
        let suffix = if escape { ".escape" } else { &component };
        let result = prepare(ShellInput {
            command: "pwd".into(), cwd: Some(format!(".volumes/data/{suffix}")), timeout: None,
        }, &mounts, invocation(&mounts.root), 1);
        if escape {
            prop_assert!(matches!(result, Err(Error::Invalid(_))));
        } else {
            let prepared = result.unwrap();
            prop_assert_eq!(prepared.cwd, native.join(&component));
            prop_assert_eq!(prepared.environment.get("ACYCLIC_VOLUME_DATA").unwrap(), native.to_str().unwrap());
        }
    }
}

#[tokio::test]
async fn shell_preserves_explicit_environment_without_inheriting_host_secrets() -> Result<()> {
    use proptest::prelude::*;
    use std::os::unix::{ffi::OsStringExt, fs::PermissionsExt};
    const CHILD: &str = "ACYCLIC_TEST_BASH_HOST_ENVIRONMENT";
    let temp = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    if crate::native_environment::variable(CHILD).is_none() {
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
        // Arbitrary non-NUL native values must stay outside the shell, without
        // interpreting UTF-8 or enumerating credential names in production.
        proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
            .run(&prop::collection::vec(1u8..=255, 0..32), |value| {
                let result = std::process::Command::new(&executable)
                    .args([test_name.as_str(), "--exact", "--nocapture"])
                    .env(CHILD, &home)
                    .env("PATH", &bin)
                    .env("HOME", "ambient-home")
                    .env("ANTHROPIC_API_KEY", "fixture-only-credential")
                    .env(
                        "ACYCLIC_TEST_RENAMED_SECRET",
                        "fixture-only-renamed-credential",
                    )
                    .env("ACYCLIC_VOLUME_DATA", "stale mount")
                    .env("ACYCLIC_VOLUME_DATA_MODE", "ro")
                    .env("BASH_ENV", &startup)
                    .env(
                        "ACYCLIC_TEST_NATIVE_BYTES",
                        std::ffi::OsString::from_vec(value),
                    )
                    .env(
                        std::ffi::OsString::from_vec(b"ACYCLIC_TEST_NATIVE_\xff".to_vec()),
                        std::ffi::OsString::from_vec(b"fixture-native-\xff".to_vec()),
                    )
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
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
        return Ok(());
    }
    let home = crate::native_environment::variable(CHILD).expect("fixture-owned child home");
    let home = home.to_str().expect("UTF-8 fixture path");
    let path = crate::native_environment::variable("PATH").expect("fixture-owned tool path");
    std::fs::create_dir_all(temp.path().join(".volumes/data"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mounts = table(
        temp.path()
            .canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
    let input = ShellInput {
        command: r#"test -z "${ANTHROPIC_API_KEY+x}" && test -z "${ACYCLIC_TEST_RENAMED_SECRET+x}" && test -z "${ACYCLIC_TEST_NATIVE_BYTES+x}" && test -z "${ACYCLIC_TEST_BASH_HOST_ENVIRONMENT+x}" && test -z "${BASH_ENV+x}" && test "$ACYCLIC_VOLUME_DATA_MODE" = rw && acyclic-explicit-path-only"#.into(),
        cwd: None,
        timeout: Some(5),
    };
    let configured = |inherit_environment| {
        let mut invocation = invocation(&mounts.root);
        invocation.inherit_environment = inherit_environment;
        invocation.environment.insert("HOME".into(), home.into());
        invocation.environment.insert(
            "PATH".into(),
            path.to_str().expect("UTF-8 fixture path").into(),
        );
        invocation
            .environment
            .insert("ACYCLIC_TEST_ORDINARY".into(), "approved".into());
        invocation.environment.insert(
            "ACYCLIC_VOLUME_DATA_MODE".into(),
            "stale explicit mode".into(),
        );
        invocation
    };
    for inherit_environment in [false, true] {
        let prepared = prepare(input.clone(), &mounts, configured(inherit_environment), 1)?;
        assert!(!prepared.inherit_environment);
        assert_eq!(
            prepared.environment.get("ACYCLIC_VOLUME_DATA"),
            mounts.environment()?.get("ACYCLIC_VOLUME_DATA")
        );
        let output = capture::capture_with_custody(prepared, |(), _| Ok(()), || Ok(()), (), None)
            .await?
            .0
            .ok_or_else(|| Error::Storage("process capture was indeterminate".into()))?;
        assert!(output.success);
        assert_eq!(
            output.stdout,
            format!("ordinary: approved\nhome: {home}\n").as_bytes()
        );
        assert!(output.stderr.is_empty());
    }
    let mut invalid = configured(false);
    invalid
        .environment
        .insert("invalid=key".into(), "fixture-only-credential".into());
    let error = prepare(input.clone(), &mounts, invalid, 1)
        .err()
        .expect("invalid settings refuse");
    assert!(matches!(error, Error::Invalid(_)));
    assert!(!error.to_string().contains("fixture-only-credential"));
    // The old inheritance choice physically exposes both named credentials and
    // the ambient shell startup hook to exactly the same command.
    let mut broken = prepare(input, &mounts, configured(false), 1)?;
    broken.inherit_environment = true;
    let output = capture::capture_with_custody(broken, |(), _| Ok(()), || Ok(()), (), None)
        .await?
        .0
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
    std::fs::create_dir_all(root.join(".volumes/data"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(root.join("src")).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::write(root.join(".volumes/data/input"), b"from data\n")
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mounts = table(
        root.canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
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
    let output = run(ShellInput { command: r#"test "$ACYCLIC_VOLUME_DATA_MODE" = rw; IFS= read -r line < "$ACYCLIC_VOLUME_DATA/input"; printf '%s' "$line" > result; printf streamed; printf diagnostic >&2; exit 7"#.into(), cwd: Some("src".into()), timeout: Some(5) },
        &mounts, invocation(&mounts.root), 1, Some(observer), || Ok(())).await?
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
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_cwd_cannot_escape_the_task_view() -> Result<()> {
    let temp = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let root = temp.path().join("view");
    std::fs::create_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    std::os::unix::fs::symlink(temp.path(), root.join("escape"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mounts = table(
        root.canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
    let result = run(
        ShellInput {
            command: "printf escaped > should-not-exist".into(),
            cwd: Some("escape".into()),
            timeout: None,
        },
        &mounts,
        invocation(&mounts.root),
        1,
        None,
        || Ok(()),
    )
    .await;
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert!(!temp.path().join("should-not-exist").exists());
    Ok(())
}
