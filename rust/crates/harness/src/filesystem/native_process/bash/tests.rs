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
        cancellation_poll_ms: 25,
        maximum_output_bytes: 32_768,
        mcp_stdio: None,
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
        let result = prepare(BashInput {
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
async fn host_home_and_path_survive_mount_environment_overrides() -> Result<()> {
    use proptest::prelude::*;
    use std::os::unix::{ffi::OsStringExt, fs::PermissionsExt};
    const CHILD: &str = "ACYCLIC_TEST_BASH_HOST_ENVIRONMENT";
    let temp = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    if std::env::var_os(CHILD).is_none() {
        let bin = temp.path().join("bin");
        let home = temp.path().join("home");
        std::fs::create_dir(&bin).map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(&home).map_err(|error| Error::Storage(error.to_string()))?;
        let tool = bin.join("acyclic-host-path-only");
        std::fs::write(&tool, b"#!/bin/sh\nprintf 'host tool: %s\\nnative: %s\\n' \"$HOME\" \"$ACYCLIC_TEST_NATIVE_BYTES\"\n")
            .map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| Error::Storage(error.to_string()))?;
        // Isolate host settings in a child test process; never mutate this
        // process's environment while other tests or native workers run.
        // libtest names omit the crate; derive the path so a module move
        // cannot leave a stale literal behind.
        let test_name = format!(
            "{}::host_home_and_path_survive_mount_environment_overrides",
            module_path!().split_once("::").map_or(module_path!(), |(_, path)| path)
        );
        let executable =
            std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?;
        // Property: every non-NUL native value survives launch byte-for-byte,
        // while host tools remain reachable and mount hints override the host.
        proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32)).run(
            &prop::collection::vec(1u8..=255, 0..32),
            |value| {
        let result = std::process::Command::new(&executable)
        .args([
            test_name.as_str(),
            "--exact",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("PATH", &bin)
        .env("HOME", &home)
        .env("ACYCLIC_VOLUME_DATA", "stale mount")
        .env("ACYCLIC_VOLUME_DATA_MODE", "ro")
        .env("ACYCLIC_TEST_NATIVE_BYTES", std::ffi::OsString::from_vec(value))
        .env(std::ffi::OsString::from_vec(b"ACYCLIC_TEST_UNRELATED_\xff".to_vec()), "native key")
        .env_remove("BASH_ENV")
        .output()
        .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        // A drifted test name selects nothing and exits successfully; the
        // child must actually have run exactly this test.
        prop_assert!(
            String::from_utf8_lossy(&result.stdout).contains("1 passed"),
            "child selected no test: {}",
            String::from_utf8_lossy(&result.stdout)
        );
        Ok(())
        }).map_err(|error| Error::Invalid(error.to_string()))?;
        return Ok(());
    }
    std::fs::create_dir_all(temp.path().join(".volumes/data"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mounts = table(
        temp.path()
            .canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
    let input = BashInput {
        command:
            r#"test -n "$HOME" && test "$ACYCLIC_VOLUME_DATA_MODE" = rw && acyclic-host-path-only"#
                .into(),
        cwd: None,
        timeout: Some(5),
    };
    let prepared = prepare(input.clone(), &mounts, invocation(&mounts.root), 1)?;
    assert_eq!(
        prepared.environment.get("ACYCLIC_VOLUME_DATA"),
        mounts.environment()?.get("ACYCLIC_VOLUME_DATA")
    );
    // Negative control: mount-only settings cannot find the host-PATH tool,
    // even after restoring HOME independently.
    let mut broken = prepared;
    broken.inherit_environment = false;
    broken.environment = mounts.environment()?;
    broken
        .environment
        .insert("HOME".into(), std::env::var("HOME").unwrap());
    assert!(
        !capture::capture_checked(broken, None, || Ok(()))
            .await?
            .unwrap()
            .success
    );
    let output = run(input, &mounts, invocation(&mounts.root), 1, None, || Ok(()))
        .await?
        .ok_or_else(|| Error::Storage("process capture was indeterminate".into()))?;
    assert!(
        output.success,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut expected =
        format!("host tool: {}\nnative: ", std::env::var("HOME").unwrap()).into_bytes();
    expected.extend(
        std::env::var_os("ACYCLIC_TEST_NATIVE_BYTES")
            .unwrap()
            .into_vec(),
    );
    expected.push(b'\n');
    assert_eq!(output.stdout, expected);
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
    let output = run(BashInput { command: r#"test "$ACYCLIC_VOLUME_DATA_MODE" = rw; IFS= read -r line < "$ACYCLIC_VOLUME_DATA/input"; printf '%s' "$line" > result; printf streamed; printf diagnostic >&2; exit 7"#.into(), cwd: Some("src".into()), timeout: Some(5) },
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
        BashInput {
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
