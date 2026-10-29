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
async fn shell_preparation_keeps_explicit_environment_and_excludes_ambient_values() -> Result<()> {
    const CHILD: &str = "ACYCLIC_TEST_CLOSED_SHELL_ENVIRONMENT";
    const SENTINEL: &str = "ACYCLIC_TEST_PREPARATION_API_KEY";
    const APPROVED: &str = "ACYCLIC_TEST_APPROVED_ORDINARY";
    if crate::native_environment::variable(CHILD).is_none() {
        // Isolate a harmless ambient sentinel in the real test binary. No
        // process-global mutation races other tests or native workers.
        let name = format!(
            "{}::shell_preparation_keeps_explicit_environment_and_excludes_ambient_values",
            module_path!()
                .split_once("::")
                .map_or(module_path!(), |(_, path)| path)
        );
        let executable =
            std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?;
        let tools = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let tool = tools.path().join("acyclic-approved-tool");
        std::fs::write(&tool, b"#!/bin/sh\nprintf 'custom-tool\\n'\n")
            .map_err(|error| Error::Storage(error.to_string()))?;
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| Error::Storage(error.to_string()))?;
        let output = std::process::Command::new(executable)
            .args([name.as_str(), "--exact", "--nocapture"])
            .env(CHILD, "child")
            .env(SENTINEL, "harmless-fixture")
            .env("PATH", tools.path())
            .env_remove(APPROVED)
            .env_remove("BASH_ENV")
            .output()
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return Ok(());
    }
    let temp = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir_all(temp.path().join(".volumes/data"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mounts = table(
        temp.path()
            .canonicalize()
            .map_err(|error| Error::Storage(error.to_string()))?,
    )?;
    let input = ShellInput {
        // Report presence only; even this harmless sentinel's value is never
        // returned. The ordinary value is selected explicitly for this launch.
        command: format!(
            "if test \"${{{SENTINEL}+x}}\" = x; then printf 'ambient-present\n'; else printf 'ambient-absent\n'; fi; printf '%s\n' \"${APPROVED}\"; acyclic-approved-tool; test \"$ACYCLIC_VOLUME_DATA_MODE\" = rw"
        ),
        cwd: None,
        timeout: Some(5),
    };
    let explicit = || {
        let mut value = invocation(&mounts.root);
        value.environment = crate::native_environment::tool_environment().unwrap();
        value.environment.insert(APPROVED.into(), "selected".into());
        value
    };
    for inherit_environment in [false, true] {
        let mut requested = explicit();
        requested.inherit_environment = inherit_environment;
        let output = run(input.clone(), &mounts, requested, 1, None, || Ok(()))
            .await?
            .ok_or_else(|| Error::Storage("process capture was indeterminate".into()))?;
        assert!(output.success);
        assert_eq!(output.stdout, b"ambient-absent\nselected\ncustom-tool\n");
    }

    // Selective former-order negative: discard explicit values and reopen
    // ambient inheritance exactly as the old preparation boundary did.
    let mut former = prepare(input.clone(), &mounts, explicit(), 1)?;
    former.environment = mounts.environment()?;
    former.inherit_environment = true;
    let output = capture::capture_with_custody(former, |(), _| Ok(()), || Ok(()), (), None)
        .await?
        .0
        .ok_or_else(|| Error::Storage("negative capture was indeterminate".into()))?;
    assert_eq!(output.stdout, b"ambient-present\n\ncustom-tool\n");

    // Removing the approved PATH reproduces the broken production caller:
    // the shell's default search cannot locate this real owner-installed tool.
    let mut missing_path = prepare(input.clone(), &mounts, explicit(), 1)?;
    missing_path.environment.remove("PATH");
    let output = capture::capture_with_custody(missing_path, |(), _| Ok(()), || Ok(()), (), None)
        .await?
        .0
        .ok_or_else(|| Error::Storage("tool search capture was indeterminate".into()))?;
    assert_eq!(output.stdout, b"ambient-absent\nselected\n");

    let mut collision = explicit();
    collision
        .environment
        .insert("ACYCLIC_VOLUME_DATA".into(), "unrelated".into());
    assert!(matches!(
        prepare(input, &mounts, collision, 1),
        Err(Error::Invalid(_))
    ));
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
