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
        timeout_ms: 300_000,
        control_timeout_ms: 1000,
        cancellation_poll_ms: 25,
        maximum_output_bytes: 32_768,
        mcp_stdio: None,
        standard_input: None,
    }
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
