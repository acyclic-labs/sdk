//! `agent.ts` selects the session's sampling at open, and each home version's
//! admission is recorded in the journal.

use super::{text, volume, volume_config};
use crate::{
    Error, IdempotencyKey, LocalHarness, Result,
    config::{Admission, AdmissionStatus, Sampling},
    conversation::{ContentAccess, ContentGrant, VolumeOperation},
};
use std::path::Path;

async fn open(directory: &Path) -> Result<LocalHarness> {
    let project = volume(directory).await?;
    LocalHarness::open(
        volume_config(directory, project, text())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await
}

/// Writes a home settings module, returning its version.
pub(in crate::local) async fn write(
    harness: &LocalHarness,
    path: &str,
    source: &str,
) -> Result<String> {
    let grant = ContentGrant::verify(
        &harness.verifier,
        &harness.signed,
        &harness.scratch,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let file = harness
        .files
        .put_content(
            &harness.scratch,
            &grant,
            path,
            source.as_bytes(),
            "text/typescript",
            path.rsplit('/').next().unwrap_or(path),
            u64::MAX,
            &IdempotencyKey::new(format!(
                "settings:{path}:{}",
                blake3::hash(source.as_bytes())
            ))?,
            None,
            ContentAccess::default(),
        )
        .await?;
    Ok(file.version().to_owned())
}

async fn admission(harness: &LocalHarness, path: &str, version: &str) -> Result<Admission> {
    let grant = ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        &harness.journal,
        "",
        &mut None,
    )?;
    let (_, bytes, _) = harness
        .files
        .read_private_path_indexed(
            &harness.journal,
            &grant,
            &format!(
                "live/agents/{}/admissions/{path}/{version}.json",
                harness.root
            ),
            None,
            u64::MAX,
            ContentAccess::default(),
        )
        .await?;
    serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))
}

#[tokio::test]
async fn system_reads_keep_the_run_generation_and_require_original_authority() -> Result<()> {
    use crate::{
        Capabilities, OperationId,
        filesystem::{FilesystemContentPublisher, WorkspaceMutation, workspace_ref},
        runtime::{Bindings, ContentBindings, RuntimeScope, ToolContext},
        tool::{Tool, ToolInvocation, files::FileReadSource},
    };
    use acyclic_stream::OwnedBytes;
    use serde_json::json;
    use std::sync::Arc;

    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let path = "sys/models/catalog.json";
    let original = OwnedBytes::from_static(b"{\"version\":1}");
    let mut config =
        volume_config(directory.path(), project, text())?.system_file(path, original.clone());
    // Generated contract inputs test noninterference: changing a plain value
    // cannot change any published byte, including its content-addressed volume.
    let mut variables: Vec<_> = ["a/b", "a%2Fb", "clé"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| acyclic_auth::contract::SecretVariable {
            kind: acyclic_auth::contract::SecretVariableKind::Plain(
                acyclic_auth::contract::PlainSecretVariable {
                    name: name.into(),
                    version: u64::MAX - index as u64,
                    value: format!("private-value-{index}"),
                },
            ),
        })
        .collect();
    variables.push(acyclic_auth::contract::SecretVariable {
        kind: acyclic_auth::contract::SecretVariableKind::Protected(
            acyclic_auth::contract::ProtectedSecretVariable {
                name: "protected".into(),
                version: 1,
            },
        ),
    });
    config
        .secret_metadata("explicit-scope", u64::MAX, &variables)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let metadata = config.system_files.clone();
    for variable in &mut variables {
        if let acyclic_auth::contract::SecretVariableKind::Plain(row) = &mut variable.kind {
            row.value.push_str("changed");
        }
    }
    config
        .secret_metadata("explicit-scope", u64::MAX, &variables)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(config.system_files, metadata);
    let harness = LocalHarness::open(config).await?;
    let private = || -> Result<_> {
        Ok(ContentBindings {
            volume: harness.journal.clone(),
            reader: harness.client_reader()?,
            writer: Some(FilesystemContentPublisher::new(
                harness.files.clone(),
                &harness.journal,
                &harness.verifier,
                &harness.signed,
                harness.limits.file_bytes,
                None,
            )?),
        })
    };
    let task = Arc::new(
        harness
            .harness
            .agent_context(&Capabilities::default(), private()?)?,
    );
    let run = |task: Arc<crate::runtime::TaskContext>, tool: Tool, arguments| async move {
        let context = ToolContext::new(task, OperationId::new(), "system-file")?;
        let invocation = ToolInvocation {
            operation_id: context.operation_id(),
            call_id: context.call_id().into(),
            name: tool.definition.name.clone(),
            arguments,
        };
        tool.executor.execute(invocation, Some(context)).await
    };
    let effective = format!("{}{path}", crate::layers::EFFECTIVE_PATH);
    let first = run(
        task.clone(),
        crate::tool::files::read()?,
        json!({"path": effective}),
    )
    .await?;
    assert_eq!(first.value["text"], "{\"version\":1}");
    let source: FileReadSource = serde_json::from_value(first.value["file"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let FileReadSource::Authored(selected) = source else {
        panic!("original authored identity")
    };
    let shared = harness.files.system_layer(&metadata).await?;
    assert_eq!(
        (&shared.volume, &shared.generation),
        (&selected.volume, &selected.generation)
    );
    assert!(
        ContentGrant::verify(
            &harness.verifier,
            &harness.signed,
            &selected.volume,
            VolumeOperation::Write,
            1,
            &mut None,
        )
        .is_err()
    );

    for variable in &variables {
        let (name, version) = variable.identity();
        let result = run(task.clone(), crate::tool::files::read()?, json!({
            "path": format!("{}sys/secrets/{}.toml", crate::layers::EFFECTIVE_PATH, acyclic_auth::encode_component(name.as_bytes()))
        })).await?;
        let metadata: toml::Table =
            toml::from_str(result.value["text"].as_str().expect("TOML text"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(metadata["scope"].as_str(), Some("explicit-scope"));
        assert_eq!(metadata["name"].as_str(), Some(name));
        assert_eq!(
            metadata["version"].as_str(),
            Some(version.to_string().as_str())
        );
        assert!(!metadata.contains_key("value"));
    }

    // A versioned effective read follows its original generation even after
    // the home entry is replaced by an off marker. Ordinal reads use the same
    // selection, while an unversioned read observes the new disabled entry.
    let note = format!("{}notes.txt", crate::layers::EFFECTIVE_PATH);
    write(&harness, "notes.txt", "first\nsecond\n").await?;
    let before = run(
        task.clone(),
        crate::tool::files::read()?,
        json!({"path": note}),
    )
    .await?;
    let home_source: FileReadSource = serde_json::from_value(before.value["file"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let FileReadSource::Authored(home_selected) = home_source else {
        panic!("authored home")
    };
    let version = format!(
        "authored-file-v1:{}",
        serde_json::to_string(&home_selected).map_err(|error| Error::Invalid(error.to_string()))?
    );
    write(&harness, "notes.txt", "off: true").await?;
    let reread = run(
        task.clone(),
        crate::tool::files::read()?,
        json!({
            "path": note, "version": version
        }),
    )
    .await?;
    assert_eq!(reread.value, before.value);
    let line = run(task.clone(), crate::tool::files::read()?, json!({
        "path": note, "version": version, "selection": {"kind": "lines", "offset": 1, "limit": 1}
    })).await?;
    assert_eq!(line.value["text"], "second\n");
    assert_eq!(line.value["file"], before.value["file"]);
    assert!(
        run(
            task.clone(),
            crate::tool::files::read()?,
            json!({"path": note})
        )
        .await
        .is_err()
    );

    // Neither a home shadow nor a subsequent trusted host commit changes this run.
    write(&harness, path, "home shadow").await?;
    let reference = workspace_ref(
        selected.volume.provider().clone(),
        &selected.volume.storage_name(&mut None)?,
    )?;
    harness
        .files
        .apply(
            &reference,
            None,
            &[WorkspaceMutation::PutFile {
                path: format!("/{path}"),
                bytes: OwnedBytes::from_static(b"changed"),
            }],
            &IdempotencyKey::new("replace-system-catalog")?,
        )
        .await?;
    let direct = format!("{}{path}", crate::layers::SYSTEM_PATH);
    for path in [&effective, &direct] {
        let result = run(
            task.clone(),
            crate::tool::files::read()?,
            json!({"path": path}),
        )
        .await?;
        assert_eq!(result.value["text"], first.value["text"]);
        assert_eq!(result.value["file"], first.value["file"]);
        assert!(
            run(
                task.clone(),
                crate::tool::files::write()?,
                json!({"path": path, "content": "forbidden"})
            )
            .await
            .is_err()
        );
    }
    // Deferring effective-path authorization grants nothing: the selected layer
    // still requires the caller's original volume and directory read authority.
    let denied = Bindings {
        scope: RuntimeScope::new(
            Capabilities::new(["tool:call:read".into()]),
            harness.limits.clone(),
        )?,
        content: Some(ContentBindings {
            volume: harness.project.clone(),
            reader: harness.client_reader()?,
            writer: None,
        }),
        system: Some(shared),
        ..Bindings::local()
    }
    .build()?;
    let denied = Arc::new(denied.agent_context(&Capabilities::default(), private()?)?);
    let refusal = run(
        denied,
        crate::tool::files::read()?,
        json!({"path": effective}),
    )
    .await
    .expect_err("directory grant required");
    assert!(
        refusal
            .to_string()
            .contains("original directory read grant"),
        "{refusal}"
    );
    Ok(())
}

#[tokio::test]
async fn home_agent_settings_apply_at_open_and_bad_ones_fall_back_to_the_seed() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let harness = open(directory.path()).await?;
    // The seed's default passes the started model through, untouched.
    assert_eq!(harness.sampling, None);
    let applied = write(
        &harness,
        "agent.ts",
        "import type { AgentConfig } from 'sys/types/config';\n\
         export default (base: AgentConfig): AgentConfig => ({ ...base, sampling: { temperature: 0.25 } });",
    )
    .await?;
    drop(harness);
    let harness = open(directory.path()).await?;
    assert_eq!(
        harness
            .sampling
            .as_ref()
            .and_then(|sampling| sampling.temperature),
        Some(0.25)
    );
    drop(harness);
    // Reopening identical source must not reuse an upload identity with a
    // newly timestamped admission body.
    let mut harness = open(directory.path()).await?;
    let sampling = Sampling {
        temperature: Some(0.25),
        ..Sampling::default()
    };
    assert_eq!(harness.sampling, Some(sampling));
    let record = admission(&harness, "agent.ts", &applied).await?;
    assert_eq!(
        (record.path.as_str(), record.status, record.error),
        ("agent.ts", AdmissionStatus::Applied, None)
    );

    // Each refusal names its reason; the seed's settings stay in force.
    for (source, status, reason) in [
        (
            "export default (base) => ({ ...base, colour: 1 });",
            AdmissionStatus::Invalid,
            "colour",
        ),
        (
            "export default (base) => ({ ...base, effort: 'high' });",
            AdmissionStatus::Rejected,
            "effort",
        ),
        (
            "export default (base) => ({ ...base, sampling: { maxOutputTokens: 8192 } });",
            AdmissionStatus::Rejected,
            "requested output exceeds selected model capacity",
        ),
        (
            "export default { model: 'other/model' };",
            AdmissionStatus::Rejected,
            "bound by its host",
        ),
    ] {
        let version = write(&harness, "agent.ts", source).await?;
        drop(harness);
        harness = open(directory.path()).await?;
        assert_eq!(harness.sampling, None, "{source}");
        let record = admission(&harness, "agent.ts", &version).await?;
        assert_eq!(record.status, status, "{source}");
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|error| error.contains(reason)),
            "{record:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn conflicting_sampling_is_rejected_before_a_profile_bound_session_opens() -> Result<()> {
    use crate::model::{
        Model, ModelDispatch, ModelEvent, ModelProfileBinding, ModelProvider, PreparedModelRequest,
    };
    use crate::profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve};
    use std::sync::Arc;

    struct Bound(ModelProfileBinding);
    impl ModelProvider for Bound {
        fn model_profile(&self, _: &Model) -> Result<Option<ModelProfileBinding>> {
            Ok(Some(self.0))
        }
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(self.0.capacity())
        }
        fn generate<'a>(
            &'a self,
            _: &'a PreparedModelRequest,
            _: ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            panic!("settings admission must not dispatch a model")
        }
    }

    let capacity = super::capacity();
    let model = ModelProfile::parse(
        "---\nversion: 1\ntools: []\nreasoning: {mode: disabled}\nsampling: {temperature: 0.25}\ncache: automatic\nbudget_tokens: 8192\nwatermark_tokens: 4096\n---\n",
        "---\nversion: 1\nkey: scripted/test\n---\n",
        capacity,
        "test-renderer",
    )?;
    let role = RoleFile::parse(
        "---\nversion: 1\nname: worker\nmodel: scripted/test\ncontext: fork\nmay_spawn: []\n---\n",
    )?;
    let project = ProjectLayer::parse("---\nversion: 1\n---\n")?;
    let approvals = crate::approvals::ApprovalPolicy::default().bind()?;
    let agent = AgentOverride::parse("---\nversion: 1\n---\n", &approvals)?;
    let binding =
        resolve(&model, &role, &project, &agent)?.bind_model(&super::model()?, capacity)?;
    let provider = Arc::new(Bound(binding));
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let volume = volume(directory.path()).await?;
    let config = || {
        volume_config(directory.path(), volume.clone(), provider.clone())
            .map(|config| config.compaction(crate::context::CompactionPolicy::Disabled))
    };
    let mut harness = LocalHarness::open(config()?).await?;
    for (temperature, expected) in [
        (0.25, AdmissionStatus::Applied),
        (0.75, AdmissionStatus::Rejected),
    ] {
        let version = write(
            &harness,
            "agent.ts",
            &format!(
                "export default (base) => ({{...base, sampling: {{temperature: {temperature}}}}});"
            ),
        )
        .await?;
        drop(harness);
        harness = LocalHarness::open(config()?).await?;
        let record = admission(&harness, "agent.ts", &version).await?;
        assert_eq!(record.status, expected);
        if expected == AdmissionStatus::Rejected {
            assert_eq!(harness.sampling, None);
            assert!(
                record
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("pinned profile"))
            );
        }
    }
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn each_settings_manifest_retains_its_own_admission_on_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let harness = open(directory.path()).await?;
    let source = "export default (base) => base;";
    write(&harness, "agent.ts", source).await?;
    let version = write(&harness, "plugins.ts", source).await?;
    drop(harness);
    let harness = open(directory.path()).await?;
    // Both reads select the same current home generation.
    for path in ["agent.ts", "plugins.ts"] {
        let record = admission(&harness, path, &version).await?;
        assert_eq!(record.path, path);
        assert_eq!(record.status, AdmissionStatus::Applied);
    }
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn a_directory_at_the_selected_plugins_entry_retains_seed_settings() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let harness = open(directory.path()).await?;
    write(&harness, "plugins.ts/child.ts", "export default {}; ").await?;
    drop(harness);
    let harness = open(directory.path()).await?;
    assert!(harness.plugin_hooks.is_empty());
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn an_enabled_guard_file_read_refusal_cannot_remove_the_guard() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let harness = open(directory.path()).await?;
    for (path, source) in [
        (
            "plugins/guard/plugin.ts",
            "export default {name: 'guard', description: 'Guard every tool'};",
        ),
        (
            "plugins/guard/hooks/before-tool.sh/child.ts",
            "export default {};",
        ),
        ("plugins.ts", "export default {guard: {enabled: true}};"),
    ] {
        write(&harness, path, source).await?;
    }
    drop(harness);
    match open(directory.path()).await {
        Err(Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            ..
        }) => Ok(()),
        Err(error) => panic!("original guard read refusal required: {error}"),
        Ok(_) => panic!("an unreadable enabled guard cannot fall back to the empty seed"),
    }
}
