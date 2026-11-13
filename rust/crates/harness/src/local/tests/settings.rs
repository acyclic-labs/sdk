//! `agent.ts` selects the session's sampling at open, and each home version's
//! admission is recorded in the journal.

use super::{text, volume, volume_config};
use crate::{
    Error, IdempotencyKey, LocalHarness, Result,
    config::{Admission, AdmissionStatus, Sampling},
    conversation::{ContentAccess, ContentGrant, VolumeOperation},
};

#[cfg(not(target_arch = "wasm32"))]
async fn open(
    directory: &std::path::Path,
    project: &crate::conversation::VolumeRef,
) -> Result<LocalHarness> {
    super::open_scripted(
        volume_config(directory, project.clone(), text())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn enabling_feedback_preserves_authored_settlement_settings() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
    write(
        &harness,
        "plugins.ts",
        "export default base => ({ ...base, feedback: { ...base.feedback, enabled: false, config: { retrospective: false } } });",
    )
    .await?;
    drop(harness);
    let provider = text();
    let harness = std::sync::Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .feedback_plugin(true),
        )
        .await?,
    );
    let mut observed = super::ObservedConversation::open(&harness).await?;
    let entry = observed.send("task", "Finish the task").await?;
    assert_eq!(entry.outcome, crate::agent::TurnOutcome::Completed);
    assert_eq!(
        observed
            .conversation
            .output_text_at(observed.observer.seen() - 1, &entry)
            .await?,
        "done"
    );
    assert_eq!(provider.request_count(), 1);
    Ok(())
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
            None,
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
    let volumes = super::super::agent_settings::Volumes {
        files: &harness.files,
        verifier: &harness.verifier,
        signed: &harness.signed,
        home: &harness.scratch,
        journal: &harness.journal,
        seed: (&harness.seed.0, &harness.seed.1),
        configuration: harness.harness.issuer_configuration(),
        maximum_bytes: harness.limits.file_bytes,
    };
    let tree = super::super::agent_settings::Tree::new(&volumes, path, None).await?;
    let cut = tree.cut()?.to_hex();
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
                "live/agents/{}/admissions/{path}/{cut}/{version}.json",
                harness.root
            ),
            None,
            None,
            ContentAccess::default(),
        )
        .await?
        .ok_or_else(|| Error::NotFound("settings admission record".into()))?;
    serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))
}

async fn run(
    task: std::sync::Arc<crate::runtime::TaskContext>,
    tool: crate::tool::Tool,
    arguments: serde_json::Value,
) -> Result<crate::tool::ToolResult> {
    let context =
        crate::runtime::ToolContext::new(task, crate::OperationId::new(), "settings-file")?;
    let invocation = crate::tool::ToolInvocation {
        operation_id: context.operation_id(),
        call_id: context.call_id().into(),
        name: tool.definition.name.clone(),
        arguments,
    };
    tool.executor.execute(invocation, Some(context)).await
}

#[tokio::test]
async fn issuer_settings_share_one_cut_across_selection_admission_and_imports() -> Result<()> {
    use crate::{
        conversation::{ContentResidencyVerifier, VolumeClass, VolumeOwner, VolumeRef},
        layers::PinnedLayer,
    };
    use std::sync::Arc;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = super::open_scripted(volume_config(
        directory.path(),
        project.clone(),
        provider.clone(),
    )?)
    .await?;
    let issuer = harness
        .root_authority
        .as_ref()
        .expect("fixture owns its issuer");
    let mut sources = Vec::new();
    let mut grants = Vec::new();
    for name in ["user-org", "scope", "template", "parent"] {
        let source = VolumeRef::new(
            harness.scratch.provider().clone(),
            format!("settings-{name}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(harness.root),
            &mut None,
        )?;
        harness.files.create_volume(&source).await?;
        grants.extend([
            source.directory_read_capability("", &mut None)?,
            source.capability(VolumeOperation::Write, &mut None)?,
        ]);
        sources.push(source);
    }
    let signed = issuer.bootstrap_scope(
        "configuration",
        &crate::Capabilities::new(grants),
        Some(harness.root),
        &mut None,
    )?;
    let reader = crate::filesystem::FilesystemContentVerifier::new(
        harness.files.clone(),
        &harness.verifier,
        &signed,
        None,
        None,
    )?;
    let access = ContentAccess::default();
    let mut pins: [Option<PinnedLayer>; 4] = Default::default();
    for (index, source) in sources.iter().enumerate() {
        let grant = ContentGrant::verify(
            &harness.verifier,
            &signed,
            source,
            VolumeOperation::Write,
            &mut None,
        )?;
        let entry = format!(
            "import {{increment}} from './increment-{index}.ts'; export default base => ({{...base, model: 'available/selected', sampling: {{temperature: (base.sampling?.temperature ?? 0) + increment}}}});"
        );
        for (path, body) in [
            ("agent.ts".to_owned(), entry),
            (
                format!("increment-{index}.ts"),
                "export const increment = 1;".to_owned(),
            ),
            (
                "plugins.ts".to_owned(),
                "export default base => base;".to_owned(),
            ),
        ] {
            harness
                .files
                .put_content(
                    source,
                    &grant,
                    &path,
                    body.as_bytes(),
                    "text/typescript",
                    &path,
                    None,
                    &IdempotencyKey::new(format!("issuer-{index}-{path}"))?,
                    None,
                    access,
                )
                .await?;
        }
        let (generation, _hold) = reader
            .select_private_generation(source, "", "agent.ts", None, access)?
            .await?;
        pins[index] = Some(PinnedLayer {
            volume: source.clone(),
            generation,
        });
    }
    let version = write(&harness, "agent.ts", "export default base => ({...base, sampling: {temperature: base.sampling.temperature + 1}});").await?;
    let parent = &sources[3];
    let parent_grant = ContentGrant::verify(
        &harness.verifier,
        &signed,
        parent,
        VolumeOperation::Write,
        &mut None,
    )?;
    harness
        .files
        .put_content(
            parent,
            &parent_grant,
            "increment-3.ts",
            b"export const increment = 2;",
            "text/typescript",
            "increment-3.ts",
            None,
            &IdempotencyKey::new("parent-import-change")?,
            None,
            access,
        )
        .await?;
    let (latest_parent, _hold) = reader
        .select_private_generation(parent, "", "agent.ts", None, access)?
        .await?;
    let models = Arc::new(crate::model::ModelResolver::new("available/initial", {
        let provider = provider.clone();
        move |selection| {
            let name = selection
                .strip_prefix("available/")
                .ok_or_else(|| Error::NotFound(selection.into()))?;
            Ok((
                crate::model::Model::new("available".into(), name.into(), "1".into())?,
                provider.clone(),
            ))
        }
    }));
    let configure = |pins| -> Result<_> {
        let mut config = volume_config(directory.path(), project.clone(), provider.clone())?
            .configuration_layers(pins);
        config.model = None;
        config.models = Some((models.clone(), None));
        Ok(config
            .feedback_plugin(false)
            .compaction(crate::context::CompactionPolicy::Disabled))
    };
    drop(reader);
    drop(harness);
    let prepared = LocalHarness::prepare(configure(pins.clone())?).await?;
    assert_eq!(prepared.model_selection(None).await?, "available/selected");
    let harness = Arc::new(prepared.open().await?);
    assert_eq!(
        harness.sampling.and_then(|sampling| sampling.temperature),
        Some(5.0)
    );
    let original = admission(&harness, "agent.ts", &version).await?;
    assert_eq!(original.status, AdmissionStatus::Applied);
    let original_cut = {
        let volumes = super::super::agent_settings::Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            seed: (&harness.seed.0, &harness.seed.1),
            configuration: harness.harness.issuer_configuration(),
            maximum_bytes: None,
        };
        super::super::agent_settings::Tree::new(&volumes, "agent.ts", None)
            .await?
            .cut()?
    };
    let mut conversation = super::ObservedConversation::open(&harness).await?;
    conversation.send("configuration", "Finish").await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model.name, "selected");
    assert_eq!(requests[0].sampling.temperature, Some(5.0));
    // A cached successful fold must still require the original issuer reads.
    let narrow = harness
        .root_authority
        .as_ref()
        .expect("fixture owns its issuer")
        .bootstrap_scope(
            "home-only",
            &crate::Capabilities::new([harness
                .scratch
                .directory_read_capability("", &mut None)?]),
            Some(harness.root),
            &mut None,
        )?;
    let volumes = super::super::agent_settings::Volumes {
        files: &harness.files,
        verifier: &harness.verifier,
        signed: &narrow,
        home: &harness.scratch,
        journal: &harness.journal,
        seed: (&harness.seed.0, &harness.seed.1),
        configuration: harness.harness.issuer_configuration(),
        maximum_bytes: None,
    };
    let tree = super::super::agent_settings::Tree::new(&volumes, "agent.ts", None).await?;
    let denied = super::super::agent_settings::admit(
        &tree,
        harness.root,
        harness.agent.clone(),
        None,
        None,
        harness.clock.as_ref(),
    )
    .await;
    assert!(
        matches!(denied, Err(ref error) if error.code() == crate::wire::ErrorCode::Unauthorized)
    );
    drop(tree);
    drop(conversation);
    drop(harness);
    pins[3].as_mut().expect("parent is bound").generation = latest_parent;
    let harness = super::open_scripted(configure(pins)?).await?;
    assert_eq!(
        harness.sampling.and_then(|sampling| sampling.temperature),
        Some(6.0)
    );
    let current = admission(&harness, "agent.ts", &version).await?;
    assert_eq!(current.status, AdmissionStatus::Applied);
    // The unchanged home file retains its earlier admitted outcome as well.
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
                "live/agents/{}/admissions/agent.ts/{}/{version}.json",
                harness.root,
                original_cut.to_hex()
            ),
            None,
            None,
            access,
        )
        .await?
        .ok_or_else(|| Error::NotFound("original configuration admission".into()))?;
    let retained: Admission =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(retained, original);
    Ok(())
}

#[tokio::test]
async fn off_markers_require_end_of_file_across_effective_reads_and_discovery() -> Result<()> {
    use crate::{
        Capabilities, filesystem::FilesystemContentPublisher, runtime::ContentBindings,
        wire::ErrorCode,
    };
    use serde_json::json;

    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
    let task = std::sync::Arc::new(harness.harness.agent_context(
        &Capabilities::default(),
        ContentBindings {
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
        },
    )?);
    let header_bytes = crate::context::ContextDiscoveryLimits::default().header_bytes as usize;
    let path = "skills/marker-control/SKILL.md";
    let mut observed = Vec::new();
    for body in [
        format!("{}off: true\n", " ".repeat(header_bytes + 1)),
        format!("off: true{}\nnot a marker", " ".repeat(header_bytes)),
    ] {
        write(&harness, path, &body).await?;
        let read = run(
            task.clone(),
            crate::tool::files::read()?,
            json!({"path": format!("{}{path}", crate::layers::EFFECTIVE_PATH)}),
        )
        .await;
        let skills = harness
            .skills_with_work(None, &mut crate::context::DiscoveryWork::default())
            .await;
        observed.push((
            read.err().map(|error| error.code()),
            skills
                .as_ref()
                .is_ok_and(|skills| skills.iter().all(|skill| skill.source.path != path)),
            skills.err().map(|error| error.code()),
        ));
    }
    assert_eq!(
        observed,
        [
            (Some(ErrorCode::NotFound), true, None),
            (None, false, Some(ErrorCode::Invalid)),
        ],
        "classification must reach EOF at the original generation; a prefix cannot disable a body"
    );
    Ok(())
}

#[tokio::test]
async fn effective_media_versions_require_current_source_authority() -> Result<()> {
    use crate::{
        Capabilities,
        filesystem::FilesystemContentPublisher,
        runtime::{Bindings, ContentBindings, RuntimeScope},
    };
    use serde_json::json;
    use std::sync::Arc;

    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
    let path = "authority-control.png";
    let grant = ContentGrant::verify(
        &harness.verifier,
        &harness.signed,
        &harness.scratch,
        VolumeOperation::Write,
        &mut None,
    )?;
    // Reuse the recorded encoder output and its provenance in notebook-output.md.
    let file = harness
        .files
        .put_content(
            &harness.scratch,
            &grant,
            path,
            include_bytes!("../../../../harness-core/tests/fixtures/notebook-output.png"),
            "image/png",
            path,
            None,
            &IdempotencyKey::new("media-authority-control")?,
            None,
            ContentAccess::default(),
        )
        .await?;
    let version =
        serde_json::to_string(&file).map_err(|error| Error::Invalid(error.to_string()))?;
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
    for (grants, authorized) in [
        (Capabilities::default(), false),
        (Capabilities::new([file.read_capability(&mut None)?]), false),
        (
            Capabilities::new([harness.scratch.directory_read_capability("", &mut None)?]),
            true,
        ),
    ] {
        let bindings = Bindings {
            scope: RuntimeScope::new(
                Capabilities::new(["tool:call:read"]),
                harness.limits.clone(),
            )?,
            artifacts: Some(ContentBindings {
                volume: harness.scratch.clone(),
                reader: harness.client_reader()?,
                writer: None,
            }),
            ..Bindings::local()
        }
        .build()?;
        let task = Arc::new(bindings.agent_context(&grants, private()?)?);
        let result = run(
            task,
            crate::tool::files::read()?,
            json!({
                "path": format!("{}{path}", crate::layers::EFFECTIVE_PATH),
                "version": version, "selection": {"kind": "image", "detail": "auto"}
            }),
        )
        .await;
        if authorized {
            let result = result?;
            assert_eq!(
                result.value["file"]["content"],
                serde_json::to_value(&file).map_err(|error| Error::Invalid(error.to_string()))?
            );
        } else {
            let error = result.expect_err("an old file reference cannot grant a mounted read");
            assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
        }
    }
    Ok(())
}

#[tokio::test]
async fn system_reads_keep_the_run_generation_and_require_original_authority() -> Result<()> {
    use crate::{
        Capabilities,
        filesystem::{FilesystemContentPublisher, WorkspaceMutation, workspace_ref},
        runtime::{Bindings, ContentBindings, RuntimeScope},
        tool::files::FileReadSource,
    };
    use acyclic_stream::OwnedBytes;
    use serde_json::json;
    use std::sync::Arc;

    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let path = "sys/models/catalog.json";
    let original = OwnedBytes::from_static(b"{\"version\":1}");
    let config =
        volume_config(directory.path(), project, text())?.system_file(path, original.clone());
    let metadata = config.system_files.clone();
    let harness = super::open_scripted(config).await?;
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
            &mut None,
        )
        .is_err()
    );

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
        "authored-path-v2:{}",
        serde_json::to_string(&crate::conversation::PathSelection {
            volume: home_selected.volume,
            path: home_selected.path,
            generation: home_selected.generation,
        })
        .map_err(|error| Error::Invalid(error.to_string()))?
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

    // A legacy built-in copy inherits the seed's source. Changing only the
    // body or appending after that body must retain the authored home source.
    let skill_path = "skills/swarm-topology/SKILL.md";
    let seed_body = std::str::from_utf8(crate::seed::file(skill_path).expect("built-in skill"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for (body, inherited) in [
        (seed_body.to_owned(), true),
        (format!("{seed_body}\nuser appendix\n"), false),
        (
            seed_body.replace("# Choose the dependency graph", "# User dependency graph"),
            false,
        ),
    ] {
        write(&harness, skill_path, &body).await?;
        let result = run(
            task.clone(),
            crate::tool::files::read()?,
            json!({
                "path": format!("{}{skill_path}", crate::layers::EFFECTIVE_PATH)
            }),
        )
        .await?;
        let source: FileReadSource = serde_json::from_value(result.value["file"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let FileReadSource::Authored(source) = source else {
            panic!("layer retains authored identity")
        };
        let expected = if inherited {
            &harness.seed.0
        } else {
            &harness.scratch
        };
        assert_eq!(&source.volume, expected);
        for header_bytes in [16_384, 1_024] {
            let mut work = crate::context::DiscoveryWork {
                limits: crate::context::ContextDiscoveryLimits {
                    header_bytes,
                    ..Default::default()
                },
                ..Default::default()
            };
            let skills = harness.skills_with_work(None, &mut work).await?;
            let selected = skills
                .iter()
                .find(|skill| skill.source.path == skill_path)
                .expect("layered skill");
            assert_eq!(&selected.source.root.volume, expected);
        }
    }

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
    // A content version of that later commit names the pinned volume, yet its
    // generation is outside this run, exactly like an authored version.
    let system_grant = ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        &selected.volume,
        "",
        &mut None,
    )?;
    let (newer, _) = harness
        .files
        .read_private_path(
            &selected.volume,
            &system_grant,
            &selected.path,
            None,
            u64::MAX,
            ContentAccess::default(),
        )
        .await?;
    let newer = serde_json::to_string(&newer).map_err(|error| Error::Invalid(error.to_string()))?;
    for path in [&effective, &direct] {
        let refusal = run(
            task.clone(),
            crate::tool::files::read()?,
            json!({"path": path, "version": newer}),
        )
        .await
        .expect_err("a later host generation is outside the run");
        assert!(
            refusal.to_string().contains("differs from this run"),
            "{refusal}"
        );
    }
    // Absence has a generation, without listing a directory or manufacturing a
    // file identity. A subsequent write cannot change that original selection.
    let reader = harness.client_reader()?;
    let absent_path = "selection-control.ts";
    let (absent_generation, generation_hold) = reader
        .select_private_generation(
            &harness.scratch,
            "",
            absent_path,
            None,
            ContentAccess::default(),
        )?
        .await?;
    write(&harness, absent_path, "export default {};\n").await?;
    let range = acyclic_fs::ByteRange {
        offset: 0,
        length: 1,
    };
    let absent = reader
        .read_private_range(
            &harness.scratch,
            "",
            absent_path,
            Some(&absent_generation),
            range,
            ContentAccess::default(),
        )?
        .await
        .err()
        .expect("the original generation has no settings module");
    assert_eq!(absent.code(), crate::wire::ErrorCode::NotFound);
    let (present, bytes, present_hold) = reader
        .read_private_range(
            &harness.scratch,
            "",
            absent_path,
            None,
            range,
            ContentAccess::default(),
        )?
        .await?;
    assert_ne!(present.generation, absent_generation);
    assert_eq!(&bytes[..], b"e");
    let no_read = Capabilities::default();
    let denied_selection = reader
        .select_private_generation(
            &harness.scratch,
            "",
            absent_path,
            Some(&absent_generation),
            ContentAccess {
                ceiling: Some(&no_read),
                io: None,
            },
        )?
        .await
        .err()
        .expect("an immutable selection never grants read authority");
    assert_eq!(
        denied_selection.code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    cancellation.cancel();
    let before = work.receipt();
    let cancelled = match reader.select_private_generation(
        &harness.scratch,
        "",
        absent_path,
        None,
        ContentAccess {
            ceiling: None,
            io: Some(&io),
        },
    ) {
        Err(error) => error,
        Ok(_) => panic!("cancelled selection must refuse before allocating its future"),
    };
    assert_eq!(cancelled.code(), crate::wire::ErrorCode::Cancelled);
    assert_eq!(work.receipt().items_examined, before.items_examined);
    assert_eq!(work.receipt().bytes_hashed, before.bytes_hashed);
    drop(present);
    drop(present_hold);
    drop(absent_generation);
    drop(generation_hold);

    // Deferring effective-path authorization grants nothing: the selected layer
    // still requires the caller's original volume and directory read authority.
    let denied = Bindings {
        scope: RuntimeScope::new(
            Capabilities::new([String::from("tool:call:read")]),
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
async fn effective_skill_reads_stay_within_the_reader_allowance() -> Result<()> {
    use crate::{Capabilities, filesystem::FilesystemContentPublisher, runtime::ContentBindings};
    use serde_json::json;

    // The built-in skill is larger than this allowance; a short home override
    // and its effective selection must not request the seed's whole length.
    const FILE_BYTES: u64 = 1024;
    let skill_path = "skills/swarm-topology/SKILL.md";
    assert!(crate::seed::file(skill_path).expect("built-in skill").len() as u64 > FILE_BYTES);
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = super::open_scripted(volume_config(directory.path(), project, text())?.limits(
        crate::conversation::Limits {
            file_bytes: Some(FILE_BYTES),
            ..Default::default()
        },
    ))
    .await?;
    let task = std::sync::Arc::new(harness.harness.agent_context(
        &Capabilities::default(),
        ContentBindings {
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
        },
    )?);
    write(
        &harness,
        skill_path,
        "user skill
",
    )
    .await?;
    let result = run(
        task,
        crate::tool::files::read()?,
        json!({"path": format!("{}{skill_path}", crate::layers::EFFECTIVE_PATH)}),
    )
    .await?;
    assert_eq!(
        result.value["text"],
        "user skill
"
    );
    let source: crate::tool::files::FileReadSource =
        serde_json::from_value(result.value["file"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let crate::tool::files::FileReadSource::Authored(source) = source else {
        panic!("layer retains authored identity")
    };
    assert_eq!(source.volume, harness.scratch);
    Ok(())
}

#[tokio::test]
async fn initial_home_settings_apply_before_open_and_preserve_later_edits() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let config = || {
        volume_config(directory.path(), project.clone(), text()).map(|config| {
            config
                .compaction(crate::context::CompactionPolicy::Disabled)
                .home_file(
                    "agent.ts",
                    acyclic_stream::OwnedBytes::from_static(
                        b"export default (base) => ({ ...base, sampling: { temperature: 0.25 } });",
                    ),
                )
        })
    };
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(
        harness.sampling.as_ref().and_then(|s| s.temperature),
        Some(0.25)
    );
    write(
        &harness,
        "agent.ts",
        "export default (base) => ({ ...base, sampling: { temperature: 0.5 } });",
    )
    .await?;
    drop(harness);
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(
        harness.sampling.as_ref().and_then(|s| s.temperature),
        Some(0.5)
    );
    Ok(())
}

#[tokio::test]
async fn home_agent_settings_apply_or_refuse_the_original_version() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let config = || {
        volume_config(directory.path(), project.clone(), text())
            .map(|config| config.compaction(crate::context::CompactionPolicy::Disabled))
    };
    let harness = super::open_scripted(config()?).await?;
    // The seed's default passes the started model through, untouched.
    assert_eq!(harness.sampling, None);
    let source = "import type { AgentConfig } from 'sys/types/config';\n\
         export default (base: AgentConfig): AgentConfig => ({ ...base, sampling: { temperature: 0.25 } });";
    let applied = write(&harness, "agent.ts", source).await?;
    drop(harness);
    let harness = super::open_scripted(config()?).await?;
    let sampling = Sampling {
        temperature: Some(0.25),
        ..Sampling::default()
    };
    assert_eq!(harness.sampling.as_ref(), Some(&sampling));
    let record = admission(&harness, "agent.ts", &applied).await?;
    assert_eq!(
        (record.path.as_str(), record.status, record.error),
        ("agent.ts", AdmissionStatus::Applied, None)
    );

    // Re-admit identical source and every refusal through the original volume
    // admission path. Only startup selection needs a harness reopen.
    for (source, status, reason) in [
        (source, AdmissionStatus::Applied, None),
        (
            "export default (base) => ({ ...base, colour: 1 });",
            AdmissionStatus::Invalid,
            Some("colour"),
        ),
        (
            "export default (base) => ({ ...base, effort: 'high' });",
            AdmissionStatus::Rejected,
            Some("effort"),
        ),
        (
            "export default { model: 'other/model' };",
            AdmissionStatus::Rejected,
            Some("bound by its host"),
        ),
    ] {
        let version = write(&harness, "agent.ts", source).await?;
        let volumes = super::super::agent_settings::Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            seed: (&harness.seed.0, &harness.seed.1),
            configuration: harness.harness.issuer_configuration(),
            maximum_bytes: harness.limits.file_bytes,
        };
        let tree =
            super::super::agent_settings::Tree::new(&volumes, crate::config::AGENT, None).await?;
        let result = super::super::agent_settings::admit(
            &tree,
            harness.root,
            harness.agent.clone(),
            None,
            None,
            harness.clock.as_ref(),
        )
        .await;
        match (reason, result) {
            (None, Ok(settings)) => assert_eq!(settings.sampling.as_ref(), Some(&sampling)),
            (Some(reason), Err(error)) => assert!(error.to_string().contains(reason), "{error}"),
            (Some(_), Ok(_)) => {
                panic!("invalid enforcement settings cannot fall back to seed: {source}")
            }
            (None, Err(error)) => return Err(error),
        }
        let record = admission(&harness, "agent.ts", &version).await?;
        assert_eq!(record.status, status, "{source}");
        match reason {
            Some(reason) => assert!(
                record
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains(reason)),
                "{record:?}"
            ),
            None => {
                assert_eq!(version, applied);
                assert_eq!(record.error, None);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn profile_bound_settings_keep_output_caps_and_reject_sampling_conflicts() -> Result<()> {
    use crate::model::{
        Model, ModelDispatch, ModelProfileBinding, ModelProvider, PreparedModelRequest,
    };
    use crate::profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve};
    use std::sync::Arc;

    struct Bound(
        ModelProfileBinding,
        Arc<crate::test_support::ScriptedModelProvider>,
    );
    impl ModelProvider for Bound {
        fn model_profile(&self, _: &Model) -> Result<Option<ModelProfileBinding>> {
            Ok(Some(self.0))
        }
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(self.0.capacity())
        }
        fn generate<'a>(
            &'a self,
            request: &'a PreparedModelRequest,
            dispatch: ModelDispatch,
        ) -> crate::model::ModelStream<'a> {
            self.1.generate(request, dispatch)
        }
    }

    let capacity = super::capacity();
    let model = ModelProfile::parse(
        "---\nversion: 1\ntools: []\nreasoning: {mode: disabled}\nsampling: {temperature: 0.25}\ncache: automatic\nwatermark_tokens: 4096\n---\n",
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
    let provider = Arc::new(Bound(binding, super::text()));
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let volume = volume(directory.path()).await?;
    let config = || {
        volume_config(directory.path(), volume.clone(), provider.clone())
            .map(|config| config.compaction(crate::context::CompactionPolicy::Disabled))
    };
    let mut harness = Arc::new(super::open_scripted(config()?).await?);
    for (sampling, expected) in [
        ("{temperature: 0.25}", AdmissionStatus::Applied),
        ("{temperature: 0.75}", AdmissionStatus::Rejected),
    ] {
        let version = write(
            &harness,
            "agent.ts",
            &format!("export default (base) => ({{...base, sampling: {sampling}}});"),
        )
        .await?;
        drop(harness);
        harness = Arc::new(match super::open_scripted(config()?).await {
            Ok(harness) if expected == AdmissionStatus::Applied => harness,
            Ok(_) => panic!("conflicting sampling cannot open a profile-bound session"),
            Err(error) if expected == AdmissionStatus::Rejected => {
                assert!(error.to_string().contains("pinned profile"), "{error}");
                return Ok(());
            }
            Err(error) => return Err(error),
        });
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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn each_settings_manifest_retains_its_own_admission_on_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
    let source = "export default (base) => base;";
    write(&harness, "agent.ts", source).await?;
    let version = write(&harness, "plugins.ts", source).await?;
    drop(harness);
    let harness = open(directory.path(), &project).await?;
    // Both reads select the same current home generation.
    for path in ["agent.ts", "plugins.ts"] {
        let record = admission(&harness, path, &version).await?;
        assert_eq!(record.path, path);
        assert_eq!(record.status, AdmissionStatus::Applied);
    }
    let version = write(
        &harness,
        "plugins.ts",
        "export default {guard: {enabled: 'yes'}};",
    )
    .await?;
    let volumes = super::super::agent_settings::Volumes {
        files: &harness.files,
        verifier: &harness.verifier,
        signed: &harness.signed,
        home: &harness.scratch,
        journal: &harness.journal,
        seed: (&harness.seed.0, &harness.seed.1),
        configuration: harness.harness.issuer_configuration(),
        maximum_bytes: harness.limits.file_bytes,
    };
    let tree =
        super::super::agent_settings::Tree::new(&volumes, crate::plugins::MANIFEST, None).await?;
    let reason = match super::super::agent_settings::plugins(
        false,
        None,
        &tree,
        harness.root,
        harness.clock.as_ref(),
    )
    .await
    {
        Err(error @ Error::Invalid(_)) => error.to_string(),
        Err(error) => return Err(error),
        Ok(_) => panic!("invalid enforcement manifest cannot fall back to seed"),
    };
    let record = admission(&harness, "plugins.ts", &version).await?;
    assert_eq!(record.status, AdmissionStatus::Invalid);
    assert_eq!(record.error.as_deref(), Some(reason.as_str()));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn a_directory_at_the_selected_plugins_entry_refuses_startup() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
    write(&harness, "plugins.ts/child.ts", "export default {}; ").await?;
    drop(harness);
    match open(directory.path(), &project).await {
        Err(Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            ..
        }) => Ok(()),
        Err(error) => panic!("original selected entry read refusal required: {error}"),
        Ok(_) => panic!("an invalid enforcement entry cannot fall back to seed"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn an_enabled_guard_file_read_refusal_cannot_remove_the_guard() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
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
    match open(directory.path(), &project).await {
        Err(Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            ..
        }) => Ok(()),
        Err(error) => panic!("original guard read refusal required: {error}"),
        Ok(_) => panic!("an unreadable enabled guard cannot fall back to the empty seed"),
    }
}
