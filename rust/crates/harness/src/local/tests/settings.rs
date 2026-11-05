//! `agent.ts` selects the session's sampling at open, and each home version's
//! admission is recorded in the journal.

use super::{text, volume, volume_config};
use crate::{
    Error, IdempotencyKey, LocalHarness, Result,
    config::{Admission, AdmissionStatus, Sampling},
    conversation::{ContentAccess, ContentGrant, VolumeOperation},
};

#[cfg(feature = "hooks")]
async fn open(
    directory: &std::path::Path,
    project: &crate::conversation::VolumeRef,
) -> Result<LocalHarness> {
    LocalHarness::open(
        volume_config(directory, project.clone(), text())?
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
async fn home_agent_settings_apply_at_open_and_bad_ones_fall_back_to_the_seed() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let config = || {
        volume_config(directory.path(), project.clone(), text())
            .map(|config| config.compaction(crate::context::CompactionPolicy::Disabled))
    };
    let harness = LocalHarness::open(config()?).await?;
    // The seed's default passes the started model through, untouched.
    assert_eq!(harness.sampling, None);
    let source = "import type { AgentConfig } from 'sys/types/config';\n\
         export default (base: AgentConfig): AgentConfig => ({ ...base, sampling: { temperature: 0.25 } });";
    let applied = write(&harness, "agent.ts", source).await?;
    drop(harness);
    let harness = LocalHarness::open(config()?).await?;
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
            "export default (base) => ({ ...base, sampling: { maxOutputTokens: 8192 } });",
            AdmissionStatus::Rejected,
            Some("requested output exceeds selected model capacity"),
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
            maximum_bytes: harness.limits.file_bytes,
        };
        let tree = super::super::agent_settings::Tree::new(&volumes).await?;
        let settings = super::super::agent_settings::admit(
            &tree,
            harness.root,
            harness.agent.clone().expect("the configured model"),
            None,
            (
                &crate::context::CompactionPolicy::Disabled,
                &Some(crate::context::CompactionPolicy::Disabled),
            ),
            harness.clock.as_ref(),
        )
        .await?;
        let expected = (status == AdmissionStatus::Applied).then_some(&sampling);
        assert_eq!(settings.sampling.as_ref(), expected, "{source}");
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
async fn conflicting_sampling_is_rejected_before_a_profile_bound_session_opens() -> Result<()> {
    use crate::model::{
        Model, ModelDispatch, ModelProfileBinding, ModelProvider, PreparedModelRequest,
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
        ) -> crate::model::ModelStream<'a> {
            panic!("settings admission must not dispatch a model")
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
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn a_directory_at_the_selected_plugins_entry_retains_seed_settings() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let project = volume(directory.path()).await?;
    let harness = open(directory.path(), &project).await?;
    write(&harness, "plugins.ts/child.ts", "export default {}; ").await?;
    drop(harness);
    let harness = open(directory.path(), &project).await?;
    assert!(harness.plugin_hooks.is_empty());
    Ok(())
}

#[cfg(feature = "hooks")]
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
