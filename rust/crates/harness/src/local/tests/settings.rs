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
async fn write(harness: &LocalHarness, path: &str, source: &str) -> Result<String> {
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
            path,
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
