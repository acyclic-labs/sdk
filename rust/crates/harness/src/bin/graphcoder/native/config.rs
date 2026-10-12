use super::*;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityBinding {
    pub authority: Authority,
    pub issuer: String,
    pub key: [u8; 32],
    pub scope: Scope,
}
impl AuthorityBinding {
    pub(crate) fn verifier(&self) -> Result<AuthorityVerifier> {
        let verifier = AuthorityIssuer::new(&self.issuer, self.key, self.authority.clone())
            .verifier(&mut None)?;
        verifier.verify(&self.scope, &mut None)?;
        Ok(verifier)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub filesystem_root: PathBuf,
    pub stream_root: PathBuf,
    pub filesystem_provider: ProviderRef,
    pub stream_provider: ProviderRef,
    pub private_volume: VolumeRef,
    pub owner: AuthorityBinding,
    pub observed_authorities: Vec<AuthorityBinding>,
    pub generation: String,
    pub model_invocation_ledger: PathBuf,
    pub limits: Limits,
    pub run_limits: TaskRunLimits,
    pub session_limits: SessionLimits,
    pub concurrency: usize,
    #[serde(default)]
    pub deterministic_delay_ms: u32,
    #[serde(default)]
    pub fork_sources: Vec<ForkSourceConfig>,
    #[serde(default)]
    pub tools: crate::native_tools::NativeToolsConfig,
    #[serde(default)]
    pub context: WorkflowContext,
    #[serde(default)]
    pub native_publications: Vec<crate::native_writeback::NativePublicationConfig>,
    #[serde(default)]
    pub project_source_root: Option<TaskId>,
}
impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        use std::io::Read as _;
        const MAXIMUM: u64 = 1024 * 1024;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .and_then(|file| file.take(MAXIMUM + 1).read_to_end(&mut bytes))
            .map_err(|error| Error::Storage(error.to_string()))?;
        if bytes.len() as u64 > MAXIMUM {
            return Err(Error::Invalid(
                "native configuration exceeds one MiB".into(),
            ));
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| Error::Invalid(format!("native configuration: {error}")))
    }
    pub(super) fn validate(&self) -> Result<AuthorityVerifier> {
        if !self.filesystem_root.is_absolute()
            || !self.stream_root.is_absolute()
            || self.generation.is_empty()
            || self.generation.len() > 128
            || self.observed_authorities.len() > 64
            || self.concurrency == 0
            || self.deterministic_delay_ms > 5000
            || self.fork_sources.len() > 64
            || self.native_publications.len() > 64
        {
            return Err(Error::Invalid(
                "native composition requires absolute paths and finite bounds".into(),
            ));
        }
        self.limits.validate(&mut None)?;
        if !cfg!(feature = "native-execution")
            && (!self.native_publications.is_empty() || !self.tools.commands.is_empty())
        {
            return Err(Error::Unsupported(
                "native providers require native-execution".into(),
            ));
        }
        let verifier = self.owner.verifier()?;
        for binding in &self.observed_authorities {
            binding.verifier()?;
        }
        for source in &self.fork_sources {
            let verifier = source.owner.verifier()?;
            source.project.validate(&mut None)?;
            source.request.validate(&mut None)?;
            if source.request.parent != source.owner.authority
                || source.request.parent_revision != source.snapshot.revision
            {
                return Err(Error::Unauthorized(
                    "original fork source request differs from its authenticated cut".into(),
                ));
            }
            acyclic_harness::core::Reducer::restore(
                source.snapshot.clone(),
                verifier,
                SchemaRegistry::default(),
                &mut None,
            )?;
        }
        for publication in &self.native_publications {
            publication.validate()?;
        }
        Ok(verifier)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkSourceConfig {
    pub caller: TaskId,
    pub owner: AuthorityBinding,
    pub project: VolumeRef,
    pub snapshot: Snapshot,
    pub request: acyclic_harness::fork::ForkRequest,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowContext {
    pub discovery: Option<ContextDiscovery>,
    pub discovered: Option<DiscoveredContext>,
    #[serde(default)]
    pub attributes: Vec<ContextAttribute>,
    pub attribute_mode: Option<ContextRenderMode>,
}
impl WorkflowContext {
    pub(super) fn pipeline(&self) -> Result<ContextPipeline> {
        let mut pipeline = ContextPipeline::default();
        if let Some(discovered) = &self.discovered {
            pipeline = pipeline.with(Arc::new(
                discovered.clone().stage(ContextPlacement::Prepend)?,
            ));
        }
        let mode = self.attribute_mode.unwrap_or(ContextRenderMode::Prompt);
        for attribute in &self.attributes {
            attribute.validate(&mut None)?;
            let text = serde_json::to_string(attribute)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            pipeline = pipeline.with(Arc::new(SourceStage::new(
                attribute.type_name.clone(),
                attribute.state_revision.clone(),
                Arc::new(Context {
                    messages: vec![ModelMessage {
                        role: ModelRole::System,
                        content: ModelContent::Text(format!("{mode:?}:{text}")),
                    }],
                    ..Context::default()
                }),
                if mode == ContextRenderMode::Update {
                    ContextPlacement::Append
                } else {
                    ContextPlacement::Prepend
                },
            )));
        }
        Ok(pipeline)
    }
}
