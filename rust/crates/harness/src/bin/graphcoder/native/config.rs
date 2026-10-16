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
    pub context_manifest: Option<acyclic_harness::conversation::FileRef>,
    #[serde(default)]
    pub native_publications: Vec<crate::native_writeback::NativePublicationConfig>,
    #[serde(default)]
    pub project_source_root: Option<TaskId>,
}
impl Config {
    pub(crate) fn bindings(&self) -> impl Iterator<Item = &AuthorityBinding> {
        std::iter::once(&self.owner)
            .chain(self.tools.callers.iter().map(|caller| &caller.owner))
            .chain(self.tools.forks.iter().map(|fork| &fork.child))
    }
    pub fn from_file(path: &Path) -> Result<Self> {
        serde_json::from_slice(&std::fs::read(path).map_err(storage)?)
            .map_err(|error| Error::Invalid(format!("native configuration: {error}")))
    }
    pub(super) fn validate(&self) -> Result<AuthorityVerifier> {
        if !self.filesystem_root.is_absolute()
            || !self.stream_root.is_absolute()
            || self.generation.is_empty()
            || self.deterministic_delay_ms > 5000
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
        for binding in self.bindings() {
            binding.verifier()?;
        }
        for source in &self.fork_sources {
            source.request.validate(&mut None)?;
            if !self.tools.callers.iter().any(|caller| {
                caller.task == source.caller && caller.owner.authority == source.request.parent
            }) {
                return Err(Error::Unauthorized(
                    "original fork source request differs from its authenticated cut".into(),
                ));
            }
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
    pub request: acyclic_harness::fork::ForkRequest,
}
