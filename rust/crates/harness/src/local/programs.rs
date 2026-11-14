//! File programs borrow the agent's existing home and original authority.
use super::*;
use crate::tool::{Tool, ToolSource};
use acyclic_stream::BoxProviderFuture;

pub(super) struct Programs {
    pub(super) local: std::sync::Weak<LocalHarness>,
    pub(super) home: VolumeRef,
    pub(super) verifier: AuthorityVerifier,
    pub(super) scope: Scope,
    pub(super) shell: Arc<Tool>,
    pub(super) catalog: ToolRegistry,
}

impl ToolSource for Programs {
    fn refresh(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn snapshot(&self) -> Result<ToolRegistry> {
        // Names are an ordinary paged tools/ directory query. Inheritance adds
        // no descriptions, schemas, copied files, scans or idle followers.
        Ok(self.catalog.clone())
    }

    fn has_lazy_tools(&self) -> bool {
        true
    }

    fn capability(&self) -> Option<&str> {
        Some("tool:call:shell")
    }

    fn load<'a>(
        &'a self,
        name: &'a str,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Arc<Tool>>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if !crate::contract::is_valid_component_label(name)
                || crate::conversation::validate_content_path(name, &mut None).is_err()
                || name.contains('/')
            {
                return Ok(None);
            }
            let local = self
                .local
                .upgrade()
                .ok_or_else(|| Error::Unsupported("local harness is closed".into()))?;
            let volumes = agent_settings::Volumes {
                decisions: crate::config::SettingsAdmissions::new(
                    local.agent_stream.clone().ok_or_else(|| {
                        Error::Unsupported("settings needs its owner stream".into())
                    })?,
                ),
                files: &local.files,
                verifier: &self.verifier,
                signed: &self.scope,
                home: &self.home,
                seed: (&local.seed.0, &local.seed.1),
                configuration: local.harness.issuer_configuration(),
                maximum_bytes: local.limits.file_bytes,
            };
            let attempt = io.map(crate::executor::PreparationIo::scoped).transpose()?;
            let temporary = attempt.as_ref();
            let directory = crate::executor::with_history_io(temporary, |work| {
                crate::executor::preparation_format(format_args!("tools/{name}"), work)
            })?;
            let entrypoint = crate::executor::with_history_io(temporary, |work| {
                crate::executor::preparation_format(format_args!("{directory}/program.ts"), work)
            })?;
            let tree = agent_settings::Tree::new(&volumes, &entrypoint, io).await?;
            match crate::plugins::load_program(&tree, &directory).await {
                Ok(program) => program.bind(Arc::clone(&self.shell)).map(Some),
                Err(Error::NotFound(path)) if path == entrypoint => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
}
