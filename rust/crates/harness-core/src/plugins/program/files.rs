//! File programs share one authenticated, generation-pinned effective tree.
use super::*;
use crate::conversation::{
    BoundedContentFuture, ContentAccess, ContentHold, ContentResidencyVerifier, VolumeRef,
};
use crate::filesystem::PreparationIo;
use crate::layers::PinnedLayer;
use crate::resources::GenerationRef;
use crate::tool::{Tool, ToolRegistry, ToolSource};
use std::sync::Arc;

/// Demand-only program files on the same content reader as the original task.
/// Construction reads no manifest, scans no directory and starts no follower.
pub struct ProgramFiles {
    reader: Arc<dyn ContentResidencyVerifier>,
    home: VolumeRef,
    configuration: [Option<PinnedLayer>; 4],
    shell: Arc<Tool>,
    maximum_bytes: Option<u64>,
}

impl ProgramFiles {
    /// Bind original content authority, configuration pins and shell capability.
    /// These identities grant no permissions beyond the reader or executable.
    pub fn new(
        reader: Arc<dyn ContentResidencyVerifier>,
        home: VolumeRef,
        configuration: [Option<PinnedLayer>; 4],
        shell: Arc<Tool>,
        maximum_bytes: Option<u64>,
    ) -> Self {
        Self {
            reader,
            home,
            configuration,
            shell,
            maximum_bytes,
        }
    }

    async fn load_at<'a>(
        &'a self,
        kind: &str,
        name: &str,
        generation: Option<&GenerationRef>,
        io: Option<&'a PreparationIo>,
    ) -> Result<(LoadedProgram, GenerationRef, Option<ContentHold<'a>>)> {
        let directory = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{kind}/{name}"), work)
        })?;
        let entrypoint = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{directory}/program.ts"), work)
        })?;
        let (generation, hold) = self
            .reader
            .select_private_generation(
                &self.home,
                "",
                &entrypoint,
                generation,
                ContentAccess { io, ceiling: None },
            )?
            .await?;
        let tree = crate::layers::ContentTree {
            reader: self.reader.as_ref(),
            home: (&self.home, &generation),
            configuration: &self.configuration,
            maximum_bytes: self.maximum_bytes,
            io,
        };
        let program = load_program(&tree, &directory).await?;
        Ok((program, generation, hold))
    }

    /// Load a hook from its original home cut. Retain that cut across approval
    /// and recovery so an edit cannot replace an already selected program.
    pub fn hook_at<'a>(
        &'a self,
        name: &'a str,
        generation: Option<&'a GenerationRef>,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (BoundHook, GenerationRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let (program, generation, hold) = self.load_at("hooks", name, generation, io).await?;
            Ok((program.bind_hook(self.shell.clone())?, generation, hold))
        })
    }
}

impl ToolSource for ProgramFiles {
    fn refresh(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn snapshot(&self) -> Result<ToolRegistry> {
        Ok(ToolRegistry::default())
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
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, Option<Arc<Tool>>>> {
        BoundedContentFuture::new(io, || async move {
            if !crate::contract::is_valid_component_label(name)
                || crate::conversation::validate_content_path(name, &mut None).is_err()
                || name.contains('/')
            {
                return Ok(None);
            }
            match self.load_at("tools", name, None, io).await {
                Ok((program, _generation, _hold)) => program.bind(self.shell.clone()).map(Some),
                Err(Error::NotFound(path)) if path == format!("tools/{name}/program.ts") => {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        })
    }
}
