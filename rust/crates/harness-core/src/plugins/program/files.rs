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
mod catalogue;
pub use catalogue::{HookCatalogueCursor, HookCataloguePage};

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
        configuration: Option<&[Option<PinnedLayer>; 4]>,
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
            configuration: configuration.unwrap_or(&self.configuration),
            maximum_bytes: self.maximum_bytes,
            io,
        };
        let program = load_program(&tree, &directory).await?;
        Ok((program, generation, hold))
    }

    /// Select once in the original execution journal, before loading a hook.
    /// Recovery retains the complete home/issuer cut across approval, so edits
    /// cannot replace an already admitted program or one of its imports.
    pub fn hook_for_execution<'a>(
        &'a self,
        journal: &'a dyn crate::executor::ExecutionJournal,
        operation: crate::OperationId,
        name: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (BoundHook, crate::layers::RetainedContentCut<'a>)>> {
        BoundedContentFuture::new(io, || async move {
            let attempt = io.map(PreparationIo::scoped).transpose()?;
            let temporary = attempt.as_ref();
            let (key, path) = crate::executor::with_history_io(temporary, |work| {
                Ok((
                    crate::executor::preparation_format(
                        format_args!("program:hooks/{name}:sources"),
                        work,
                    )?,
                    crate::executor::preparation_format(
                        format_args!("hooks/{name}/program.ts"),
                        work,
                    )?,
                ))
            })?;
            let cut = crate::layers::ContentSources {
                reader: self.reader.as_ref(),
                seed: None,
                home: &self.home,
                configuration: &self.configuration,
                maximum_bytes: self.maximum_bytes,
                io,
            }
            .retain(journal, operation, &key, &path)
            .await?;
            let (program, _generation, _hold) = self
                .load_at(
                    "hooks",
                    name,
                    Some(&cut.selection.home.1),
                    Some(&cut.selection.configuration),
                    io,
                )
                .await?;
            Ok((program.bind_hook(self.shell.clone())?, cut))
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
            match self.load_at("tools", name, None, None, io).await {
                Ok((program, _generation, _hold)) => program.bind(self.shell.clone()).map(Some),
                Err(Error::NotFound(path)) if path == format!("tools/{name}/program.ts") => {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        })
    }
}
