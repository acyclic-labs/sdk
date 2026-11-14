//! Effective module reads over the original authenticated content adapter.
use super::*;
use crate::conversation::{ContentAccess, ContentResidencyVerifier, VolumeRef};
use crate::filesystem::PreparationIo;
use crate::resources::GenerationRef;

/// Borrowed effective tree at one complete original home/configuration cut.
/// This starts no IO and retains the caller's existing authority and work.
pub struct ContentTree<'a> {
    /// Reader that owns the original source authority.
    pub reader: &'a dyn ContentResidencyVerifier,
    /// Writable home volume and its selected immutable generation.
    pub home: (&'a VolumeRef, &'a GenerationRef),
    /// User/org, scope, template and parent pins, lowest first.
    pub configuration: &'a [Option<PinnedLayer>; 4],
    /// Complete-module allowance; `None` is unbounded.
    pub maximum_bytes: Option<u64>,
    /// Original preparation owner.
    pub io: Option<&'a PreparationIo>,
}

impl ContentTree<'_> {
    async fn source(
        &self,
        volume: &VolumeRef,
        generation: &GenerationRef,
        path: &str,
    ) -> crate::Result<Option<acyclic_stream::OwnedBytes>> {
        let selected = self
            .reader
            .read_private_path(
                volume,
                "",
                path,
                Some(generation),
                self.maximum_bytes,
                ContentAccess {
                    io: self.io,
                    ceiling: None,
                },
            )?
            .await?;
        Ok(selected.map(|(_file, bytes, _hold)| bytes))
    }
}

impl crate::config::Tree for ContentTree<'_> {
    fn preparation_io(&self) -> Option<&PreparationIo> {
        self.io
    }
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, crate::Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            let seed = || crate::seed::file(path).map(acyclic_stream::OwnedBytes::from_static);
            if !layered(path) {
                return Ok(seed());
            }
            if let Some(bytes) = self.source(self.home.0, self.home.1, path).await? {
                return Ok((!is_off(&bytes)).then_some(bytes));
            }
            for layer in self.configuration.iter().rev().flatten() {
                if let Some(bytes) = self.source(&layer.volume, &layer.generation, path).await? {
                    return Ok((!is_off(&bytes)).then_some(bytes));
                }
            }
            Ok(seed())
        })
    }
}
