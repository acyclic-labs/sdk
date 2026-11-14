//! Original effective-tree selections retained by the execution journal.
use super::*;
use crate::{
    Error, OperationId, Result,
    conversation::{ContentAccess, ContentHold, ContentResidencyVerifier, VolumeRef},
    executor::ExecutionJournal,
    filesystem::PreparationIo,
    resources::GenerationRef,
};
use serde::Deserialize;

/// Complete original sources; addresses grant no read or execution authority.
#[derive(serde::Serialize, serde::Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct ContentCut {
    version: u32,
    /// Optional immutable seed volume; built-in-only trees have no volume seed.
    #[serde(deserialize_with = "Option::<(VolumeRef, GenerationRef)>::deserialize")]
    #[heap_layout(as = Option<(VolumeRef, GenerationRef)>)]
    pub seed: Option<(VolumeRef, GenerationRef)>,
    /// Original writable home and its selected generation.
    pub home: (VolumeRef, GenerationRef),
    /// Original user/org, scope, template and parent generations.
    pub configuration: [Option<PinnedLayer>; 4],
}
impl crate::filesystem::PreparationRecord for ContentCut {}

/// Selection and the original decoder's retained allocation ownership.
pub struct RetainedContentCut<'a> {
    /// Original source descriptors, retired before their credit.
    pub selection: ContentCut,
    /// Original decode custody; never replaced with an unmetered clone.
    pub custody: Option<ContentHold<'a>>,
}

/// Authority and original sources from which one effective-tree cut is selected.
pub struct ContentSources<'a, 'io> {
    /// Current authenticated reader, including on recovery.
    pub reader: &'a dyn ContentResidencyVerifier,
    /// Optional original immutable seed volume and generation.
    pub seed: Option<(&'a VolumeRef, &'a GenerationRef)>,
    /// Original writable home volume.
    pub home: &'a VolumeRef,
    /// Original issuer source positions, lowest first.
    pub configuration: &'a [Option<PinnedLayer>; 4],
    /// Source and descriptor bound; `None` is unbounded.
    pub maximum_bytes: Option<u64>,
    /// Original preparation owner.
    pub io: Option<&'io PreparationIo>,
}

#[derive(serde::Serialize)]
struct Captured<'a> {
    version: u32,
    seed: Option<(&'a VolumeRef, &'a GenerationRef)>,
    home: (&'a VolumeRef, &'a GenerationRef),
    configuration: &'a [Option<PinnedLayer>; 4],
}

impl<'io> ContentSources<'_, 'io> {
    /// Capture once, then recover the original journaled cut before looking at
    /// live files. The caller retains execution ownership through selection.
    /// Current source authority is still checked by every subsequent read.
    pub async fn retain(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        key: &str,
        path: &str,
    ) -> Result<RetainedContentCut<'io>> {
        self.retain_using(journal, operation, key, path, None).await
    }

    /// Retain a producer's already selected cut in a new execution. Recovery
    /// reads that execution's original selection first, even if its producer
    /// now proposes another generation. The current reader still authorizes
    /// every source read; transferring a descriptor grants no authority.
    pub async fn retain_at(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        key: &str,
        path: &str,
        original: &ContentCut,
    ) -> Result<RetainedContentCut<'io>> {
        self.retain_using(journal, operation, key, path, Some(original))
            .await
    }

    async fn retain_using(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        key: &str,
        path: &str,
        original: Option<&ContentCut>,
    ) -> Result<RetainedContentCut<'io>> {
        let attempt = self.io.map(PreparationIo::scoped).transpose()?;
        let temporary = attempt.as_ref();
        let reference = match journal.staged_ref(operation, key, temporary)?.await? {
            Some(reference) => reference,
            None => {
                let selected;
                let captured = match original {
                    Some(original) => {
                        self.check(original)?;
                        Captured {
                            version: 1,
                            seed: original
                                .seed
                                .as_ref()
                                .map(|(volume, generation)| (volume, generation)),
                            home: (&original.home.0, &original.home.1),
                            configuration: &original.configuration,
                        }
                    }
                    None => {
                        selected = self
                            .reader
                            .select_private_generation(
                                self.home,
                                "",
                                path,
                                None,
                                ContentAccess {
                                    io: temporary,
                                    ceiling: None,
                                },
                            )?
                            .await?;
                        Captured {
                            version: 1,
                            seed: self.seed,
                            home: (self.home, &selected.0),
                            configuration: self.configuration,
                        }
                    }
                };
                let bounded;
                let unbounded;
                let bytes: &[u8] = match temporary {
                    Some(io) => {
                        bounded = io.encode(&captured, self.maximum_bytes, true)?;
                        &bounded
                    }
                    None => {
                        unbounded = crate::executor::json::canonical_json_bytes_bounded(
                            &captured,
                            crate::executor::json::JsonBounds {
                                maximum_bytes: self.maximum_bytes,
                                maximum_items: None,
                                maximum_depth: 256,
                            },
                            None,
                            &mut |_| Ok(()),
                        )?;
                        &unbounded
                    }
                };
                journal
                    .stage(operation, key, bytes, "application/json", temporary)?
                    .await?
            }
        };
        let bytes = journal.load(&reference, temporary)?.await?;
        let (selection, custody) = crate::executor::decode_json::<ContentCut>(&bytes, self.io)?;
        self.check(&selection)?;
        Ok(RetainedContentCut { selection, custody })
    }

    fn check(&self, selection: &ContentCut) -> Result<()> {
        if selection.version != 1
            || &selection.home.0 != self.home
            || selection
                .seed
                .as_ref()
                .map(|(volume, generation)| (volume, generation))
                != self.seed
            || selection
                .configuration
                .iter()
                .zip(self.configuration)
                .any(|(selected, original)| {
                    selected.as_ref().map(|layer| &layer.volume)
                        != original.as_ref().map(|layer| &layer.volume)
                })
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "retained content cut has another version or original owner",
            });
        }
        Ok(())
    }
}
