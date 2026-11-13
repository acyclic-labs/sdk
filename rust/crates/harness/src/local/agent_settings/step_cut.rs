//! The original settings sources retained before a model-step selection.
use super::*;

#[derive(serde::Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct Cut {
    version: u32,
    seed: (VolumeRef, crate::resources::GenerationRef),
    home: (VolumeRef, crate::resources::GenerationRef),
    configuration: [Option<crate::layers::PinnedLayer>; 4],
}

impl crate::filesystem::PreparationRecord for Cut {}

struct Retained<'a> {
    cut: Cut,
    hold: Option<crate::conversation::ContentHold<'a>>,
}

#[derive(serde::Serialize)]
struct Captured<'a> {
    version: u32,
    seed: (&'a VolumeRef, &'a crate::resources::GenerationRef),
    home: (&'a VolumeRef, &'a crate::resources::GenerationRef),
    configuration: &'a [Option<crate::layers::PinnedLayer>; 4],
}

impl<'a> Tree<'a> {
    /// Retains the original complete source cut under `(operation, step)`.
    /// The caller holds the operation's execution ownership through selection.
    /// Replay consults the original journal before any live source selection.
    pub(in crate::local) async fn for_step(
        volumes: &'a Volumes<'a>,
        journal: &dyn crate::executor::ExecutionJournal,
        operation: crate::OperationId,
        step: u32,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let attempt = io.map(crate::executor::PreparationIo::scoped).transpose()?;
        let temporary = attempt.as_ref();
        let key = crate::executor::with_history_io(temporary, |work| {
            crate::executor::preparation_format(format_args!("settings:{step}:sources"), work)
        })?;
        let reference = match journal.staged_ref(operation, &key, temporary)?.await? {
            Some(reference) => reference,
            None => {
                let current = Tree::new(volumes, config::AGENT, temporary).await?;
                let captured = Captured {
                    version: 1,
                    seed: volumes.seed,
                    home: (volumes.home, &current.generation),
                    configuration: current.configuration.as_ref(),
                };
                let bounded;
                let unbounded;
                let bytes: &[u8] = match temporary {
                    Some(io) => {
                        bounded = io.encode(&captured, volumes.maximum_bytes, true)?;
                        &bounded
                    }
                    None => {
                        unbounded = crate::executor::json::canonical_json_bytes_bounded(
                            &captured,
                            crate::executor::json::JsonBounds {
                                maximum_bytes: volumes.maximum_bytes,
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
                    .stage(operation, &key, bytes, "application/json", temporary)?
                    .await?
            }
        };
        let bytes = journal.load(&reference, temporary)?.await?;
        let (cut, hold) = crate::executor::decode_json::<Cut>(&bytes, io)?;
        let retained = Retained { cut, hold };
        if retained.cut.version != 1
            || &retained.cut.home.0 != volumes.home
            || &retained.cut.seed.0 != volumes.seed.0
            || &retained.cut.seed.1 != volumes.seed.1
            || retained
                .cut
                .configuration
                .iter()
                .zip(volumes.configuration)
                .any(|(selected, original)| {
                    selected.as_ref().map(|layer| &layer.volume)
                        != original.as_ref().map(|layer| &layer.volume)
                })
        {
            let reason = "retained settings cut has another version or original owner";
            #[cfg(not(target_arch = "wasm32"))]
            if io
                .and_then(crate::executor::PreparationIo::original_native_work)
                .is_some()
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason,
                });
            }
            return Err(Error::Storage(reason.into()));
        }
        let mut tree = Self::pinned(volumes, retained.cut.home.1, io)?;
        tree.configuration = std::borrow::Cow::Owned(retained.cut.configuration);
        tree._cut_hold = retained.hold;
        Ok(tree)
    }
}
