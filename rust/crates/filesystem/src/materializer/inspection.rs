use super::*;

/// Current non-authorizing observation of the original root.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationRootObservation {
    /// Backend has no independent allocation witness.
    Unverified,
    /// Original allocation still holds.
    Identical([u8; 16]),
    /// Named root differs from the original allocation.
    Diverged {
        /// Original allocation.
        original: [u8; 16],
        /// Current allocation, when present.
        current: Option<[u8; 16]>,
    },
    /// Allocation could not be inspected.
    Unavailable,
}
/// Current non-authorizing observation of the original preparation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPreparationObservation {
    /// No allocation witness exists.
    Unverified,
    /// Directory creation remains incomplete.
    Incomplete(MaterializationPreparationProgress),
    /// All original allocations still hold.
    Identical(MaterializationPreparationIdentity),
    /// At least one named allocation changed.
    Diverged {
        /// Original allocations.
        original: MaterializationPreparationIdentity,
        /// Current allocations, when present.
        current: Option<MaterializationPreparationIdentity>,
    },
    /// Preparation could not be inspected.
    Unavailable,
}
/// Original journal with its current physical observations.
#[derive(Debug)]
pub struct MaterializationInspection {
    /// Root allocation observation.
    pub root: MaterializationRootObservation,
    /// Preparation allocation observation.
    pub preparation: MaterializationPreparationObservation,
    /// Captured endpoints, when their containing allocations still match.
    pub observations: Vec<MaterializationObservation>,
    /// Original durable progress and allocation of this inspection.
    pub journal: MaterializationJournal,
}
impl<S: MaterializationJournalStore, B: MaterializationBackend> JournaledMaterializer<S, B> {
    /// Inspects original progress without granting permission to resume it.
    pub async fn inspect(
        &self,
        operation: OperationId,
    ) -> Result<Option<MaterializationInspection>, MaterializationError<S::Error, B::Error>> {
        let Some(mut journal) = self.load(operation).await? else {
            return Ok(None);
        };
        let root = match journal.root_identity {
            None => MaterializationRootObservation::Unverified,
            Some(original) => match self.backend.observe_allocation_identity().await {
                Ok(Some(current)) if current == original => {
                    MaterializationRootObservation::Identical(original)
                }
                Ok(current) => MaterializationRootObservation::Diverged { original, current },
                Err(error) if observation_interrupted(&error) => {
                    return Err(MaterializationError::Backend(error));
                }
                Err(_) => MaterializationRootObservation::Unavailable,
            },
        };
        let preparation = match journal.preparation_identity {
            None => journal.preparation_progress.map_or(
                MaterializationPreparationObservation::Unverified,
                MaterializationPreparationObservation::Incomplete,
            ),
            Some(original) => match self.backend.observe_preparation_identity().await {
                Ok(Some(current)) if current == original => {
                    MaterializationPreparationObservation::Identical(original)
                }
                Ok(current) => {
                    MaterializationPreparationObservation::Diverged { original, current }
                }
                Err(error) if observation_interrupted(&error) => {
                    return Err(MaterializationError::Backend(error));
                }
                Err(_) => MaterializationPreparationObservation::Unavailable,
            },
        };
        let mut observations = Vec::new();
        if matches!(
            root,
            MaterializationRootObservation::Unverified
                | MaterializationRootObservation::Identical(_)
        ) && matches!(
            preparation,
            MaterializationPreparationObservation::Unverified
                | MaterializationPreparationObservation::Identical(_)
        ) {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.backend.native_work() {
                let (capacity, mut allocation) =
                    crate::native_host::buffer(journal.preimages.len(), &mut &*work)
                        .map_err(MaterializationError::Native)?;
                observations = capacity;
                journal
                    .custody
                    .as_mut()
                    .ok_or(NativeTreeMaterializationError::MissingCustody)?
                    .absorb(&mut allocation)?;
            }
            for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
                observations.push(
                    self.backend
                        .observe(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?,
                );
            }
        }
        Ok(Some(MaterializationInspection {
            root,
            preparation,
            observations,
            journal,
        }))
    }
}
fn observation_interrupted<E: std::error::Error + 'static>(_error: &E) -> bool {
    #[cfg(not(target_arch = "wasm32"))]
    return matches!(
        (_error as &dyn std::error::Error).downcast_ref::<NativeTreeMaterializationError>(),
        Some(
            NativeTreeMaterializationError::Cancelled
                | NativeTreeMaterializationError::Work(_)
                | NativeTreeMaterializationError::AllocationFailed(_)
        )
    );
    #[cfg(target_arch = "wasm32")]
    false
}
