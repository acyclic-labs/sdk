//! One retry path for independent immutable task-file publishers: after
//! another writer advances the volume, or after the owner's coordinator
//! revision advances under an unchanged volume, both of which leave nothing
//! published. Every retry first re-derives the owner condition, which fails
//! once the owner has actually lost its execution fence.
use super::*;

#[doc(hidden)]
pub async fn publish<P, F, Fut, E>(
    volume: &TaskVolume<P>,
    mut generation: Generation,
    write: &Write,
    mut authorize: F,
) -> std::result::Result<Generation, E>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(Vec<CommitCondition>, Option<u64>), E>>,
    E: From<Error>,
{
    // The owner conditions a fence conflict failed; the same conditions
    // would fail again, so only a changed authorization is retried.
    let mut fenced: Option<Vec<CommitCondition>> = None;
    loop {
        let (conditions, deadline) = authorize().await?;
        if fenced.as_ref() == Some(&conditions) {
            return Err(Error::FenceConflict.into());
        }
        match volume
            .append_if(generation, write, conditions.clone(), deadline, None)
            .await
        {
            Err(Error::Conflict(current)) => {
                #[cfg(acyclic_harness_loom)]
                if model_checking::enabled(model_checking::Fault::StopAfterGenerationConflict) {
                    return Err(Error::Conflict(generation).into());
                }
                let current = super::refresh::advanced(current, &generation)?;
                volume
                    .validate_anchor(generation.position, &generation.commit)
                    .await?;
                generation = current;
            }
            // The owner condition names the shared coordinator's revision, so
            // any coordinator event, such as a sibling's, fails it; only the
            // next authorization knows whether this owner still holds its fence.
            Err(Error::FenceConflict) => {
                #[cfg(acyclic_harness_loom)]
                if model_checking::enabled(model_checking::Fault::StopAfterFenceConflict) {
                    return Err(Error::FenceConflict.into());
                }
                fenced = Some(conditions);
            }
            result => return result.map_err(Into::into),
        }
    }
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod models;

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
