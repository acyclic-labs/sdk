//! One generation-retry path for independent immutable task-file publishers.
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
    loop {
        let (conditions, deadline) = authorize().await?;
        match volume
            .append_if(generation, write, conditions, deadline, None)
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
