//! One generation-retry path for independent immutable task-file publishers.
use super::*;

pub(crate) async fn publish<P, F, Fut>(
    volume: &TaskVolume<P>,
    mut generation: Generation,
    write: &Write,
    mut authorize: F,
) -> crate::Result<Generation>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = crate::Result<(Vec<CommitCondition>, Option<u64>)>>,
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
                if current.volume != generation.volume
                    || current.epoch != generation.epoch
                    || current.position < generation.position
                {
                    return Err(Error::ResetRequired.into());
                }
                generation = current;
            }
            result => return result.map_err(Into::into),
        }
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;
