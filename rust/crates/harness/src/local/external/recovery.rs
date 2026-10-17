use crate::Result;

// A published result is not the workflow's settlement acknowledgement. Finish
// retained work before replying or attempting to fill the next input slot.
pub(super) async fn recover_before_delivery<F: Future<Output = Result<()>>>(
    initial: bool,
    answered: bool,
    idle: bool,
    recovery: impl FnOnce() -> F,
) -> Result<()> {
    if answered || (!initial && !idle) {
        recovery().await?;
    }
    Ok(())
}
