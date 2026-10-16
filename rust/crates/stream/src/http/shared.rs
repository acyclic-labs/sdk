use crate::{StreamError, query_sync::SyncMutex};

pub(super) fn shared<T: Clone>(
    slot: &SyncMutex<Option<T>>,
    initialize: impl FnOnce() -> Result<T, StreamError>,
) -> Result<T, StreamError> {
    // One lock: never nested with provider/session locks or held across await.
    // Initialization creates a lazy transport; it does not poll that transport.
    let mut slot = slot.lock().map_err(|_| StreamError::Unavailable)?;
    if let Some(value) = &*slot {
        return Ok(value.clone());
    }
    let value = initialize()?;
    *slot = Some(value.clone());
    Ok(value)
}
