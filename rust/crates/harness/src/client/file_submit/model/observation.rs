//! Input-derived obligations, captured before joining the durability publisher.
use super::Barrier;

#[derive(Debug)]
pub(super) struct Event {
    pub(super) barrier: Barrier,
    pub(super) premature: bool,
    pub(super) progress: bool,
}

pub(super) fn safety(event: &Event) -> Result<(), String> {
    if event.premature {
        Err(format!(
            "{:?}: effect preceded its required prefix",
            event.barrier
        ))
    } else {
        Ok(())
    }
}

pub(super) fn progress(event: &Event) -> Result<(), String> {
    if !event.progress {
        Err(format!(
            "{:?}: confirmed intent was lost, duplicated or refused",
            event.barrier
        ))
    } else {
        Ok(())
    }
}
