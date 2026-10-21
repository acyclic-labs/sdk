//! One policy check for every durable prefix consumed by the client.
use super::{FileClientDurability, FileClientError, Result};
use crate::{
    Error,
    task_volume::{TaskVolume, Write},
};
use acyclic_stream::StreamProvider;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Barrier {
    Intent,
    Target,
    Checkpoint,
}
impl Barrier {
    fn message(self) -> &'static str {
        match self {
            Self::Intent => "outbox publication awaits durability",
            Self::Target => "target publication awaits durability",
            Self::Checkpoint => "outbox checkpoint awaits durability",
        }
    }
}

pub(super) async fn confirm<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    write: &Write,
    durability: FileClientDurability,
    barrier: Barrier,
) -> Result<()> {
    if durability == FileClientDurability::Accepted {
        return Ok(());
    }
    #[cfg(test)]
    #[cfg(acyclic_harness_loom)]
    match super::model::fault() {
        Some(super::model::Fault::Skip(omitted)) if omitted == barrier => return Ok(()),
        Some(super::model::Fault::Refuse) => {
            return Err(Error::Conflict(barrier.message().into()).into());
        }
        _ => (),
    }
    if !volume
        .file_is_durable(write)
        .await
        .map_err(FileClientError::publication)?
    {
        return Err(Error::Conflict(barrier.message().into()).into());
    }
    Ok(())
}
