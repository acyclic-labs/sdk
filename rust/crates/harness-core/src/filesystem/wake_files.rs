//! Admission of the original file documents on the original preparation ledger.
use super::{PreparationCustody, PreparationIo, PreparationRecord};
use crate::{
    Result,
    agent::{AgentPath, Error, wake::*},
    core::ownership::{OwnedBytes, owned_enum, owned_struct},
};
use std::{marker::PhantomData, mem::size_of};

owned_struct!(MonitorKeyRange { start, end });
owned_struct!(MonitorCursor { next, preceding });
owned_enum!(Lifetime { Session, Daemon });
owned_enum!(Missed { CatchUp, Coalesce });
owned_enum!(Recurrence { Every { milliseconds }, Cron { expression } });

impl PreparationRecord for Monitor {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        let Self {
            version,
            id,
            source,
            cursor,
            filter: MonitorFilter { keys, attributes },
            wake,
            deadline_unix_ms,
            coalesce_key,
        } = self;
        // A freshly decoded empty map has no retained node allocation.
        crate::core::owned_sum([
            size_of::<Self>() as u64,
            version.owned_heap_bytes()?,
            id.owned_heap_bytes()?,
            source.owned_heap_bytes()?,
            cursor.owned_heap_bytes()?,
            keys.owned_heap_bytes()?,
            if attributes.is_empty() {
                0
            } else {
                attributes.owned_heap_bytes()?
            },
            wake.owned_heap_bytes()?,
            deadline_unix_ms.owned_heap_bytes()?,
            coalesce_key.owned_heap_bytes()?,
        ])
        .map(Some)
    }
}

impl PreparationRecord for Schedule {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        let Self {
            version,
            id,
            owner,
            body,
            timezone,
            next_due_unix_ms,
            recurrence,
            missed,
            lifetime,
            expires_unix_ms,
            remaining,
        } = self;
        crate::core::owned_sum([
            size_of::<Self>() as u64,
            version.owned_heap_bytes()?,
            id.owned_heap_bytes()?,
            owner.owned_heap_bytes()?,
            crate::executor::json::json_value_retained_owned_bytes(body)?,
            timezone.owned_heap_bytes()?,
            next_due_unix_ms.owned_heap_bytes()?,
            recurrence.owned_heap_bytes()?,
            missed.owned_heap_bytes()?,
            lifetime.owned_heap_bytes()?,
            expires_unix_ms.owned_heap_bytes()?,
            remaining.owned_heap_bytes()?,
        ])
        .map(Some)
    }

    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::Schedule
    }
}

/// The existing document shape; no envelope or second persisted format.
pub enum WakeDefinition {
    /// An authorized source follow, whose provider admits the actual subscription.
    Monitor(Monitor),
    /// An occurrence in the owning agent's due index.
    Schedule(Schedule),
}

/// An admitted declaration retaining its original decode capacity.
/// This carries no file or source authority. Read its bytes through the original
/// authorized, pinned query or authored transaction before decoding them here.
pub struct WakeFile {
    definition: WakeDefinition,
    // Last: document storage retires before its original credit.
    _custody: PreparationCustody,
}

impl WakeFile {
    /// Decode one selected document and verify its original indexed name/owner.
    /// Admission, typed decoding, payload inventory and custody transfer all use
    /// `io`; the result can outlive that observer without another reservation.
    pub fn decode(path: &str, owner: &AgentPath, bytes: &[u8], io: &PreparationIo) -> Result<Self> {
        if path.starts_with("schedules/") {
            Self::admit(
                bytes,
                io,
                |schedule: &Schedule| schedule.validate_file(path, owner),
                WakeDefinition::Schedule,
            )
        } else if path.starts_with("monitors/") {
            Self::admit(
                bytes,
                io,
                |monitor: &Monitor| monitor.validate_file(path),
                WakeDefinition::Monitor,
            )
        } else {
            Err(io.invalid("wake declaration is outside its indexed collection"))
        }
    }

    fn admit<T: PreparationRecord>(
        bytes: &[u8],
        io: &PreparationIo,
        validate: impl FnOnce(&T) -> std::result::Result<(), Error>,
        wrap: impl FnOnce(T) -> WakeDefinition,
    ) -> Result<Self> {
        // Tuple fields retire in declaration order on every refusal: the
        // decoded value is destroyed before its original reservation.
        let mut decoded = io.decode(PhantomData::<T>, bytes, crate::wire::ErrorCode::Invalid)?;
        if let Err(error) = validate(&decoded.0) {
            return Err(io.invalid(match error {
                Error::Record(reason) => reason,
                _ => "invalid wake declaration source",
            }));
        }
        let metadata = (size_of::<Self>() - size_of::<T>()) as u64;
        decoded.1.absorb(io.reserve_decode(metadata)?)?;
        let custody = decoded.1.take_custody()?;
        Ok(Self {
            definition: wrap(decoded.0),
            _custody: custody,
        })
    }

    /// Borrow the document while its original construction capacity remains held.
    #[must_use]
    pub const fn definition(&self) -> &WakeDefinition {
        &self.definition
    }
}

#[cfg(test)]
mod tests;
