//! Retire the consumed driver prefix without losing its original byte custody.
use super::*;

pub(super) fn retire_prefix(
    messages: &mut Vec<ModelMessage>,
    offset: &mut u64,
    through: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let invalid = || {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "working prefix exceeds retained history",
        )
    };
    let count = through
        .checked_sub(*offset)
        .and_then(|count| usize::try_from(count).ok())
        .filter(|count| *count <= messages.len())
        .ok_or_else(invalid)?;
    #[cfg(feature = "filesystem")]
    let retirement = {
        let bytes = messages.get(..count).ok_or_else(invalid)?.iter().try_fold(
            0_u64,
            |bytes, message| {
                bytes
                    .checked_add(message.content.retained_payload_bytes()?)
                    .ok_or_else(invalid)
            },
        )?;
        io.map(|io| io.guard_owned(bytes)).transpose()?
    };
    // Vector capacity belongs to the next working tail. Nested payloads must
    // die before their funding; acquiring that funding is fallible and first.
    drop(messages.drain(..count));
    *offset = through;
    #[cfg(feature = "filesystem")]
    drop(retirement);
    Ok(())
}

#[cfg(all(test, feature = "filesystem"))]
mod tests;
