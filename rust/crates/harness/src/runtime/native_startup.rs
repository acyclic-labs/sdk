//! Optional observation of the original harness configuration and graph walk.

use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type StartupIo<'a> = Option<&'a crate::filesystem::PreparationIo>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type StartupIo<'a> = Option<&'a ()>;

pub(super) fn error(
    io: StartupIo<'_>, code: crate::wire::ErrorCode, reason: &'static str,
    ordinary: impl FnOnce() -> Error,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() { return Error::NativeBoundary { code, reason }; }
    let _ = (io, code, reason);
    ordinary()
}

pub(super) fn item(io: StartupIo<'_>, bytes: usize) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let observed = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
        io.preflight(observed)?;
        io.retain_owned(bytes as u64)?;
        io.charge(observed)?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = (io, bytes);
    Ok(())
}

pub(super) fn text(value: &str, io: StartupIo<'_>) -> Result<String> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        use crate::store::history::HistoryIo as _;
        return io.history_io().clone_text(value);
    }
    let _ = io;
    Ok(value.to_owned())
}

pub(super) fn content_write(
    content: Option<&ContentBindings>, scope: &RuntimeScope, io: StartupIo<'_>,
) -> Result<bool> {
    let Some(writer) = content.and_then(|binding| binding.writer.as_ref()) else { return Ok(false); };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let grant = writer.volume().capability(VolumeOperation::Write, &mut Some(&mut io.history_io()))?;
        return Ok(scope.grants.contains(&grant));
    }
    let _ = io;
    Ok(writer.volume().capability(VolumeOperation::Write, &mut None)
        .is_ok_and(|grant| scope.grants.contains(&grant)))
}
