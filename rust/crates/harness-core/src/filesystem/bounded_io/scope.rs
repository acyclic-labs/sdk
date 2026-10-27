//! Isolated ownership for a fallible preparation, on its caller's ledger.
use super::*;

pub(crate) struct PreparationScope<'a> {
    original: Option<&'a PreparationIo>,
    local: Option<PreparationIo>,
}

impl<'a> PreparationScope<'a> {
    pub(crate) fn new(original: Option<&'a PreparationIo>) -> Result<Self> {
        Ok(Self {
            original,
            local: original.map(PreparationIo::scoped).transpose()?,
        })
    }

    pub(crate) fn io(&self) -> Option<&PreparationIo> {
        self.local.as_ref()
    }

    /// Release scratch and transfer only the inventoried output. Admission
    /// already happened on this ledger; transfer never allocates a second copy.
    pub(crate) fn finish(&self, retained: u64) -> Result<()> {
        let (Some(original), Some(local)) = (self.original, &self.local) else {
            return Ok(());
        };
        // Keep custody in this scope through every fallible check. A detached
        // temporary guard would release the output too early if cancellation
        // raced with substitution, before the caller could destroy its value.
        local.check_cancellation()?;
        let scratch = local
            .owned_bytes()?
            .checked_sub(retained)
            .ok_or_else(|| local.invalid("audited output exceeds preparation custody"))?;
        local.release(scratch)?;
        match local {
            PreparationIo::Owned { custody, .. } => {
                let target = match original {
                    PreparationIo::Owned { custody, .. } => custody,
                    #[cfg(not(target_arch = "wasm32"))]
                    PreparationIo::Native { .. } => {
                        return Err(local.invalid("preparation scope ledger differs"));
                    }
                };
                custody.transfer_to(target).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationIo::Native { .. } => {
                original.absorb_native_custody(local.take_native_custody(local.owned_bytes()?)?)
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
