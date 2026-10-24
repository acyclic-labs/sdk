//! A query owns its temporary claims; only its completed output reaches callers.
use super::*;

pub(super) struct QueryIo<'a>(crate::filesystem::PreparationScope<'a>);

impl<'a> QueryIo<'a> {
    pub(super) fn new(original: Option<&'a PreparationIo>) -> Result<Self> {
        crate::filesystem::PreparationScope::new(original).map(Self)
    }

    pub(super) fn io(&self) -> Option<&PreparationIo> {
        self.0.io()
    }

    pub(super) fn finish(self) -> Result<()> {
        let retained = self
            .io()
            .map(PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        self.0.finish(retained)
    }
}
