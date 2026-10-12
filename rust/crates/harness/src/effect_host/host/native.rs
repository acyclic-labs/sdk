use super::*;

impl<P: StreamProvider> ConversationEffectHost<P> {
    pub(super) fn require_native_metadata(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if self
            .native_metadata
            .as_ref()
            .is_some_and(|guard| guard.work().shares_ledger(work))
        {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect host does not retain the original native metadata",
            })
        }
    }

    /// Copies metadata through the original SDK producers.
    pub fn copy(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let authority = io.clone_authority(&self.authority)?;
        let issuer = self.issuer.copy(&mut Some(&mut io))?;
        let scope = self.scope.copy(&mut Some(&mut io))?;
        let schemas = self.schemas.copy(&mut Some(&mut io))?;
        let providers = self.providers.copy(work)?;
        Self::new(
            self.stream.clone(),
            authority,
            issuer,
            scope,
            schemas,
            Arc::clone(&self.content),
            providers,
            Some(io.into_custody()?),
            Some(work),
        )
    }

    /// Funds the shared host before allocating its Arc body.
    pub fn into_arc(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        use crate::store::history::HistoryIo as _;
        self.require_native_metadata(work)?;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let bytes = std::mem::size_of::<Self>() as u64;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        let mut custody = io.into_custody()?;
        self.native_metadata
            .as_mut()
            .expect("validated native host")
            .absorb(&mut custody)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        Ok(Arc::new(self))
    }
}
