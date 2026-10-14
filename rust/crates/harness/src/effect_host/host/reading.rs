use super::*;
impl<P: StreamProvider> ConversationEffectHost<P> {
    pub(super) async fn effect_for_host<'a>(
        &self,
        aggregate: &'a StreamAggregate<P>,
        id: EffectId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(std::borrow::Cow<'a, EffectState>, NativeCustody)> {
        if let Some(effect) = aggregate.reducer().effect(id) {
            return Ok((std::borrow::Cow::Borrowed(effect), None));
        }
        let (effect, retained) = aggregate
            .effect(id, default_projection_read_limits(), work)
            .await?
            .ok_or_else(|| boundary(work, NotFound, "effect is not in its owning conversation"))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = match work.as_deref_mut() {
            Some(io) if io.original_native_work().is_some() => {
                Some(io.take_native_custody(retained)?)
            }
            _ => None,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = {
            let _ = retained;
            None
        };
        Ok((std::borrow::Cow::Owned(effect), custody))
    }
    pub(super) async fn aggregate(
        &self,
        stream: &Arc<P>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(StreamAggregate<P>, NativeCustody)> {
        #[cfg(feature = "filesystem")]
        let mut io = {
            #[cfg(not(target_arch = "wasm32"))]
            let native = work.as_deref().and_then(HistoryIo::original_native_work);
            #[cfg(target_arch = "wasm32")]
            let native = None;
            history(native)?
        };
        #[cfg(feature = "filesystem")]
        let mut observed = match io.as_mut() {
            Some(io) => Some(io as &mut dyn HistoryIo),
            None => work.as_deref_mut().map(|io| io as &mut dyn HistoryIo),
        };
        #[cfg(not(feature = "filesystem"))]
        let mut observed = work.as_deref_mut().map(|io| io as &mut dyn HistoryIo);
        let authority = match observed.as_deref_mut() {
            Some(io) => io.clone_authority(&self.authority)?,
            None => self.authority.clone(),
        };
        let verifier = self.issuer.verifier(&mut observed)?;
        let aggregate = StreamAggregate::open(
            stream,
            (authority, verifier, self.schemas.copy(&mut observed)?),
            None,
            default_projection_read_limits(),
            DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut observed,
        )
        .await?
        .with_content_verifier(self.content.clone());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = io.map(HistoryWork::into_custody).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        Ok((aggregate, custody))
    }
}
