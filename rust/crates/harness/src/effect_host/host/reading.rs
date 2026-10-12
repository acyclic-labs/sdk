use super::*;

impl<P: StreamProvider> ConversationEffectHost<P> {
    pub(super) async fn effect_for_host<'a>(
        &self,
        aggregate: &'a StreamAggregate<P>,
        effect_id: EffectId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(std::borrow::Cow<'a, EffectState>, crate::NativeCustody)> {
        if let Some(effect) = aggregate.reducer().effect(effect_id) {
            return Ok((std::borrow::Cow::Borrowed(effect), None));
        }
        let custody;
        let (effect, retained) = aggregate
            .effect(effect_id, default_projection_read_limits(), work)
            .await?
            .ok_or_else(|| {
                effect_error(
                    work,
                    crate::wire::ErrorCode::NotFound,
                    "effect is not in its owning conversation",
                )
            })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            custody = match work.as_deref_mut() {
                Some(io) if io.original_native_work().is_some() => {
                    Some(io.take_native_custody(retained)?)
                }
                _ => None,
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let _ = retained;
            custody = None;
        }
        Ok((std::borrow::Cow::Owned(effect), custody))
    }

    pub(super) async fn aggregate(
        &self,
        stream: &std::sync::Arc<P>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(StreamAggregate<P>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut native_io = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?
            .map(crate::store::HistoryWork::new);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observed = match native_io.as_mut() {
            Some(io) => Some(io as &mut dyn crate::store::history::HistoryIo),
            None => work
                .as_deref_mut()
                .map(|io| io as &mut dyn crate::store::history::HistoryIo),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observed = work
            .as_deref_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let authority = match observed.as_deref_mut() {
            Some(io) => io.clone_authority(&self.authority)?,
            None => self.authority.clone(),
        };
        let verifier = self.issuer.verifier(&mut observed)?;
        let aggregate = StreamAggregate::open(
            stream,
            authority,
            verifier,
            self.schemas.clone(),
            None,
            default_projection_read_limits(),
            DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut observed,
        )
        .await?
        .with_content_verifier(Arc::clone(&self.content));
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = native_io.map(|io| io.into_custody()).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        Ok((aggregate, custody))
    }
}
