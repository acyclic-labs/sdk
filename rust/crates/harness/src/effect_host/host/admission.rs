use super::*;

pub(crate) fn effect_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .and_then(|io| io.original_native_work())
        .is_some()
    {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

#[cfg(feature = "filesystem")]
pub(super) fn validate_task_plan<P: StreamProvider>(
    owner: &crate::durable_host::TaskJournalOwner<P>,
    plan: &TaskEffectPlan,
    planning: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    owner.require_effect_grants(&plan.provider, planning, work)?;
    #[cfg(not(target_arch = "wasm32"))]
    let io = work
        .as_deref()
        .and_then(|io| io.original_native_work())
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let io = None;
    owner.validate_input_file(&plan.request, io.as_ref())?;
    owner.validate_input_file(&plan.result_schema, io.as_ref())
}

impl<P: StreamProvider> ConversationEffectHost<P> {
    pub(super) fn validate_binding(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.authority.kind != crate::core::AggregateKind::Conversation {
            return Err(effect_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "effect host requires a conversation aggregate",
            ));
        }
        let verifier = self.issuer.verifier(work)?;
        verifier.verify_audience(&self.authority, work)?;
        verifier.verify(&self.scope, work)?;
        if !self.scope.capabilities().contains(capability::EFFECT_RUN) {
            return Err(effect_error(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "effect host scope lacks effect:run",
            ));
        }
        Ok(())
    }
}
