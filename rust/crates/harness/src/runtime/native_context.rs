use super::content_write::bound_content_writer;
use super::{
    ContentBindings, InteractionRouter, IssuedContextPurpose, Limits, RuntimeScope, TaskContext,
    ToolContext,
};
use crate::{Error, Result};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::sync::Arc;

pub(super) fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

fn validate_scope_attenuation(
    original: &RuntimeScope,
    requested: &RuntimeScope,
    limits: Limits,
    work: &NativeMaterializationWork,
) -> Result<()> {
    original.validate_narrowing_with_native_work(&requested.grants, &limits, work)?;
    original.validate_run_limit_attenuation(
        &requested.run_limits,
        &mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )),
    )
}

impl TaskContext {
    /// Requires the SDK's original metadata source and cancellation ledger.
    pub(crate) fn require_original_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default())?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context differs from its original native metadata source",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_original_native_dispatch(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        if self.native_purpose != Some(IssuedContextPurpose::Dispatch) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native task metadata was not issued for fresh dispatch",
            ));
        }
        Ok(())
    }

    /// Revalidates the original issued execution owner before a native SDK read.
    pub async fn verify_original_execution_owner(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_dispatch(work)?;
        let (task, fence) = self.owned_task_binding(Some(
            &crate::executor::PreparationIo::from_native_work(work)?,
        ))?;
        let host = self.harness.host.as_ref().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unsupported,
                "native execution has no original durable owner callback",
            )
        })?;
        self.harness.assert_durable_policy_bindings(Some(work))?;
        host.verify_dispatch_owner(
            task,
            fence,
            Some(&crate::executor::PreparationIo::from_native_work(work)?),
        )?
        .await
    }

    /// Checks stock scope restrictions against the issued source before copying metadata or constructing a callback.
    pub(crate) fn validate_stock_scope_with_native_work(
        &self,
        requested: &RuntimeScope,
        limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        validate_scope_attenuation(&self.scope, requested, limits, work)
    }
}

impl TaskContext {
    pub(super) async fn project_output_binding(
        self: &Arc<Self>,
        binding: &ContentBindings,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        self.require_original_native_work(work)?;
        let purpose = self.native_purpose.ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unauthorized,
                "original native metadata purpose is missing",
            )
        })?;
        if purpose == IssuedContextPurpose::Read {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "readonly metadata cannot select a native output writer",
            ));
        }
        bound_content_writer(
            Some(binding),
            &self.scope,
            Some(&crate::executor::PreparationIo::from_native_work(work)?),
        )?;
        let (task, fence) = self.owned_task_binding(Some(
            &crate::executor::PreparationIo::from_native_work(work)?,
        ))?;
        let host = self.harness.host.as_ref().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unsupported,
                "output projection has no original task host",
            )
        })?;
        self.harness.assert_durable_policy_bindings(Some(work))?;
        match purpose {
            IssuedContextPurpose::Dispatch => {
                host.verify_dispatch_owner(
                    task,
                    fence,
                    Some(&crate::executor::PreparationIo::from_native_work(work)?),
                )?
                .await?;
            }
            IssuedContextPurpose::Reconciliation => {
                host.verify_execution_owner(
                    task,
                    fence,
                    Some(&crate::executor::PreparationIo::from_native_work(work)?),
                )?
                .await?;
            }
            IssuedContextPurpose::Read => unreachable!("readonly projection rejected"),
        }
        let mut custody = work.claim(
            (std::mem::size_of::<TaskContext>() - std::mem::size_of::<RuntimeScope>()) as u64,
            0,
            WorkCounters::default(),
        )?;
        let (scope, mut scope_custody) = self.scope.clone_with_native_work(work)?;
        custody.absorb(&mut scope_custody)?;
        let mut context = self.copy_with_scope(scope);
        context.output_content = Some(binding.clone());
        context.native_purpose = Some(purpose);
        context.native_custody = Some(custody);
        context.into_shared()
    }

    /// Borrows the original metadata ledger without granting resource authority.
    pub(crate) fn native_work(&self) -> Result<&NativeMaterializationWork> {
        self.native_custody
            .as_ref()
            .filter(|custody| self.native_metadata_matches(custody.work()))
            .map(NativeAllocation::work)
            .ok_or_else(|| {
                boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "task context has no original native custody",
                )
            })
    }

    /// Checks custody for this context's original metadata.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        self.native_purpose.is_some()
            && self
                .native_custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(work))
    }
}

impl ToolContext {
    /// Checks stock restrictions against the effective scope.
    pub(crate) fn validate_stock_scope_with_native_work(
        &self,
        requested: &RuntimeScope,
        limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.task().require_original_native_work(work)?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "stock scope has no original native tool metadata source",
            ));
        }
        validate_scope_attenuation(self.effective_scope(), requested, limits, work)
    }

    /// Narrows tool authority and execution limits while preserving its issued task.
    pub fn with_native_scope(
        mut self,
        requested: &RuntimeScope,
        execution_limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        self.validate_stock_scope_with_native_work(requested, execution_limits, work)?;
        let original = self.effective_scope();
        let grants = crate::Capabilities::copy(requested.grants.iter(), work)?;
        let mut narrowed = original.narrow_with_native_work(grants, execution_limits, work)?;
        narrowed.0 = narrowed.0.with_run_limits(
            requested.run_limits,
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )?;
        self.effective_scope = Some((narrowed.0, Some(narrowed.1)));
        Ok(self)
    }

    /// Retains the issued interaction router.
    pub(crate) fn with_native_interactions(
        mut self,
        router: Arc<dyn InteractionRouter>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.charge(WorkCounters::default())?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "interaction override has no original native tool metadata source",
            ));
        }
        self.native_interactions = Some(router);
        Ok(self)
    }

    /// Borrows the actual per-command router for the original policy pipeline.
    pub(crate) fn stock_native_interactions(&self) -> Option<&dyn InteractionRouter> {
        self.native_interactions.as_deref()
    }

    /// Borrows the original shared task without reconstructing its owner.
    pub(crate) fn native_task(&self) -> Option<&Arc<TaskContext>> {
        self.task.native_custody.as_ref().map(|_| &self.task)
    }

    /// Checks genuine SDK tool metadata funding without granting tool authority.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        let task = &self.task;
        let Some(custody) = &self.native_custody else {
            return false;
        };
        task.native_metadata_matches(work)
            && custody.work().shares_allocation_context(work)
            && (std::mem::size_of::<Self>() as u64)
                .checked_add(self.call_id.capacity() as u64)
                .is_some_and(|bytes| custody.bytes() >= bytes)
            && self.effective_scope.as_ref().is_none_or(|(_, guard)| {
                guard
                    .as_ref()
                    .is_some_and(|guard| guard.work().shares_allocation_context(work))
            })
    }
}

#[cfg(test)]
mod tests;
