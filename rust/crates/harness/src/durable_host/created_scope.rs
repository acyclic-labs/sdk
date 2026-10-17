//! Created authority extends the existing runtime scope after receipt verification.
use super::*;
#[cfg(not(target_arch = "wasm32"))]
use crate::Capabilities;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) fn scope_from_access(
        &self,
        admission: &TaskAdmissionRecord,
        access: Option<crate::resource_preparation::CreatedResourceAccess>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(RuntimeScope, u64)> {
        if let Some(access) = &access {
            if access.operation != admission.operation_id
                || access.admission_digest != admission.canonical_digest_with_work(work)?
                || access.proofs != admission.resource_proofs
            {
                return Err(Error::Unauthorized(
                    "created access changed admission".into(),
                ));
            }
        } else if !admission.resource_proofs.is_empty() {
            return Err(Error::Unauthorized(
                "created proof is not authenticated".into(),
            ));
        }
        if self.root_scope.extensions() != admission.extensions.as_ref() {
            return Err(Error::Conflict(
                "created scope changes extension selection".into(),
            ));
        }
        for grant in admission.grants.iter() {
            crate::core::inspect_metadata(grant, work)?;
            if !self.root_scope.grants().contains(grant)
                && !access
                    .as_ref()
                    .is_some_and(|access| access.grants.contains(grant))
            {
                return Err(Error::Unauthorized(
                    "created scope widens ancestor authority".into(),
                ));
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut scope = if let Some((observer, native)) =
            quota.work().as_deref_mut().and_then(|observer| {
                observer
                    .original_native_work()
                    .cloned()
                    .map(|native| (observer, native))
            }) {
            self.root_scope.validate_narrowing_with_native_work(
                self.root_scope.grants(),
                &admission.limits,
                &native,
            )?;
            let grants = Capabilities::copy(admission.grants.iter(), &native)?;
            let (scope, custody) =
                self.root_scope
                    .clone_native_metadata(grants, admission.limits, true, &native)?;
            let held = custody.bytes();
            observer.absorb_native_custody(custody)?;
            quota.adopt(held)?;
            scope
        } else {
            let mut scope = self
                .root_scope
                .narrow(self.root_scope.grants().clone(), admission.limits)?;
            scope.grants = admission.grants.clone();
            scope
        };
        #[cfg(target_arch = "wasm32")]
        let mut scope = {
            let mut scope = self
                .root_scope
                .narrow(self.root_scope.grants().clone(), admission.limits)?;
            scope.grants = admission.grants.clone();
            scope
        };
        scope.resource_proofs =
            RuntimeScope::copy_resource_proofs(&admission.resource_proofs, &mut quota)?;
        if access.is_some() && quota.work().is_some() {
            quota.replace(
                quota
                    .retained()
                    .checked_add((2 * std::mem::size_of::<usize>()) as u64)
                    .ok_or_else(|| Error::Invalid("scope ownership overflow".into()))?,
            )?;
        }
        scope.created_access = access
            .map(Arc::new)
            .or_else(|| self.root_scope.created_access());
        scope = scope.with_run_limits(admission.run_limits, quota.work())?;
        let held = if quota.work().is_some() {
            quota.retained()
        } else {
            0
        };
        quota.transfer();
        Ok((scope, held))
    }
}
