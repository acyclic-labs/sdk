//! Exact admitted resource preparation.
use super::*;
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Authenticates one immutable resource request under its current owner lease.
    #[allow(
        clippy::too_many_lines,
        reason = "The accepted preparation future retains the original request, observer and custody together."
    )]
    pub async fn resource_preparation(
        self: &Arc<Self>,
        request: crate::resource_preparation::ResourcePreparationRequest,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::resource_preparation::TaskResourcePreparation> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = work
            .as_deref()
            .and_then(|observer| observer.original_native_work())
            .cloned();
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_some() && native.is_none() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "preparation producer requires its original native observer",
            });
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.retain_owned(
                (std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>()
                    - std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>(
                    )) as u64,
            )?;
        }
        let (parent_task, request_digest) = {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let input = crate::store::history::canonical_bytes_with_work(
                &request,
                self.admission.limits.file_bytes,
                quota.work(),
            )?;
            quota.adopt(input.capacity() as u64)?;
            request.validate(quota.work())?;
            if let Some(checkpoint) = &request.source_checkpoint {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(native) = native.as_ref() {
                    self.validate_input_file(
                        checkpoint,
                        Some(&crate::filesystem::PreparationIo::from_native_work(native)?),
                    )?;
                } else {
                    self.validate_input_file(checkpoint, None)?;
                }
                #[cfg(target_arch = "wasm32")]
                self.validate_input_file(checkpoint, None)?;
            }
            if self.read_proof.is_some() {
                self.verify_read_access(quota.work()).await?;
            } else {
                self.verify(false, quota.work()).await?;
            }
            self.host
                .coordinator
                .lock()
                .await
                .scheduler()
                .session_limits(self.operation_id(), &mut None)?
                .validate(quota.work())?;
            let admission = self
                .host
                .admission(self.operation_id(), quota.work())
                .await?;
            if quota.work().is_some() {
                let extent = (std::mem::size_of::<crate::runtime::TaskAdmissionRecord>() as u64)
                    .checked_add(admission.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        crate::store::guarded_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Unauthorized,
                            "preparation admission ownership overflows",
                        )
                    })?;
                quota.adopt(extent)?;
            }
            let retained = crate::store::history::canonical_bytes_with_work(
                &admission.input,
                self.admission.limits.file_bytes,
                quota.work(),
            )?;
            quota.adopt(retained.capacity() as u64)?;
            let parent = admission.parent.ok_or_else(|| {
                crate::store::guarded_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "resource preparation requires an admitted parent",
                )
            })?;
            if let Some(observer) = quota.work().as_deref_mut() {
                let comparisons = (request.resources().count() as u64)
                    .checked_mul(request.child.resource_proofs.len() as u64)
                    .and_then(|comparisons| comparisons.checked_add(1))
                    .ok_or_else(|| {
                        crate::store::guarded_boundary(
                            &Some(&mut *observer),
                            crate::wire::ErrorCode::Invalid,
                            "preparation identity comparison work overflows",
                        )
                    })?;
                observer.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                    bytes: input.len() as u64,
                    items: comparisons,
                })?;
            }
            if retained != input
                || request.child.parent != Some(parent)
                || request.child.operation_id == admission.operation_id
                || request.child.resource_proofs.iter().any(|proof| {
                    request
                        .resources()
                        .any(|resource| resource == &proof.resource)
                        && proof.preparation != self.task_id()
                })
                || !self.admission.grants.contains(&request.creation_grant)
            {
                return Err(crate::store::guarded_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "resource request differs from its admitted input, parent or creation authority",
                ));
            }
            let source_bound = crate::store::history::canonical_bytes_with_work(
                &(&request, &self.admission.grants),
                request.maximum_bytes,
                quota.work(),
            )?;
            quota.adopt(source_bound.capacity() as u64)?;
            if let Some(observer) = quota.work().as_deref_mut() {
                observer.hashed_bytes(input.len() as u64)?;
            }
            (parent, *blake3::hash(&input).as_bytes())
        };
        #[cfg(not(target_arch = "wasm32"))]
        let source_io = native
            .as_ref()
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let source_io: Option<crate::filesystem::PreparationIo> = None;
        let project_source = self
            .host
            .seal_prepared_project_source(&request, parent_task, source_io.as_ref())
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(io), Some(observer)) = (source_io, work.as_deref_mut()) {
            let retained = io.owned_bytes()?;
            observer.absorb_native_custody(io.into_native_custody(retained)?)?;
            if project_source.is_some() {
                observer.release_owned(std::mem::size_of::<
                    crate::source_authorization::ProjectSourceReadHandle,
                >() as u64)?;
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let owner = crate::core::copy_authority(self.host.verifier.audience(), &mut quota)?;
        let creation_fence = crate::scheduler::LeaseFence {
            reservation_id: crate::core::copy_text(&self.fence.reservation_id, &mut quota)?,
            placement: crate::core::copy_text(&self.fence.placement, &mut quota)?,
        };
        let source_grants = crate::core::copy(&self.admission.grants, &mut quota, |grants, _| {
            grants.logical_owned_bytes()
        })?;
        let origin = crate::resource_preparation::ResourceCreationOrigin {
            owner,
            parent_task,
            preparation_task: self.task_id(),
            intended_child: request.child.operation_id,
            admission_digest: self.admission.canonical_digest_with_work(quota.work())?,
            request_digest,
            creation_fence,
            source_grants,
        };
        let token = crate::resource_preparation::TaskResourcePreparation {
            owner: self.clone(),
            request,
            origin,
            project_source,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        };
        quota.transfer();
        Ok(token)
    }
}
