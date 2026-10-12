//! Resources.
use super::*;

pub(super) struct Verification {
    pub(super) remaining: Option<(u64, u64)>,
    pub(super) path: Vec<OperationId>,
}

impl Verification {
    pub(super) fn charge(
        &mut self,
        units: u64,
        bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if let Some((remaining_work, remaining_bytes)) = self.remaining.as_mut() {
            *remaining_work = remaining_work.checked_sub(units).ok_or_else(|| {
                admission_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "project source verification exceeds work bound",
                )
            })?;
            *remaining_bytes = remaining_bytes.checked_sub(bytes).ok_or_else(|| {
                admission_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "project source verification exceeds byte bound",
                )
            })?;
        }
        Ok(())
    }
}

impl<P: StreamProvider> CreatedTaskPublicationLease<P> {
    async fn verify_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.scope,
            &self.verifier,
            self.operation,
            work,
        )?;
        let fence = self.fence.as_ref().ok_or_else(|| {
            admission_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "created resource task has no current execution owner",
            )
        })?;
        crate::scheduler::require_execution_owner(operation, fence, false, work)?;
        if self
            .deadline_epoch_ms
            .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            let mut text = String::new();
            if let Some(io) = work.as_deref_mut() {
                io.admit_copy(48)?;
                io.reserve_text(&mut text, 48)?;
                io.copy_bytes(48)?;
            }
            use std::fmt::Write as _;
            write!(&mut text, "task-cancel:{}", self.operation).expect("writing into String");
            coordinator
                .cancel_operation(
                    self.verifier.audience(),
                    &self.scope,
                    &self.verifier,
                    self.operation,
                    IdempotencyKey::new(text)?,
                    true,
                    None,
                    work,
                )
                .await?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .is_some()
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "created resource task reached its original execution deadline",
                });
            }
            return Err(Error::Indeterminate(self.operation));
        }
        Ok(())
    }
}

impl<P: StreamProvider> crate::resource_preparation::CreatedTaskLease
    for CreatedTaskPublicationLease<P>
{
    fn fence(&self) -> Result<crate::scheduler::LeaseFence> {
        self.fence.clone().ok_or_else(|| {
            Error::Conflict("created resource task has no execution reservation".into())
        })
    }
    fn verify<'a>(
        &'a self,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            if self._native_custody.as_ref().is_none_or(|custody|
                !custody.work().shares_allocation_context(work))
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "created publication lease differs from its original SDK metadata ledger",
                });
            }
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                return self.verify_with_work(&mut Some(&mut io.history_io())).await;
            }
            self.verify_with_work(&mut None).await
        })
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn source_admission(
        &self,
        task: TaskId,
        budget: &mut Verification,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        budget.charge(1, 0, work)?;
        let operation = OperationId::from_bytes(task.into_bytes());
        let spec = self
            .retained_declaration_with_work(operation, work)
            .await?
            .ok_or_else(|| {
                admission_boundary(
                    work,
                    crate::wire::ErrorCode::NotFound,
                    "project source admission is missing",
                )
            })?;
        let held = if work.is_some() {
            spec.logical_owned_bytes()?
        } else {
            0
        };
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, held);
        budget.charge(0, spec.state.descriptor().byte_length(), quota.work())?;
        let admitted = self
            .read_declared_admission_counted(&spec, quota.work())
            .await;
        drop(spec);
        let released = quota.relinquish(held);
        let record = admitted?;
        released?;
        Ok(record)
    }

    pub(super) fn created_publication_lease_with_work(
        &self,
        operation: OperationId,
        fence: Option<crate::scheduler::LeaseFence>,
        deadline_epoch_ms: Option<u64>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Arc<dyn crate::resource_preparation::CreatedTaskLease>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work
            .as_deref()
            .is_some_and(|io| io.original_native_work().is_some());
        let mut lease = CreatedTaskPublicationLease {
            coordinator: Arc::clone(&self.coordinator),
            scope: self.owner_scope.copy(work)?,
            verifier: self.verifier.copy(work)?,
            operation,
            fence,
            clock: Arc::clone(&self.clock),
            deadline_epoch_ms,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_custody: None,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native {
            let mut bytes = (std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64)
                .checked_add(lease.scope.logical_owned_bytes()?)
                .and_then(|bytes| bytes.checked_add(lease.verifier.logical_owned_bytes().ok()?))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "created publication lease custody overflows",
                })?;
            if let Some(fence) = &lease.fence {
                bytes = bytes
                    .checked_add(fence.reservation_id.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(fence.placement.capacity() as u64))
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "created publication fence custody overflows",
                    })?;
            }
            lease._native_custody = Some(
                work.as_deref_mut()
                    .expect("native lease source")
                    .take_native_custody(bytes)?,
            );
        }
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::AllocationOperation {
                bytes: std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64,
            })?;
        }
        let lease = Arc::new(lease);
        Ok(lease)
    }

    pub(crate) async fn authenticated_scope_with_work(
        &self,
        admission: &TaskAdmissionRecord,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(RuntimeScope, u64)> {
        let mut verification = Verification {
            path: Vec::new(),
            remaining: None,
        };
        let access = self
            .verify_created_access(admission, &mut verification, work)
            .await;
        let visited_bytes =
            (verification.path.capacity() * std::mem::size_of::<OperationId>()) as u64;
        drop(verification);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(visited_bytes)?;
        }
        self.root_scope
            .with_created_access_with_work(admission, access?, work)
    }

    pub(super) fn reserve_proof_slots<T>(
        values: &mut Vec<T>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        if values.len() < values.capacity() {
            return Ok(0);
        }
        let target = values
            .capacity()
            .checked_mul(2)
            .unwrap_or(usize::MAX)
            .max(4);
        let bytes = target
            .checked_sub(values.capacity())
            .and_then(|slots| slots.checked_mul(std::mem::size_of::<T>()))
            .ok_or_else(|| {
                admission_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "resource proof vector ownership overflows",
                )
            })? as u64;
        let old = values.capacity();
        if let Some(io) = work.as_deref_mut() {
            io.admit_allocation(bytes)?;
        }
        values
            .try_reserve_exact(target - values.len())
            .map_err(|cause| match work.as_deref() {
                Some(io) => io.allocation_failure(cause),
                None => Error::Storage(cause.to_string()),
            })?;
        let retained = (values.capacity() - old)
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| {
                admission_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "resource proof vector ownership overflows",
                )
            })? as u64;
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(retained)?;
        }
        Ok(retained)
    }

    pub(super) fn proof_contains<T: PartialEq>(
        values: &[T],
        wanted: &T,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        for value in values {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if value == wanted {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn verify_created_access<'a, 'b>(
        &'a self,
        admission: &'a TaskAdmissionRecord,
        verification: &'a mut Verification,
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> impl std::future::Future<
        Output = Result<Option<crate::resource_preparation::CreatedResourceAccess>>,
    > + acyclic_stream::ProviderTask
    + 'a {
        async move {
            verification.charge(1, 0, work)?;
            if admission.resource_proofs.is_empty() {
                return Ok(None);
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .cloned()
            {
                return crate::NativeFutureBundle::new(&native, || {
                    self.verify_created_access_body(admission, verification, work)
                })?
                .await;
            }
            Box::pin(self.verify_created_access_body(admission, verification, work)).await
        }
    }

    pub(super) async fn verify_created_access_body(
        &self,
        admission: &TaskAdmissionRecord,
        verification: &mut Verification,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::resource_preparation::CreatedResourceAccess>> {
        use crate::wire::ErrorCode;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if admission.resource_proofs.is_empty() {
            return Ok(None);
        }
        if Self::proof_contains(&verification.path, &admission.operation_id, work)? {
            return Err(admission_boundary(
                work,
                ErrorCode::Unauthorized,
                "resource preparation proof is cyclic",
            ));
        }
        Self::reserve_proof_slots(&mut verification.path, work)?;
        verification.path.push(admission.operation_id);
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let parent = admission.parent.ok_or_else(|| {
            admission_boundary(
                custody.work(),
                ErrorCode::Unauthorized,
                "created resource admission has no parent",
            )
        })?;
        let (parent_admission, held) = self
            .source_admission(parent, verification, custody.work())
            .await?;
        custody.adopt(held)?;
        let mut receipts = Vec::new();
        let mut current_default: Option<(usize, OperationId)> = None;
        for proof in &admission.resource_proofs {
            verification.charge(1, 0, custody.work())?;
            let mut selected = None;
            for provider in &self.resource_creation {
                if let Some(io) = custody.work().as_deref_mut() {
                    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if provider.provider() == &proof.provider {
                    selected = Some(provider);
                    break;
                }
            }
            let provider = selected.ok_or_else(|| {
                admission_boundary(
                    custody.work(),
                    ErrorCode::Unsupported,
                    "resource proof verifier is not bound",
                )
            })?;
            let mut proof_custody = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
            let (preparation, held) = self
                .source_admission(proof.preparation, verification, proof_custody.work())
                .await?;
            proof_custody.adopt(held)?;
            let (request, held) =
                crate::resource_preparation::ResourcePreparationRequest::decode(
                    &preparation.input, proof_custody.work(),
                )?;
            proof_custody.adopt(held)?;
            verification.charge(
                request.maximum_work,
                request.maximum_bytes,
                proof_custody.work(),
            )?;
            if preparation.parent != request.child.parent
                || preparation.operation_id.into_bytes() != proof.preparation.into_bytes()
                || request.provider != proof.provider
                || !request.resources().any(|resource| resource == &proof.resource)
                || !request.child.resource_proofs.contains(proof)
                || !preparation.grants.contains(&request.creation_grant)
            {
                return Err(admission_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "resource proof differs from admitted preparation",
                ));
            }
            let preparation_access = self
                .verify_created_access(&preparation, verification, proof_custody.work())
                .await?;
            let (preparation_scope, scope_held) = self.root_scope.with_created_access_with_work(
                &preparation,
                preparation_access,
                proof_custody.work(),
            )?;
            drop(preparation_scope);
            if let Some(io) = proof_custody.work().as_deref_mut() {
                io.release_owned(scope_held)?;
            }
            let preparation_parent = preparation.parent.ok_or_else(|| {
                admission_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "resource preparation has no original parent",
                )
            })?;
            let (original_parent, held) = self
                .source_admission(preparation_parent, verification, proof_custody.work())
                .await?;
            proof_custody.adopt(held)?;
            if !original_parent.grants.contains(&request.creation_grant)
                || !preparation.grants.is_subset_of(&original_parent.grants)
            {
                return Err(admission_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "creation authority was not narrowed from the parent",
                ));
            }
            let inherited = request.child != *admission;
            let mut origin_distance = 0;
            if inherited {
                if !parent_admission.resource_proofs.contains(proof) {
                    return Err(admission_boundary(
                        proof_custody.work(),
                        ErrorCode::Unauthorized,
                        "created resource proof is not inherited from the parent",
                    ));
                }
                let (original_child, held) = self
                    .source_admission(
                        TaskId::from_bytes(request.child.operation_id.into_bytes()),
                        verification,
                        proof_custody.work(),
                    )
                    .await?;
                proof_custody.adopt(held)?;
                let matches = original_child == request.child;
                drop(original_child);
                proof_custody.relinquish(held)?;
                if !matches {
                    return Err(admission_boundary(
                        proof_custody.work(),
                        ErrorCode::Unauthorized,
                        "created resource origin admission changed",
                    ));
                }
            }
            #[cfg(feature = "filesystem")]
            if inherited && verification.remaining.is_some() {
                self.source_graph_root(parent, verification, proof_custody.work())
                    .await?;
            }
            {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh(proof_custody.work()).await?;
                let prepared = coordinator.observe_operation(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    preparation.operation_id,
                    proof_custody.work(),
                )?;
                if prepared.cancellation_requested
                    || !matches!(prepared.outcome, Some(Outcome::Succeeded(_)))
                {
                    return Err(admission_boundary(
                        proof_custody.work(),
                        ErrorCode::Unauthorized,
                        "resource preparation did not succeed",
                    ));
                }
                if inherited {
                    let mut lineage_custody =
                        crate::wire_codec::CodecQuota::new(proof_custody.work(), 0)?;
                    let mut lineage = Vec::new();
                    let mut cursor = Some(OperationId::from_bytes(parent.into_bytes()));
                    let mut found = false;
                    while let Some(operation) = cursor {
                        if let Some(io) = lineage_custody.work().as_deref_mut() {
                            io.codec_charge(crate::contract::JsonCodecCharge::Inspect {
                                items: 1,
                            })?;
                        }
                        if operation == request.child.operation_id {
                            origin_distance = lineage.len() + 1;
                            found = true;
                            break;
                        }
                        if Self::proof_contains(&lineage, &operation, lineage_custody.work())? {
                            return Err(admission_boundary(
                                lineage_custody.work(),
                                ErrorCode::Conflict,
                                "indexed operation lineage is cyclic",
                            ));
                        }
                        let held = Self::reserve_proof_slots(&mut lineage, lineage_custody.work())?;
                        lineage_custody.adopt(held)?;
                        lineage.push(operation);
                        let state = coordinator.observe_operation(
                            self.verifier.audience(),
                            &self.owner_scope,
                            &self.verifier,
                            operation,
                            lineage_custody.work(),
                        )?;
                        cursor = state.spec.parent.as_ref().map(|link| link.operation_id);
                    }
                    if !found {
                        return Err(admission_boundary(
                            lineage_custody.work(),
                            ErrorCode::Unauthorized,
                            "resource proof belongs to another child lineage",
                        ));
                    }
                }
            }
            let receipt;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = proof_custody
                .work()
                .as_deref()
                .and_then(|io| io.original_native_work())
                .cloned()
            {
                let (verified, guard) = provider
                    .verify_with_native_work(&request, proof, &native)?
                    .await?;
                if !guard.work().shares_allocation_context(&native) {
                    drop(verified);
                    return Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                        custody: Some(guard),
                    });
                }
                proof_custody
                    .work()
                    .as_deref_mut()
                    .expect("native proof observer")
                    .absorb_native_custody(guard)?;
                receipt = verified;
            } else {
                receipt = provider.verify(&request, proof).await?;
            }
            #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
            {
                receipt = provider.verify(&request, proof).await?;
            }
            if &receipt.origin.owner != self.verifier.audience()
                || receipt.origin.parent_task != preparation_parent
                || receipt.origin.preparation_task != proof.preparation
                || receipt.origin.intended_child != request.child.operation_id
                || receipt.origin.admission_digest
                    != crate::contract::canonical_json_digest(
                        &preparation,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        proof_custody.work(),
                    )?
                || receipt.origin.request_digest
                    != crate::contract::canonical_json_digest(
                        &request,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        proof_custody.work(),
                    )?
            {
                return Err(admission_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "resource allocation provenance changed",
                ));
            }
            if receipt.content.iter().any(|binding| binding.default_writer)
                && current_default.is_none_or(|(distance, _)| origin_distance < distance)
            {
                current_default = Some((origin_distance, receipt.origin.intended_child));
            }
            drop(original_parent);
            drop(request);
            drop(preparation);
            drop(proof_custody);
            Self::reserve_proof_slots(&mut receipts, custody.work())?;
            receipts.push(receipt);
        }
        verification.path.pop();
        let (access, _) = crate::resource_preparation::CreatedResourceAccess::consume(
            admission,
            &mut receipts,
            current_default.map(|(_, origin)| origin),
            custody.work(),
        )?;
        Ok(Some(access))
    }
}
