//! Authenticated creation receipts and inherited resource access.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Registers the trusted creators used to verify retained resource evidence.
    pub fn with_resource_creation_providers(
        mut self,
        providers: &[Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        if !self.resource_creation.is_empty() {
            return Err(Error::Conflict(
                "creation registry already configured".into(),
            ));
        }
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let mut configured = crate::core::reserve_copy_vector(providers.len(), &mut quota)?;
        for provider in providers {
            provider.provider().validate(quota.work())?;
            if configured.iter().any(
                |bound: &Arc<dyn crate::resource_preparation::ResourceCreationProvider>| {
                    bound.provider() == provider.provider()
                },
            ) {
                return Err(Error::Invalid("duplicate creation provider".into()));
            }
            configured.push(Arc::clone(provider));
        }
        quota.transfer();
        self.resource_creation = configured;
        Ok(self)
    }

    pub(super) fn reserve_proof_slots<T>(
        values: &mut Vec<T>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<u64> {
        let before = values.capacity();
        if let Some(io) = work.as_deref_mut() {
            crate::store::history::ensure_slot(values, usize::MAX, io)?;
        } else if values.len() == before {
            values.try_reserve(1).map_err(Error::Allocation)?;
        }
        Ok(((values.capacity() - before) * std::mem::size_of::<T>()) as u64)
    }

    pub(super) fn proof_contains<T: PartialEq>(
        values: &[T],
        wanted: &T,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        for value in values {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if value == wanted {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[allow(
        clippy::manual_async_fn,
        reason = "The recursive future must expose its native Send guarantee through ProviderTask."
    )]
    pub(super) fn created_access<'a>(
        &'a self,
        admission: &'a TaskAdmissionRecord,
        visited: &'a mut Vec<OperationId>,
        work: &'a mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> impl std::future::Future<
        Output = Result<Option<crate::resource_preparation::CreatedResourceAccess>>,
    > + acyclic_stream::ProviderTask
    + 'a {
        async move {
            if let Some(io) = work.as_deref_mut() {
                io.check_cancellation()?;
            }
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
                    self.created_access_body(admission, visited, work)
                })?
                .await;
            }
            Box::pin(self.created_access_body(admission, visited, work)).await
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "One recursive verification frame retains original proof custody through access sealing."
    )]
    pub(super) async fn created_access_body(
        &self,
        admission: &TaskAdmissionRecord,
        visited: &mut Vec<OperationId>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::resource_preparation::CreatedResourceAccess>> {
        use crate::wire::ErrorCode;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if admission.resource_proofs.is_empty() {
            return Ok(None);
        }
        if Self::proof_contains(visited, &admission.operation_id, work)? {
            return Err(crate::store::guarded_boundary(
                work,
                ErrorCode::Unauthorized,
                "resource preparation proof is cyclic",
            ));
        }
        Self::reserve_proof_slots(visited, work)?;
        visited.push(admission.operation_id);
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let parent = admission.parent.ok_or_else(|| {
            crate::store::guarded_boundary(
                custody.work(),
                ErrorCode::Unauthorized,
                "created resource admission has no parent",
            )
        })?;
        let parent_admission = self
            .admission(OperationId::from_bytes(parent.into_bytes()), custody.work())
            .await?;
        let held = if custody.work().is_some() {
            std::mem::size_of::<TaskAdmissionRecord>() as u64
                + parent_admission.logical_owned_bytes()?
        } else {
            0
        };
        custody.adopt(held)?;
        let mut receipts = Vec::new();
        let mut current_default: Option<(usize, OperationId)> = None;
        for proof in &admission.resource_proofs {
            let mut selected = None;
            for provider in &self.resource_creation {
                if let Some(io) = custody.work().as_deref_mut() {
                    io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if provider.provider() == &proof.provider {
                    selected = Some(provider);
                    break;
                }
            }
            let provider = selected.ok_or_else(|| {
                crate::store::guarded_boundary(
                    custody.work(),
                    ErrorCode::Unsupported,
                    "resource proof verifier is not bound",
                )
            })?;
            let mut proof_custody = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
            let preparation = self
                .admission(
                    OperationId::from_bytes(proof.preparation.into_bytes()),
                    proof_custody.work(),
                )
                .await?;
            let held = if proof_custody.work().is_some() {
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + preparation.logical_owned_bytes()?
            } else {
                0
            };
            proof_custody.adopt(held)?;
            let (request, held) = crate::resource_preparation::ResourcePreparationRequest::decode(
                &preparation.input,
                proof_custody.work(),
            )?;
            proof_custody.adopt(held)?;
            if preparation.parent != request.child.parent
                || preparation.operation_id.into_bytes() != proof.preparation.into_bytes()
                || request.provider != proof.provider
                || !request
                    .resources()
                    .any(|resource| resource == &proof.resource)
                || !request.child.resource_proofs.contains(proof)
                || !preparation.grants.contains(&request.creation_grant)
            {
                return Err(crate::store::guarded_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "resource proof differs from admitted preparation",
                ));
            }
            let preparation_access = self
                .created_access(&preparation, visited, proof_custody.work())
                .await?;
            let (preparation_scope, scope_held) =
                self.scope_from_access(&preparation, preparation_access, proof_custody.work())?;
            drop(preparation_scope);
            if let Some(io) = proof_custody.work().as_deref_mut() {
                io.release_owned(scope_held)?;
            }
            let preparation_parent = preparation.parent.ok_or_else(|| {
                crate::store::guarded_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "resource preparation has no original parent",
                )
            })?;
            let original_parent = self
                .admission(
                    OperationId::from_bytes(preparation_parent.into_bytes()),
                    proof_custody.work(),
                )
                .await?;
            let held = if proof_custody.work().is_some() {
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + original_parent.logical_owned_bytes()?
            } else {
                0
            };
            proof_custody.adopt(held)?;
            if !original_parent.grants.contains(&request.creation_grant)
                || !preparation.grants.is_subset_of(&original_parent.grants)
            {
                return Err(crate::store::guarded_boundary(
                    proof_custody.work(),
                    ErrorCode::Unauthorized,
                    "creation authority was not narrowed from the parent",
                ));
            }
            let inherited = request.child != *admission;
            let mut origin_distance = 0;
            if inherited {
                if !parent_admission.resource_proofs.contains(proof) {
                    return Err(crate::store::guarded_boundary(
                        proof_custody.work(),
                        ErrorCode::Unauthorized,
                        "created resource proof is not inherited from the parent",
                    ));
                }
                let original_child = self
                    .admission(request.child.operation_id, proof_custody.work())
                    .await?;
                let held = if proof_custody.work().is_some() {
                    std::mem::size_of::<TaskAdmissionRecord>() as u64
                        + original_child.logical_owned_bytes()?
                } else {
                    0
                };
                proof_custody.adopt(held)?;
                let matches = original_child == request.child;
                drop(original_child);
                proof_custody.relinquish(held)?;
                if !matches {
                    return Err(crate::store::guarded_boundary(
                        proof_custody.work(),
                        ErrorCode::Unauthorized,
                        "created resource origin admission changed",
                    ));
                }
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
                    return Err(crate::store::guarded_boundary(
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
                            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                                items: 1,
                            })?;
                        }
                        if operation == request.child.operation_id {
                            origin_distance = lineage.len() + 1;
                            found = true;
                            break;
                        }
                        if Self::proof_contains(&lineage, &operation, lineage_custody.work())? {
                            return Err(crate::store::guarded_boundary(
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
                        return Err(crate::store::guarded_boundary(
                            lineage_custody.work(),
                            ErrorCode::Unauthorized,
                            "resource proof belongs to another child lineage",
                        ));
                    }
                }
            }
            let receipt;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some((observer, native)) =
                proof_custody.work().as_deref_mut().and_then(|observer| {
                    observer
                        .original_native_work()
                        .cloned()
                        .map(|native| (observer, native))
                })
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
                observer.absorb_native_custody(guard)?;
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
                    != preparation.canonical_digest_with_work(proof_custody.work())?
                || receipt.origin.request_digest
                    != crate::executor::json::canonical_json_digest(
                        &request,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        proof_custody.work(),
                    )?
            {
                return Err(crate::store::guarded_boundary(
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
        visited.pop();
        let access = consume_receipts(
            admission,
            &parent_admission.grants,
            &mut receipts,
            current_default.map(|(_, origin)| origin),
            custody.work(),
        )?;
        Ok(Some(access))
    }
}

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Verifies only a kernel-authenticated created-volume input.
    pub(crate) async fn verify_created_input_with_io(
        &self,
        file: &FileRef,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        self.validate_input_file(file, io)?;
        let mut reader = None;
        if let Some(access) = &self.created {
            for binding in &access.references {
                if let Some(io) = io {
                    let step = acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    };
                    io.preflight(step)?;
                    io.charge(step)?;
                }
                if &binding.0 == file {
                    reader = Some(&binding.1);
                    break;
                }
            }
            if reader.is_none() {
                for binding in &access.content {
                    if let Some(io) = io {
                        let step = acyclic_fs::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        };
                        io.preflight(step)?;
                        io.charge(step)?;
                    }
                    if &binding.volume == file.volume() {
                        reader = Some(&binding.binding.reader);
                        break;
                    }
                }
            }
        }
        let Some(reader) = reader else {
            return Ok(false);
        };
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        self.verify(true, &mut work).await?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let (bytes, retained) = match quota.work().as_deref_mut() {
            Some(observer) => {
                crate::store::read_content_with_work(observer, reader.as_ref(), file).await?
            }
            None => (
                reader
                    .read(file, file.descriptor().byte_length(), None)?
                    .await?,
                0,
            ),
        };
        quota.adopt(retained)?;
        file.descriptor().verify(&bytes, quota.work())?;
        self.verify(true, quota.work()).await?;
        drop(bytes);
        Ok(true)
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn authenticated_scope(
        &self,
        admission: &TaskAdmissionRecord,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(RuntimeScope, u64)> {
        let mut visited = Vec::new();
        let access = self.created_access(admission, &mut visited, work).await?;
        let result = self.scope_from_access(admission, access, work);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned((visited.capacity() * std::mem::size_of::<OperationId>()) as u64)?;
        }
        result
    }
}
