//! Preparation.
use super::*;

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Consumes the genuine paid request on this original SDK-shared owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn resource_preparation_with_native_work<'a>(
        self: &'a Arc<Self>,
        request: (
            crate::resource_preparation::ResourcePreparationRequest,
            acyclic_fs::NativeAllocation,
        ),
        native: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, crate::resource_preparation::TaskResourcePreparation>>
    {
        if !self.native_shared_work_matches(native) || !request.1.matches_work(native) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation request differs from its original native owner or custody",
            });
        }
        let required =
            (std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>() as u64)
                .checked_add(request.0.logical_owned_bytes()?)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "preparation request ownership overflows",
                })?;
        if request.1.bytes() < required {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation request differs from its original native owner or custody",
            });
        }
        let mut pending = Some(request);
        let future = crate::NativeFutureBundle::new(native, || {
            let (request, mut custody) = pending
                .take()
                .expect("funded preparation builder runs once");
            async move {
                let mut io = match crate::store::history::native_work_io(native) {
                    Ok(io) => io,
                    Err(cause) => {
                        drop(request);
                        return Err(Error::NativeWork {
                            cause,
                            custody: Some(custody),
                        });
                    }
                };
                let token = self.resource_preparation(request, &mut Some(&mut io)).await;
                let mut metadata = io.into_custody();
                match token {
                    Ok(mut token) => {
                        if let Err(cause) = custody.absorb(&mut metadata) {
                            drop(token);
                            return Err(Error::NativeWork {
                                cause,
                                custody: Some(custody),
                            });
                        }
                        token.native_custody = Some(custody);
                        Ok(token)
                    }
                    Err(Error::NativeWork {
                        cause,
                        custody: None,
                    }) => {
                        drop(metadata);
                        Err(Error::NativeWork {
                            cause,
                            custody: Some(custody),
                        })
                    }
                    Err(error) => {
                        drop(metadata);
                        drop(custody);
                        Err(error)
                    }
                }
            }
        });
        match future {
            Ok(future) => Ok(future),
            Err(error) => {
                let (request, custody) = pending.take().expect("denied builder retains its input");
                drop(request);
                match error {
                    Error::NativeWork {
                        cause,
                        custody: None,
                    } => Err(Error::NativeWork {
                        cause,
                        custody: Some(custody),
                    }),
                    error => {
                        drop(custody);
                        Err(error)
                    }
                }
            }
        }
    }

    /// Authenticates one immutable resource request under its current owner lease.
    pub async fn resource_preparation(
        self: &Arc<Self>,
        request: crate::resource_preparation::ResourcePreparationRequest,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
                self.input_limits.file_bytes,
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
            if self.verify_original_read_witness()? {
                self.verify_read_access(quota.work()).await?;
            } else {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(native) = native.as_ref() {
                    self.verify(
                        false,
                        &mut Some(&mut crate::store::history::native_work_io(native)?),
                    )
                    .await?;
                } else {
                    self.verify(false, &mut None).await?;
                }
                #[cfg(target_arch = "wasm32")]
                self.verify(false, &mut None).await?;
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
                .admission_with_work(self.operation_id(), quota.work())
                .await?;
            if quota.work().is_some() {
                let extent = (std::mem::size_of::<crate::runtime::TaskAdmissionRecord>() as u64)
                    .checked_add(admission.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        admission_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Unauthorized,
                            "preparation admission ownership overflows",
                        )
                    })?;
                quota.adopt(extent)?;
            }
            let retained = crate::store::history::canonical_bytes_with_work(
                &admission.input,
                self.input_limits.file_bytes,
                quota.work(),
            )?;
            quota.adopt(retained.capacity() as u64)?;
            let parent = admission.parent.ok_or_else(|| {
                admission_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "resource preparation requires an admitted parent",
                )
            })?;
            if let Some(observer) = quota.work().as_deref_mut() {
                let comparisons = (request.resources.len() as u64)
                    .checked_mul(request.child.resource_proofs.len() as u64)
                    .and_then(|comparisons| comparisons.checked_add(1))
                    .ok_or_else(|| {
                        admission_boundary(
                            &Some(&mut *observer),
                            crate::wire::ErrorCode::Invalid,
                            "preparation identity comparison work overflows",
                        )
                    })?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                    bytes: input.len() as u64,
                    items: comparisons,
                })?;
            }
            if retained != input
                || request.child.parent != Some(parent)
                || request.child.operation_id == admission.operation_id
                || request.child.resource_proofs.iter().any(|proof| {
                    request.resources.contains(&proof.resource) && proof.preparation != self.task_id
                })
                || !self.input_grants.contains(&request.creation_grant)
            {
                return Err(admission_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "resource request differs from its admitted input, parent or creation authority",
                ));
            }
            let source_bound = crate::store::history::canonical_bytes_with_work(
                &(&request, &self.input_grants),
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
        if let Some(io) = source_io {
            let retained = io.owned_bytes()?;
            let observer = work.as_deref_mut().expect("native preparation observer");
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
        let source_grants = crate::core::copy_capabilities(&self.input_grants, &mut quota)?;
        let origin = crate::resource_preparation::ResourceCreationOrigin {
            owner,
            parent_task,
            preparation_task: self.task_id,
            intended_child: request.child.operation_id,
            admission_digest: self.original_admission_digest,
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
