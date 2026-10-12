//! Original child allocation authentication through the protected fork journals.
use super::*;

pub(super) struct AuthenticatedAllocation {
    pub(super) origin: ResourceCreationOrigin,
    pub(super) volume: VolumeRef,
    pub(super) generation: GenerationRef,
    pub(super) prepared_generation: GenerationRef,
    pub(super) agent: crate::AgentId,
    pub(super) authority: Option<Authority>,
    pub(super) creation_operation: acyclic_fs::OperationId,
    pub(super) reference_files: Vec<FileRef>,
}

pub(super) async fn authenticate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    owner: &Authority,
    request: &ResourcePreparationRequest,
    proof: &ResourceCreationProofRef,
    io: Option<&PreparationIo>,
) -> Result<AuthenticatedAllocation> {
    let owned;
    let io = match io {
        Some(io) => {
            request.validate(&mut None)?;
            io
        }
        None => {
            owned = PreparationIo::new(request, &CancellationToken::new())?;
            &owned
        }
    };
    if request.provider != host.provider
        || proof.provider != host.provider
        || request.creation_grant != capability::FORK_PUBLISH
        || !request.child.resource_proofs.contains(proof)
    {
        return Err(Error::Unauthorized(
            "created access differs from its admitted provider boundary".into(),
        ));
    }
    io.encode(request, request.maximum_bytes, false)?;
    let maximum = request.maximum_bytes.min(MAX_REPORT_BYTES);
    #[cfg(not(target_arch = "wasm32"))]
    let fork = if io.original_native_work().is_some() {
        bounded_io::retain_decoded(io.value_decode_preparation::<ForkRequest>(
            &request.payload,
            crate::wire::ErrorCode::Storage,
        )?)
    } else {
        decode_fork(&request.payload, maximum, io)?
    };
    #[cfg(target_arch = "wasm32")]
    let fork = decode_fork(&request.payload, maximum, io)?;
    fork.validate(&mut None)?;
    let hold;
    let encoded = io.encode(&fork, maximum, false)?;
    hold = io.guard_bytes(&encoded)?;
    let decoded = io.decode(
        std::marker::PhantomData::<serde_json::Value>,
        &encoded,
        crate::wire::ErrorCode::Storage,
    )?;
    if decoded.0 != request.payload {
        return Err(Error::Unauthorized(
            "fork payload is not the exact admitted request".into(),
        ));
    }
    drop(decoded);
    let digest = *bounded_io::digest(&encoded, Some(io))?.as_bytes();
    drop(encoded);
    drop(hold);
    if request.resources().count() != creation_volumes(&fork).count() {
        return Err(Error::Unauthorized(
            "created identities differ from the exact fork allocation".into(),
        ));
    }
    for (volume, resource) in creation_volumes(&fork).zip(request.resources()) {
        if volume.provider() != &host.provider {
            return Err(Error::Unauthorized(
                "created volume belongs to another provider".into(),
            ));
        }
        let workspace = bounded_io::content_workspace(&host.provider, volume, Some(io))?;
        let held = io.guard_owned(workspace.logical_owned_bytes()?)?;
        if workspace.as_resource() != resource {
            return Err(Error::Unauthorized(
                "created identities differ from the exact fork allocation".into(),
            ));
        }
        drop(workspace);
        drop(held);
    }
    let index = request.resources()
        .position(|resource| resource == &proof.resource)
        .ok_or_else(|| Error::Unauthorized("proof does not name a created fork volume".into()))?;
    let volume = creation_volumes(&fork)
        .nth(index)
        .ok_or_else(|| Error::Unauthorized("created allocation index is absent".into()))?;
    let evidence = allocation_ref(&host.provider, volume, Some(io))?;
    if evidence.as_resource() != &proof.evidence {
        return Err(Error::Unauthorized(
            "proof does not name this protected allocation journal".into(),
        ));
    }
    let journal = preparation_journal(&host.provider, fork.operation_id, Some(io))?;
    if !read_claim(host, &journal, &fork, maximum, Some(io)).await? {
        return Err(Error::Unauthorized(
            "fork preparation request is not retained".into(),
        ));
    }
    let report = read_report(host, &journal, &fork, maximum, Some(io))
        .await?
        .ok_or_else(|| Error::Unauthorized("fork allocation has no completed report".into()))?;
    let seed = report.into_seed(&mut Some(&mut io.history_io()))?;
    let hold;
    let canonical = io.encode(request, request.maximum_bytes, true)?;
    hold = io.guard_bytes(&canonical)?;
    let canonical_digest = *bounded_io::digest(&canonical, Some(io))?.as_bytes();
    drop(canonical);
    drop(hold);
    let binding = seed_digest(&seed, maximum, Some(io))?;
    let mut selected: Option<AuthenticatedAllocation> = None;
    let mut original_origin = None;
    for (number, volume) in creation_volumes(&fork).enumerate() {
        let (mut claim, held) = host
            .read_fork_allocation(&seed, volume, binding, maximum, Some(io))
            .await?;
        let origin = claim.task_origin.as_ref().ok_or_else(|| {
            Error::Unauthorized(
                "raw trusted fork allocations cannot grant created-task access".into(),
            )
        })?;
        if claim.preparation_digest != digest
            || &origin.owner != owner
            || origin.preparation_task != proof.preparation
            || Some(origin.parent_task) != request.child.parent
            || origin.intended_child != request.child.operation_id
            || origin.request_digest != canonical_digest
            || selected
                .as_ref()
                .map(|known| &known.origin)
                .or(original_origin.as_ref())
                .is_some_and(|prior| prior != origin)
        {
            return Err(Error::Unauthorized(
                "created allocation does not authenticate its original task request".into(),
            ));
        }
        let prepared_generation = if volume == &fork.preparation.child_private_volume {
            &seed.child_private_generation
        } else if Some(volume) == fork.preparation.child_result_volume.as_ref() {
            seed.child_result_generation.as_ref().ok_or_else(|| {
                Error::Unauthorized("original child result generation is absent".into())
            })?
        } else {
            seed.resources
                .iter()
                .find_map(|resource| match &resource.revision {
                    ResourceRevision::Project {
                        volume: project,
                        generation,
                    } if project == volume => Some(generation),
                    _ => None,
                })
                .ok_or_else(|| {
                    Error::Unauthorized("original child project generation is absent".into())
                })?
        };
        let reference = bounded_io::content_workspace(&host.provider, volume, Some(io))?;
        let workspace = bounded_io::open(host, &reference, io).await?;
        let prepared = bounded_io::generation(host, &workspace, prepared_generation, io).await?;
        let suffix = if volume == &fork.preparation.child_private_volume {
            "scratch"
        } else if Some(volume) == fork.preparation.child_result_volume.as_ref() {
            "result"
        } else {
            "project"
        };
        let creation = IdempotencyKey::new(crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!(
                "fork:{}:{}:{suffix}",
                fork.operation_id,
                blake3::Hash::from(digest)
            ), work))?)?;
        let key = bounded_io::operation_key(&creation, Some(io))?;
        io.remaining()?;
        let initial = io
            .sdk(
                workspace
                    .operation_generation(key, io, io.cancellation())
                    .await,
            )?
            .ok_or_else(|| {
                Error::Unauthorized("original fork allocation operation is missing".into())
            })?;
        let generation = bounded_io::generation_ref(&host.provider, initial.id(), Some(io))?;
        let captured = if volume == &fork.preparation.child_private_volume {
            seed.resources
                .iter()
                .find_map(|resource| match &resource.revision {
                    ResourceRevision::PrivateVolume {
                        volume: captured,
                        generation,
                        ..
                    } if captured == volume => Some(generation),
                    _ => None,
                })
                .ok_or_else(|| {
                    Error::Unauthorized("original allocated volume is absent from the seed".into())
                })?
        } else {
            prepared_generation
        };
        if captured != &generation {
            return Err(Error::Unauthorized(
                "seed capture differs from the original allocation operation".into(),
            ));
        }
        if number == index {
            selected = Some(AuthenticatedAllocation {
                origin: claim
                    .task_origin
                    .take()
                    .expect("original task origin was authenticated"),
                volume: claim.volume,
                generation,
                prepared_generation: bounded_io::generation_ref(
                    &host.provider,
                    prepared.id(),
                    Some(io),
                )?,
                agent: fork.child_agent,
                authority: None,
                creation_operation: acyclic_fs::OperationId::from_bytes(key.into_bytes()),
                reference_files: Vec::new(),
            });
        }
        if selected.is_none() {
            original_origin = claim.task_origin.take();
        }
        if let Some(held) = held {
            held.into_retained();
        }
    }
    let mut selected = selected
        .ok_or_else(|| Error::Unauthorized("created allocation identity is absent".into()))?;
    if selected.volume == fork.preparation.child_private_volume {
        selected.authority = Some(fork.child);
    }
    let mut granted = BTreeSet::new();
    for reference in seed.reference_grants {
        require_reference_donor(&selected.origin.source_grants, &reference, None)?;
        if reference
            .attachment_manifest
            .as_ref()
            .is_some_and(|manifest| !seed.attachment_manifests.contains(manifest))
        {
            return Err(Error::Unauthorized(
                "inherited member proof is not retained in the original seed".into(),
            ));
        }
        if reference.reader == seed.child_agent {
            let capability = reference.capability()?;
            if !request.child.grants.contains(&capability) {
                return Err(Error::Unauthorized(
                    "child admission omitted its exact-file read grant".into(),
                ));
            }
            if selected.authority.is_some() && granted.insert(capability) {
                selected.reference_files.push(reference.file);
            }
        }
    }
    for file in &request.input_files {
        if !crate::runtime::read_granted(&selected.origin.source_grants, file)?
            || !request.child.grants.contains(&file.read_capability(&mut None)?)
        {
            return Err(Error::Unauthorized(
                "child input lacks original donor and exact child read authority".into(),
            ));
        }
        if selected.authority.is_some() {
            let bytes = host
                .read_pinned_io(file, request.maximum_bytes, Some(io))
                .await?;
            let held = io.guard_bytes(&bytes)?;
            drop(bytes);
            drop(held);
            if granted.insert(file.read_capability(&mut None)?) {
                selected.reference_files.push(file.clone());
            }
        }
    }
    Ok(selected)
}

fn decode_fork(
    payload: &serde_json::Value,
    maximum: u64,
    io: &PreparationIo,
) -> Result<ForkRequest> {
    let hold;
    let bytes = io.encode(payload, maximum, false)?;
    hold = io.guard_bytes(&bytes)?;
    let request = bounded_io::retain_decoded(io.decode(
        std::marker::PhantomData::<ForkRequest>,
        &bytes,
        crate::wire::ErrorCode::Storage,
    )?);
    drop(bytes);
    drop(hold);
    Ok(request)
}
