fn workspace_ref<A, O>(workspace: &Workspace<A, O>) -> wire::WorkspaceRef {
    wire::WorkspaceRef {
        workspace_id: workspace.id().into_bytes().to_vec(),
        name: workspace.name().as_str().to_owned(),
    }
}

fn generation_ref<A, O>(generation: &Generation<A, O>) -> wire::GenerationRef {
    generation_ref_for(&generation.workspace, generation.id())
}

fn generation_ref_for<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
) -> wire::GenerationRef {
    wire::GenerationRef {
        workspace: Some(workspace_ref(workspace)),
        generation_id: generation.digest().into_bytes().to_vec(),
    }
}

fn join_common_ancestor_ref<A, O>(
    plan: &crate::JoinPlan<A, O>,
    source: &Workspace<A, O>,
    target: &Workspace<A, O>,
) -> wire::GenerationRef {
    let volume = plan.base.volume_id;
    let workspace = if volume == source.id().volume_id() {
        Some(workspace_ref(source))
    } else if volume == target.id().volume_id() {
        Some(workspace_ref(target))
    } else {
        None
    };
    wire::GenerationRef {
        workspace,
        generation_id: plan.common_ancestor().digest().into_bytes().to_vec(),
    }
}

fn observed_mutation<A, O>(
    workspace: &Workspace<A, O>,
    commit: &DurableCommit,
) -> Result<wire::MutationResponse, Status> {
    const MAXIMUM_EVENT_BYTES: u64 = 4 * 1024;
    let expected = workspace.id().volume_id();
    if let Ok(deleted) =
        crate::kernel::decode_workspace_deleted(&commit.payload, MAXIMUM_EVENT_BYTES)
    {
        if deleted != expected {
            return Err(Status::data_loss("operation workspace identity mismatch"));
        }
        return Ok(mutation(wire::MutationStatus::Committed, None, None));
    }
    let root = if let Ok(publication) =
        crate::kernel::decode_published_generation(&commit.payload, MAXIMUM_EVENT_BYTES, |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        }) {
        if publication.volume_id != expected {
            return Err(Status::data_loss("operation workspace identity mismatch"));
        }
        publication.generation_root
    } else if let Ok(created) =
        crate::kernel::decode_volume_created(&commit.payload, MAXIMUM_EVENT_BYTES, &mut |_| {
            Ok::<_, crate::kernel::VolumeCreatedError>(())
        })
    {
        if created.volume_id != expected {
            return Err(Status::data_loss("operation workspace identity mismatch"));
        }
        created.initial_generation_root
    } else {
        return Err(Status::data_loss(
            "operation does not contain a workspace mutation",
        ));
    };
    Ok(mutation(
        wire::MutationStatus::Committed,
        Some(wire::GenerationRef {
            workspace: Some(workspace_ref(workspace)),
            generation_id: root.digest.into_bytes().to_vec(),
        }),
        None,
    ))
}

async fn workspace_message<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
) -> Result<wire::Workspace, Status> {
    let head = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| status(&failure.error))?
        .value;
    Ok(wire::Workspace {
        workspace: Some(workspace_ref(workspace)),
        name: workspace.name().as_str().to_owned(),
        profile: profile_message(workspace.profile()),
        head: Some(generation_ref(&head)),
        deleted: false,
    })
}

async fn generation_message<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
) -> Result<wire::GenerationResponse, Status> {
    let reference = generation_ref(generation);
    let workspace = reference.workspace.clone();
    let parents = generation
        .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| status(&failure.error))?
        .value
        .into_iter()
        .map(|id| wire::GenerationRef {
            workspace: workspace.clone(),
            generation_id: id.digest().into_bytes().to_vec(),
        })
        .collect();
    Ok(wire::GenerationResponse {
        generation: Some(reference),
        parents,
    })
}

fn commit_message<A, O>(value: TransactionCommit<A, O>) -> wire::MutationResponse {
    match value {
        TransactionCommit::Committed(generation) => mutation(
            wire::MutationStatus::Committed,
            Some(generation_ref(&generation)),
            None,
        ),
        TransactionCommit::AlreadyCommitted(generation) => mutation(
            wire::MutationStatus::AlreadyCommitted,
            Some(generation_ref(&generation)),
            None,
        ),
        TransactionCommit::Conflict { actual } => mutation(
            wire::MutationStatus::Conflict,
            None,
            Some(generation_ref(&actual)),
        ),
        TransactionCommit::Fenced => mutation(wire::MutationStatus::Fenced, None, None),
        TransactionCommit::IdempotencyConflict => {
            mutation(wire::MutationStatus::IdempotencyConflict, None, None)
        }
    }
}

fn join_message<A, O>(value: JoinOutcome<A, O>) -> wire::JoinResponse {
    match value {
        JoinOutcome::Applied(generation) => {
            join_response(wire::JoinStatus::Applied, Some(&generation.generation))
        }
        JoinOutcome::AlreadyApplied(generation) => join_response(
            wire::JoinStatus::AlreadyApplied,
            Some(&generation.generation),
        ),
        JoinOutcome::NoChanges(generation) => {
            join_response(wire::JoinStatus::NoChanges, Some(&generation))
        }
        JoinOutcome::StaleTarget(actual) => {
            join_response(wire::JoinStatus::StaleTarget, Some(&actual))
        }
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => wire::JoinResponse {
            status: wire::JoinStatus::Conflicted as i32,
            generation: None,
            conflicts: conflicts.into_iter().map(merge_conflict).collect(),
            truncated,
        },
        JoinOutcome::Fenced => join_response::<A, O>(wire::JoinStatus::Fenced, None),
        JoinOutcome::IdempotencyConflict => {
            join_response::<A, O>(wire::JoinStatus::IdempotencyConflict, None)
        }
    }
}

fn join_response<A, O>(
    status: wire::JoinStatus,
    generation: Option<&Generation<A, O>>,
) -> wire::JoinResponse {
    wire::JoinResponse {
        status: status as i32,
        generation: generation.map(generation_ref),
        conflicts: Vec::new(),
        truncated: false,
    }
}
