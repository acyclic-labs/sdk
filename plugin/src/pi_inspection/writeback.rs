use super::*;

pub(crate) async fn prepare_writeback(
    root: &PiWritebackRoot,
    generations: &PiWritebackGenerations<'_>,
    state: &acyclic_fs::LocalCoreStateStore,
    options: &acyclic_fs::MaterializeOptions,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::MaterializationJournal> {
    root.verify(native_work).await?;
    let from = &generations.from;
    let candidate = &generations.candidate;
    let paths = generations.paths;
    if paths.is_empty()
        || paths.len() > MAX_PRIVATE_DIRECTORY_PAGE
        || generations.original_snapshot.workspace_id != root.workspace
        || from.workspace_id() != candidate.workspace_id()
    {
        return Err(selected_unauthorized(
            "writeback candidate is not the original selected root workspace",
        ));
    }
    let operation = acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes());
    let guard_work = native_work.clone();
    let guard_cause = std::sync::Mutex::new(None);
    let prepared = acyclic_fs::materializer::publish_native_transition_inner(
        from,
        candidate,
        state,
        acyclic_fs::NativeWorkspacePublication {
            root: &root.path,
            source_root_identity: root.identity,
            operation_directory: &root.operation_directory,
            operation_id: operation,
            from: from.id(),
            to: candidate.id(),
            excluded_names: &[".git"],
            options,
            budget: native_work.budget(),
            cancellation: native_work.cancellation(),
        },
        native_work.clone(),
        true,
        &mut || async {
            root.verify(&guard_work)
                .await
                .map_err(|error| native_guard_cause(error, &guard_cause))
        },
    )
    .await;
    let journal = match prepared {
        Ok(journal) => journal,
        Err(source) => {
            return Err(guard_cause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| Error::NativeWorkspacePublication {
                    source,
                    custody: None,
                }));
        }
    };
    if journal.root_identity != Some(root.identity.to_bytes())
        || journal.phase != acyclic_fs::MaterializationPhase::Prepared
    {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "writeback preparation has another original root or is not ready for approval",
        });
    }
    require_selected_edits(&journal.plan.edits, paths)?;
    root.verify(native_work).await?;
    Ok(journal)
}

/// Publishes only the exact user-confirmed preparation.
pub(crate) async fn publish_writeback(
    root: &PiWritebackRoot,
    generations: &PiWritebackGenerations<'_>,
    journal: &acyclic_fs::MaterializationJournal,
    options: &acyclic_fs::MaterializeOptions,
    state: &acyclic_fs::LocalCoreStateStore,
    confirmed_digest: &str,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::MaterializationJournal> {
    let operation = acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes());
    root.verify(native_work).await?;
    let from = &generations.from;
    let candidate = &generations.candidate;
    let digest = crate::pi_root_provider::digest_pi_value(
        &writeback_digest_value(root, generations, journal),
        native_work.budget().bytes_encoded,
        native_work,
    )?;
    if generations.original_snapshot.workspace_id != root.workspace || digest.0 != confirmed_digest
    {
        return Err(selected_unauthorized(
            "writeback lacks exact original-root user approval",
        ));
    }
    drop(digest);
    let work = native_work.clone();
    let guard_work = work.clone();
    let guard_cause = std::sync::Mutex::new(None);
    let receipt = acyclic_fs::publish_native_generation_transition(
        from,
        candidate,
        state,
        acyclic_fs::NativeWorkspacePublication {
            root: &root.path,
            source_root_identity: root.identity,
            operation_directory: &root.operation_directory,
            operation_id: operation,
            from: from.id(),
            to: candidate.id(),
            excluded_names: &[".git"],
            options,
            budget: native_work.budget(),
            cancellation: native_work.cancellation(),
        },
        work,
        || async {
            root.verify(&guard_work)
                .await
                .map_err(|error| native_guard_cause(error, &guard_cause))?;
            Ok(())
        },
    )
    .await;
    let value = match receipt {
        Ok(receipt) => receipt.value,
        Err((source, _producer_work)) => {
            return Err(guard_cause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| match source {
                    acyclic_fs::NativeWorkspacePublicationError::Native(cause) => {
                        Error::NativeWork {
                            cause,
                            custody: None,
                        }
                    }
                    source => Error::NativeWorkspacePublication {
                        source,
                        custody: None,
                    },
                }));
        }
    };
    Ok(value)
}

/// A replay can acknowledge only the SDK's authenticated completed original journal, with the same exact user digest.
pub(crate) fn validate_applied_writeback(
    root: &PiWritebackRoot,
    generations: &PiWritebackGenerations<'_>,
    journal: &acyclic_fs::MaterializationJournal,
    confirmed_digest: &str,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if journal.phase != acyclic_fs::MaterializationPhase::Applied
        || journal.root_identity != Some(root.identity.to_bytes())
        || journal.plan.operation_id
            != acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes())
        || !matches!(journal.plan.intent, acyclic_fs::MaterializationIntent::GenerationTransition { from, to }
            if from == generations.from.id() && to == generations.candidate.id())
        || generations.original_snapshot.workspace_id != root.workspace
    {
        return Err(selected_unauthorized(
            "completed writeback differs from its original root and generations",
        ));
    }
    require_selected_edits(&journal.plan.edits, &generations.paths)?;
    let digest = crate::pi_root_provider::digest_pi_value(
        &writeback_digest_value(root, generations, journal),
        native_work.budget().bytes_encoded,
        native_work,
    )?;
    if digest.0 != confirmed_digest {
        return Err(selected_unauthorized(
            "completed writeback changed its exact user approval",
        ));
    }
    Ok(())
}

fn native_guard_cause(
    error: Error,
    cause: &std::sync::Mutex<Option<Error>>,
) -> acyclic_fs::NativeWorkspacePublicationError {
    let mut retained = cause
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if retained.is_none() {
        *retained = Some(error);
    }
    acyclic_fs::NativeTreeMaterializationError::Io(std::io::Error::from(
        std::io::ErrorKind::PermissionDenied,
    ))
    .into()
}

pub(crate) fn writeback_digest_value<'a>(
    root: &'a PiWritebackRoot,
    generations: &'a PiWritebackGenerations<'_>,
    journal: &'a acyclic_fs::MaterializationJournal,
) -> impl Serialize + 'a {
    (
        root.task,
        &root.volume,
        root.workspace,
        root.identity.to_bytes(),
        &root.path,
        &root.operation_directory,
        acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes()),
        generations.from.id(),
        generations.candidate.id(),
        generations.paths,
        generations.original_snapshot,
        generations.imported,
        &journal.plan,
        journal.root_identity,
        journal.preparation_identity,
        &journal.preimages,
    )
}

fn require_selected_edits(
    edits: &[acyclic_fs::MaterializationEdit],
    paths: &[String],
) -> Result<()> {
    let selected = |path: &str| {
        paths.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    };
    for edit in edits {
        let allowed = match edit {
            acyclic_fs::MaterializationEdit::Install { path, .. }
            | acyclic_fs::MaterializationEdit::Remove { path }
            | acyclic_fs::MaterializationEdit::SetMetadata { path, .. } => selected(path),
            acyclic_fs::MaterializationEdit::Rename { from, to } => selected(from) && selected(to),
            _ => false,
        };
        if !allowed {
            return Err(selected_unauthorized(
                "native candidate changes an unselected writeback path",
            ));
        }
    }
    Ok(())
}
