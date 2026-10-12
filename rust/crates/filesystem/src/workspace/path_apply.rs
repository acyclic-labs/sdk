use super::*;

/// One exact path rejected by a three-way path application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspacePathConflict {
    /// Portable path relative to the workspace root.
    pub path: String,
    /// Semantic category suitable for driver or UI selection.
    pub kind: ConflictKind,
}

/// Terminal outcome of applying a generation delta to selected paths.
pub enum WorkspacePathApply<A, O> {
    /// A new generation became durable.
    Applied(Generation<A, O>),
    /// The exact idempotent application was already durable.
    AlreadyApplied(Generation<A, O>),
    /// The requested delta was already represented by the live workspace.
    NoChanges(Generation<A, O>),
    /// Independent live changes overlap the requested generation delta.
    Conflicted(Vec<WorkspacePathConflict>),
    /// The live workspace advanced after the caller's observation.
    Stale(Generation<A, O>),
    /// Workspace writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously used for different inputs.
    IdempotencyConflict,
}


impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    async fn same_path_state(
        &self,
        left: Option<crate::kernel::FileRecord>,
        right: Option<crate::kernel::FileRecord>,
    ) -> Result<bool, WorkspaceError> {
        match (left, right) {
            (Some(left), Some(right)) if left != right => {
                self.volume
                    .fs
                    .same_file_state(&self.volume, left, right)
                    .await
            }
            (left, right) => Ok(left == right),
        }
    }

    /// Applies an exact three-way path delta under one writer permit.
    pub async fn apply_paths_from(
        &self,
        base: Option<&Generation<A, O>>,
        source: Option<&Generation<A, O>>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<WorkspacePathApply<A, O>, WorkspaceError> {
        if base.is_some_and(|base| !self.volume.fs.same_deployment(&base.workspace.volume.fs))
            || source
                .is_some_and(|source| !self.volume.fs.same_deployment(&source.workspace.volume.fs))
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let current = self.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?.value;
        if current.id != if_current {
            return Ok(WorkspacePathApply::Stale(current));
        }
        let mut base_checkout = match base {
            Some(base) => Some(
                base.workspace
                    .checkout(
                        GenerationSelector::Exact(base.id),
                        CheckoutMode::read_only_pinned(),
                        WorkBudget::UNBOUNDED, &CancellationToken::new(),
                    )
                    .await.map_err(|failure| failure.error)?.value,
            ),
            None => None,
        };
        let mut source_checkout = match source {
            Some(source) => Some(
                source
                    .workspace
                    .checkout(
                        GenerationSelector::Exact(source.id),
                        CheckoutMode::read_only_pinned(),
                        WorkBudget::UNBOUNDED, &CancellationToken::new(),
                    )
                    .await.map_err(|failure| failure.error)?.value,
            ),
            None => None,
        };
        let mut transaction = self.begin_transaction(idempotency_key, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error)?.value;
        let config = self.volume.config;
        let cancellation = crate::CancellationToken::new();
        let parsed = paths
            .iter()
            .map(|path| {
                let relative = path.trim_start_matches('/');
                let absolute = format!("/{relative}");
                customer_path(&absolute, config).map(|path| (relative, path))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let lookup_paths = parsed
            .iter()
            .map(|(_, path)| path.clone())
            .collect::<Vec<_>>();
        let base_records = match &mut base_checkout {
            Some(checkout) => checkout
                .lookup_batch_no_follow(&lookup_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect(),
            None => vec![None; parsed.len()],
        };
        let source_records = match &mut source_checkout {
            Some(checkout) => checkout
                .lookup_batch_no_follow(&lookup_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect(),
            None => vec![None; parsed.len()],
        };
        let changed_paths = parsed
            .iter()
            .zip(base_records.iter().zip(&source_records))
            .filter(|(_, (base, source))| base != source)
            .map(|((_, path), _)| path.clone())
            .collect::<Vec<_>>();
        let current_records = if changed_paths.is_empty() {
            Vec::new()
        } else {
            transaction
                .checkout
                .lookup_batch_no_follow(&changed_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect()
        };
        let mut current_records = current_records.into_iter();
        let mut operations = Vec::with_capacity(paths.len());
        let mut conflicts = Vec::new();
        for (((relative, path), base_record), source_record) in
            parsed.into_iter().zip(base_records).zip(source_records)
        {
            if base_record == source_record {
                continue;
            }
            let current_record = current_records
                .next()
                .ok_or_else(|| WorkspaceError::Engine("missing batch lookup result".to_owned()))?;
            if self.same_path_state(current_record, source_record).await? {
                continue;
            }
            if !self.same_path_state(current_record, base_record).await? {
                conflicts.push(WorkspacePathConflict {
                    path: relative.to_owned(),
                    kind: path_conflict_kind(base_record, current_record, source_record),
                });
                continue;
            }
            match source_record {
                Some(record) => {
                    if record.kind != FileKind::Directory
                        && let Some((parent, _)) = relative.rsplit_once('/')
                    {
                        transaction.create_dir_all(&format!("/{parent}"), WorkBudget::UNBOUNDED, &cancellation).await.map_err(|failure| failure.error)?;
                    }
                    operations.push(crate::kernel::Mutation::Restore { path, record });
                }
                None => {
                    if let Some(record) = current_record {
                        operations.push(crate::kernel::Mutation::Remove {
                            path,
                            expected_file_id: MetadataField::Value(record.file_id),
                        });
                    }
                }
            }
        }
        if !conflicts.is_empty() {
            return Ok(WorkspacePathApply::Conflicted(conflicts));
        }
        if operations.is_empty() {
            return Ok(WorkspacePathApply::NoChanges(current));
        }
        // Parents are restored before their children, and children removed
        // before their parents.
        operations.sort_by_key(|operation| match operation {
            crate::kernel::Mutation::Remove { path, .. } => {
                (1, std::cmp::Reverse(path.components().len()))
            }
            _ => (0, std::cmp::Reverse(0)),
        });
        transaction
            .checkout
            .mutate(operations, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(match transaction.commit(None, permit, WorkBudget::UNBOUNDED, &cancellation, |_| async { Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await.map_err(|failure| failure.error)?.value {
            TransactionCommit::Committed(generation) => WorkspacePathApply::Applied(generation),
            TransactionCommit::AlreadyCommitted(generation) => {
                WorkspacePathApply::AlreadyApplied(generation)
            }
            TransactionCommit::Conflict { actual } => WorkspacePathApply::Stale(actual),
            TransactionCommit::Fenced => WorkspacePathApply::Fenced,
            TransactionCommit::IdempotencyConflict => WorkspacePathApply::IdempotencyConflict,
        })
    }

}

fn path_conflict_kind(
    base: Option<crate::kernel::FileRecord>,
    current: Option<crate::kernel::FileRecord>,
    source: Option<crate::kernel::FileRecord>,
) -> ConflictKind {
    let (Some(base), Some(current), Some(source)) = (base, current, source) else {
        return ConflictKind::Binding;
    };
    if base.kind != current.kind
        || base.kind != source.kind
        || base.file_id != current.file_id
        || base.file_id != source.file_id
    {
        return ConflictKind::Binding;
    }
    if base.link_count != current.link_count || base.link_count != source.link_count {
        return ConflictKind::HardLink;
    }
    if base.metadata != current.metadata || base.metadata != source.metadata {
        return ConflictKind::Metadata;
    }
    match base.kind {
        FileKind::Regular => ConflictKind::Binary,
        FileKind::Directory => ConflictKind::Directory,
        FileKind::SymbolicLink => ConflictKind::SymbolicLink,
        FileKind::Fifo
        | FileKind::Socket
        | FileKind::CharacterDevice
        | FileKind::BlockDevice
        | FileKind::ReparsePoint
        | FileKind::MountBoundary => ConflictKind::Special,
    }
}

