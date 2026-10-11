//! Parent-authenticated Filesystem implementation of the neutral project boundary.

use super::{FilesystemHost, ParentMergePlan, ParentProjectController};
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ConversationMessage, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    merge::{
        ProjectConflict, ProjectConflictSelection, ProjectConflictSide, ProjectJoinOutcome,
        ProjectJoinPlan, ProjectWorkspaceProvider,
    },
    resources::{GenerationRef, ProviderRef},
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, ConflictSide, FileId, JoinOutcome, MergeConflict,
    kernel::{LogicalName, NameEncoding},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::{collections::BTreeMap, sync::Arc};

/// One signed parent binding.
pub struct FilesystemProjectWorkspaces<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    parent: Authority,
    project: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
}

impl<A, O> Clone for FilesystemProjectWorkspaces<A, O> {
    fn clone(&self) -> Self {
        Self {
            host: Arc::clone(&self.host),
            parent: self.parent.clone(),
            project: self.project.clone(),
            verifier: self.verifier.clone(),
            scope: self.scope.clone(),
        }
    }
}

impl<A, O> FilesystemProjectWorkspaces<A, O> {
    /// Admits only the bound conversation's agent and project volume.
    pub fn new(
        host: &Arc<FilesystemHost<A, O>>,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        project: VolumeRef,
    ) -> Result<Self> {
        let controller =
            ParentProjectController::new(host.as_ref(), parent, verifier, scope, project)?;
        Ok(Self {
            host: Arc::clone(host),
            parent: controller.parent,
            project: controller.project,
            verifier: controller.verifier,
            scope: controller.scope,
        })
    }

    fn controller(&self) -> ParentProjectController<'_, A, O> {
        ParentProjectController {
            host: &self.host,
            parent: self.parent.clone(),
            project: self.project.clone(),
            verifier: self.verifier.clone(),
            scope: self.scope.clone(),
        }
    }

    fn require_scope(&self, scope: &Scope) -> Result<()> {
        self.verifier.verify(scope)?;
        if scope != &self.scope {
            return Err(Error::Unauthorized(
                "project operation belongs to another parent scope".into(),
            ));
        }
        Ok(())
    }
}

impl<A, O> ProjectWorkspaceProvider for FilesystemProjectWorkspaces<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
    }

    fn project(&self) -> &VolumeRef {
        &self.project
    }

    fn fork_project<'a>(
        &'a self,
        scope: &'a Scope,
        source_generation: &'a GenerationRef,
        child: &'a VolumeRef,
        key: &'a IdempotencyKey,
    ) -> BoxFuture<'a, Result<GenerationRef>> {
        Box::pin(async move {
            self.require_scope(scope)?;
            Ok(self
                .controller()
                .fork_project(source_generation, child, key)
                .await?
                .generation)
        })
    }

    fn prepare_project_import<'a>(
        &'a self,
        scope: &'a Scope,
        destination: &'a Reducer,
        source: &'a Authority,
        source_project: &'a VolumeRef,
        source_generation: &'a GenerationRef,
    ) -> BoxFuture<'a, Result<Box<dyn ProjectJoinPlan>>> {
        Box::pin(async move {
            self.require_scope(scope)?;
            if destination.authority() != &self.parent
                || destination.conversation().and_then(|state| state.agent) != scope.agent()
                || source.kind != crate::core::AggregateKind::Conversation
            {
                return Err(Error::Unauthorized(
                    "import is outside the bound destination conversation".into(),
                ));
            }
            source.stream_path()?;
            let plan = self
                .controller()
                .prepare_project_import(source_project, source_generation)
                .await?;
            let source_generation = self.host.generation_ref_id(plan.source_head())?;
            let target = self.host.generation_ref_id(plan.target_head())?;
            Ok(Box::new(FilesystemProjectJoinPlan {
                binding: self.clone(),
                plan,
                child: source.clone(),
                source: source_generation,
                target,
            }) as Box<dyn ProjectJoinPlan>)
        })
    }
}

struct FilesystemProjectJoinPlan<A, O> {
    binding: FilesystemProjectWorkspaces<A, O>,
    plan: ParentMergePlan<A, O>,
    child: Authority,
    source: GenerationRef,
    target: GenerationRef,
}

impl<A, O> ProjectJoinPlan for FilesystemProjectJoinPlan<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn source_generation(&self) -> &GenerationRef {
        &self.source
    }

    fn expected_target_generation(&self) -> &GenerationRef {
        &self.target
    }

    fn apply<'a>(
        &'a self,
        scope: &'a Scope,
        operation_id: OperationId,
        child: &'a Authority,
        notice: &'a ConversationMessage,
        selections: &'a [ProjectConflictSelection],
    ) -> BoxFuture<'a, Result<ProjectJoinOutcome>> {
        Box::pin(async move {
            self.binding.require_scope(scope)?;
            if child.kind != crate::core::AggregateKind::Conversation {
                return Err(Error::Invalid(
                    "project join child is not a conversation".into(),
                ));
            }
            child.stream_path()?;
            if &self.child != child {
                return Err(Error::Unauthorized(
                    "project join child is not the inspected fork".into(),
                ));
            }
            notice.validate()?;
            if notice.kind != crate::conversation::MessageKind::Merge {
                return Err(Error::Invalid("project join notice is not a merge".into()));
            }
            let controller = self.binding.controller();
            let outcome = if selections.is_empty() {
                controller
                    .apply_project_merge(&self.plan, operation_id)
                    .await?
            } else {
                let mut sides = BTreeMap::new();
                for selection in selections {
                    selection.conflict.validate()?;
                    if selection.conflict.provider != *self.binding.provider() {
                        return Err(Error::Unauthorized(
                            "conflict belongs to another provider".into(),
                        ));
                    }
                    let conflict = decode_conflict(&selection.conflict.key)?;
                    let side = match selection.side {
                        ProjectConflictSide::Base => ConflictSide::Base,
                        ProjectConflictSide::Target => ConflictSide::Ours,
                        ProjectConflictSide::Source => ConflictSide::Theirs,
                    };
                    if sides.insert(conflict, side).is_some() {
                        return Err(Error::Invalid("conflict side was selected twice".into()));
                    }
                }
                controller
                    .apply_project_merge_sides(&self.plan, operation_id, sides)
                    .await?
            };
            match &outcome {
                JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => {
                    let receipt = controller.merge_receipt(
                        &self.plan,
                        &outcome,
                        child.clone(),
                        operation_id,
                        notice.clone(),
                    )?;
                    Ok(if matches!(&outcome, JoinOutcome::AlreadyApplied(_)) {
                        ProjectJoinOutcome::AlreadyApplied(receipt)
                    } else {
                        ProjectJoinOutcome::Applied(receipt)
                    })
                }
                JoinOutcome::NoChanges(generation) => Ok(ProjectJoinOutcome::NoChanges(
                    self.binding.host.generation_ref(generation)?,
                )),
                JoinOutcome::StaleTarget(generation) => Ok(ProjectJoinOutcome::StaleTarget(
                    self.binding.host.generation_ref(generation)?,
                )),
                JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                } => Ok(ProjectJoinOutcome::Conflicted {
                    conflicts: conflicts
                        .iter()
                        .map(|conflict| ProjectConflict {
                            provider: self.binding.provider().clone(),
                            key: encode_conflict(conflict),
                        })
                        .collect(),
                    truncated: *truncated,
                }),
                JoinOutcome::Fenced => Ok(ProjectJoinOutcome::Fenced),
                JoinOutcome::IdempotencyConflict => Ok(ProjectJoinOutcome::IdempotencyConflict),
            }
        })
    }
}

pub(super) fn encode_conflict(conflict: &MergeConflict) -> Vec<u8> {
    match conflict {
        MergeConflict::File(id) => {
            let mut key = vec![1];
            key.extend_from_slice(&id.into_bytes());
            key
        }
        MergeConflict::Binding { directory_id, name } => {
            let mut key = vec![2];
            key.extend_from_slice(&directory_id.into_bytes());
            key.push(match name.encoding() {
                NameEncoding::Utf8 => 1,
                NameEncoding::PosixBytes => 2,
                NameEncoding::WindowsUtf16Le => 3,
            });
            key.extend_from_slice(name.as_bytes());
            key
        }
    }
}

pub(super) fn decode_conflict(key: &[u8]) -> Result<MergeConflict> {
    decode_conflict_inner(key, None).map(|(conflict, _)| conflict)
}

/// Decodes the same canonical selected conflict without an uncharged name copy.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn decode_conflict_with_native_work(
    key: &[u8], work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(MergeConflict, acyclic_fs::NativeAllocation)> {
    let (conflict, custody) = decode_conflict_inner(key, Some(work))?;
    Ok((conflict, custody.expect("native decoder retains its original custody")))
}

#[cfg(not(target_arch = "wasm32"))]
type ConflictDecodeCustody = Option<acyclic_fs::NativeAllocation>;
#[cfg(target_arch = "wasm32")]
type ConflictDecodeCustody = Option<()>;

fn decode_conflict_inner(
    key: &[u8],
    #[cfg(not(target_arch = "wasm32"))] work: Option<&acyclic_fs::NativeMaterializationWork>,
    #[cfg(target_arch = "wasm32")] _work: Option<&()>,
) -> Result<(MergeConflict, ConflictDecodeCustody)> {
    let invalid = |message: &'static str| {
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_some() {
            return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: message };
        }
        Error::Invalid(message.into())
    };
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = work.map(|work| work.retain_owned(std::mem::size_of::<MergeConflict>() as u64))
        .transpose().map_err(Error::from)?;
    #[cfg(target_arch = "wasm32")]
    let custody = None;
    let (tag, rest) = key.split_first()
        .ok_or_else(|| invalid("empty conflict key"))?;
    let id_bytes: [u8; 16] = rest
        .get(..16)
        .ok_or_else(|| invalid("short conflict key"))?
        .try_into()
        .map_err(|_| invalid("invalid conflict identity"))?;
    let id = FileId::from_bytes(id_bytes);
    match (*tag, rest.get(16..)) {
        (1, Some([])) => Ok((MergeConflict::File(id), custody)),
        (2, Some([encoding, name @ ..])) => {
            let encoding = match *encoding {
                1 => NameEncoding::Utf8,
                2 => NameEncoding::PosixBytes,
                3 => NameEncoding::WindowsUtf16Le,
                _ => return Err(invalid("unknown conflict name encoding")),
            };
            #[cfg(not(target_arch = "wasm32"))]
            let name = if let Some(work) = work {
                work.charge(acyclic_fs::WorkCounters { items_examined: 1,
                    ..acyclic_fs::WorkCounters::default() })
                    .map_err(Error::from)?;
                let (copied, mut allocation) = work.copy_bytes(name)
                    .map_err(Error::from)?;
                let name = LogicalName::new(encoding, copied, u32::MAX).map_err(|error|
                    Error::NativeFilesystem {
                        source: acyclic_fs::FsError::Path(acyclic_fs::kernel::PathLookupError::NamespacePath(
                            acyclic_fs::kernel::NamespacePathError::Name(error))),
                        custody: None,
                    })?;
                custody.as_mut().expect("native header custody").absorb(&mut allocation)
                    .map_err(Error::from)?;
                name
            } else {
                LogicalName::new(encoding, name.to_vec(), u32::MAX)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            };
            #[cfg(target_arch = "wasm32")]
            let name = LogicalName::new(encoding, name.to_vec(), u32::MAX)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            Ok((MergeConflict::Binding { directory_id: id, name }, custody))
        }
        _ => Err(invalid("invalid conflict key")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_conflict_decode_retains_real_name_custody_and_denies_before_copy() -> Result<()> {
        let conflict = MergeConflict::Binding {
            directory_id: FileId::from_bytes([9; 16]),
            name: LogicalName::new(NameEncoding::PosixBytes, vec![0xff, b'x'], 255)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let key = encode_conflict(&conflict);
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkCounters {
            allocation_operations: 8, peak_allocation_bytes: 65_536,
            bytes_copied: 2048, items_examined: 64, ..acyclic_fs::WorkCounters::default()
        };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("finite actual decoder context");
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let (decoded, custody) = decode_conflict_with_native_work(&key, &work)?;
        assert_eq!(decoded, conflict);
        assert_eq!(work.receipt().bytes_copied - before.bytes_copied, 2);
        assert!(work.live_allocation_bytes() > live);
        drop(decoded);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), live);
        let denied = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkCounters { bytes_copied: 0, ..budget }, &cancellation)
            .expect("zero-copy allowance is valid");
        let before = denied.receipt();
        assert!(decode_conflict_with_native_work(&key, &denied).is_err());
        assert_eq!(denied.receipt().bytes_copied, before.bytes_copied);
        let file_key = encode_conflict(&MergeConflict::File(FileId::from_bytes([7; 16])));
        let (file, file_custody) = decode_conflict_with_native_work(&file_key, &denied)?;
        assert_eq!(file, MergeConflict::File(FileId::from_bytes([7; 16])));
        drop(file);
        drop(file_custody);
        Ok(())
    }

    #[test]
    fn conflict_keys_round_trip_without_losing_native_name_bytes() -> Result<()> {
        let file = MergeConflict::File(FileId::from_bytes([7; 16]));
        assert_eq!(decode_conflict(&encode_conflict(&file))?, file);
        let name = LogicalName::new(NameEncoding::PosixBytes, vec![0xff, b'x'], 255)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let binding = MergeConflict::Binding {
            directory_id: FileId::from_bytes([9; 16]),
            name,
        };
        assert_eq!(decode_conflict(&encode_conflict(&binding))?, binding);
        assert!(decode_conflict(&[2, 1]).is_err());
        Ok(())
    }
}
