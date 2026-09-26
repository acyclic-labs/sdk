//! Parent-authenticated Filesystem implementation of the neutral project boundary.

use super::{FilesystemHost, ParentMergePlan, ParentProjectController};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, ConflictSide, FileId, JoinOutcome, MergeConflict,
    kernel::{LogicalName, NameEncoding},
};
use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ConversationMessage, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::ResourceRevision,
    merge::{
        ProjectConflict, ProjectConflictSelection, ProjectConflictSide, ProjectJoinOutcome,
        ProjectJoinPlan, ProjectWorkspaceProvider,
    },
    resources::{GenerationRef, ProviderRef},
};
use futures::future::BoxFuture;
use std::{collections::BTreeMap, sync::Arc};

/// One signed parent binding. Its child plans retain the same authenticated
/// parent authority and exact inspected Filesystem generations.
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
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
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

    fn prepare_project_merge<'a>(
        &'a self,
        scope: &'a Scope,
        parent: &'a Reducer,
        child: &'a Authority,
        child_project: &'a VolumeRef,
    ) -> BoxFuture<'a, Result<Box<dyn ProjectJoinPlan>>> {
        Box::pin(async move {
            self.require_scope(scope)?;
            if parent.authority() != &self.parent
                || parent.conversation().and_then(|state| state.agent) != scope.agent()
            {
                return Err(Error::Unauthorized(
                    "project merge requires the current parent conversation".into(),
                ));
            }
            if child.kind != acyclic_harness::core::AggregateKind::Conversation {
                return Err(Error::Invalid(
                    "project join child is not a conversation".into(),
                ));
            }
            child.stream_path()?;
            // The reducer is supplied for this call so a binding created before
            // publication can authorize a later fork. Never consult or retain a
            // fork registry captured when this provider was constructed.
            let seed = parent.fork(child).ok_or_else(|| {
                Error::Unauthorized("project join child has no published parent fork".into())
            })?;
            seed.validate()?;
            if !seed.resources.iter().any(|resource| {
                matches!(
                    (&resource.source, &resource.revision),
                    (ResourceRevision::Project { volume: source, .. },
                     ResourceRevision::Project { volume: forked, .. })
                        if source == &self.project && forked == child_project
                )
            }) {
                return Err(Error::Unauthorized(
                    "project join is outside the published fork".into(),
                ));
            }
            let plan = self
                .controller()
                .prepare_project_merge(child_project)
                .await?;
            let source = self.host.generation_ref_id(plan.source_head())?;
            let target = self.host.generation_ref_id(plan.target_head())?;
            Ok(Box::new(FilesystemProjectJoinPlan {
                binding: self.clone(),
                plan,
                child: child.clone(),
                source,
                target,
            }) as Box<dyn ProjectJoinPlan>)
        })
    }
}

struct FilesystemProjectJoinPlan<A, O> {
    binding: FilesystemProjectWorkspaces<A, O>,
    plan: ParentMergePlan<A, O>,
    // Publication was authenticated against the current parent reducer during
    // preparation. The plan keeps only the child identity; its Filesystem
    // generations remain the independent CAS boundary for application.
    child: Authority,
    source: GenerationRef,
    target: GenerationRef,
}

impl<A, O> ProjectJoinPlan for FilesystemProjectJoinPlan<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
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
            if child.kind != acyclic_harness::core::AggregateKind::Conversation {
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
            if notice.kind != acyclic_harness::conversation::MessageKind::Merge {
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

fn encode_conflict(conflict: &MergeConflict) -> Vec<u8> {
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

fn decode_conflict(key: &[u8]) -> Result<MergeConflict> {
    let (tag, rest) = key
        .split_first()
        .ok_or_else(|| Error::Invalid("empty conflict key".into()))?;
    let id_bytes: [u8; 16] = rest
        .get(..16)
        .ok_or_else(|| Error::Invalid("short conflict key".into()))?
        .try_into()
        .map_err(|_| Error::Invalid("invalid conflict identity".into()))?;
    let id = FileId::from_bytes(id_bytes);
    match (*tag, rest.get(16..)) {
        (1, Some([])) => Ok(MergeConflict::File(id)),
        (2, Some([encoding, name @ ..])) => {
            let encoding = match *encoding {
                1 => NameEncoding::Utf8,
                2 => NameEncoding::PosixBytes,
                3 => NameEncoding::WindowsUtf16Le,
                _ => return Err(Error::Invalid("unknown conflict name encoding".into())),
            };
            let name = LogicalName::new(encoding, name.to_vec(), 4_096)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            Ok(MergeConflict::Binding {
                directory_id: id,
                name,
            })
        }
        _ => Err(Error::Invalid("invalid conflict key".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
