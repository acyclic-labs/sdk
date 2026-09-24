//! Workspace forks, diffs, merges, and discards for Harness speculation.

use super::{FilesystemHost, filesystem_key, map_error, workspace_ref};
use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ForkOptions, JoinOutcome, WorkspaceDelete,
    WorkspaceError,
};
use acyclic_harness::{
    Error, IdempotencyKey, Result,
    resources::{GenerationRef, WorkspaceRef},
    speculation::{AttemptWorkspace, DiffSummary, MAXIMUM_SUMMARY_PATHS, SpeculationWorkspaces},
};
use futures::future::BoxFuture;

impl<A, O> SpeculationWorkspaces for FilesystemHost<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn fork<'a>(
        &'a self,
        source: &'a WorkspaceRef,
        generation: Option<&'a GenerationRef>,
        key: &'a IdempotencyKey,
    ) -> BoxFuture<'a, Result<AttemptWorkspace>> {
        Box::pin(async move {
            let workspace = self.open(source).await?;
            let at = match generation {
                Some(reference) => self.generation(&workspace, reference).await?,
                None => workspace.head().await.map_err(map_error)?,
            };
            let name = format!(
                "speculation-{}",
                blake3::hash(key.as_str().as_bytes())
                    .to_hex()
                    .get(..32)
                    .unwrap_or_default()
            );
            let fork = workspace
                .fork(&name, ForkOptions::from_generation(at, filesystem_key(key)))
                .await
                .map_err(map_error)?;
            let base = fork.head().await.map_err(map_error)?;
            Ok(AttemptWorkspace {
                workspace: workspace_ref(self.provider.clone(), &name)?,
                base: self.generation_ref(&base)?,
            })
        })
    }

    fn head<'a>(&'a self, workspace: &'a WorkspaceRef) -> BoxFuture<'a, Result<GenerationRef>> {
        Box::pin(async move { Ok(self.resolve(workspace).await?.generation) })
    }

    fn diff<'a>(
        &'a self,
        attempt: &'a AttemptWorkspace,
        generation: &'a GenerationRef,
    ) -> BoxFuture<'a, Result<DiffSummary>> {
        Box::pin(async move {
            let workspace = self.open(&attempt.workspace).await?;
            let from = self.generation(&workspace, &attempt.base).await?;
            let to = self.generation(&workspace, generation).await?;
            let bound = u32::try_from(MAXIMUM_SUMMARY_PATHS).unwrap_or(u32::MAX);
            let changes = match workspace.diff(&from, &to, bound).await {
                Ok(changes) => changes.changed_paths(bound).await,
                Err(error) => Err(error),
            };
            let changed = match changes {
                Ok(changed) => changed,
                Err(WorkspaceError::JoinLimit | WorkspaceError::ChangedPathLimit) => {
                    return Ok(DiffSummary {
                        changed_paths: Vec::new(),
                        truncated: true,
                    });
                }
                Err(error) => return Err(map_error(error)),
            };
            let mut changed_paths = changed
                .iter()
                .map(|change| {
                    let mut path = String::new();
                    for component in change.path.components() {
                        path.push('/');
                        match component.unicode_text() {
                            Some(text) => path.push_str(&text),
                            None => path.push_str(&String::from_utf8_lossy(component.as_bytes())),
                        }
                    }
                    path
                })
                // macOS stores unsendable extended attributes as `._name` files;
                // they are host metadata, not agent work, and would skew judging.
                .filter(|path| {
                    !path
                        .rsplit('/')
                        .next()
                        .is_some_and(|name| name.starts_with("._"))
                })
                .collect::<Vec<_>>();
            changed_paths.dedup();
            Ok(DiffSummary {
                changed_paths,
                truncated: false,
            })
        })
    }

    fn merge<'a>(
        &'a self,
        attempt: &'a AttemptWorkspace,
        generation: &'a GenerationRef,
        target: &'a WorkspaceRef,
        key: &'a IdempotencyKey,
    ) -> BoxFuture<'a, Result<GenerationRef>> {
        Box::pin(async move {
            // Both endpoints are reopened here: a handle taken before another
            // merge advanced the target would plan against a stale head.
            let source = self.open(&attempt.workspace).await?;
            let destination = self.open(target).await?;
            let pinned = self.generation(&source, generation).await?;
            let target_head = destination.head().await.map_err(map_error)?;
            let target_id = target_head.id();
            let plan = source
                .join_into(&destination)
                .plan_pinned(pinned, target_head)
                .await
                .map_err(map_error)?;
            // A retry after a merge committed sees an advanced target, so the
            // retry identity is bound to the exact target head being merged into.
            let retry = IdempotencyKey::new(format!(
                "{}:{}",
                blake3::hash(key.as_str().as_bytes()).to_hex(),
                blake3::hash(&target_id.digest().into_bytes()).to_hex()
            ))?;
            match plan
                .apply(ApplyOptions {
                    if_target: target_id,
                    idempotency_key: filesystem_key(&retry),
                })
                .await
                .map_err(map_error)?
            {
                JoinOutcome::Applied(merged)
                | JoinOutcome::AlreadyApplied(merged)
                | JoinOutcome::NoChanges(merged) => self.generation_ref(&merged),
                JoinOutcome::StaleTarget(_) => Err(Error::Conflict(
                    "target workspace advanced during the merge".into(),
                )),
                JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                } => Err(Error::Conflict(format!(
                    "{}{} merge conflicts",
                    conflicts.len(),
                    if truncated { "+" } else { "" }
                ))),
                JoinOutcome::Fenced => {
                    Err(Error::Conflict("target workspace writer was fenced".into()))
                }
                JoinOutcome::IdempotencyConflict => Err(Error::Conflict(
                    "merge retry identity belongs to another join".into(),
                )),
            }
        })
    }

    fn discard<'a>(
        &'a self,
        workspace: &'a WorkspaceRef,
        key: &'a IdempotencyKey,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            workspace.validate()?;
            self.validate_provider(workspace.as_resource().provider())?;
            let name = std::str::from_utf8(workspace.as_resource().key())
                .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
            match self
                .filesystem
                .delete_workspace(name, filesystem_key(key))
                .await
            {
                Ok(WorkspaceDelete::Deleted | WorkspaceDelete::AlreadyDeleted)
                | Err(WorkspaceError::NotFound) => Ok(()),
                Ok(WorkspaceDelete::Conflict) => Err(Error::Conflict(
                    "workspace changed while it was being discarded".into(),
                )),
                Ok(WorkspaceDelete::IdempotencyConflict) => Err(Error::Conflict(
                    "discard retry identity belongs to another deletion".into(),
                )),
                Err(error) => Err(map_error(error)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::WorkspaceMutation;
    use acyclic_fs::Fs;
    use acyclic_harness::{
        OperationId, Outcome,
        core::{AggregateKind, Authority},
        distributed::{DistributedCoordinator, Worker},
        resources::ProviderRef,
        scheduler::{
            DurableOwner, LeaseFence, Orchestration, ResourceRequest, ResourceSnapshot,
            SchedulerEvent,
        },
        speculation::{
            AttemptCheck, AttemptSpec, CheckReport, FewestChangesJudge, JudgeTiming, LoserPolicy,
            SpeculationPolicy, SpeculationRequest, fewest_changes_entrypoint,
        },
    };
    use acyclic_stream::{MemoryStream, StreamClient};
    use serde_json::{Value, json};
    use std::sync::Arc;

    /// Passes when the probe holds `/retries.py`, and litters the probe like a real
    /// test run would, proving check artifacts never reach the target.
    struct ProbeCheck<'a, A, O>(&'a FilesystemHost<A, O>);

    impl<A, O> AttemptCheck for ProbeCheck<'_, A, O>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        fn run<'a>(
            &'a self,
            _: &'a Value,
            probe: &'a WorkspaceRef,
        ) -> BoxFuture<'a, Result<CheckReport>> {
            Box::pin(async move {
                self.0
                    .apply(
                        probe,
                        None,
                        &[WorkspaceMutation::PutFile {
                            path: "/.pytest_cache".into(),
                            bytes: b"cache".to_vec(),
                        }],
                        &IdempotencyKey::new(format!("litter-{:?}", probe.as_resource().key()))?,
                    )
                    .await?;
                let passed = self.0.stat(probe, None, "/retries.py").await.is_ok();
                Ok(CheckReport::new(passed, "1 passed"))
            })
        }
    }

    fn key(value: &str) -> Result<IdempotencyKey> {
        IdempotencyKey::new(value)
    }

    async fn write<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        host: &FilesystemHost<A, O>,
        workspace: &WorkspaceRef,
        files: &[(&str, &str)],
    ) -> Result<()> {
        let mutations = files
            .iter()
            .map(|(path, bytes)| WorkspaceMutation::PutFile {
                path: (*path).into(),
                bytes: bytes.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        host.apply(
            workspace,
            None,
            &mutations,
            &key(&format!("write-{files:?}"))?,
        )
        .await?;
        Ok(())
    }

    async fn request<A, O>(
        host: &FilesystemHost<A, O>,
        target: &WorkspaceRef,
    ) -> Result<SpeculationRequest>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let mut attempts = Vec::new();
        for (index, slot) in (0_u8..).zip(["a", "b", "c"]) {
            let workspace = host
                .fork(target, None, &key(&format!("fork-{slot}"))?)
                .await?;
            attempts.push(AttemptSpec {
                slot: slot.into(),
                operation_id: OperationId::from_bytes([20 + index; 16]),
                workspace,
                entrypoint: fewest_changes_entrypoint(),
                resources: ResourceRequest::default(),
                placement: Value::Null,
                orchestration: Orchestration::Leaf,
                state: json!({"approach": slot}),
            });
        }
        Ok(SpeculationRequest {
            operation_id: OperationId::from_bytes([19; 16]),
            parent: None,
            owner: DurableOwner::Attached {
                authority: Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            },
            policy: SpeculationPolicy {
                judge: fewest_changes_entrypoint(),
                check: Some(json!({"shell": "pytest"})),
                timing: JudgeTiming::AllSettled,
                losers: LoserPolicy::CancelOnDecision,
            },
            target: target.clone(),
            attempts,
        })
    }

    async fn run_attempts(coordinator: &mut DistributedCoordinator<MemoryStream>) -> Result<()> {
        let worker = Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
        };
        while let Some(lease) = coordinator.pull(&worker).await? {
            let id = lease.operation.operation_id;
            let fence = LeaseFence::from(&lease.reservation);
            let started = SchedulerEvent::Started {
                operation_id: id,
                fence: fence.clone(),
            };
            coordinator
                .apply(id, key(&format!("start-{id}"))?, started)
                .await?;
            let outcome = if id == OperationId::from_bytes([22; 16]) {
                Outcome::Failed {
                    message: "gave up".into(),
                }
            } else {
                Outcome::Succeeded(json!(format!("{id} done")))
            };
            let completed = SchedulerEvent::Completed {
                operation_id: id,
                outcome,
                fence: Some(fence),
            };
            coordinator
                .apply(id, key(&format!("done-{id}"))?, completed)
                .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn winner_merges_into_memory_filesystem_and_losers_are_deleted() -> Result<()> {
        let filesystem = Fs::memory();
        filesystem
            .create_workspace("root")
            .await
            .map_err(map_error)?;
        let provider = ProviderRef::new("example", "filesystem", "1")?;
        let host = FilesystemHost::new(filesystem.clone(), provider.clone())?;
        let target = workspace_ref(provider, "root")?;
        write(&host, &target, &[("/README.md", "retry library")]).await?;
        let request = request(&host, &target).await?;
        let [a, b, c] = [0, 1, 2].map(|index| {
            request
                .attempts
                .get(index)
                .map(|attempt| attempt.workspace.workspace.clone())
        });
        let (Some(a), Some(b), Some(c)) = (a, b, c) else {
            return Err(Error::Invalid("three attempts".into()));
        };
        // `b` touches fewer real paths once its AppleDouble companion is ignored.
        write(&host, &a, &[("/retries.py", "a"), ("/notes.md", "a")]).await?;
        write(
            &host,
            &b,
            &[("/retries.py", "b"), ("/._retries.py", "xattr")],
        )
        .await?;
        write(&host, &c, &[("/retries.py", "c")]).await?;

        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        coordinator
            .open_speculation(&request, &key("speculate")?)
            .await?;
        run_attempts(&mut coordinator).await?;
        let check = ProbeCheck(&host);
        for slot in ["a", "b"] {
            coordinator
                .evaluate_attempt(request.operation_id, slot, &host, Some(&check), &key(slot)?)
                .await?;
        }
        let verdict = coordinator
            .judge_speculation(
                request.operation_id,
                &FewestChangesJudge::new(),
                &key("judge")?,
            )
            .await?;
        assert_eq!(verdict.and_then(|value| value.winner), Some("b".into()));
        let merged = coordinator
            .settle_speculation(request.operation_id, &host, &key("settle")?)
            .await?
            .ok_or_else(|| Error::NotFound("merge".into()))?;

        assert_eq!(
            host.read(&target, Some(&merged), "/retries.py", 16).await?,
            "b"
        );
        assert_eq!(
            host.read(&target, None, "/README.md", 64).await?,
            "retry library"
        );
        assert!(host.stat(&target, None, "/notes.md").await.is_err());
        assert!(host.stat(&target, None, "/.pytest_cache").await.is_err());
        for workspace in [&a, &b, &c] {
            assert!(
                host.resolve(workspace).await.is_err(),
                "attempt fork deleted"
            );
        }
        // Replaying the coordinator reproduces the settled outcome exactly.
        let replayed = DistributedCoordinator::open(&client).await?;
        assert_eq!(replayed.scheduler(), coordinator.scheduler());
        Ok(())
    }

    #[tokio::test]
    async fn retrying_a_committed_merge_is_idempotent() -> Result<()> {
        let filesystem = Fs::memory();
        filesystem
            .create_workspace("root")
            .await
            .map_err(map_error)?;
        let provider = ProviderRef::new("example", "filesystem", "1")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let target = workspace_ref(provider, "root")?;
        let attempt = host.fork(&target, None, &key("fork")?).await?;
        write(&host, &attempt.workspace, &[("/retries.py", "fixed")]).await?;
        let pinned = host.head(&attempt.workspace).await?;
        let first = host
            .merge(&attempt, &pinned, &target, &key("merge")?)
            .await?;
        // A crash before the settlement commits retries against the advanced target.
        let second = host
            .merge(&attempt, &pinned, &target, &key("merge")?)
            .await?;
        assert_eq!(first, second);
        assert_eq!(host.read(&target, None, "/retries.py", 16).await?, "fixed");
        Ok(())
    }
}
