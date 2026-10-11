//! Stock effect-host composition only. This fixture does not allocate native
//! roots and its successful file is not production allocation evidence.

use super::*;
use acyclic_harness::{
    EffectAttemptId, IdempotencyKey,
    core::{EffectGuarantee, EffectStatus},
    distributed::Worker,
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
    scheduler::ResourceSnapshot,
};
use acyclic_stream::{BoxProviderFuture, LocalStream, LocalStreamLimits, StreamClient};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

// Explicit ordinary fixture infrastructure, not a production task-input source.
fn fixture_content(owner: &PiOwnerConfig,
    filesystem: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    deny_inputs: bool)
    -> Arc<dyn ContentResidencyVerifier> {
    let authority = owner.authority().unwrap();
    let signed = if deny_inputs {
        authority.issuer.root_for_agent(owner.policy.agent, "denied-root-inputs",
            Capabilities::new(std::iter::empty::<String>()))
    } else { authority.signed };
    Arc::new(acyclic_harness::filesystem::FilesystemContentVerifier::new(
        filesystem, authority.verifier, signed, owner.policy.maximum_payload_bytes,
    ).unwrap())
}

fn fixture_allocation(
    binding: &crate::PiTaskBinding,
) -> crate::pi_root_allocation::PiRootAllocation {
    crate::pi_root_allocation::PiRootAllocation {
        roots: binding
            .project_volumes
            .keys()
            .map(|key| {
                (
                    key.clone(),
                    crate::RootCreation {
                        repository_workspace_id: [91; 16],
                        source_identity: [92; 16],
                        source_epoch: 0,
                        native_root_identity: [93; 16],
                    },
                )
            })
            .collect(),
        scratch: crate::PiScratchCreation {
            workspace_id: [94; 16],
            generation: acyclic_harness::resources::GenerationRef::new(
                binding.scratch.volume.provider().clone(),
                [95; 32],
                None,
            )
            .unwrap(),
        },
    }
}

struct FixtureProvider {
    id: &'static str,
    result: FileRef,
    lose_acknowledgement: bool,
    dispatches: AtomicUsize,
    reconciliations: AtomicUsize,
    observation: Mutex<Option<EffectObservation>>,
}

impl EffectProvider for FixtureProvider {
    fn id(&self) -> &str {
        self.id
    }

    fn guarantees(&self, effect_kind: &str) -> std::collections::BTreeSet<EffectGuarantee> {
        if effect_kind == ROOT_ALLOCATION_EFFECT {
            [EffectGuarantee::AtMostOnce].into()
        } else {
            Default::default()
        }
    }

    fn linearizable_reconciliation(&self) -> bool {
        false
    }

    fn dispatch<'a>(
        &'a self,
        request: EffectDispatch,
    ) -> BoxProviderFuture<'a, acyclic_harness::Result<EffectObservation>> {
        Box::pin(async move {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.effect_kind, ROOT_ALLOCATION_EFFECT);
            let observation = EffectObservation {
                provider: request.provider,
                effect_id: request.effect_id,
                attempt_id: request.attempt_id,
                request_digest: request.request_digest,
                guarantee: request.guarantee,
                status: EffectStatus::Succeeded {
                    result: self.result.clone(),
                },
            };
            *self.observation.lock().unwrap() = Some(observation.clone());
            if self.lose_acknowledgement {
                Err(acyclic_harness::Error::Storage(
                    "fixture lost dispatch acknowledgement".to_owned(),
                ))
            } else {
                Ok(observation)
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        attempt_id: EffectAttemptId,
    ) -> BoxProviderFuture<'a, acyclic_harness::Result<Option<EffectObservation>>> {
        Box::pin(async move {
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            let observation = self.observation.lock().unwrap().clone();
            assert_eq!(
                observation.as_ref().map(|value| value.attempt_id),
                Some(attempt_id)
            );
            Ok(observation)
        })
    }
}

#[tokio::test]
async fn pi_root_stock_worker_reopens_original_effect_without_redispatch() {
    for case in [
        "success",
        "scratch_only",
        "lost_ack",
        "denied_content",
        "missing_root",
        "foreign_provider",
        "zero_workspace",
        "extra_field",
    ] {
        let lose_acknowledgement = case == "lost_ack";
        let valid_result = matches!(case, "success" | "scratch_only" | "lost_ack" | "denied_content");
        let session = tempfile::tempdir().unwrap();
        let (owner, filesystem, mut request, _raw) = root_input_fixture(session.path(), |policy| {
            policy.grants = Capabilities::new(policy.grants.iter().map(str::to_owned).chain([
                "effect:run".to_owned(),
                "effect:plan".to_owned(),
                format!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"),
            ]));
        })
        .await;
        if case == "scratch_only" {
            request.binding.project_volumes.clear();
            request.root = crate::pi_root::PiRootIntent::ScratchOnly {};
        }
        let authority = owner.authority().unwrap();
        let write = ContentGrant::verify(
            &authority.verifier,
            &authority.signed,
            &owner.policy.journal,
            VolumeOperation::Write,
        )
        .unwrap();
        let allocation = fixture_allocation(&request.binding);
        allocation.validate(&request.binding).unwrap();
        let mut value = serde_json::to_value(&allocation).unwrap();
        match case {
            "missing_root" => value
                .get_mut("roots")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .clear(),
            "foreign_provider" => {
                *value
                    .pointer_mut("/scratch/generation/provider/namespace")
                    .unwrap() = json!("foreign");
            }
            "zero_workspace" => {
                *value.pointer_mut("/scratch/workspace_id").unwrap() = json!(vec![0u8; 16]);
            }
            "extra_field" => {
                value
                    .as_object_mut()
                    .unwrap()
                    .insert("unknown".to_owned(), json!(true));
            }
            _ => {}
        }
        let decoded =
            serde_json::from_value::<crate::pi_root_allocation::PiRootAllocation>(value.clone());
        assert_eq!(
            decoded.is_ok_and(|allocation| allocation.validate(&request.binding).is_ok()),
            valid_result
        );
        let result = filesystem
            .put_content(
                &owner.policy.journal,
                &write,
                "pi-inputs/result.json",
                &serde_json::to_vec(&value).unwrap(),
                "application/json",
                "result.json",
                owner.policy.maximum_payload_bytes,
                &IdempotencyKey::new("fixture-root-result").unwrap(),
            )
            .await
            .unwrap();
        let work = startup_work();
        let staged = stage_root_command(&owner, &filesystem, &request, &work).await.unwrap();
        let filesystem = Arc::new(filesystem);
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(session.path().join("stream"), LocalStreamLimits::default())
                .await
                .unwrap(),
        ));
        let provider = Arc::new(FixtureProvider {
            id: ROOT_ALLOCATION_PROVIDER,
            result,
            lose_acknowledgement,
            dispatches: AtomicUsize::new(0),
            reconciliations: AtomicUsize::new(0),
            observation: Mutex::new(None),
        });
        let runtime = crate::pi_runtime::PiTaskRuntime::open_with_native_work(
            stream.clone(), filesystem.clone(), &owner.policy.journal,
            owner.authority_with_native_work(&work).unwrap(), &work,
        )
        .await
        .unwrap();
        let admission = runtime.admit_root_with_native_work(
            request.binding.clone(), request.root.clone(), staged.0, &work,
        ).await.unwrap();
        drop(admission.0);
        drop(admission.1);
        let authorization = owner.effect_authority_with_native_work(&work).unwrap();
        let effects = crate::pi_owner::root_effects_with_native_work(
            stream.clone(), authorization.0, authorization.1,
            fixture_content(&owner, filesystem.clone(), case == "denied_content"),
            provider.clone(), &work,
        ).unwrap();
        let worker = Worker {
            id: "pi-root-worker".to_owned(),
            available: ResourceSnapshot::default(),
            labels: Default::default(),
        };
        // Stop after committing the command, before any provider call. Reopen
        // the original task runtime and credentials, retaining this exact lease.
        let attempt = runtime
            .runtime
            .run_operation(
                &worker,
                request.binding.task_id,
                &runtime.runtime.commands().with_effects(effects.clone()),
                1,
            )
            .await
            .unwrap()
            .unwrap();
        let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = attempt else {
            panic!("root did not retain its preparation lease");
        };
        assert_eq!(provider.dispatches.load(Ordering::SeqCst), 0);
        drop(effects);
        drop(runtime);
        drop(owner);
        let owner = PiOwnerConfig::load_with_native_work(session.path(), &work).unwrap();
        let runtime = crate::pi_runtime::PiTaskRuntime::open_with_native_work(
            stream.clone(), filesystem.clone(), &owner.policy.journal,
            owner.authority_with_native_work(&work).unwrap(), &work,
        )
        .await
        .unwrap();
        let authorization = owner.effect_authority_with_native_work(&work).unwrap();
        let effects = crate::pi_owner::root_effects_with_native_work(
            stream, authorization.0, authorization.1,
            fixture_content(&owner, filesystem, case == "denied_content"),
            provider.clone(), &work,
        ).unwrap();
        let commands = runtime.runtime.commands().with_effects(effects);
        let original_lease = lease.clone();
        let attempt = runtime.runtime.resume_task(lease, &commands, 1).await;
        if case == "denied_content" {
            assert!(matches!(attempt, TaskWorkerAttempt::Unresolved { error: Error::Unauthorized(_), .. }));
            assert_eq!(provider.dispatches.load(Ordering::SeqCst), 0);
            assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 0);
            assert!(!session.path().join("unopened-selected-root").exists());
            continue;
        }
        if !valid_result {
            let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) = attempt
            else {
                panic!("invalid allocation advanced instead of retaining reconciliation: {case}");
            };
            assert_eq!(lease, original_lease);
            let attempt = runtime.runtime.resume_task(lease, &commands, 1).await;
            assert!(
                matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) if lease == original_lease)
            );
            assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
            assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 1);
            assert!(!session.path().join("unopened-selected-root").exists());
            continue;
        }
        let attempt = if lose_acknowledgement {
            let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) = attempt
            else {
                panic!("lost acknowledgement did not retain its exact lease");
            };
            assert_eq!(lease, original_lease);
            assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
            assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 0);
            runtime.runtime.resume_task(lease, &commands, 1).await
        } else {
            attempt
        };
        match attempt {
            TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { task, revision }) => {
                assert_eq!(task, request.binding.task_id);
                assert_eq!(revision, 2);
            }
            TaskWorkerAttempt::Unresolved { error, .. } => {
                panic!("root preparation failed: {error}")
            }
            _ => panic!("root preparation did not suspend after known result"),
        }
        assert!(
            runtime
                .runtime
                .run_operation(&worker, request.binding.task_id, &commands, 1)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.reconciliations.load(Ordering::SeqCst),
            usize::from(lose_acknowledgement)
        );
        assert!(!session.path().join("unopened-selected-root").exists());
    }
}
