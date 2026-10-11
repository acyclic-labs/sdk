//! Native lifetime ownership in the existing session actor and task journals.
//! Cold sources come only from sealed admissions and their original creator.
//! Shutdown signals every owned supervisor before joining jobs; stop receipts
//! never replace the SDK's scratch-creation and current-WRITE deletion guards.

use super::*;
use acyclic_harness::filesystem::TaskWorkerAttempt;
use acyclic_harness::distributed::WorkPull;
use acyclic_harness::{Error, Result as RootResult};

impl PiSession {
    pub(super) fn native_host_with_native_work(
        &mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> RootResult<Arc<pi_native_host::PiNativeCommandHost>> {
        if let Some(host) = &self.native {
            return Ok(host.clone());
        }
        let protected = pi_owner::PiOwnerConfig::load_with_native_work(
            &self.resources.private_state_root, &self.relative_directory, work)?;
        let PluginTaskBinding::Pi(root) = &self.state.task_binding else {
            return Err(Error::Invalid("Pi native lifetime has no original root binding".to_owned()));
        };
        protected.validate_root_with_native_work(root, work)?;
        self.runtime.verify_owner_with_native_work(&protected, work)?;
        let host = pi_native_host::PiNativeCommandHost::new_with_native_work(
            self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, protected, work,
        )?;
        self.native = Some(host.clone());
        Ok(host)
    }

    pub(super) async fn cancel_native_subtree(
        &mut self, caller: TaskId, target: TaskId,
        query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> RootResult<()> {
        let cleanup = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            pi_root_provider::allocation_budget(), &cleanup,
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        // Authenticate the complete original graph and the actual caller before
        // publishing cancellation or changing its route catalog.
        let _graph_receipt = pi_recovery::observe_pi_session_with_native_work(
            &self.runtime.runtime, &self.state, query_work).await?;
        self.binding(caller)?;
        self.authorize_subtree(caller, target)?;
        let caller_admission = self.runtime.runtime.task_host()
            .observe_admission_with_native_work(caller, query_work).await?;
        if !caller_admission.0.grants.contains("operation:cancel") {
            return Err(Error::Invalid("Pi caller lacks its original cancellation grant".to_owned()));
        }
        let tasks = self.native_subtree(target)?;
        let host = self.native_host_with_native_work(query_work)?;
        let PluginTaskBinding::Pi(root) = &self.state.task_binding else {
            return Err(Error::Invalid("Pi cancellation lost its original root".to_owned()));
        };
        let _caller_source = if caller != root.task_id {
            let live = host.live_source(caller, &work).await?.ok_or_else(|| Error::Invalid("Pi cancellation caller has no current original native owner".to_owned()))?;
            if &live.binding != self.binding(caller)?
                || !live.context.scope().grants().contains("operation:cancel")
            {
                return Err(Error::Invalid("Pi cancellation caller differs from its current native authority".to_owned()));
            }
            Some(live)
        } else { None };
        let mut failure = None;
        retain_first_failure(&mut failure,
            self.runtime.runtime.task_host().cancel_with_native_work(target, &work).await.map(|_| ()));
        for route in self.state.routes.values_mut() {
            if let PluginTaskBinding::Pi(binding) = &route.task_binding
                && tasks.contains(&binding.task_id) && !route.lifecycle.is_frozen()
            {
                route.lifecycle = RouteLifecycle::StopRequested;
            }
        }
        retain_first_failure(&mut failure,
            self.slots.save_with_native_work(&self.resources.private_state_root,
                &self.relative_directory, &self.state, Survives::PowerLoss, &work));

        // Order every independent provider stop before awaiting any process
        // future. Those futures may still be waiting on an RPC to this actor.
        for task in &tasks {
            let _outcome_receipt = match self.runtime.runtime.task_host()
                .outcome_with_native_work(*task, &work).await {
                Ok(receipt) => Some(receipt),
                Err(error) => {
                    retain_first_failure(&mut failure, Err(error));
                    None
                }
            };
            let terminal = _outcome_receipt.as_ref().is_some_and(|receipt|
                matches!(&receipt.0, Some(Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled)));
            if !terminal {
                retain_first_failure(&mut failure,
                    host.cancel_child_with_native_work(*task, &work).await.map(|_| ()));
            }
            if !self.native_jobs.contains_key(task) {
                match self.runtime.runtime.task_host().recover_work_with_native_work(*task, &work).await {
                    Ok(observed) => match &observed.0 {
                        WorkPull::Claimed(_) | WorkPull::Unresolved { .. } => {
                            let native = host.clone();
                            let task = *task;
                            self.native_jobs.insert(task,
                                tokio::spawn(async move { native.recover_child(task).await }));
                        }
                        WorkPull::Idle => {}
                    },
                    Err(error) => retain_first_failure(&mut failure, Err(error)),
                }
            }
        }
        for task in &tasks {
            if let Err(error) = host.signal_stop_child(*task, &work).await {
                retain_first_failure(&mut failure, Err(error));
            }
        }
        self.join_native_jobs(&tasks, &mut failure).await;
        self.drain_native_tasks(&host, &tasks, NativeDrainMode::Cancel,
            &work, &mut failure).await;
        retain_first_failure(&mut failure,
            self.slots.save_with_native_work(&self.resources.private_state_root,
                &self.relative_directory, &self.state, Survives::PowerLoss, &work));
        finish_native_drain(failure)
    }

    pub(super) async fn recover_native_children(
        &mut self, query_work: &acyclic_fs::NativeMaterializationWork,
    ) -> RootResult<()> {
        let cancellation = query_work.cancellation().clone();
        let budget = query_work.remaining()
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let _graph_receipt = pi_recovery::observe_pi_session_with_native_work(
            &self.runtime.runtime, &self.state, query_work).await?;
        if self.state.routes.is_empty() { return Ok(()); }
        let native = self.native_host_with_native_work(query_work)?;
        let protected = native.original_owner();
        self.runtime.verify_owner_with_native_work(&protected, query_work)?;
        let root_task = protected.policy().root_task;
        let coordinator = pi_fork::PiForkCoordinator::new_with_native_work(
            self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, protected.clone(), query_work,
        )?;
        let mut plans = BTreeMap::new();
        for route in self.state.routes.values() {
            let parent = self.alias_task(&route.parent_agent_id)?;
            let original = pi_fork::original_operation(root_task, parent, &route.turn_id);
            let plan = coordinator.load_original_plan(original, budget, &cancellation)
                .await?.ok_or_else(|| Error::Invalid("Pi Created route has no original immutable fork plan".to_owned()))?;
            validate_created_native_route(route, parent, &plan)?;
            plans.insert(plan.commit.child.task_id, (route, parent, plan));
        }

        // Source selection is a separate graph from parentage. In particular,
        // a foreign selected child is never replaced by its parent or Root.
        let mut remaining: Vec<TaskId> = plans.keys().copied().collect();
        let mut root_sources = BTreeMap::new();
        let mut child_sources = BTreeMap::new();
        while !remaining.is_empty() {
            let before = remaining.len();
            let mut index = 0;
            while index < remaining.len() {
                let task = remaining[index];
                let (route, parent, plan) = &plans[&task];
                let root_key = self.roots.keys()
                    .find(|key| pi_root::root_identity_key_matches(key, &route.root_id))
                    .ok_or_else(|| Error::Invalid("Pi original selected Root was not restored before child recovery".to_owned()))?
                    .as_str();
                let source = if plan.commit.source_task == root_task {
                    if let Some(source) = root_sources.get(root_key) {
                        Arc::clone(source)
                    } else {
                        let root = self.roots.get(root_key).ok_or_else(|| Error::Invalid("Pi original selected Root was not restored before child recovery".to_owned()))?;
                        let source = Arc::new(root.workspace.clone());
                        root_sources.insert(root_key, source.clone());
                        source
                    }
                } else if let Some(source) = child_sources.get(&plan.commit.source_task) {
                    Arc::clone(source)
                } else {
                    index += 1;
                    continue;
                };
                let observed = coordinator.observe_prepared(plan, source.clone(), budget,
                    cancellation.clone()).await?.ok_or_else(|| Error::Invalid("Pi Created route has no authenticated successful original creator".to_owned()))?;
                validate_created_native_report(route, &observed)?;
                // This retains source-only capabilities. First native dispatch
                // must still hydrate under its actual current child owner.
                native.register_observed_source(observed).await?;
                let workspace = native.live_workspace(task).ok_or_else(|| Error::Invalid("Pi observed child lost its actual creator workspace".to_owned()))?;
                child_sources.insert(task, workspace);
                native.register_cold_locator_with_native_work(
                    task, *parent, &route.turn_id, root_key, source, query_work,
                ).await?;
                remaining.swap_remove(index);
            }
            if remaining.len() == before {
                return Err(Error::Invalid("Pi original selected-source graph is incomplete or cyclic".to_owned()));
            }
        }

        // All original source capabilities are in place before the actor is
        // released to real native children. Reopen never waits on their tool RPC.
        for (task, (route, _, plan)) in &plans {
            if route.lifecycle.is_frozen() || self.native_jobs.contains_key(task) { continue; }
            let observed = self.runtime.runtime.task_host().recover_work_with_native_work(*task, query_work).await?;
            match &observed.0 {
                WorkPull::Claimed(_) | WorkPull::Unresolved { .. } => {
                    let native = native.clone();
                    let task = *task;
                    self.native_jobs.insert(task,
                        tokio::spawn(async move { native.recover_child(task).await }));
                }
                WorkPull::Idle if original_native_first_dispatch_pending(
                    &self.runtime, &self.stream, &protected, plan, query_work,
                ).await? => {
                    let native = native.clone();
                    let task = *task;
                    self.native_jobs.insert(task,
                        tokio::spawn(async move { native.run_child(task).await }));
                }
                WorkPull::Idle => {}
            }
        }
        Ok(())
    }

    pub(super) async fn drain_native(&mut self, deactivate: bool) -> RootResult<()> {
        let cleanup = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            pi_root_provider::allocation_budget(), &cleanup,
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut failure = None;
        let graph = pi_recovery::observe_pi_session_with_native_work(
            &self.runtime.runtime, &self.state, &work).await;
        let authenticated = graph.is_ok();
        let _graph_receipt = match graph {
            Ok(receipt) => Some(receipt),
            Err(error) => {
                retain_first_failure(&mut failure, Err(error));
                None
            }
        };
        let root = match &self.state.task_binding {
            PluginTaskBinding::Pi(binding) => Some(binding.task_id),
            PluginTaskBinding::HostManaged => {
                retain_first_failure(&mut failure,
                    Err(Error::Invalid("Pi close has no original root task".to_owned())));
                None
            }
        };
        let mut tasks = std::collections::BTreeSet::new();
        tasks.extend(self.native_jobs.keys().copied());
        for route in self.state.routes.values() {
            if let PluginTaskBinding::Pi(binding) = &route.task_binding {
                tasks.insert(binding.task_id);
            }
        }
        let tasks: Vec<TaskId> = tasks.into_iter().collect();
        let native = if tasks.is_empty() { None } else {
            match self.native_host_with_native_work(&work) {
                Ok(native) => Some(native),
                Err(error) => { retain_first_failure(&mut failure, Err(error)); None }
            }
        };
        if deactivate && authenticated {
            if let Some(native) = &native {
                // Request disposal while the actual child still owns settlement.
                // This is not a deletion permit and does not remove any path.
                for task in &tasks {
                    retain_first_failure(&mut failure,
                        native.request_scratch_disposal(*task).await);
                }
            }
            if let Some(root) = root {
                retain_first_failure(&mut failure,
                    self.runtime.runtime.task_host().cancel_with_native_work(root, &work).await.map(|_| ()));
            }
            for route in self.state.routes.values_mut() {
                if !route.lifecycle.is_frozen() {
                    route.lifecycle = RouteLifecycle::StopRequested;
                }
            }
            retain_first_failure(&mut failure,
                self.slots.save_with_native_work(&self.resources.private_state_root,
                    &self.relative_directory, &self.state, Survives::PowerLoss, &work));
        }
        if let Some(native) = &native {
            // Never wait for a supervisor or a run mutex while another native
            // child may still be waiting on an RPC to this actor.
            for task in &tasks {
                if let Err(error) = native.signal_stop_child(*task, &work).await {
                    retain_first_failure(&mut failure, Err(error));
                }
            }
        }
        self.join_native_jobs(&tasks, &mut failure).await;
        if let Some(native) = &native {
            let mode = if deactivate && authenticated {
                NativeDrainMode::Deactivate
            } else {
                NativeDrainMode::Preserve
            };
            self.drain_native_tasks(native, &tasks, mode, &work, &mut failure).await;
        }
        if deactivate && authenticated {
            retain_first_failure(&mut failure,
                self.slots.save_with_native_work(&self.resources.private_state_root,
                    &self.relative_directory, &self.state, Survives::PowerLoss, &work));
        }
        // Normal close preserves the original route/effect. Releasing a lease
        // requires the native host's actual journal-quiescent tail CAS; an
        // uncertain command must keep the same reservation for observation.
        finish_native_drain(failure)
    }

    async fn drain_native_tasks(
        &mut self, native: &Arc<pi_native_host::PiNativeCommandHost>, tasks: &[TaskId],
        mode: NativeDrainMode, work: &acyclic_fs::NativeMaterializationWork,
        failure: &mut Option<Error>,
    ) {
        let mut drains = Vec::with_capacity(tasks.len());
        for task in tasks {
            let native = native.clone();
            let work = work.clone();
            let task = *task;
            drains.push((task, tokio::spawn(async move {
                native.drain_stopped_child(task, mode == NativeDrainMode::Deactivate,
                    &work).await
            })));
        }
        for (task, drain) in drains {
            match drain.await {
                Ok(Ok(_)) if mode != NativeDrainMode::Preserve => {
                    for route in self.state.routes.values_mut() {
                        if matches!(&route.task_binding, PluginTaskBinding::Pi(binding)
                            if binding.task_id == task)
                        {
                            route.lifecycle = RouteLifecycle::Frozen;
                        }
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(error)) => retain_first_failure(failure, Err(error)),
                Err(error) => retain_first_failure(failure,
                    Err(Error::Storage(format!("Pi original native drainage {task} failed: {error}")))),
            }
        }
    }

    fn native_subtree(&self, target: TaskId) -> RootResult<Vec<TaskId>> {
        let mut tasks = Vec::new();
        for route in self.state.routes.values() {
            let PluginTaskBinding::Pi(binding) = &route.task_binding else {
                return Err(Error::Invalid("Pi native subtree contains another host's route".to_owned()));
            };
            if self.authorize_subtree(target, binding.task_id).is_ok() {
                tasks.push(binding.task_id);
            }
        }
        Ok(tasks)
    }

    async fn join_native_jobs(&mut self, tasks: &[TaskId], failure: &mut Option<Error>) {
        for task in tasks {
            let Some(job) = self.native_jobs.remove(task) else { continue; };
            let result = match job.await {
                Ok(Ok((attempt, allocation))) => {
                    let result = match attempt {
                        Some(TaskWorkerAttempt::Unresolved { error, .. }) => Err(error),
                        Some(attempt) => { drop(attempt); Ok(()) }
                        None => Ok(()),
                    };
                    drop(allocation);
                    result
                }
                Ok(Err(error)) => Err(error),
                Err(error) => Err(Error::Storage(format!("Pi original native worker {task} failed: {error}"))),
            };
            retain_first_failure(failure, result);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum NativeDrainMode { Preserve, Cancel, Deactivate }

fn retain_first_failure(failure: &mut Option<Error>, result: RootResult<()>) {
    if failure.is_none() {
        if let Err(error) = result { *failure = Some(error); }
    }
}

fn finish_native_drain(failure: Option<Error>) -> RootResult<()> {
    match failure { Some(error) => Err(error), None => Ok(()) }
}

fn validate_created_native_route(
    route: &Route, parent: TaskId, plan: &pi_fork::PiForkPlan,
) -> RootResult<()> {
    let PluginTaskBinding::Pi(binding) = &route.task_binding else {
        return Err(Error::Invalid("Pi Created route substituted another host".to_owned()));
    };
    let original = &plan.commit.child;
    let key = hex::encode(route.root_id);
    if route.agent_id != plan.commit.alias || route.turn_id != plan.commit.sdk_operation
        || plan.commit.caller != parent
        || binding.task_id != original.task_id || binding.agent_id != original.agent_id
        || binding.native_session_id != original.native_session_id
        || binding.native_session_id != plan.commit.launch.child_session_id
        || binding.project_volumes != original.project_volumes
        || binding.scratch.volume != original.scratch.volume
        || binding.results.as_ref().map(|result| &result.volume)
            != original.results.as_ref().map(|result| &result.volume)
        || binding.project_volumes.len() != 1 || !binding.project_volumes.contains_key(&key)
        || route.roots.len() != 1 || !route.roots.contains_key(&key)
        || route.context_id != pi_fork::derived(plan.original, "fork").into_bytes()
        || !matches!(binding.scratch.allocation, PiScratchAllocation::Created(_))
        || binding.results.as_ref().is_none_or(|result|
            !matches!(result.allocation, PiScratchAllocation::Created(_)))
    {
        return Err(Error::Invalid("Pi Created catalog differs from its original sealed native fork".to_owned()));
    }
    Ok(())
}

fn validate_created_native_report(
    route: &Route, prepared: &pi_fork::PreparedPiFork,
) -> RootResult<()> {
    let PluginTaskBinding::Pi(binding) = &route.task_binding else {
        return Err(Error::Invalid("Pi Created report substituted another host".to_owned()));
    };
    let key = hex::encode(route.root_id);
    let root = route.roots.get(&key).ok_or_else(|| Error::Invalid("Pi Created report lost its original selected root".to_owned()))?;
    if binding.as_ref() != &prepared.binding || root.root_id != route.root_id
        || root.repository_workspace_id != prepared.workspace.workspace().id().into_bytes()
        || root.published_generation != prepared.native_lazy.authored_generation.digest().into_bytes()
    {
        return Err(Error::Invalid("Pi Created route differs from its actual original filesystem creator".to_owned()));
    }
    Ok(())
}

async fn original_native_first_dispatch_pending(
    runtime: &PiRuntime, stream: &StreamClient<LocalStream>,
    protected: &pi_owner::PiOwnerConfig, plan: &pi_fork::PiForkPlan,
    work: &acyclic_fs::NativeMaterializationWork,
) -> RootResult<bool> {
    let task = plan.commit.child.task_id;
    let host = runtime.runtime.task_host();
    let admission_receipt = host.observe_admission_with_native_work(task, work).await?;
    let admission = &admission_receipt.0;
    if admission != &plan.request.child {
        return Err(Error::Invalid("Pi first dispatch differs from its original sealed child admission".to_owned()));
    }
    let reservation = host.recover_work_with_native_work(task, work).await?;
    if !matches!(&reservation.0, WorkPull::Idle) {
        return Ok(false);
    }
    let outcome = host.outcome_with_native_work(task, work).await?;
    if host.cancellation_requested_with_native_work(task, work).await?
        || outcome.0.is_some()
    {
        return Ok(false);
    }
    let limits = acyclic_harness::store::HistoryReadLimits {
        maximum_events: admission.limits.context_messages,
        maximum_bytes: admission.limits.file_bytes.min(MAXIMUM_ADAPTER_STATE_BYTES),
    };
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(Error::Invalid("Pi first dispatch has no original finite effect-history allowance".to_owned()));
    }
    let conversation = protected.conversation_authority_with_native_work(
        task, plan.commit.child.agent_id, &admission.grants, work,
    )?;
    let verifier = conversation.0.1.verifier_with_native_work(work)?;
    let history = acyclic_harness::store::HistoryReader::new_with_native_work(
        stream, &conversation.0.0, &verifier.0, work)?;
    let pin = history.0.pin_with_native_work(0, work).await?;
    let effect = acyclic_harness::effect_host::task_effect_id_with_native_work(
        task, pi_fork::derived(plan.original, "native-effect"), work,
    )?;
    // NativeProcessProvider can write its original launch receipt only after
    // the canonical effect intent/dispatch. Absence proves first dispatch,
    // never authority to repeat a dispatched or indeterminate process.
    let observed = history.0.effect_with_native_work(&pin.0, effect, limits, work).await?;
    Ok(observed.0.is_none())
}

#[cfg(test)]
#[path = "pi_lifecycle_session_tests.rs"]
mod tests;
