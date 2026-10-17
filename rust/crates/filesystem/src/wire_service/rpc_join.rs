impl<A, O> FilesystemWireService<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn rpc_plan_join(
        &self,
        request: Request<wire::PlanJoinRequest>,
    ) -> Result<Response<wire::JoinPlan>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_generations == 0
            || request.maximum_generations > self.limits.maximum_page_items
            || request.maximum_changes == 0
            || request.maximum_changes > self.limits.maximum_page_items
            || request.maximum_conflicts == 0
            || request.maximum_conflicts > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("join bounds are invalid"));
        }
        let history = join_history(request.history)?;
        let source = self.generation(request.source).await?;
        let target = self.generation(request.target).await?;
        let source_workspace = source.workspace.clone();
        let target_workspace = target.workspace.clone();
        if source_workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| status(&failure.error))?
            .value
            .id()
            != source.id()
            || target_workspace
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| status(&failure.error))?
                .value
                .id()
                != target.id()
        {
            return Err(Status::failed_precondition(
                "join endpoints must be current workspace heads",
            ));
        }
        let plan = source_workspace
            .join_into(&target_workspace)
            .history(history)
            .bounds(
                request.maximum_generations,
                request.maximum_changes,
                request.maximum_conflicts,
            )
            .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        if plan.source_head() != source.id() || plan.target_head() != target.id() {
            return Err(Status::failed_precondition(
                "join endpoints changed while the plan was computed",
            ));
        }
        // The builder re-reads both live heads. Bind the wire plan to that exact
        // snapshot rather than the endpoint generations observed before planning.
        let source_ref = generation_ref_for(&source_workspace, plan.source_head());
        let target_ref = generation_ref_for(&target_workspace, plan.target_head());
        let base_ref = join_common_ancestor_ref(&plan, &source_workspace, &target_workspace);
        let plan_id = join_plan_id(
            &source_ref,
            &target_ref,
            &base_ref,
            history,
            request.maximum_generations,
            request.maximum_changes,
            request.maximum_conflicts,
        );
        let changes = source_workspace
            .volume
            .fs
            .workspace_join_changes(
                plan.common_ancestor(),
                &source,
                request.maximum_changes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value
            .0;
        let file_changes = changes.files.iter().map(file_record_change).collect();
        let binding_changes = changes
            .bindings
            .iter()
            .map(directory_binding_change)
            .collect();
        Ok(Response::new(wire::JoinPlan {
            plan_id: plan_id.to_vec(),
            source: Some(source_ref),
            expected_target: Some(target_ref),
            file_changes,
            conflicts: Vec::new(),
            truncated: changes.truncated,
            common_ancestor: Some(base_ref),
            maximum_generations: request.maximum_generations,
            maximum_changes: request.maximum_changes,
            maximum_conflicts: request.maximum_conflicts,
            history: request.history,
            binding_changes,
        }))
    }

    async fn rpc_apply_join(
        &self,
        request: Request<wire::ApplyJoinRequest>,
    ) -> Result<Response<wire::JoinResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        let supplied = required(request.plan, "join plan")?;
        let source_ref = required(supplied.source.clone(), "join source")?;
        let target_ref = required(supplied.expected_target.clone(), "join target")?;
        let base_ref = required(supplied.common_ancestor.clone(), "join common ancestor")?;
        let history = join_history(supplied.history)?;
        if supplied.maximum_generations == 0
            || supplied.maximum_generations > self.limits.maximum_page_items
            || supplied.maximum_changes == 0
            || supplied.maximum_changes > self.limits.maximum_page_items
            || supplied.maximum_conflicts == 0
            || supplied.maximum_conflicts > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument("join bounds are invalid"));
        }
        if supplied.plan_id
            != join_plan_id(
                &source_ref,
                &target_ref,
                &base_ref,
                history,
                supplied.maximum_generations,
                supplied.maximum_changes,
                supplied.maximum_conflicts,
            )
        {
            return Err(Status::invalid_argument("join plan identity mismatch"));
        }
        let source = self.generation(Some(source_ref)).await?;
        let target = self.generation(Some(target_ref)).await?;
        let source_workspace = source.workspace.clone();
        let target_workspace = target.workspace.clone();
        let source_id = source.id();
        let target_id = target.id();
        let operation = operation(request.operation)?;
        let (target_authority_head, existing_operation) = match self
            .join_target_head(&target_workspace, target_id, operation)
            .await?
        {
            JoinTargetHead::Expected {
                head,
                existing_operation,
            } => (head, existing_operation),
            JoinTargetHead::Stale(actual) => {
                return Ok(Response::new(join_response(
                    wire::JoinStatus::StaleTarget,
                    Some(&actual),
                )));
            }
        };
        let plan = source_workspace
            .join_into(&target_workspace)
            .history(history)
            .bounds(
                supplied.maximum_generations,
                supplied.maximum_changes,
                supplied.maximum_conflicts,
            )
            .plan_generations(
                source,
                target,
                target_authority_head,
                &mut crate::kernel::KernelAccounting::standalone(
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                ),
                &CancellationToken::new(),
            )
            .await
            .map_err(|error| status(&error))?;
        let expected_base_ref =
            join_common_ancestor_ref(&plan, &source_workspace, &target_workspace);
        if plan.source_head() != source_id
            || plan.target_head() != target_id
            || base_ref != expected_base_ref
        {
            let actual = target_workspace
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| status(&failure.error))?
                .value;
            return Ok(Response::new(wire::JoinResponse {
                status: wire::JoinStatus::StaleTarget as i32,
                generation: Some(generation_ref(&actual)),
                conflicts: Vec::new(),
                truncated: false,
            }));
        }
        let outcome = plan
            .apply(
                ApplyOptions {
                    if_target: target_id,
                    idempotency_key: operation,
                },
                std::collections::BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await
            .map_err(|error| status(&error))?;
        let outcome = if existing_operation
            && !matches!(
                &outcome,
                JoinOutcome::AlreadyApplied(_) | JoinOutcome::IdempotencyConflict
            ) {
            JoinOutcome::IdempotencyConflict
        } else {
            outcome
        };
        Ok(Response::new(join_message(outcome)))
    }
}
