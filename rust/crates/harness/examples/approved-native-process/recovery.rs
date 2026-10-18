//! Reconciliation of one retained native process after restart.
use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Restart {
    pub(super) task: TaskId,
    pub(super) fence: LeaseFence,
    pub(super) scope: Scope,
    pub(super) command: OperationId,
    pub(super) approval: InteractionId,
    pub(super) plan: TaskEffectPlan,
    pub(super) results_volume: VolumeRef,
    pub(super) destination: VolumeRef,
    pub(super) request: NativeProcessRequest,
    pub(super) first: Option<EffectStatus>,
    /// The MCP call ran in the child.
    pub(super) applied: bool,
    /// The capture settled and preparation verified its publication. A host
    /// that dies after the call but before settlement leaves this false.
    pub(super) published: bool,
}

pub(super) async fn run<P, A, O>(
    stream: std::sync::Arc<P>,
    filesystem: Fs<A, O>,
    seed: &Restart,
    receipt_fault: Option<(&str, bool)>,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let issuer = issuer();
    let files = FilesystemHost::new(filesystem, seed.destination.provider(), None)?;
    let runtime = task::open(
        stream.clone(),
        files.clone(),
        seed.results_volume.clone(),
        &issuer,
        &seed.scope,
        None,
    )
    .await?;
    let (owner, effects, content) =
        effects(stream.clone(), files.clone(), &runtime, seed, &issuer).await?;
    let recovered_status = effects
        .run_task_effect(&owner, seed.command, &seed.plan, None, true)
        .await
        .map(|result| result.0)?;
    verify_status(&recovered_status, seed.first.as_ref(), receipt_fault)?;
    let mcp = seed.request.mcp_stdio.is_some();
    if seed
        .request
        .mcp_stdio
        .as_ref()
        .is_some_and(|request| request.method == "tools/call")
        && (receipt_fault.is_some() || seed.first.is_some())
    {
        let journal = execution_journal(
            stream.clone(),
            files.clone(),
            &seed.results_volume,
            &issuer,
            &seed.scope,
        )?;
        model_tool::run(
            model_tool::Approved {
                owner: owner.clone(),
                effects: effects.clone(),
                content: content.clone(),
                task: seed.task,
                command: seed.command,
                plan: seed.plan.clone(),
                request: seed.request.clone(),
            },
            runtime
                .harness()
                .durable_context(
                    seed.task,
                    OperationId::from_bytes(seed.task.into_bytes()),
                    None,
                )
                .await?,
            journal,
            true,
            // A lost MCP response stays indeterminate even though the
            // process effect itself completed.
            matches!(recovered_status, EffectStatus::Succeeded { .. }) && !lost_response,
        )
        .await?;
    }
    if matches!(recovered_status, EffectStatus::Succeeded { .. }) {
        verify_stored_result(
            content.as_ref(),
            &seed.request,
            &recovered_status,
            lost_response,
        )
        .await?;
    }
    if effects
        .run_task_effect(&owner, seed.command, &seed.plan, None, false)
        .await
        .map(|result| result.0)?
        != recovered_status
        || seed.request.cwd.join("destination/output.txt").exists()
    {
        return Err("receipt recovery repeated native execution".into());
    }
    if mcp {
        verify_mcp_calls(&seed.request, seed.applied)?;
    }
    verify_publication(&files, &seed.destination, seed.published).await?;
    if receipt_fault.is_none() && !lost_response {
        println!(
            "approved {} execution, SDK publication and receipt-only recovery passed",
            if mcp { "MCP stdio" } else { "native" }
        );
    }
    Ok(())
}

pub(super) async fn effects<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    runtime: &Runtime<P, A, O>,
    seed: &Restart,
    issuer: &AuthorityIssuer,
) -> Result<(
    Arc<TaskJournalOwner<P>>,
    Arc<ConversationEffectHost<P>>,
    Arc<dyn ContentResidencyVerifier>,
)>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let owner = Arc::new(
        runtime
            .task_host()
            .journal_owner(seed.task, &seed.fence, None)
            .await?,
    );
    let verifier = issuer.verifier(&mut None)?;
    let content: Arc<dyn ContentResidencyVerifier> =
        FilesystemContentVerifier::new(files.clone(), &verifier, &seed.scope, 1_000_000, None)?;
    let authority = NativeProcessAuthority {
        verifier: verifier.copy(&mut None)?,
        schemas: SchemaRegistry::default(),
        content: content.clone(),
        results: FilesystemContentPublisher::new(
            files.clone(),
            &seed.results_volume,
            &verifier,
            &seed.scope,
            1_000_000,
            None,
        )?,
        approvals: execution_journal(
            stream.clone(),
            files,
            &seed.results_volume,
            issuer,
            &seed.scope,
        )?,
    };
    let recovered = Arc::new(
        NativeProcessProvider::<P, A, O>::new(
            owner.clone(),
            seed.command,
            seed.plan.clone(),
            None,
            seed.approval,
            authority,
        )
        .await?,
    );
    let mut registry = EffectRegistry::new(None, Some(content.clone()))?;
    registry.register(recovered, None)?;
    let effects = ConversationEffectHost::new(
        stream,
        (
            issuer
                .verifier(&mut None)
                .expect("verifier copy")
                .audience()
                .clone(),
            issuer.clone(),
            seed.scope.clone(),
        ),
        SchemaRegistry::new(),
        content.clone(),
        registry,
        None,
        None,
    )?;
    Ok((owner, effects, content))
}

fn verify_status(
    recovered: &EffectStatus,
    first: Option<&EffectStatus>,
    receipt_fault: Option<(&str, bool)>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if let Some((kind, lost_reply)) = receipt_fault {
        if kind == "observed" && lost_reply {
            if !matches!(recovered, EffectStatus::Succeeded { .. }) {
                return Err("committed observation was not recovered".into());
            }
        } else if *recovered != EffectStatus::Indeterminate {
            return Err("unknown MCP outcome became authoritative".into());
        }
    } else if first.is_none() {
        if *recovered != EffectStatus::Indeterminate {
            return Err("launch without an observation became authoritative".into());
        }
    } else if Some(recovered) != first {
        return Err("receipt recovery changed the observed result".into());
    }
    Ok(())
}
