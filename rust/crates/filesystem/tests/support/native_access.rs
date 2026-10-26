use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, MountOptions, WorkBudget,
    Workspace,
    access::*,
    model::{CheckoutMode, GenerationSelector},
};
use std::sync::Arc;

/// Compile every native fixture view with one stable caller binding.
pub fn compile_view(
    policy: &Policy,
    resource: Resource<'_>,
    context: Context,
    capability: &Capability,
    head: acyclic_fs::GenerationId,
    revision: acyclic_fs::kernel::MetadataField<acyclic_fs::ObjectId>,
) -> Result<PathTrie, InvalidPolicy> {
    let principal = Principal {
        kind: PrincipalKind::Service,
        id: "mount-test".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    Ok(
        PathTrie::compile(policy, &principal, resource, context, capability)?
            .with_policy_revision(head, revision),
    )
}

/// Explicit authority for privileged raw-SDK fixtures, outside the measured span.
pub async fn options<
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
>(
    workspace: &Workspace<A, O>,
    options: MountOptions,
) -> Result<MountOptions, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let mut head = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let view = checkout_view(&mut head).await?;
    Ok(options
        .access_view(view)
        .access_refresh(Arc::new(SelectionOwner {
            workspace: workspace.clone(),
        })))
}

/// One explicit fixture authority for both workspace and raw-checkout mounts.
pub async fn checkout_view<
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
>(
    checkout: &mut Checkout<A, O>,
) -> Result<Arc<AccessView>, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let resource = ResourcePattern {
        lineage_root: None,
        volume: None,
        generations: Generations::All,
        path: "**".into(),
    };
    let fallback = Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![vec![Rule {
            id: "fixture".into(),
            effect: Effect::Allow,
            principal: PrincipalPattern::default(),
            actions: Action::ALL.into_iter().collect(),
            resource: resource.clone(),
            conditions: Conditions::default(),
        }]],
    };
    let (stored, revision) = checkout
        .read_access_policy(WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    let policy = match stored {
        Some(record) => serde_json::from_slice(&record.bytes)?,
        None => fallback,
    };
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource,
        }]],
        expires_at: None,
    };
    let volume = uuid::Uuid::from_bytes(checkout.volume_id().into_bytes()).to_string();
    let view = compile_view(
        &policy,
        Resource {
            config: checkout.volume_config(),
            lineage: &[],
            volume: &volume,
            generation: 1,
            head: 1,
            path: "",
        },
        Context {
            now: 0,
            approvals: 0,
        },
        &capability,
        checkout.generation_id(),
        revision,
    )?;
    Ok(Arc::new(AccessView::new(view)))
}

struct SelectionOwner<A: 'static, O: 'static> {
    workspace: Workspace<A, O>,
}

impl<A, O> acyclic_fs::native_mount::NativeAccessOwner for SelectionOwner<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn check<'a>(
        &'a self,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::native_mount::NativeMountCallbackFuture<'a>, acyclic_fs::NativeMountError>
    {
        Err(acyclic_fs::MountSourceError::NativeUnsupported.into())
    }

    fn select<'a>(
        &'a self,
        _generation: acyclic_fs::GenerationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_fs::native_mount::NativeMountCallbackFuture<'a, MountOptions>,
        acyclic_fs::NativeMountError,
    > {
        acyclic_fs::native_mount::NativeMountCallbackFuture::new(work, || async move {
            let options = options(&self.workspace, MountOptions::read_only())
                .await
                .map_err(|_| acyclic_fs::MountSourceError::AccessDenied)?;
            Ok(options)
        })
    }
}

/// Uses the shared fixture authority for a writable manually published mount.
#[cfg(windows)]
pub async fn manual_mount_options<
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
>(
    workspace: &Workspace<A, O>,
) -> Result<MountOptions, Box<dyn std::error::Error>> {
    options(
        workspace,
        MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual),
    )
    .await
}
