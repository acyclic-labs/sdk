use super::*;

use crate::core::{AuthorityVerifier, Scope};

use crate::{AgentId, Capabilities};

use acyclic_fs::{CancellationToken, WorkBudget};

use acyclic_stream::StreamPath;

use bytes::Bytes;

async fn publish(
    parent: &mut StreamAggregate<MemoryStream>,
    report: ForkReport,
    scope: Scope,
    owner: &crate::durable_host::TaskJournalOwner<MemoryStream>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<ApplyResult>, Error> {
    let mut work = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?,
    );
    let result = async {
        work.preflight()?;
        parent
            .publish_fork_report(report, scope, Some(owner), &mut Some(&mut work))
            .await
    }
    .await;
    work.finish(result)
}

async fn bind(
    child: &mut StreamAggregate<MemoryStream>,
    parent: &StreamAggregate<MemoryStream>,
    seed: &ForkSeed,
    scope: Scope,
    owner: &crate::durable_host::TaskJournalOwner<MemoryStream>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
    let mut work = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?,
    );
    let result = async {
        work.preflight()?;
        child
            .bind_published_child(parent, seed, scope, Some(owner), &mut Some(&mut work))
            .await
    }
    .await;
    work.finish(result)
}

fn publication_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_bytes_read: 1_048_576,
        backend_read_operations: 32,
        source_bytes_read: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        authority_records_appended: 32,
        authority_bytes_written: 1_048_576,
        backend_write_operations: 32,
        output_bytes: 1_048_576,
        items_examined: 32,
        items_returned: 32,
        allocation_operations: 128,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[cfg(feature = "filesystem")]
struct GuardedForkFixture {
    fixture: crate::durable_host::tests::created_resource_access::CreationFixture,
    owner: Arc<crate::durable_host::TaskJournalOwner<MemoryStream>>,
    lease: crate::distributed::WorkLease,
    parent: StreamAggregate<MemoryStream>,
    report: crate::fork::ForkReport,
    parent_scope: Scope,
    parent_verifier: AuthorityVerifier,
    child_scope: Scope,
    child_verifier: AuthorityVerifier,
}

#[cfg(feature = "filesystem")]
async fn guarded_fork_fixture() -> Result<GuardedForkFixture> {
    use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
    use crate::core::SchemaRegistry;
    use crate::filesystem::{
        FilesystemContentVerifier, FilesystemForkPreparer, FilesystemForkVerifier,
    };
    use crate::fork::{
        CompositeForkVerifier, ForkPreparer, ResourceRevision, StreamHistoryForkVerifier,
    };
    use crate::resources::StreamRef;
    let fixture =
        crate::durable_host::tests::created_resource_access::CreationFixture::new().await?;
    let created = fixture
        .create_filesystem_child_with_grants(&["fork:publish", "conversation:bind"])
        .await?;
    let stream_provider = crate::resources::ProviderRef::new(
        "created-controls".into(),
        "stream".into(),
        "2".into(),
        &mut None,
    )?;
    let verifier = CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(fixture.filesystem.clone(), 65_536, None, None)?,
            StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?;
    let mut original = StreamAggregate::open(
        &fixture.stream,
        created.seed.parent.clone(),
        created.parent_verifier.clone(),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_fork_verifier(verifier.clone());
    original
        .publish_fork_report(
            created.report.clone(),
            created.parent_scope.clone(),
            None,
            &mut None,
        )
        .await?;
    let parent_issuer =
        crate::core::AuthorityIssuer::new("created-owner", [80; 32], created.seed.child.clone());
    let parent_scope = parent_issuer
        .issue(
            "measured-fork-parent",
            &Capabilities::new([
                "conversation:bind".into(),
                "fork:publish".into(),
                created
                    .private
                    .capability(VolumeOperation::Read, &mut None)?,
                created
                    .project
                    .capability(VolumeOperation::Read, &mut None)?,
            ]),
            Some(created.seed.child_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut parent = StreamAggregate::open(
        &fixture.stream,
        created.seed.child.clone(),
        parent_issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_fork_verifier(verifier);
    parent
        .bind_published_child(
            &original,
            &created.seed,
            parent_scope.clone(),
            None,
            &mut None,
        )
        .await?;
    let task = crate::TaskId::from_bytes([84; 16]);
    assert!(
        matches!(fixture.admit_child(created.request.child.clone()).await?, crate::Admission::Accepted(id) if id == task)
    );
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.recover_work(task, &mut None).await?
    else {
        return Err(Error::NotFound("measured fork original owner lease".into()));
    };
    let owner = fixture
        .host
        .journal_owner(task, (&lease.reservation).into(), None)
        .await?;
    let child_agent = AgentId::from_bytes([94; 16]);
    let project = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "measured-child-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("created-project".into()),
        &mut None,
    )?;
    let private = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "measured-child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    let project_generation = created
        .seed
        .resources
        .iter()
        .find_map(|resource| match &resource.revision {
            ResourceRevision::Project { generation, .. } => Some(generation.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::Invalid("original created project generation missing".into()))?;
    let mut fork = created.report.request.clone();
    fork.operation_id = OperationId::from_bytes([95; 16]);
    fork.parent = created.seed.child.clone();
    fork.parent_revision = parent.reducer().revision();
    fork.child = Authority {
        kind: AggregateKind::Conversation,
        id: "measured-fork-grandchild".into(),
    };
    fork.child_agent = child_agent;
    fork.preparation.child_project_volume = project.clone();
    fork.preparation.child_private_volume = private.clone();
    fork.selections[0].revision = ResourceRevision::History(StreamRef::new(
        stream_provider.clone(),
        fork.parent.stream_path(&mut None)?.into_bytes(),
        Some(fork.parent_revision.to_string()),
    )?);
    fork.selections[1].revision = ResourceRevision::Project {
        volume: created.project.clone(),
        generation: project_generation,
    };
    fork.selections[2].revision = ResourceRevision::PrivateVolume {
        volume: created.private.clone(),
        generation: created.seed.child_private_generation.clone(),
        paths: Vec::new(),
    };
    let history = crate::store::HistoryReader::new(
        &fixture.stream,
        &fork.parent,
        &(parent_issuer.verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?
    .fork_history(
        parent.reducer(),
        0,
        crate::store::HistoryReadLimits {
            maximum_events: 2,
            maximum_bytes: 65_536,
        },
        &mut None,
    )
    .await?;
    let resolver = FilesystemContentVerifier::new(
        fixture.filesystem.clone(),
        &parent_issuer.verifier(&mut None).expect("verifier copy"),
        &parent_scope,
        65_536,
        None,
    )?;
    let preparer = FilesystemForkPreparer::new(
        fixture.filesystem.clone(),
        parent.reducer().copy(&mut None)?,
        parent_issuer.verifier(&mut None).expect("verifier copy"),
        parent_scope.clone(),
        created.project.clone(),
        stream_provider,
        resolver,
        Some(history),
        None,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?;
    let mut admission = fixture.admission(85, serde_json::json!({"action":"filesystem-io"}));
    admission.parent = Some(task);
    admission.grants = Capabilities::new([
        private.capability(VolumeOperation::Read, &mut None)?,
        private.capability(VolumeOperation::Write, &mut None)?,
        project.capability(VolumeOperation::Read, &mut None)?,
        project.capability(VolumeOperation::Write, &mut None)?,
        "conversation:bind".into(),
    ]);
    let (resources, proofs) = preparer.creation_allocations(&fork, task, None)?;
    admission = admission.with_resource_proofs(proofs)?;
    let mut request = created.request.clone();
    request.child = admission;
    request.resources = resources;
    request.payload =
        serde_json::to_value(&fork).map_err(|error| Error::Invalid(error.to_string()))?;
    let owner = Arc::new(owner);
    let preparation = owner.resource_preparation(request, &mut None).await?;
    let report = preparer
        .with_task_preparation(Arc::new(preparation))?
        .prepare(fork)
        .await?;
    let child_issuer =
        crate::core::AuthorityIssuer::new("created-owner", [80; 32], report.request.child.clone());
    let child_scope = child_issuer
        .issue(
            "measured-fork-child",
            &Capabilities::new(["conversation:bind".into()]),
            Some(child_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    Ok(GuardedForkFixture {
        fixture,
        owner,
        lease,
        parent,
        report,
        parent_scope,
        parent_verifier: parent_issuer.verifier(&mut None).expect("verifier copy"),
        child_scope,
        child_verifier: child_issuer.verifier(&mut None).expect("verifier copy"),
    })
}

#[cfg(feature = "filesystem")]
fn guarded_publication_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 4_096,
        authority_bytes_read: 16_777_216,
        backend_read_operations: 4_096,
        source_bytes_read: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        authority_records_appended: 4_096,
        authority_bytes_written: 16_777_216,
        backend_write_operations: 4_096,
        output_bytes: 16_777_216,
        items_examined: 1_000_000,
        items_returned: 4_096,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 268_435_456,
        ..WorkBudget::default()
    }
}

fn projection_codec_budget() -> WorkBudget {
    WorkBudget {
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 16 * 1_048_576,
        bytes_copied: 16 * 1_048_576,
        bytes_encoded: 16 * 1_048_576,
        bytes_hashed: 16 * 1_048_576,
        output_bytes: 16 * 1_048_576,
        ..publication_budget()
    }
}

mod projections;
mod publication;
mod roots;
