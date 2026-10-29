use acyclic_harness_conformance::{
    runner,
    substrate::{self, Substrate},
};
use acyclic_stream::*;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

// A remote session owns no process-wide storage exclusion. Its durable service
// happens to be local here; only the public Streams contract crosses this seam.
struct Session(Arc<LocalStream>, OperationClaims);

fn object_store() -> acyclic_fs::MemoryObjectBackend {
    let provider = acyclic_objects::MemoryObjects::memory();
    acyclic_fs::LogicalObjectStore::new(Arc::new(provider))
}

#[async_trait::async_trait]
impl StreamProvider for Session {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(&self.1)
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.0.inspect_idempotency(key).await
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.0.heads(request).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        self.0.write(request)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.0.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::FollowStream, StreamError> {
        self.0.follow(request).await
    }

    async fn read_commit(&self, commit: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.0.read_commit(commit).await
    }
}

struct Streams;
impl Substrate for Streams {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from(["filesystem-policy".into()])
    }
    fn execute(
        &self,
        recipe: serde_json::Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<
        '_,
        Result<
            acyclic_harness_conformance::CaseObservation,
            Box<dyn std::error::Error + Send + Sync>,
        >,
    > {
        Box::pin(async move {
            let directory = tempfile::tempdir()?;
            let durable = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
            let session = Arc::new(Session(durable, OperationClaims::default()));
            let authority = acyclic_fs::StreamAuthorityStore::new(session.clone());
            assert_eq!(
                acyclic_fs::AsyncAuthorityStore::policy_owner(&authority),
                acyclic_fs::PolicyOwner::AtomicPublication
            );
            let filesystem = acyclic_fs::Fs::new(
                authority,
                object_store(),
                acyclic_fs::EmbeddedCapabilities::MEMORY,
            );
            acyclic_harness_conformance::collect_on(recipe, paths, filesystem, session).await
        })
    }
}

fn cases() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scenarios")
}

#[tokio::test]
async fn durable_stream_policy_owner_runs_through_substrate_runner()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for host in [
        Arc::new(Streams) as Arc<dyn Substrate>,
        Arc::new(substrate::Local),
    ] {
        let report = runner::run(&cases(), host, None, 1).await?;
        assert_eq!(report.skipped, 0);
        assert_eq!(report.passed, 1, "{report:?}");
        assert!(report.success());
    }
    // The same client oracle must reject an incorrect observation expectation.
    let directory = tempfile::tempdir()?;
    std::fs::write(
        directory.path().join("negative.json"),
        r#"{
        "driver":"filesystem-policy",
        "expect":{"forked":false,"revoked":true,"policy_version":2}
    }"#,
    )?;
    let report = runner::run(directory.path(), Arc::new(Streams), None, 1).await?;
    assert_eq!(report.failed, 1);
    assert!(!report.success());
    Ok(())
}

#[tokio::test]
async fn stream_policy_revocation_survives_durable_provider_restart()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_fs::{CancellationToken, Fs, WorkBudget};
    let directory = tempfile::tempdir()?;
    let objects = object_store();
    {
        let durable = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let session = Arc::new(Session(durable, OperationClaims::default()));
        let filesystem = Fs::new(
            acyclic_fs::StreamAuthorityStore::new(session.clone()),
            objects.clone(),
            acyclic_fs::EmbeddedCapabilities::MEMORY,
        );
        acyclic_harness_conformance::collect_on(
            serde_json::json!({"driver":"filesystem-policy"}),
            Vec::new(),
            filesystem,
            session,
        )
        .await?;
    }
    // Every authority handle and process claim has gone away. Reopen the actual
    // journal under a new session, keeping only immutable object storage.
    let durable = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let session = Arc::new(Session(durable, OperationClaims::default()));
    let filesystem = Fs::new(
        acyclic_fs::StreamAuthorityStore::new(session),
        objects,
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let source = filesystem
        .open_workspace("policy-owner-source", WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let denied = source
        .with_stable_policy(
            WorkBudget::UNBOUNDED,
            &cancellation,
            || -> BoxProviderFuture<'_, _> {
                Box::pin(
                    source
                        .clone()
                        .bind_coarse_access(&[acyclic_fs::access::Action::Fork], &cancellation),
                )
            },
        )
        .await?
        .value;
    assert!(matches!(
        denied,
        Err(acyclic_fs::WorkspaceError::AccessDenied)
    ));
    let mut checkout = source
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let policy = checkout
        .read_access_policy(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .0
        .ok_or("missing policy")?;
    let policy: acyclic_fs::access::Policy = serde_json::from_slice(&policy.bytes)?;
    assert_eq!(policy.version, 2);
    Ok(())
}
