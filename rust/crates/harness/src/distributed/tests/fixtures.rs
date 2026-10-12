use super::*;

pub(super) type LostSessionAck = crate::test_stream::LostSessionAck<acyclic_stream::MemoryStream>;

pub(super) fn state_ref() -> Result<FileRef> {
    let volume = VolumeRef::new(
        ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()), &mut None,
    )?;
    FileRef::new(
        volume,
        "state/initial.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(b"null", "application/json", &mut None)?,
        "initial.json".into(), &mut None,
    )
}

pub(super) fn owner_issuer() -> (Authority, AuthorityIssuer) {
    let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
    let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    (owner, issuer)
}

pub(super) fn declaration_scope(owner: &Authority) -> Scope {
    AuthorityIssuer::new("test-runtime", [9; 32], owner.clone()).issue("original-declare", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof")
}

pub(super) fn spec(operation_id: OperationId, cpu: u64) -> Result<OperationSpec> {
    let (authority, issuer) = owner_issuer();
    Ok(OperationSpec {
        operation_id,
        parent: None,
        owner_scope: issuer.issue("original-declare", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof"),
        owner: DurableOwner::Detached { authority },
        entrypoint: EntrypointRef {
            name: "example.task".into(),
            version: "1".into(),
            digest: [2; 32],
            result_schema: Value::Object(Default::default()),
        },
        dependencies: BTreeSet::new(),
        resources: ResourceRequest(BTreeMap::from([("cpu".into(), cpu)])),
        placement: BTreeMap::new(),
        orchestration: Orchestration::Leaf,
        state: state_ref()?,
    })
}

pub(super) async fn declare<P: StreamProvider>(
    coordinator: &mut DistributedCoordinator<P>,
    spec: OperationSpec,
    key: &str,
) -> Result<bool> {
    let owner = spec.owner.authority().clone();
    let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let scope = issuer.issue("declare", &Capabilities::new(["operation:declare"]), None, None, &mut None).expect("scope proof");
    coordinator
        .declare_operation(
            &owner,
            &scope,
            &issuer.verifier(&mut None).expect("verifier copy"),
            spec,
            IdempotencyKey::new(key)?,
            None, &mut None,
        )
        .await
}

pub(super) async fn start<P: StreamProvider>(
    coordinator: &mut DistributedCoordinator<P>, worker: &Worker, key: &str,
) -> Result<WorkLease> {
    let lease = coordinator.pull(worker).await?
        .ok_or_else(|| Error::NotFound("lease".into()))?;
    coordinator.apply(IdempotencyKey::new(key)?, SchedulerEvent::Started {
        operation_id: lease.operation.operation_id,
        fence: LeaseFence::from(&lease.reservation),
    }, &mut None).await?;
    Ok(lease)
}

pub(super) struct TestContentVerifier;

impl ContentResidencyVerifier for TestContentVerifier {
    fn read<'a>(&'a self, _: &'a FileRef, maximum: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, OwnedBytes>> {
        if io.is_some() || maximum < 4 {
            return Err(Error::Unsupported("fixture has no finite content reader".into()));
        }
        crate::conversation::BoundedContentFuture::new(io, || async {
            Ok(OwnedBytes::from_static(b"null"))
        })
    }
}

#[derive(Default)]
pub(super) struct PausedChildVerifier {
    pub(super) armed: std::sync::atomic::AtomicBool,
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
}

impl ContentResidencyVerifier for PausedChildVerifier {
    fn verify<'a>(&'a self, reference: &'a FileRef, maximum: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            TestContentVerifier.verify(reference, maximum, io)?.await
        })
    }
}

pub(super) struct DenyContentVerifier;

impl ContentResidencyVerifier for DenyContentVerifier {}
