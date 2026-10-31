use super::*;
use futures::{Future, FutureExt, executor::block_on, future::pending};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Wake(AtomicBool);
impl futures::task::ArcWake for Wake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.0.store(true, Ordering::SeqCst);
    }
}

fn fixture() -> Result<(AuthorityRoot, Scope)> {
    let root = AuthorityRoot::new(
        "custody-port",
        [142; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "original-task".into(),
        },
    );
    let caller = root.bootstrap_scope(
        "caller",
        &Capabilities::new(["read", "list"]),
        None,
        &mut None,
    )?;
    Ok((root, caller))
}

fn lifetime(
    root: &AuthorityRoot,
    caller: &Scope,
) -> Result<(Context, futures::channel::oneshot::Sender<()>)> {
    let (owner, closed) = futures::channel::oneshot::channel();
    Ok((
        Context::new(
            root.verifier(&mut None)?.expiry(caller)? * 1000,
            pending(),
            async {
                let _ = closed.await;
            },
        ),
        owner,
    ))
}

#[test]
fn guest_binding_stop_during_projection_refuses_ready_value() -> Result<()> {
    use acyclic_auth::guest::{GuestAdmission, GuestAdmissionKind};
    let (root, scope) = fixture()?;
    let (context, stop) = lifetime(&root, &scope)?;
    let binding = GuestAdmission::admit(root.admit(scope.clone(), context.clone())?)?;
    let result = binding.project(GuestAdmissionKind::HostAccess, |_, _| {
        drop(stop);
        Ok(())
    });
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(
        root.admit(scope, context)
            .and_then(GuestAdmission::admit)
            .is_err()
    );
    Ok(())
}

struct Forward {
    native: AdmittedAuthority,
    calls: AtomicUsize,
    different_id: bool,
    close: std::sync::Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    hold: bool,
    derive_dropped: AtomicUsize,
    derive_stop: std::sync::Mutex<Option<acyclic_actors::guest::CancellationToken>>,
    revoke: AtomicBool,
    omit_parent: AtomicBool,
    export_append: AtomicBool,
    checkpoint_hold: AtomicBool,
    checkpoint_calls: AtomicUsize,
    checkpoint_dropped: AtomicUsize,
    checkpoint_stop: std::sync::Mutex<Option<acyclic_actors::guest::CancellationToken>>,
}

impl AuthorityOperations for Forward {
    fn check(&self, scope: &Scope) -> Result<()> {
        AuthorityOperations::check(&self.native, scope)
    }
    fn derive<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        parent: &'a Scope,
        id: &'a str,
        request: ScopeDerivation<'a>,
    ) -> BoxProviderFuture<'a, Result<Scope>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            let (capabilities, agent) = request.resolve(parent)?;
            let capabilities = capabilities.as_ref();
            let mut scope = self
                .native
                .derive_request(
                    context,
                    operation,
                    parent,
                    if self.different_id {
                        "different-request"
                    } else {
                        id
                    },
                    request,
                )
                .await?;
            if self.omit_parent.load(Ordering::SeqCst) {
                let Custody::Native(root) = &self.native.custody else {
                    panic!("test source must retain actual native custody");
                };
                let original = workload::verify(
                    &root.verifier(&mut None)?,
                    &root.audience,
                    self.native.caller().1,
                    workload::now(),
                    &mut None,
                )?;
                scope = root.issue_bound(
                    id,
                    capabilities,
                    agent,
                    Some(*parent.proof),
                    Some(&original),
                    &mut None,
                )?;
                // This is a genuinely signed and currently authentic scope,
                // with the requested parent in its signed claims. Only the
                // complete ancestry-preservation check can reject this splice.
                root.verifier(&mut None)?.verify(&scope, &mut None)?;
            }
            if !self.export_append.load(Ordering::SeqCst) {
                scope = scope.seal(&self.native.caller.verifier)?;
            }
            if self.revoke.load(Ordering::SeqCst) {
                let Custody::Native(root) = &self.native.custody else {
                    panic!("test source must retain actual native custody");
                };
                root.revoke_workload(self.native.caller().1)?;
            }
            self.close.lock().expect("test owner lock").take();
            if let Some(stop) = self.derive_stop.lock().expect("test stop lock").take() {
                stop.cancel();
            }
            if self.hold {
                struct Held<'a>(&'a AtomicUsize);
                impl Drop for Held<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let _held = Held(&self.derive_dropped);
                pending::<()>().await;
            }
            Ok(scope)
        })
    }
    fn checkpoint<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        kind: CheckpointKind,
        mut digest: [u8; 32],
    ) -> BoxProviderFuture<'a, Result<[u8; 64]>> {
        self.checkpoint_calls.fetch_add(1, Ordering::SeqCst);
        if self.different_id {
            digest[0] ^= 1;
        }
        Box::pin(async move {
            if let Some(stop) = self.checkpoint_stop.lock().expect("test stop lock").take() {
                stop.cancel();
            }
            if self.checkpoint_hold.load(Ordering::SeqCst) {
                struct Held<'a>(&'a AtomicUsize);
                impl Drop for Held<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let _held = Held(&self.checkpoint_dropped);
                pending::<()>().await;
            }
            self.native
                .checkpoint(context, operation, kind, digest)
                .await
        })
    }
}

fn forwarded(
    root: &AuthorityRoot,
    caller: &Scope,
    context: Context,
    different_id: bool,
    close: Option<futures::channel::oneshot::Sender<()>>,
    hold: bool,
) -> Result<(
    AdmittedAuthority,
    Arc<Forward>,
    futures::channel::oneshot::Sender<()>,
)> {
    let (native_context, native_owner) = lifetime(root, caller)?;
    let source = Arc::new(Forward {
        native: root.admit(caller.clone(), native_context)?,
        calls: AtomicUsize::new(0),
        different_id,
        close: std::sync::Mutex::new(close),
        hold,
        derive_dropped: AtomicUsize::new(0),
        derive_stop: std::sync::Mutex::new(None),
        revoke: AtomicBool::new(false),
        omit_parent: AtomicBool::new(false),
        export_append: AtomicBool::new(false),
        checkpoint_hold: AtomicBool::new(false),
        checkpoint_calls: AtomicUsize::new(0),
        checkpoint_dropped: AtomicUsize::new(0),
        checkpoint_stop: std::sync::Mutex::new(None),
    });
    let verifier = AuthorityVerifier::from_public(
        root.id.clone(),
        root.audience.clone(),
        root.public_workload_trust()?,
        root.verifier(&mut None)?.public_checkpoint_key(),
        root.trust_changes(),
    )?;
    Ok((
        AdmittedAuthority::from_platform(
            verifier,
            source.native.public_scope()?,
            context,
            source.clone(),
            root.trust_changes(),
        )?,
        source,
        native_owner,
    ))
}

#[test]
fn platform_boundary_refuses_append_secrets_and_exports_only_sealed_evidence() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) =
            forwarded(&root, &caller, context.clone(), false, None, false)?;
        assert!(matches!(
            AdmittedAuthority::from_platform(
                port.caller.verifier.clone(),
                caller.clone(),
                context,
                source.clone(),
                root.trust_changes(),
            ),
            Err(Error::Unauthorized(_))
        ));
        let evidence = port.public_scope()?;
        let capability = workload::verify(
            &port.caller.verifier.clone(),
            port.caller().0,
            &evidence,
            workload::now(),
            &mut None,
        )?;
        assert!(capability.is_sealed());
        assert_eq!(capability.block_ids().last(), Some(caller.proof.as_ref()));
        let operation = OperationId::from_bytes([150; 16]);
        let grants = Capabilities::new(["read"]);
        assert!(matches!(
            port.derive(
                &port.context,
                operation,
                &caller,
                "unsealed-request",
                &grants,
                None,
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
        source.export_append.store(true, Ordering::SeqCst);
        assert!(matches!(
            port.derive(
                &port.context,
                operation,
                port.caller().1,
                "leaked-tip",
                &grants,
                None,
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        source.export_append.store(false, Ordering::SeqCst);
        let child = port
            .derive(
                &port.context,
                operation,
                port.caller().1,
                "public-child",
                &grants,
                None,
            )
            .await?;
        let capability = workload::verify(
            &port.caller.verifier.clone(),
            port.caller().0,
            &child,
            workload::now(),
            &mut None,
        )?;
        assert!(capability.is_sealed());
        assert!(matches!(
            capability.attenuate(Vec::new(), &[151; 32]),
            Err(acyclic_auth::verify::VerifyError::Sealed)
        ));
        assert_eq!(source.calls.load(Ordering::Relaxed), 2);
        Ok(())
    })
}

#[test]
fn signed_response_cannot_drop_an_intermediate_revocation_dependency() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) = forwarded(&root, &caller, context, false, None, false)?;
        let grants = Capabilities::new(["read"]);
        let operation = OperationId::from_bytes([149; 16]);
        let parent = port
            .derive(
                &port.context,
                operation,
                port.caller().1,
                "intermediate",
                &grants,
                None,
            )
            .await?;
        source.omit_parent.store(true, Ordering::SeqCst);
        assert!(matches!(
            port.derive(
                &port.context,
                operation,
                &parent,
                "spliced-child",
                &grants,
                None
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(source.calls.load(Ordering::Relaxed), 2);
        Ok(())
    })
}

#[test]
fn admitted_port_uses_real_native_issuance_and_current_revocation() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let capability = root.admit(caller.clone(), context)?;
        let operation = OperationId::from_bytes([143; 16]);
        let narrow = Capabilities::new(["read"]);
        let child = capability
            .derive(
                &capability.context,
                operation,
                &caller,
                "child",
                &narrow,
                None,
            )
            .await?;
        root.verifier(&mut None)?.verify(&child, &mut None)?;
        assert_eq!(child.parent_proof, Some(*caller.proof));
        assert!(
            capability
                .derive(
                    &capability.context,
                    operation,
                    &caller,
                    "wide",
                    &Capabilities::new(["write"]),
                    None
                )
                .await
                .is_err()
        );
        let unrelated = root.bootstrap_scope("other-caller", &narrow, None, &mut None)?;
        assert!(
            capability
                .derive(
                    &capability.context,
                    operation,
                    &unrelated,
                    "cross-caller",
                    &narrow,
                    None
                )
                .await
                .is_err()
        );
        assert!(
            capability
                .derive(
                    &capability.context,
                    operation,
                    &caller,
                    "forged-agent",
                    &narrow,
                    Some(AgentId::from_bytes([144; 16]))
                )
                .await
                .is_err()
        );
        // Negative control: the trusted bootstrap root can issue all three.
        assert!(
            root.bootstrap_scope(
                "wide",
                &Capabilities::new(["write"]),
                Some(AgentId::from_bytes([144; 16])),
                &mut None
            )
            .is_ok()
        );
        root.revoke_workload(&caller)?;
        assert!(
            capability
                .derive(
                    &capability.context,
                    operation,
                    &child,
                    "revoked",
                    &narrow,
                    None
                )
                .await
                .is_err()
        );
        assert!(
            capability
                .checkpoint(
                    &capability.context,
                    operation,
                    CheckpointKind::Working,
                    [1; 32]
                )
                .await
                .is_err()
        );
        Ok(())
    })
}

#[test]
fn public_port_checks_exact_result_and_typed_checkpoint_domain() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        for different_id in [false, true] {
            let (context, _owner) = lifetime(&root, &caller)?;
            let (port, source, _native_owner) =
                forwarded(&root, &caller, context, different_id, None, false)?;
            let operation = OperationId::from_bytes([145; 16]);
            let result = port
                .derive(
                    &port.context,
                    operation,
                    port.caller().1,
                    "requested",
                    &Capabilities::new(["read"]),
                    None,
                )
                .await;
            assert_eq!(result.is_ok(), !different_id);
            assert_eq!(source.calls.load(Ordering::Relaxed), 1);
            let proof = port
                .checkpoint(&port.context, operation, CheckpointKind::Working, [2; 32])
                .await;
            if different_id {
                assert!(matches!(proof, Err(Error::Unauthorized(_))));
                continue;
            }
            let proof = proof?;
            let verifier = root.verifier(&mut None)?;
            verifier.verify_working_checkpoint([2; 32], &proof, &mut None)?;
            assert!(
                verifier
                    .verify_working_checkpoint([3; 32], &proof, &mut None)
                    .is_err()
            );
            assert!(
                verifier
                    .verify_canonical_proof(
                        CheckpointKind::ModelRequest.domain(),
                        &(&verifier.id, &verifier.audience, [2u8; 32]),
                        &proof,
                        &mut None
                    )
                    .is_err()
            );
        }
        Ok(())
    })
}

#[test]
fn revocation_produced_by_issuance_cannot_publish_the_ready_scope() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) = forwarded(&root, &caller, context, false, None, false)?;
        source.revoke.store(true, Ordering::SeqCst);
        assert!(matches!(
            port.derive(
                &port.context,
                OperationId::from_bytes([148; 16]),
                port.caller().1,
                "revoked-in-source",
                &Capabilities::new(["read"]),
                None
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(source.calls.load(Ordering::Relaxed), 1);
        Ok(())
    })
}

#[test]
fn dropped_original_owner_settles_pending_and_same_poll_completion_without_replay() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        let operation = OperationId::from_bytes([146; 16]);
        for (hold, stop_request) in [(false, false), (true, false), (false, true), (true, true)] {
            let (context, owner) = lifetime(&root, &caller)?;
            let (request_context, request_owner) = lifetime(&root, &caller)?;
            let mut owner = Some(owner);
            let mut request_owner = Some(request_owner);
            let stopped = if stop_request {
                request_owner.take()
            } else {
                owner.take()
            };
            let (port, source, _native_owner) =
                forwarded(&root, &caller, context, false, stopped, hold)?;
            // The actual native issue completes, then its original owner is
            // lost. Both a held foreign response and a ready response must end.
            let grants = Capabilities::new(["read"]);
            let request = port.derive(
                &request_context,
                operation,
                port.caller().1,
                "child",
                &grants,
                None,
            );
            futures::pin_mut!(request);
            let wake = Arc::new(Wake::default());
            let waker = futures::task::waker_ref(&wake);
            let mut task = std::task::Context::from_waker(&waker);
            let first = request.as_mut().poll(&mut task);
            let result = if hold {
                assert!(first.is_pending());
                // Loss of the very producer polled by Context must wake the
                // executor. Exactly that event permits the next poll.
                assert!(wake.0.load(Ordering::SeqCst));
                request
                    .now_or_never()
                    .expect("owner-loss wake must settle the wait")
            } else {
                match first {
                    std::task::Poll::Ready(result) => result,
                    std::task::Poll::Pending => {
                        panic!("ready completion must observe the same-poll owner loss")
                    }
                }
            };
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == operation));
            assert_eq!(source.calls.load(Ordering::Relaxed), 1);
            assert!(matches!(
                port.derive(
                    &request_context,
                    operation,
                    port.caller().1,
                    "child",
                    &Capabilities::new(["read"]),
                    None
                )
                .await,
                Err(Error::Cancelled)
            ));
            assert_eq!(source.calls.load(Ordering::Relaxed), 1);
            assert_eq!(port.check().is_ok(), stop_request);
        }
        Ok(())
    })
}

#[test]
fn original_request_stop_prevents_dispatch_under_a_live_custody_lifetime() -> Result<()> {
    block_on(async {
        let (root, caller) = fixture()?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) = forwarded(&root, &caller, context, false, None, false)?;
        let request = Context::new(1, futures::future::ready(()), pending());
        let operation = OperationId::from_bytes([147; 16]);
        assert!(matches!(
            port.derive(
                &request,
                operation,
                port.caller().1,
                "expired-request",
                &Capabilities::new(["read"]),
                None
            )
            .await,
            Err(Error::Cancelled)
        ));
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
        assert!(port.check().is_ok());
        let (request, request_owner) = lifetime(&root, &caller)?;
        drop(request_owner);
        assert!(matches!(
            port.derive(
                &request,
                operation,
                port.caller().1,
                "cancelled-request",
                &Capabilities::new(["read"]),
                None
            )
            .await,
            Err(Error::Cancelled)
        ));
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
        // Negative control: the same custody still makes progress under an
        // independently live request. A request stop cannot close its owner.
        port.derive(
            &port.context,
            operation,
            port.caller().1,
            "live-request",
            &Capabilities::new(["read"]),
            None,
        )
        .await?;
        assert_eq!(source.calls.load(Ordering::Relaxed), 1);
        Ok(())
    })
}

#[test]
fn typed_private_delegation_preserves_owner_policy_and_never_widens_attenuation() -> Result<()> {
    use crate::{
        conversation::{VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    block_on(async {
        let (root, _) = fixture()?;
        let owner = AgentId::from_bytes([181; 16]);
        let reader = AgentId::from_bytes([182; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new(
                "private-port".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
            &mut None,
        )?;
        // Reading another owner's volume does not confer its private ownership.
        let foreign = VolumeRef::new(
            volume.provider().clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(reader),
            &mut None,
        )?;
        let grants = Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            foreign.capability(VolumeOperation::Read, &mut None)?,
        ]);
        let caller = root.bootstrap_scope("owner", &grants, Some(owner), &mut None)?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) =
            forwarded(&root, &caller, context.clone(), false, None, false)?;
        let operation = OperationId::from_bytes([183; 16]);
        let request = ScopeDerivation::PrivateDirectoryRead {
            volume: &volume,
            prefix: "shared",
            reader,
        };
        let child = port
            .derive_request(&context, operation, port.caller().1, "reader", request)
            .await?;
        assert_eq!(child.agent(), Some(reader));
        crate::conversation::ContentGrant::verify_directory_read(
            &port.caller.verifier.clone(),
            &child,
            &volume,
            "shared",
            &mut None,
        )?;
        assert!(
            crate::conversation::ContentGrant::verify_directory_read(
                &port.caller.verifier.clone(),
                &child,
                &volume,
                "other",
                &mut None
            )
            .is_err()
        );
        assert!(
            crate::conversation::ContentGrant::verify(
                &port.caller.verifier.clone(),
                &child,
                &volume,
                VolumeOperation::Write,
                u64::MAX,
                &mut None
            )
            .is_err()
        );
        // An ordinary attenuation cannot use semantic read delegation to add a label.
        assert!(
            port.derive(
                &context,
                operation,
                port.caller().1,
                "widened",
                child.capabilities(),
                Some(reader)
            )
            .await
            .is_err()
        );
        assert!(
            port.derive_request(
                &context,
                operation,
                port.caller().1,
                "foreign",
                ScopeDerivation::PrivateDirectoryRead {
                    volume: &foreign,
                    prefix: "shared",
                    reader
                }
            )
            .await
            .is_err()
        );
        // A recipient cannot become the original owner and delegate another prefix.
        assert!(
            port.derive_request(
                &context,
                operation,
                &child,
                "escape",
                ScopeDerivation::PrivateDirectoryRead {
                    volume: &volume,
                    prefix: "other",
                    reader
                }
            )
            .await
            .is_err()
        );
        assert_eq!(source.calls.load(Ordering::Relaxed), 1);
        Ok(())
    })
}

#[test]
fn exact_child_revocation_refuses_retained_access_with_an_older_guest_snapshot() -> Result<()> {
    block_on(async {
        let (root, _) = fixture()?;
        let volume = crate::conversation::VolumeRef::new(
            crate::resources::ProviderRef::new(
                "exact-child".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "project".into(),
            crate::conversation::VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )?;
        let caller = root.bootstrap_scope(
            "caller",
            &Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]),
            None,
            &mut None,
        )?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) =
            forwarded(&root, &caller, context.clone(), false, None, false)?;
        // Separate public state models the guest's immutable admission snapshot.
        let snapshot = AuthorityVerifier::from_public(
            root.id.clone(),
            root.audience.clone(),
            root.public_workload_trust()?,
            root.verifier(&mut None)?.public_checkpoint_key(),
            root.trust_changes(),
        )?;
        let port = AdmittedAuthority::from_platform(
            snapshot.clone(),
            port.public_scope()?,
            context,
            source.clone(),
            root.trust_changes(),
        )?;
        let runtime = RuntimeAuthority::admitted(port, OperationId::from_bytes([194; 16]));
        let child = runtime
            .issue(
                None,
                "child",
                runtime.caller().expect("caller").1.capabilities(),
                None,
            )
            .await?;
        let verifier = runtime.verifier(&mut None)?;
        let retained = crate::conversation::ContentGrant::verify(
            &verifier,
            &child,
            &volume,
            VolumeOperation::Read,
            u64::MAX,
            &mut None,
        )?;
        retained.require(&volume, VolumeOperation::Read, None)?;
        root.revoke_workload(&child)?;
        source.native.check()?; // Original caller remains live.
        snapshot.verify(&child, &mut None)?; // Negative control: old public state accepts it.
        assert!(verifier.verify(&child, &mut None).is_err());
        assert!(
            retained
                .require(&volume, VolumeOperation::Read, None)
                .is_err()
        );
        Ok(())
    })
}

#[test]
fn shared_runtime_checkpoint_owner_loss_refuses_without_fabricating_custody() -> Result<()> {
    block_on(async {
        let (root, _) = fixture()?;
        let volume = crate::conversation::VolumeRef::new(
            crate::resources::ProviderRef::new(
                "retained-port".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "project".into(),
            crate::conversation::VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )?;
        let caller = root.bootstrap_scope(
            "caller",
            &Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]),
            None,
            &mut None,
        )?;
        let (context, owner) = lifetime(&root, &caller)?;
        let (port, _source, _native_owner) =
            forwarded(&root, &caller, context, false, None, false)?;
        let runtime = RuntimeAuthority::admitted(port, OperationId::from_bytes([184; 16]));
        assert!(runtime.native().is_none());
        let verifier = runtime.verifier(&mut None)?;
        let digest = [185; 32];
        let proof = verifier.working_checkpoint_proof(digest, &mut None).await?;
        verifier.verify_working_checkpoint(digest, &proof, &mut None)?;
        let mut wrong_digest = digest;
        wrong_digest[0] ^= 1;
        assert!(
            verifier
                .verify_working_checkpoint(wrong_digest, &proof, &mut None)
                .is_err()
        );
        assert!(
            runtime
                .with_audience(Authority {
                    kind: AggregateKind::Task,
                    id: "another-recipient".into()
                })
                .is_err()
        );
        let retained = crate::conversation::ContentGrant::verify(
            &verifier,
            runtime.caller().expect("admitted caller").1,
            &volume,
            VolumeOperation::Read,
            u64::MAX,
            &mut None,
        )?;
        retained.require(&volume, VolumeOperation::Read, None)?;
        verifier.verify(runtime.caller().expect("admitted caller").1, &mut None)?;
        drop(owner);
        assert!(
            retained
                .require(&volume, VolumeOperation::Read, None)
                .is_err()
        );
        assert!(
            verifier
                .verify(runtime.caller().expect("admitted caller").1, &mut None)
                .is_err()
        );
        assert!(
            verifier
                .working_checkpoint_proof(digest, &mut None)
                .await
                .is_err()
        );
        drop(runtime);
        assert!(
            verifier
                .working_checkpoint_proof(digest, &mut None)
                .await
                .is_err()
        );
        // Retired runtime custody does not erase public evidence already committed.
        verifier.verify_working_checkpoint(digest, &proof, &mut None)?;
        Ok(())
    })
}

#[cfg(feature = "filesystem")]
#[test]
fn original_turn_interrupt_releases_scope_derivation_before_journal_binding() -> Result<()> {
    block_on(async {
        use crate::agent::{AgentPath, AgentState, ForkAdopted, TurnHost, VolumeBinding};
        use crate::conversation::{VolumeClass, VolumeRef};
        use crate::filesystem::{
            FilesystemHost, FilesystemTurnHost, PRIVATE_VOLUME, TurnAdmission, TurnAuthority,
            TurnExecutors, TurnLimits,
        };
        struct Executed(AtomicUsize);
        impl TurnExecutors for Executed {
            fn executor<'a>(
                &'a self,
                _: &'a AgentState,
                _: OperationId,
                _: &'a crate::model::ModelContent,
                _: &'a TurnAuthority,
            ) -> BoxProviderFuture<'a, Result<Arc<dyn crate::executor::Executor>>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(Error::Invalid("executor reached".into())) })
            }
        }
        let (root, _) = fixture()?;
        let provider = crate::resources::ProviderRef::new(
            "derive-stop".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let agent = crate::agent::AgentId::from_bytes([195; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?;
        let host = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::memory(),
            &provider,
            None,
        )?);
        host.create_volume(&volume).await?;
        let ceiling =
            acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
                .to_scope_label()
                .map_err(|_| Error::Invalid("fixture ceiling".into()))?;
        let caller = root.bootstrap_scope(
            "turn",
            &Capabilities::new([ceiling]),
            Some(AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?;
        let state = AgentState::adopted(
            AgentPath::root(&agent),
            &ForkAdopted {
                profile: None,
                agent,
                parent: None,
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes: vec![VolumeBinding {
                    name: PRIVATE_VOLUME.into(),
                    volume,
                }],
                brief: None,
                policy: None,
            },
        )
        .expect("adopts");
        let executors = Arc::new(Executed(AtomicUsize::new(0)));
        let input = crate::model::ModelContent::Text("turn".into());
        for held in [true, false] {
            let (context, _owner) = lifetime(&root, &caller)?;
            let (port, source, _native_owner) =
                forwarded(&root, &caller, context, false, None, held)?;
            let turn_host = FilesystemTurnHost::new(
                Arc::new(acyclic_stream::MemoryStream::default()),
                Arc::clone(&host),
                RuntimeAuthority::admitted(port, OperationId::from_bytes([196; 16])),
                TurnAdmission::Admitted {
                    audience: root.audience.clone(),
                    scope: source.native.public_scope()?,
                },
                executors.clone(),
                TurnLimits {
                    maximum_bytes: u64::MAX,
                    max_steps: u32::MAX,
                    history_turns: u32::MAX,
                },
                Default::default(),
            )
            .with_goals(None);
            let stop = acyclic_actors::guest::CancellationToken::new();
            if !held {
                *source.derive_stop.lock().expect("test stop lock") = Some(stop.clone());
            }
            let mut opening = turn_host.open(
                &state,
                OperationId::from_bytes([197; 16]),
                &input,
                stop.clone(),
            );
            let wake = Arc::new(Wake::default());
            let waker = futures::task::waker_ref(&wake);
            let mut cx = std::task::Context::from_waker(&waker);
            if held {
                match opening.as_mut().poll(&mut cx) {
                    std::task::Poll::Pending => (),
                    std::task::Poll::Ready(Err(error)) => {
                        panic!("turn failed before held derivation: {error:?}")
                    }
                    std::task::Poll::Ready(Ok(_)) => panic!("held derivation completed"),
                }
                assert_eq!(source.calls.load(Ordering::SeqCst), 1);
                wake.0.store(false, Ordering::SeqCst);
                stop.cancel();
                assert!(
                    wake.0.load(Ordering::SeqCst),
                    "turn interrupt did not wake scope derivation"
                );
            }
            assert!(matches!(
                opening.as_mut().poll(&mut cx),
                std::task::Poll::Ready(Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                )))
            ));
            drop(opening);
            assert_eq!(
                source.derive_dropped.load(Ordering::SeqCst),
                usize::from(held)
            );
            assert_eq!(source.calls.load(Ordering::SeqCst), 1);
            assert_eq!(executors.0.load(Ordering::SeqCst), 0);
            let stopped = acyclic_actors::guest::CancellationToken::new();
            stopped.cancel();
            assert!(matches!(
                turn_host
                    .open(&state, OperationId::from_bytes([198; 16]), &input, stopped)
                    .await,
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                ))
            ));
            assert_eq!(source.calls.load(Ordering::SeqCst), 1);
            if !held {
                // A fresh turn still derives under this same live custodian.
                assert!(
                    matches!(turn_host.open(&state, OperationId::from_bytes([199; 16]), &input,
                    acyclic_actors::guest::CancellationToken::new()).await,
                    Err(Error::Invalid(reason)) if reason == "executor reached")
                );
                assert_eq!(source.calls.load(Ordering::SeqCst), 2);
                assert_eq!(executors.0.load(Ordering::SeqCst), 1);
            }
        }
        Ok(())
    })
}

#[cfg(feature = "filesystem")]
#[test]
fn original_turn_interrupt_wakes_every_checkpoint_and_refuses_ready_completion() -> Result<()> {
    block_on(async {
        use crate::conversation::{VolumeClass, VolumeRef};
        use crate::executor::ExecutionJournal as _;
        use crate::filesystem::{
            FilesystemExecutionJournal, FilesystemHost, FilesystemJournalBinding,
        };
        let (root, _) = fixture()?;
        let provider = crate::resources::ProviderRef::new(
            "checkpoint-stop".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let agent = AgentId::from_bytes([186; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        host.create_volume(&volume).await?;
        let caller = root.bootstrap_scope(
            "turn",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            &mut None,
        )?;
        let (context, _owner) = lifetime(&root, &caller)?;
        let (port, source, _native_owner) = forwarded(&root, &caller, context, false, None, false)?;
        let runtime = RuntimeAuthority::admitted(port, OperationId::from_bytes([187; 16]));
        let verifier = runtime.verifier(&mut None)?;
        let mut journal = FilesystemExecutionJournal::new(
            Arc::new(acyclic_stream::MemoryStream::default()),
            host,
            FilesystemJournalBinding {
                volume: &volume,
                verifier: &verifier,
                scope: runtime.caller().expect("original caller").1,
                io: None,
            },
        )?;
        let digest = [188; 32];
        for (index, kind) in [
            CheckpointKind::Execution,
            CheckpointKind::Working,
            CheckpointKind::ModelRequest,
        ]
        .into_iter()
        .enumerate()
        {
            let stop = acyclic_actors::guest::CancellationToken::new();
            journal = journal.with_stop(stop.clone());
            let bound = journal
                .working_checkpoint_authority()
                .expect("journal custody");
            source.checkpoint_hold.store(true, Ordering::SeqCst);
            let mut proof: BoxProviderFuture<'_, Result<[u8; 64]>> = match kind {
                CheckpointKind::Execution => {
                    Box::pin(bound.execution_checkpoint_proof(digest, &mut None))
                }
                CheckpointKind::Working => {
                    Box::pin(bound.working_checkpoint_proof(digest, &mut None))
                }
                CheckpointKind::ModelRequest => {
                    Box::pin(bound.model_request_proof(digest, &mut None))
                }
            };
            let wake = Arc::new(Wake::default());
            let waker = futures::task::waker_ref(&wake);
            let mut cx = std::task::Context::from_waker(&waker);
            assert!(proof.as_mut().poll(&mut cx).is_pending());
            assert_eq!(source.checkpoint_calls.load(Ordering::SeqCst), index + 1);
            wake.0.store(false, Ordering::SeqCst);
            stop.cancel();
            assert!(
                wake.0.load(Ordering::SeqCst),
                "original turn did not wake its checkpoint"
            );
            assert!(matches!(
                proof.as_mut().poll(&mut cx),
                std::task::Poll::Ready(Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                )))
            ));
        }
        assert_eq!(source.checkpoint_dropped.load(Ordering::SeqCst), 3);
        // An already stopped turn never dispatches, even though its owner is live.
        let stop = acyclic_actors::guest::CancellationToken::new();
        stop.cancel();
        journal = journal.with_stop(stop);
        let bound = journal
            .working_checkpoint_authority()
            .expect("journal custody");
        assert!(matches!(
            bound.working_checkpoint_proof(digest, &mut None).await,
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled
            ))
        ));
        assert_eq!(source.checkpoint_calls.load(Ordering::SeqCst), 3);
        // The real custodian can return a valid signature while producing the interrupt.
        let stop = acyclic_actors::guest::CancellationToken::new();
        journal = journal.with_stop(stop.clone());
        *source.checkpoint_stop.lock().expect("test stop lock") = Some(stop);
        source.checkpoint_hold.store(false, Ordering::SeqCst);
        let bound = journal
            .working_checkpoint_authority()
            .expect("journal custody");
        assert!(matches!(
            bound.working_checkpoint_proof(digest, &mut None).await,
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled
            ))
        ));
        assert_eq!(source.checkpoint_calls.load(Ordering::SeqCst), 4);
        // A fresh turn under that same original owner still signs and verifies.
        journal = journal.with_stop(acyclic_actors::guest::CancellationToken::new());
        let bound = journal
            .working_checkpoint_authority()
            .expect("journal custody");
        let proof = bound.working_checkpoint_proof(digest, &mut None).await?;
        bound.verify_working_checkpoint(digest, &proof, &mut None)?;
        Ok(())
    })
}
