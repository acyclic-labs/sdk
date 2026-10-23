use super::access::{compiled, deny, seed};
use super::*;
use crate::access::AccessView;
use crate::access::{Action, Policy};

struct Reload {
    source: std::sync::Weak<MemorySource>,
}

impl crate::native_mount::NativeAccessOwner for Reload {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, NativeMountError> {
        crate::native_mount::NativeMountCallbackFuture::new(work, || async move {
            let source = self.source.upgrade().ok_or(MountSourceError::Stale)?;
            let mut checkout = source
                .checkout
                .volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(MountSourceError::from)?;
            let (bytes, revision) = checkout
                .read_access_policy(work, work.cancellation())
                .await
                .map_err(MountSourceError::from)?;
            let policy: Policy =
                serde_json::from_slice(&bytes.ok_or(MountSourceError::AccessDenied)?.bytes)
                    .map_err(|_| MountSourceError::AccessDenied)?;
            let view = super::access::compile_policy(
                &source,
                &policy,
                None,
                checkout.generation_id(),
                revision,
            )?
            .with_data_generation(source.checkout.lock().await.generation_id());
            source
                .access_view
                .as_ref()
                .ok_or(MountSourceError::AccessDenied)?
                .publish(view)
                .map_err(|_| MountSourceError::AccessDenied)?;
            Ok(())
        })
    }

    fn select<'a>(
        &'a self,
        _generation: crate::GenerationId,
        _work: &'a crate::NativeMaterializationWork,
    ) -> Result<
        crate::native_mount::NativeMountCallbackFuture<'a, crate::MountOptions>,
        NativeMountError,
    > {
        Err(MountSourceError::NativeUnsupported.into())
    }
}

pub(super) fn publish<A, O>(
    source: &CheckoutMountSource<A, O>,
    policy: &Policy,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    invoke(&source.runtime, &work, || async {
        let mut checkout = source
            .checkout
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &work,
                work.cancellation(),
            )
            .await?;
        checkout
            .write_access_policy(
                &serde_json::to_vec(policy).unwrap(),
                crate::NamedAttributeWriteMode::Upsert,
                &work,
                work.cancellation(),
            )
            .await?;
        checkout
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                crate::PublicationPermit::Unrestricted,
                BOUNDARY_BUDGET,
                work.cancellation(),
                |_| async {
                    Ok::<_, crate::OperationFailure<FsError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| MountSourceError::from(failure.error))?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn cached_denial_observes_owner_policy_publication_before_host_probes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "file", b"pinned")?;
    let selected = checkout_state(&source).0;
    let denied = crate::access::fixtures::denied(Action::Read, "file");
    publish(&source, &denied)?;
    let view = compiled(
        &source,
        1,
        vec![deny("restricted", &[Action::Read], "file")],
        None,
    )?
    .with_data_generation(selected);
    let source = source.with_access_view(Arc::new(AccessView::new(view)))?;
    let source = Arc::new_cyclic(|weak| {
        source.with_access_refresh(Some(Arc::new(Reload {
            source: weak.clone(),
        })))
    });
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("file");
    assert!(
        source
            .require_paths(&[(Action::Read, &path)], &work)
            .is_err()
    );
    let mut allowed: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    allowed.version = denied.version + 1;
    publish(&source, &allowed)?;
    source.require_paths(&[(Action::Read, &path)], &work)?;
    assert_eq!(
        source.open_file(&path)?.read_range(0, 6)?.as_ref(),
        b"pinned"
    );
    assert!(
        source
            .access_view
            .as_ref()
            .unwrap()
            .current()?
            .decide(
                Action::Read,
                "file",
                crate::native_mount::adapter::access::access_time(
                    &crate::native_mount::clock::Clock::capture()
                )?
            )
            .allowed
    );
    Ok(())
}
