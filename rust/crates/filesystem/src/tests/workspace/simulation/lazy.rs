//! Real native demand source with a sparse authored overlay.
use super::*;
use crate::{LazyWorkspace, MemoryLazyWorkspaceStore};

pub(super) async fn scenario(steps: usize) -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let source_bytes = vec![choice(); 37];
    std::fs::write(directory.path().join("source"), &source_bytes)?;
    let cancel = CancellationToken::new();
    let source = crate::demand::native::NativeDemandSource::open(
        std::borrow::Cow::Borrowed(directory.path()),
        crate::FilesystemProfile::Portable,
        crate::VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &cancel,
    )
    .await?
    .value;
    let fs = Fs::memory();
    let mut ctx = crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let (lazy, _) = LazyWorkspace::attach(
        &fs,
        "lazy",
        Arc::new(source),
        MemoryLazyWorkspaceStore::default(),
        crate::VolumeInitialization {
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            access_policy: None,
        },
        crate::OperationId::new(),
        &mut ctx,
    )
    .await?;
    assert!(unbounded!(lazy.workspace(), stat("/source")).is_err());
    assert_eq!(
        lazy.read("/source", 64, &mut ctx).await?.bytes.as_ref(),
        source_bytes
    );
    assert!(
        unbounded!(lazy.workspace(), stat("/source")).is_err(),
        "reading an unresolved source file eagerly authored it"
    );

    let mut current = vec![choice(); 11];
    lazy.write(
        "/authored",
        current.clone().into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        &mut ctx,
    )
    .await?;
    let mut retained = Vec::new();
    for _ in 0..steps {
        let cut = lazy.snapshot(&mut ctx).await?;
        retained.push((cut, current.clone()));
        current = vec![choice(); 1 + usize::from(choice() % 31)];
        lazy.write(
            "/authored",
            current.clone().into(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            &mut ctx,
        )
        .await?;
        assert_eq!(
            lazy.read("/authored", 64, &mut ctx).await?.bytes.as_ref(),
            current
        );
        // All retained cuts must remain immutable across later publications.
        for (snapshot, expected) in &retained {
            let (bytes, from_source) = lazy
                .read_at_snapshot(*snapshot, "/authored", None, 64, &mut ctx)
                .await?;
            assert!(!from_source);
            assert_eq!(bytes.bytes.as_ref(), expected);
        }
        assert!(unbounded!(lazy.workspace(), stat("/source")).is_err());
    }
    Ok(())
}
