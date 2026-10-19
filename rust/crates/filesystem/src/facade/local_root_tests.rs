use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn failed_fence_registration_control_uses_the_real_lifecycle_lock()
-> Result<(), Box<dyn std::error::Error>> {
    for eager in [false, true] {
        let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
        let fence = lifecycle.clone().lock_owned().await;
        let owner = OwnershipAnchor::new(());
        let retained = owner.clone();
        let result = run_local_initialization(fence, move |fence| async move {
            if eager {
                let _provisional = OwnershipAnchor::fenced(owner, fence);
                Err::<(), ()>(())
            } else {
                owner
                    .initialize_fenced(fence, |_provisional| async { Err::<(), ()>(()) })
                    .await
            }
        })
        .await?;
        assert!(result.is_err());
        let mut retry = Box::pin(lifecycle.lock_owned());
        let polled = std::future::Future::poll(
            retry.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        );
        assert_eq!(
            polled.is_pending(),
            eager,
            "failed registration poisoned retry"
        );
        drop(polled);
        drop(retained);
        if eager {
            // The negative control is blocked solely by the retained caller clone.
            retry.await;
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_initialization_can_retry_with_the_same_retained_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_owned();
    let owner = OwnershipAnchor::new(directory);
    let retained = owner.clone();
    let mut invalid = LocalOptions::new(&root);
    invalid.stream.journal_operations = 0;
    assert!(
        Fs::local_with_ownership_anchor(invalid, owner.clone())
            .await
            .is_err()
    );

    // The real registry, lifecycle lock, providers and physical cleanup are used.
    // Restoring eager registration makes this same assertion time out.
    let fs = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Fs::local_with_ownership_anchor(LocalOptions::new(&root), owner),
    )
    .await??;
    let released = fs.local_root_release_barrier().ok_or("missing lifecycle")?;
    drop(fs);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !released.is_released() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(root.exists(), "the retained caller still owns the root");
    drop(retained);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while root.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[test]
fn inline_local_root_does_not_create_a_private_tokio_io_pool()
-> Result<(), Box<dyn std::error::Error>> {
    let threads = Arc::new(AtomicUsize::new(0));
    let observed = threads.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .on_thread_start(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .build()?;
    let directory = tempfile::tempdir()?;
    let requested = directory.path().join("created");
    let expected = requested.clone();
    let root = runtime.block_on(async {
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        canonical_local_root(requested, None).await
    })?;
    assert_eq!(root, std::fs::canonicalize(expected)?);
    let check = || {
        assert_eq!(
            threads.load(Ordering::SeqCst),
            0,
            "private blocking pool created"
        )
    };
    check();
    // Run the former implementation, then apply the same resource assertion.
    runtime.block_on(async {
        tokio::task::spawn_blocking(move || std::fs::canonicalize(root))
            .await
            .unwrap()
    })?;
    assert!(std::panic::catch_unwind(check).is_err());
    Ok(())
}
