use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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
        canonical_local_root(requested).await
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
