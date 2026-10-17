use super::*;

type LocalMount = LazyMount<
    crate::LocalAuthorityBackend,
    crate::LocalObjectBackend,
    crate::demand::native::NativeDemandSource,
    crate::LocalCoreStateStore,
>;

fn future_bytes<F: Future>(_: impl FnOnce() -> F) -> usize {
    std::mem::size_of::<F>()
}

#[test]
fn local_lazy_mount_shutdown_futures_stay_bounded() {
    // Infer the real local-provider futures without starting a native mount.
    // Neither factory is invoked; their exact return types determine the sizes.
    let mount: Option<&LocalMount> = None;
    for (name, size) in [
        ("sync", future_bytes(|| mount.unwrap().sync())),
        ("unmount", future_bytes(|| mount.unwrap().unmount())),
    ] {
        eprintln!("native local lazy mount {name} future: {size} bytes");
        assert!(
            size < 16 * 1024,
            "native {name} future grew to {size} bytes"
        );
    }
}
