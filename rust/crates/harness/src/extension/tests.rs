use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TestExtension {
    identity: ExtensionIdentity,
    disposed: Arc<AtomicUsize>,
}

impl NativeExtension for TestExtension {
    fn identity(&self) -> ExtensionIdentity {
        self.identity.clone()
    }

    fn dispose(&self) {
        self.disposed.fetch_add(1, Ordering::AcqRel);
    }
}

fn extension(version: u32, disposed: Arc<AtomicUsize>) -> Result<Arc<TestExtension>> {
    let digest_byte = u8::try_from(version)
        .map_err(|_| Error::Invalid("test extension version is out of range".into()))?;
    Ok(Arc::new(TestExtension {
        identity: ExtensionIdentity::new("example.extension", version, [digest_byte; 32])?,
        disposed,
    }))
}

#[test]
fn upgrade_keeps_prior_exact_version_until_explicit_removal() -> Result<()> {
    let registry = ExtensionRegistry::default();
    let disposed = Arc::new(AtomicUsize::new(0));
    let first = extension(1, Arc::clone(&disposed))?;
    let first_identity = first.identity();
    registry.install(first)?;
    let runtime = ExtensionRuntime::activate(registry.clone(), [first_identity.clone()])?;

    registry.install(extension(2, Arc::clone(&disposed))?)?;
    assert_eq!(disposed.load(Ordering::Acquire), 0);

    registry.disable("example.extension")?;
    assert!(matches!(
        registry.remove(&first_identity),
        Err(Error::Conflict(_))
    ));
    drop(runtime);
    registry.remove(&first_identity)?;
    assert_eq!(disposed.load(Ordering::Acquire), 1);
    Ok(())
}

#[test]
fn runtime_disable_is_local_to_its_composition() -> Result<()> {
    let registry = ExtensionRegistry::default();
    let disposed = Arc::new(AtomicUsize::new(0));
    let implementation = extension(1, disposed)?;
    let identity = implementation.identity();
    registry.install(implementation)?;
    let runtime = ExtensionRuntime::activate(registry.clone(), [identity.clone()])?;
    runtime.disable()?;
    ExtensionRuntime::activate(registry.clone(), [identity])?;
    assert!(matches!(
        runtime.retain(["extension:example.extension@1".into()], false),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn registry_disable_blocks_new_activation_but_not_reconciliation() -> Result<()> {
    let registry = ExtensionRegistry::default();
    let disposed = Arc::new(AtomicUsize::new(0));
    let implementation = extension(1, disposed)?;
    let identity = implementation.identity();
    registry.install(implementation)?;
    let runtime = ExtensionRuntime::activate(registry.clone(), [identity.clone()])?;
    registry.disable("example.extension")?;
    assert!(matches!(
        ExtensionRuntime::activate(registry, [identity]),
        Err(Error::Conflict(_))
    ));
    let retained = runtime.retain(["extension:example.extension@1".into()], true)?;
    assert_eq!(retained.len(), 1);
    Ok(())
}
