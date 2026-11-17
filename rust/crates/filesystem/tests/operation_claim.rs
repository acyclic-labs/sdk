//! Live operation ownership across public filesystem compositions.
//! Compositions over one backend owner share its registry across namespaces;
//! independent backend worlds retain independent registries.
#![cfg(not(loom))]

use acyclic_fs::{EmbeddedCapabilities, Fs, OperationAttempt, StreamAuthorityStore};
use std::{cell::Cell, sync::Arc};

#[test]
fn independently_composed_handles_preserve_backend_ownership() {
    let provider = acyclic_stream::MemoryStream::default();
    let first = Fs::new(
        StreamAuthorityStore::new(Arc::new(provider.clone())),
        (),
        EmbeddedCapabilities::MEMORY,
    );
    let second = Fs::new_in_namespace(
        StreamAuthorityStore::new(Arc::new(provider)),
        (),
        EmbeddedCapabilities::MEMORY,
        [9; 16],
    );
    let Ok(OperationAttempt::Owned(owned)) =
        first
            .operation_claims()
            .claim([3; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
    else {
        panic!("first composition did not acquire operation ownership")
    };
    assert!(matches!(
        second.operation_claims().claim(
            [3; 48],
            || Ok::<_, ()>(()),
            |_, _| -> Result<(), ()> { panic!("duplicate allocated storage") }
        ),
        Ok(OperationAttempt::Observed(_))
    ));
    drop(owned);
    assert!(matches!(
        second
            .operation_claims()
            .claim([3; 48], || Ok::<_, ()>(()), |_, _| Ok(())),
        Ok(OperationAttempt::Owned(_))
    ));
}

#[test]
fn aliases_share_ownership_and_independent_worlds_remain_independent() {
    let filesystem = Fs::memory();
    let alias = filesystem.clone();
    let independent = Fs::memory();
    let Ok(OperationAttempt::Owned(claim)) =
        filesystem
            .operation_claims()
            .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
    else {
        panic!("filesystem did not acquire operation ownership")
    };
    let allocations = Cell::new(0);
    assert!(matches!(
        alias.operation_claims().claim(
            [1; 48],
            || Ok::<_, ()>(()),
            |_, _| {
                allocations.set(allocations.get() + 1);
                Ok(())
            }
        ),
        Ok(OperationAttempt::Observed(_))
    ));
    assert_eq!(allocations.get(), 0);
    assert!(matches!(
        independent
            .operation_claims()
            .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(())),
        Ok(OperationAttempt::Owned(_))
    ));
    assert!(matches!(
        alias
            .operation_claims()
            .claim([2; 48], || Ok::<_, ()>(()), |_, _| Ok(())),
        Ok(OperationAttempt::Owned(_))
    ));
    drop(claim);
    assert!(matches!(
        alias
            .operation_claims()
            .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(())),
        Ok(OperationAttempt::Owned(_))
    ));
}
