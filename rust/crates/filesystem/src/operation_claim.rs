//! Ownership checks at filesystem composition boundaries.
#[cfg(test)]
use acyclic_stream::OperationClaims;

#[cfg(all(test, feature = "memory", not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn independently_composed_handles_preserve_backend_ownership() {
        let provider = acyclic_stream::MemoryStream::default();
        let first = crate::Fs::new(
            crate::StreamAuthorityStore::new(std::sync::Arc::new(provider.clone())),
            (),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let second = crate::Fs::new_in_namespace(
            crate::StreamAuthorityStore::new(std::sync::Arc::new(provider)),
            (),
            crate::EmbeddedCapabilities::MEMORY,
            [9; 16],
        );
        let owned = first
            .operation_claims()
            .claim([3; 48], || Ok::<_, ()>(()), |_| Ok(()))
            .unwrap()
            .unwrap();
        assert!(
            second
                .operation_claims()
                .claim(
                    [3; 48],
                    || Ok::<_, ()>(()),
                    |_| -> Result<(), ()> { panic!("duplicate allocated storage") }
                )
                .unwrap()
                .is_none()
        );
        drop(owned);
        assert!(
            second
                .operation_claims()
                .claim([3; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );

        let authority = std::sync::Arc::new(crate::MemoryAuthorityStore::default());
        let first = crate::Fs::new(authority.clone(), (), crate::EmbeddedCapabilities::MEMORY);
        let second = crate::Fs::new(authority, (), crate::EmbeddedCapabilities::MEMORY);
        let owned = first
            .operation_claims()
            .claim([4; 48], || Ok::<_, ()>(()), |_| Ok(()))
            .unwrap()
            .unwrap();
        assert!(
            second
                .operation_claims()
                .claim(
                    [4; 48],
                    || Ok::<_, ()>(()),
                    |_| -> Result<(), ()> { panic!("duplicate allocated storage") }
                )
                .unwrap()
                .is_none()
        );
        drop(owned);
    }

    #[test]
    fn aliases_share_ownership_and_independent_worlds_remain_independent() {
        let filesystem = crate::Fs::memory();
        let alias = filesystem.clone();
        let independent = crate::Fs::memory();
        let claim = filesystem
            .operation_claims()
            .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
            .unwrap()
            .unwrap();
        let allocations = Cell::new(0);
        assert!(
            alias
                .operation_claims()
                .claim(
                    [1; 48],
                    || Ok::<_, ()>(()),
                    |_| {
                        allocations.set(allocations.get() + 1);
                        Ok(())
                    }
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(allocations.get(), 0);
        assert!(
            independent
                .operation_claims()
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
        assert!(
            alias
                .operation_claims()
                .claim([2; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
        drop(claim);
        assert!(
            alias
                .operation_claims()
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn refused_storage_never_installs_a_claim() {
        let claims = OperationClaims::default();
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_| Err::<(), _>(()))
                .is_err()
        );
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod models {
    use super::*;
    use loom::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn concurrent_attempts_never_own_the_same_operation() {
        loom::model(|| {
            let claims = Arc::new(OperationClaims::default());
            let active = Arc::new(AtomicUsize::new(0));
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    let claims = claims.clone();
                    let active = active.clone();
                    loom::thread::spawn(move || {
                        if let Some(claim) = claims
                            .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                            .unwrap()
                        {
                            assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                            loom::thread::yield_now();
                            assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                            drop(claim);
                        }
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert!(
                claims
                    .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                    .unwrap()
                    .is_some()
            );
        });
    }
}
