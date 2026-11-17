//! Ownership checks at filesystem composition boundaries.
use acyclic_stream::OperationClaims;

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
                        if let Ok(claim) = claims
                            .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                            .unwrap()
                            .into_owner()
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
                    .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                    .unwrap()
                    .into_owner()
                    .is_ok()
            );
        });
    }
}
