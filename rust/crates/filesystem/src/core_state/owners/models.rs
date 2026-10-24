use super::*;
use loom::sync::atomic::{AtomicUsize, Ordering};

fn race<const FENCE: bool>() {
    let registry = Arc::new(Owners::new(BTreeMap::new()));
    let created = Arc::new(AtomicUsize::new(0));
    let peers: Vec<_> = (0..2)
        .map(|_| {
            let registry = registry.clone();
            let created = created.clone();
            loom::thread::spawn(move || {
                join::<_, FENCE>(&registry, "namespace".into(), [1; 16], || {
                    let id = created.fetch_add(1, Ordering::SeqCst);
                    loom::thread::yield_now();
                    Ok(Arc::new(id))
                })
                .unwrap()
            })
        })
        .collect();
    let mut owners = Vec::new();
    for peer in peers {
        match peer.join() {
            Ok(owner) => owners.push(owner),
            Err(error) => std::panic::resume_unwind(error),
        }
    }
    assert_eq!(
        created.load(Ordering::SeqCst),
        1,
        "owner initialization escaped registry lock"
    );
    assert!(Arc::ptr_eq(&owners[0], &owners[1]));
    let reopened = join::<_, FENCE>(&registry, "namespace".into(), [1; 16], || {
        panic!("reopen ignored a live owner")
    })
    .unwrap();
    assert!(Arc::ptr_eq(&owners[0], &reopened));
    drop((owners, reopened));
    join::<_, FENCE>(&registry, "namespace".into(), [2; 16], || {
        created.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(2))
    })
    .unwrap();
    assert_eq!(created.load(Ordering::SeqCst), 2);
}
#[test]
fn competing_joins_and_reopen_share_the_live_owner() {
    loom::model(race::<true>);
}
#[test]
#[should_panic(expected = "owner initialization escaped registry lock")]
fn negative_control_detects_initialization_outside_the_registry_lock() {
    loom::model(race::<false>);
}

#[test]
fn concurrent_reopen_preserves_the_joined_owner() {
    loom::model(|| {
        let registry = Arc::new(Owners::new(BTreeMap::new()));
        let created = Arc::new(AtomicUsize::new(0));
        let first = join::<_, true>(&registry, "namespace".into(), [1; 16], || {
            Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
        })
        .unwrap();
        let release = loom::thread::spawn(move || {
            loom::thread::yield_now();
            drop(first);
        });
        let opener = {
            let registry = registry.clone();
            let created = created.clone();
            loom::thread::spawn(move || {
                join::<_, true>(&registry, "namespace".into(), [1; 16], || {
                    loom::thread::yield_now();
                    Ok(Arc::new(created.fetch_add(1, Ordering::SeqCst)))
                })
                .unwrap()
            })
        };
        let current = opener.join().unwrap();
        release.join().unwrap();
        let again = join::<_, true>(&registry, "namespace".into(), [1; 16], || {
            panic!("lost the owner retained by the reopening caller")
        })
        .unwrap();
        assert!(Arc::ptr_eq(&current, &again));
        assert!(matches!(created.load(Ordering::SeqCst), 1 | 2));
    });
}
