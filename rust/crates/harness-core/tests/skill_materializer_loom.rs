//! Model the production journal owner, including its pre-fix negative control.
#![cfg(acyclic_harness_loom)]

#[path = "../src/sync/owned.rs"]
pub(crate) mod owned;
pub(crate) mod sync {
    pub(crate) use super::owned;
}
#[path = "../src/context/skill_materializer.rs"]
mod skill_materializer;

fn schedule<const SERIALIZE: bool>() {
    use loom::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let owner = skill_materializer::SkillMaterializer::new();
    let head = Arc::new(AtomicUsize::new(0));
    let tasks = (0..2)
        .map(|_| {
            let owner = owner.clone();
            let head = head.clone();
            loom::thread::spawn(move || {
                let _guard = SERIALIZE.then(|| futures::executor::block_on(owner.enter()));
                let pinned = head.load(Ordering::SeqCst);
                loom::thread::yield_now();
                assert!(
                    head.compare_exchange(pinned, pinned + 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok(),
                    "materialization conflict"
                );
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(head.load(Ordering::SeqCst), 2);
}

#[test]
fn journal_owner_serializes_first_publications() {
    loom::model(schedule::<true>);
}

#[test]
fn former_independent_publishers_can_conflict() {
    let failure = std::panic::catch_unwind(|| loom::model(schedule::<false>))
        .expect_err("negative control must conflict");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("materialization conflict"),
        "unexpected model failure: {text}"
    );
}
