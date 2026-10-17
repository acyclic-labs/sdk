use super::*;
use proptest::prelude::*;

fn context(depth: usize) -> Renderer {
    let mut out = Renderer::default();
    for _ in 0..depth {
        out.event(&Event::Start(Tag::BlockQuote(None)));
        out.event(&Event::Start(Tag::List(Some(1))));
        out.event(&Event::Start(Tag::Item));
    }
    out.flush();
    out
}

fn copies(out: &Renderer, count: usize) -> usize {
    let before = crate::work::parts()[1];
    let snapshots: Vec<_> = (0..count).map(|_| out.checkpoint()).collect();
    let used = crate::work::parts()[1] - before;
    assert_eq!(snapshots.len(), count);
    used
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn context_checkpoints_copy_only_shared_handles(depth in 0usize..17, count in 1usize..65) {
        let out = context(depth);
        prop_assert_eq!(copies(&out, count), count * std::mem::size_of::<Option<Arc<Context>>>());
    }
}

#[test]
fn checkpoint_property_detects_copying_context_arrays() {
    let out = context(8);
    let retained = copies(&out, 64);
    CLONE_CONTEXT.set(true);
    let cloned = copies(&out, 64);
    CLONE_CONTEXT.set(false);
    assert!(cloned > retained);
    assert!(cloned > 64 * std::mem::size_of::<Option<Arc<Context>>>());
}

#[test]
fn editing_a_snapshot_preserves_its_original_context() {
    let original = context(2);
    let mut snapshot = original.checkpoint();
    snapshot.event(&Event::End(TagEnd::Item));
    snapshot.event(&Event::End(TagEnd::List(true)));
    snapshot.event(&Event::End(TagEnd::BlockQuote(None)));
    assert_eq!(original.context().prefixes.as_ref().unwrap().len(), 4);
    assert_eq!(original.context().lists.as_ref().unwrap().len(), 2);
    assert_eq!(snapshot.context().prefixes.as_ref().unwrap().len(), 2);
    assert_eq!(snapshot.context().lists.as_ref().unwrap().len(), 1);
}

#[test]
fn list_items_copy_shared_counters_only_when_numbered() {
    for ordered in [false, true] {
        let mut out = Renderer::default();
        out.event(&Event::Start(Tag::List(ordered.then_some(1))));
        let original = out.checkpoint();
        out.event(&Event::Start(Tag::Item));
        assert_eq!(
            Arc::ptr_eq(
                out.context().lists.as_ref().unwrap(),
                original.context().lists.as_ref().unwrap()
            ),
            !ordered
        );
        assert_eq!(
            original.context().lists.as_ref().unwrap().last(),
            Some(&ordered.then_some(1))
        );
    }
}

#[test]
fn dropping_the_last_container_does_not_copy_its_array() {
    let mut out = Renderer::default();
    out.event(&Event::Start(Tag::List(None)));
    out.event(&Event::Start(Tag::Item));
    out.flush();
    let original = out.checkpoint();
    let before = crate::work::parts()[1];
    out.event(&Event::End(TagEnd::Item));
    out.event(&Event::End(TagEnd::List(false)));
    assert_eq!(
        crate::work::parts()[1] - before,
        std::mem::size_of::<Context>()
    );
    assert!(out.context().lists.is_none() && out.context().prefixes.is_none());
    assert_eq!(original.context().lists.as_ref().unwrap().len(), 1);
    assert_eq!(original.context().prefixes.as_ref().unwrap().len(), 1);
}
