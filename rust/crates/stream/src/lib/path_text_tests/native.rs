use super::*;
use crate::test_meter::{Meter, trace};
use std::sync::atomic::Ordering;

fn text_allocation(text: &str) -> usize {
    std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::array::<u8>(text.len()).unwrap())
        .unwrap()
        .0
        .pad_to_align()
        .size()
}
fn clone_heap_bound(heap: acyclic_test_support::AllocationInfo, text: &str) {
    let output_bytes = text.len() + text.rfind('/').unwrap();
    // Two identical output Strings, plus one shared text allocation.
    assert_eq!(heap.count_total, 3);
    assert_eq!(heap.bytes_total as usize, text_allocation(text) + output_bytes);
}
fn cloned_output(text: &str) -> (String, String) {
    let path = StreamPath::new(text).unwrap();
    let cloned = path.clone();
    (path.to_string(), cloned.parent().unwrap().to_string())
}
#[test]
fn cloned_plain_paths_allocate_once_and_original_storage_fails_the_same_bound() {
    let text = "swarm/agent/task/files";
    let (_, [before, after]) = acyclic_test_support::differential(
        || {
            let path = BaselinePath::new(text).unwrap();
            let cloned = path.clone();
            (path.to_string(), cloned.parent().unwrap().to_string())
        },
        || cloned_output(text),
    );
    eprintln!("path_heap before={before:?} after={after:?}");
    clone_heap_bound(after, text);
    assert_eq!(before.count_total, 4);
    assert!(std::panic::catch_unwind(|| clone_heap_bound(before, text)).is_err());
}
#[test]
fn copying_on_the_real_clone_path_fails_the_same_heap_assertion() {
    let text = "swarm/agent/task/files";
    let mut clean = None;
    let heap = acyclic_test_support::measure(|| clean = Some(cloned_output(text)));
    clone_heap_bound(heap, text);
    let restore = Restore(COPY_ON_CLONE.with(|flag| flag.replace(true)));
    let mut broken = None;
    let heap = acyclic_test_support::measure(|| broken = Some(cloned_output(text)));
    drop(restore);
    assert_eq!(clean, broken);
    assert!(
        std::panic::catch_unwind(|| clone_heap_bound(heap, text)).is_err(),
        "a production-path copy escaped the same allocation bound"
    );
}

struct Restore(bool);
impl Drop for Restore {
    fn drop(&mut self) {
        COPY_ON_CLONE.with(|flag| flag.set(self.0));
    }
}

fn chain(old: bool, text: &str) -> (usize, u64) {
    let mut count = 0;
    let mut hash = DefaultHasher::new();
    let mut end = text.len();
    if old {
        let mut path = BaselinePath::new(text).unwrap();
        loop {
            let cloned = path.clone();
            assert_eq!(cloned.as_str(), &text[..end]);
            cloned.as_str().hash(&mut hash);
            count += 1;
            let parent = cloned.parent();
            assert_eq!(parent.is_some(), text[..end].contains('/'));
            let Some(parent) = parent else { break };
            end = text[..end].rfind('/').unwrap();
            path = parent;
        }
    } else {
        let mut path = StreamPath::new(text).unwrap();
        loop {
            let cloned = path.clone();
            assert_eq!(cloned.as_str(), &text[..end]);
            cloned.as_str().hash(&mut hash);
            count += 1;
            let parent = cloned.parent();
            assert_eq!(parent.is_some(), text[..end].contains('/'));
            let Some(parent) = parent else { break };
            end = text[..end].rfind('/').unwrap();
            path = parent;
        }
    }
    (count, hash.finish())
}

fn chain_heap_bound(heap: acyclic_test_support::AllocationInfo, text: &str) {
    assert_eq!(heap.count_total, 1);
    assert_eq!(heap.count_max, 1);
    assert_eq!(heap.bytes_total as usize, text_allocation(text));
    assert_eq!(heap.bytes_max as usize, text_allocation(text));
    assert_eq!(heap.count_current, 0);
    assert_eq!(heap.bytes_current, 0);
}

#[test]
fn n_and_two_n_long_paths_and_deep_chains_allocate_one_owner_and_reject_copies() {
    for (width, depth) in [(16, 4), (32, 4), (8, 16), (8, 32)] {
        let text = vec!["a".repeat(width); depth].join("/");
        let (expected, [before, after]) = acyclic_test_support::differential(
            || chain(true, &text),
            || chain(false, &text),
        );
        assert_eq!(expected.0, depth);
        assert_eq!(before.count_total, 2);
        assert_eq!(
            before.bytes_total as usize,
            text.len() + 3 * std::mem::size_of::<usize>()
        );
        chain_heap_bound(after, &text);
        let restore = Restore(COPY_ON_CLONE.with(|flag| flag.replace(true)));
        let mut broken = None;
        let heap = acyclic_test_support::measure(|| broken = Some(chain(false, &text)));
        drop(restore);
        assert_eq!(broken, Some(expected));
        assert_eq!(heap.count_total as usize, depth + 1);
        let copied: usize = text
            .match_indices('/')
            .map(|(end, _)| text_allocation(&text[..end]))
            .chain(std::iter::once(text_allocation(&text)))
            .sum();
        assert_eq!(heap.bytes_total as usize, text_allocation(&text) + copied);
        assert!(std::panic::catch_unwind(|| chain_heap_bound(heap, &text)).is_err());
    }
}

fn native_observation(
    old: bool,
    text: &str,
    memory_limit: u64,
    denied: bool,
) -> (Result<Vec<String>, String>, Vec<trace::Event>, [u64; 3]) {
    let meter = Arc::new(Meter {
        memory_limit,
        ..Meter::default()
    });
    meter.deny_allocation.store(denied, Ordering::SeqCst);
    trace::begin();
    let result = if old {
        BaselinePath::new_native(text, &meter.original()).map(|result| {
            result.map(|path| {
                let clone = path.clone();
                let parent = clone.parent();
                drop(path);
                let output = vec![
                    clone.to_string(),
                    parent.map_or_else(String::new, |path| path.to_string()),
                ];
                drop(clone);
                output
            })
        })
    } else {
        StreamPath::new_native(text, &meter.original()).map(|result| {
            result.map(|path| {
                let clone = path.clone();
                let parent = clone.parent();
                drop(path);
                let output = vec![
                    clone.to_string(),
                    parent.map_or_else(String::new, |path| path.to_string()),
                ];
                drop(clone);
                output
            })
        })
    };
    let result = match result {
        Err(error) => Err(format!("io:{:?}:{error}", error.kind())),
        Ok(Err(error)) => Err(format!("path:{error:?}")),
        Ok(Ok(paths)) => Ok(paths),
    };
    let events = trace::finish();
    let counters =
        [&meter.held, &meter.allocations, &meter.copied].map(|n| n.load(Ordering::SeqCst));
    (result, events, counters)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn native_paths_keep_full_bytes_exact_errors_ordered_admission_and_refunds(text in input(), memory_limit in 0u64..256, denied in any::<bool>()) {
        // Meter, storage, script and observer state are independent each time.
        let (_, [before, after]) = acyclic_test_support::differential(
            || native_observation(true, &text, memory_limit, denied),
            || native_observation(false, &text, memory_limit, denied),
        );
        prop_assert_eq!(before.count_total, after.count_total);
        prop_assert_eq!(before.bytes_total, after.bytes_total);
    }
    #[test]
    fn native_and_plain_paths_have_identical_key_semantics(text in "[a-z0-9]{1,12}(/[a-z0-9]{1,12}){0,5}") {
        let meter = Arc::new(Meter::default());
        let native = StreamPath::new_native(&text, &meter.original()).unwrap().unwrap();
        let plain = StreamPath::new(&text).unwrap();
        prop_assert_eq!(&native, &plain);
        prop_assert_eq!(native.cmp(&plain), std::cmp::Ordering::Equal);
        prop_assert_eq!(fingerprint(&native), fingerprint(&plain));
        let parent = native.parent();
        drop(native);
        if parent.is_some() { prop_assert!(meter.held.load(Ordering::SeqCst) > 0); }
        drop(parent);
        prop_assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
}
