use super::*;
use acyclic_harness::test_support::{measure_model_view_work, measure_task_volume_work};

fn measure<T>(operation: impl FnOnce() -> T) -> (T, (u64, u64, u64)) {
    let ((value, (parsed, bytes)), visits) =
        measure_task_volume_work(|| measure_model_view_work(operation));
    (value, (visits, parsed, bytes))
}

fn run(n: usize, rebuild: bool, scan: bool) -> [(u64, u64, u64); 5] {
    let volume = [8; 32];
    let old = request("openai", "oldmodel", "1", "");
    let new = request("gemini", "newmodel", "1", "");
    let mut files = (0..n)
        .map(|turn| (key("task", turn as u64), old.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut views = new_views();
    let mut position = 1;
    let (_, initial) = measure(|| {
        views
            .query_snapshot(
                Query::Root,
                snapshot(cut(volume, position), &files),
                n + 1,
                1 << 20,
            )
            .unwrap()
    });
    let mut work = [initial; 5];
    for stage in 1..5 {
        let (_, count) = measure(|| {
            for turn in 0..n {
                position += 1;
                let (file, removed) = match stage {
                    1 | 2 => {
                        files.insert(key("task", turn as u64), new.clone());
                        (
                            Some(FileValue::new(key("task", turn as u64), new.clone())),
                            vec![],
                        )
                    }
                    3 => (
                        Some(FileValue::new(
                            FileKey::Goal,
                            format!("unrelated-{turn}").into_bytes(),
                        )),
                        vec![],
                    ),
                    _ => {
                        files.remove(&key("task", turn as u64));
                        (None, vec![key("task", turn as u64)])
                    }
                };
                views
                    .query_change(Change {
                        cursor: cut(volume, position),
                        file,
                        files: vec![],
                        removed,
                    })
                    .unwrap();
                if scan && stage == 1 {
                    let _ = views.models().count();
                }
                if rebuild && stage == 1 {
                    views
                        .query_snapshot(
                            Query::Root,
                            snapshot(cut(volume, position), &files),
                            n + 1,
                            1 << 20,
                        )
                        .unwrap();
                }
            }
        });
        work[stage] = count;
    }
    work
}
fn assert_work(n: usize, work: [(u64, u64, u64); 5]) {
    assert_eq!(work[0].1, n as u64, "snapshot must parse each request once");
    assert_eq!(work[1].1, n as u64, "changed model parsing must be linear");
    for stage in 2..5 {
        assert_eq!(
            (work[stage].1, work[stage].2),
            (0, 0),
            "unchanged, unrelated and removed files must never reparse models"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Doubling retained rows doubles decoding only for snapshot/changed input.
    // Unchanged, removed and unrelated input must do zero model parsing.
    #[test]
    fn n_and_2n_changes_parse_only_changed_requests(n in 1usize..33) {
        let small=run(n,false,false);
        let large=run(n*2,false,false);
        assert_work(n,small);
        assert_work(n*2,large);
        assert_pair(small,large);
    }
}

#[test]
#[should_panic(expected = "changed model parsing must be linear")]
fn negative_control_rebuilding_models_after_each_change_fails_the_same_bound() {
    assert_work(8, run(8, true, false));
}

fn assert_pair(small: [(u64, u64, u64); 5], large: [(u64, u64, u64); 5]) {
    for stage in 0..5 {
        assert!(
            large[stage].0 <= small[stage].0 * 2,
            "model index work is not linear"
        );
        assert_eq!(
            (large[stage].1, large[stage].2),
            (small[stage].1 * 2, small[stage].2 * 2)
        );
    }
}
#[test]
#[should_panic(expected = "model index work is not linear")]
fn negative_control_scanning_unchanged_model_rows_fails_the_same_bound() {
    assert_pair(run(8, false, true), run(16, false, true));
}
