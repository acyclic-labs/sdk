use super::*;
use acyclic_harness::test_support::measure_task_volume_work;
use futures::FutureExt as _;
use proptest::prelude::*;

pub(super) fn measured<T>(
    future: impl Future<Output = acyclic_harness::task_volume::Result<T>>,
) -> Result<(T, u64)> {
    let (output, work) =
        measure_task_volume_work(|| tokio::task::unconstrained(future).now_or_never());
    Ok((
        output.ok_or("in-memory task-volume operation is pending")??,
        work,
    ))
}

pub(super) fn query_work_is_incremental(work: u64) -> bool {
    work <= 2 // At most one stream anchor and one selected index entry.
}

#[test]
fn a_panicking_measurement_releases_the_thread_scope() {
    let failed =
        std::panic::catch_unwind(|| measure_task_volume_work(|| measure_task_volume_work(|| ())));
    assert!(failed.is_err());
    assert_eq!(measure_task_volume_work(|| ()).1, 0);
}

fn check_history(count: u64, selected: u64, text: &str) -> Result {
    let (_, volume) = memory().now_or_never().ok_or("initialization pending")??;
    let mut generation = volume
        .snapshot(&Query::Root, 0)
        .now_or_never()
        .ok_or("initial snapshot pending")??
        .generation;
    for turn in 0..count {
        let mut file = write(&format!(".tasks/t/turns/{turn}/chunks/0"), text)?;
        file.channel = Some(Channel::Text);
        let (next, work) = measured(volume.append(generation, &file))?;
        assert!(work <= 5);
        generation = next;
    }
    let query = Query::Chunks {
        task: Component::new("t")?,
        turn: selected % count,
    };
    let (snapshot, work) = measured(volume.snapshot(&query, 1))?;
    assert!(query_work_is_incremental(work));
    let (full, work) = measured(volume.snapshot(&Query::Root, count as usize))?;
    let selected_files: Vec<_> = full
        .files
        .into_iter()
        .filter(|file| file.key == snapshot.files[0].key)
        .collect();
    assert_eq!(selected_files, snapshot.files);
    assert!(work >= count);
    assert!(
        !query_work_is_incremental(work),
        "full scan passed the query work bound"
    );
    Ok(())
}

proptest! {
    // Each single-file publication and current/historical-turn selection does
    // constant work even as unrelated immutable turns accumulate in the index.
    #[test]
    fn publication_and_selected_turn_work_ignore_unrelated_history(
        count in 2_u64..33, selected in any::<u64>(), text in "[a-zλ雪]{0,24}",
    ) {
        let result = check_history(count, selected, &text);
        prop_assert!(result.is_ok(), "{result:?}");
    }
}
