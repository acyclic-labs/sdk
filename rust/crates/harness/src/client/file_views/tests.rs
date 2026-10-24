use super::*;
use crate::task_volume::{Channel, FileValue, Generation, SCHEMA};

fn fixture(n: usize) -> Result<FileViews> {
    let cursor = Cursor {
        volume: [1; 32],
        epoch: uuid::Uuid::nil(),
        schema: SCHEMA,
        query: [2; 32],
        position: 1,
        accepted: 1,
        durable: 1,
        commit: [3; 32],
    };
    let snapshot = HeadSnapshot {
        generation: Generation {
            volume: cursor.volume,
            epoch: cursor.epoch,
            position: cursor.position,
            commit: cursor.commit,
        },
        cursor,
        files: (0..n)
            .map(|i| {
                Ok(HeadFile {
                    key: FileKey::parse(&format!(".tasks/task/executions/id{i}/result.json"))?,
                    publication: None,
                    channel: None,
                    segments: vec![b"x".to_vec().into()],
                    length: 1,
                    through_sequence: None,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let mut view = FileViews::snapshot(
        snapshot,
        QueryViewLimits {
            files: n + 128,
            segments: n + 256,
            bytes: 1_000_000,
        },
    )?;
    view.window = true;
    Ok(view)
}
fn change(view: &FileViews, file: FileValue) -> Change {
    let mut cursor = view.cursor.clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    Change {
        cursor,
        file: Some(file),
        files: vec![],
        removed: vec![],
    }
}
fn accounting<const SCAN: bool>(n: usize) -> Result<u128> {
    let mut view = fixture(n)?;
    let start = std::time::Instant::now();
    for _ in 0..100 {
        let change = change(
            &view,
            FileValue::new(FileKey::parse(".tasks/task/status.json")?, b"x".to_vec()),
        );
        let (applied, visits) = crate::test_support::measure_task_volume_work(|| {
            if SCAN {
                std::hint::black_box(
                    view.files
                        .values()
                        .filter(|head| disposable(&head.key))
                        .map(|head| head.length)
                        .sum::<u64>(),
                );
            }
            view.prepare(&change)?(&mut view)
        });
        applied?;
        assert!(
            visits <= 4,
            "window accounting must not scan retained files: {visits}"
        );
    }
    Ok(start.elapsed().as_micros())
}
#[test]
fn window_accounting_visits_only_the_changed_file() -> Result<()> {
    for n in [100, 1000, 10000] {
        accounting::<false>(n)?;
    }
    Ok(())
}
#[test]
#[should_panic(expected = "window accounting must not scan retained files")]
fn negative_control_summing_disposable_files_visits_the_retained_index() {
    accounting::<true>(100).expect("valid fixture");
}
#[test]
#[ignore = "manual cached file-window accounting measurement"]
fn file_accounting_measurement() -> Result<()> {
    for n in [100, 1000, 10000] {
        eprintln!(
            "CLIENT_FILES_AFTER n={n} updates=100 elapsed_us={}",
            accounting::<false>(n)?
        );
    }
    Ok(())
}
fn totals(view: &FileViews) {
    let heads: Vec<_> = view
        .files
        .values()
        .filter(|head| disposable(&head.key))
        .collect();
    assert_eq!(
        view.task_bytes,
        heads.iter().map(|head| head.length).sum::<u64>()
    );
    assert_eq!(
        view.task_segments,
        heads.iter().map(|head| head.segments.len()).sum::<usize>()
    );
    assert_eq!(
        view.disposable,
        heads.iter().map(|head| head.key.clone()).collect()
    );
    assert_eq!(
        view.bytes,
        view.files.values().map(|head| head.length).sum::<u64>()
    );
    assert_eq!(
        view.segments,
        view.files
            .values()
            .map(|head| head.segments.len())
            .sum::<usize>()
    );
}
#[test]
fn cached_totals_follow_appends_replacements_and_prefix_eviction() -> Result<()> {
    let mut view = fixture(4)?;
    for sequence in 0..300 {
        let mut file = FileValue::new(
            FileKey::parse(&format!(
                ".tasks/task/turns/{}/chunks/{sequence}",
                sequence / 64
            ))?,
            vec![b'x'; 3],
        );
        file.channel = Some(Channel::Text);
        let change = change(&view, file);
        view.prepare(&change)?(&mut view)?;
        totals(&view);
        assert!(view.task_segments <= 128);
    }
    for size in [1, 7, 2] {
        let change = change(
            &view,
            FileValue::new(FileKey::parse(".tasks/task/status.json")?, vec![b'x'; size]),
        );
        view.replace(&change, &std::collections::BTreeSet::new());
        totals(&view);
    }
    Ok(())
}
