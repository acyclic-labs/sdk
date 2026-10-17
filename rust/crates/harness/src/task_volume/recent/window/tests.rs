//! Independent old-window differential, exact allocation/custody bounds and controls.
use super::Window;
use crate::task_volume::{
    Change, Channel, Component, Cursor, Error, FileKey, FileValue, Generation, Observation, Query,
    Result, Snapshot, TaskFile, ToolFile,
};
use acyclic_test_support::{differential, measure};
use futures::StreamExt as _;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

type Trace = (&'static str, u64);

/// The pre-window implementation, kept verbatim apart from trace records.
mod old {
    use super::*;
    type Window = BTreeMap<FileKey, (u64, FileValue)>;
    fn remove(files: &mut Window, key: &FileKey) {
        if let Some((order, _)) = files.remove(key) {
            differential::record(("remove", order));
        }
    }
    fn insert(files: &mut Window, file: &FileValue, order: u64) {
        differential::record(("insert", order));
        files.insert(file.key.clone(), (order, file.clone()));
    }
    fn kind(file: &FileValue) -> Option<u8> {
        super::super::super::file_kind(file)
    }
    fn trim(files: &mut Window, maximum: usize, bytes: usize, recent: usize) {
        for kind in 0..=7 {
            while files
                .values()
                .filter(|(_, file)| self::kind(file) == Some(kind))
                .count()
                > recent
            {
                let key = files
                    .iter()
                    .filter(|(_, (_, file))| self::kind(file) == Some(kind))
                    .min_by_key(|(_, (order, _))| order)
                    .map(|(key, _)| key.clone())
                    .unwrap();
                remove(files, &key);
            }
        }
        while files.len() > maximum
            || files
                .values()
                .map(|(_, file)| file.bytes.len())
                .sum::<usize>()
                > bytes
        {
            let Some(key) = files
                .iter()
                .min_by_key(|(_, (order, _))| order)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            remove(files, &key);
        }
    }
    pub(super) struct Old(Window, usize, usize, usize);
    pub(super) fn window(
        files: &mut Vec<FileValue>,
        maximum: usize,
        bytes: usize,
        recent: usize,
    ) -> Old {
        let mut window = Window::new();
        for (order, file) in files.drain(..).enumerate() {
            if file.bytes.len() <= bytes {
                remove(&mut window, &file.key);
                insert(&mut window, &file, order as u64);
            }
        }
        trim(&mut window, maximum, bytes, recent);
        *files = window.values().map(|(_, file)| file.clone()).collect();
        Old(window, maximum, bytes, recent)
    }
    pub(super) fn apply(Old(window, maximum, bytes, recent): &mut Old, change: &mut Change) {
        let before: BTreeSet<_> = window.keys().cloned().collect();
        for key in &change.removed {
            remove(window, key);
        }
        for file in change.file.iter().chain(&change.files) {
            remove(window, &file.key);
            if file.bytes.len() <= *bytes {
                insert(window, file, change.cursor.position);
            }
        }
        trim(window, *maximum, *bytes, *recent);
        change.removed = before
            .into_iter()
            .filter(|key| !window.contains_key(key))
            .collect();
        change.files.retain(|file| window.contains_key(&file.key));
        if change
            .file
            .as_ref()
            .is_some_and(|file| !window.contains_key(&file.key))
        {
            change.file = None;
        }
    }
    pub(super) fn observe(
        mut observation: Observation,
        query: &Query,
        maximum: usize,
        bytes: usize,
    ) -> Result<Observation> {
        let recent = match query {
            Query::TaskView { recent, .. } => *recent as usize,
            _ => maximum,
        };
        let snapshot = observation.snapshot.as_mut().ok_or(Error::ResetRequired)?;
        let window = window(&mut snapshot.files, maximum, bytes, recent);
        observation.changes = Box::pin(futures::stream::try_unfold(
            (observation.changes, window),
            |(mut changes, mut window)| async move {
                let Some(change) = changes.next().await else {
                    return Ok(None);
                };
                let mut change = change?;
                apply(&mut window, &mut change);
                Ok(Some((change, (changes, window))))
            },
        ));
        Ok(observation)
    }
}

fn component(value: impl AsRef<str>) -> Component {
    Component::new(value).unwrap()
}
fn task(file: TaskFile, size: usize) -> FileValue {
    FileValue::new(
        FileKey::Task {
            task: component("a"),
            file,
        },
        vec![size as u8; size],
    )
}
/// Pool covering every display kind, reasoning chunks, key collisions and unclassified files.
fn pooled((index, size): (u8, usize)) -> FileValue {
    let task = component(if index < 24 { "a" } else { "b" });
    let call = component(format!("c{}", index % 2));
    let turn = u64::from(index % 3);
    let sequence = u64::from(index);
    let (file, channel) = match index % 12 {
        0 => (TaskFile::Status, None),
        1 => (TaskFile::TurnRequest(turn), None),
        2 => (TaskFile::Chunk { turn, sequence }, Some(Channel::Text)),
        3 => (TaskFile::Chunk { turn, sequence }, Some(Channel::Reasoning)),
        4 => (
            TaskFile::TextHead {
                turn,
                channel: Channel::Reasoning,
            },
            None,
        ),
        5 => (
            TaskFile::Tool {
                turn,
                call,
                file: ToolFile::Request,
            },
            None,
        ),
        6 => (
            TaskFile::Tool {
                turn,
                call,
                file: ToolFile::Stdout(turn),
            },
            None,
        ),
        7 => (TaskFile::ExecutionResult(call), None),
        8 => (TaskFile::Completion(turn), None),
        9 => (
            TaskFile::TextHead {
                turn,
                channel: Channel::Text,
            },
            None,
        ),
        10 => (TaskFile::Request(call), None),
        _ => {
            let mut goal = FileValue::new(FileKey::Goal, vec![index; size]);
            goal.channel = Some(Channel::Reasoning);
            return goal;
        }
    };
    let key = FileKey::Task { task, file };
    FileValue {
        key,
        channel,
        publication: None,
        bytes: vec![index; size].into(),
    }
}
fn unclassified(index: usize, size: usize) -> FileValue {
    task(TaskFile::Request(component(format!("r{index:05}"))), size)
}
fn cursor(position: u64) -> Cursor {
    Cursor {
        volume: [0; 32],
        epoch: uuid::Uuid::nil(),
        schema: 0,
        query: [0; 32],
        position,
        accepted: position,
        durable: position,
        commit: [0; 32],
    }
}
fn change(
    removed: Vec<FileKey>,
    file: Option<FileValue>,
    files: Vec<FileValue>,
    position: u64,
) -> Change {
    Change {
        file,
        files,
        removed,
        cursor: cursor(position),
    }
}
/// Fresh payload storage per side: neither run shares the other's allocations.
fn copied(file: &FileValue) -> FileValue {
    FileValue {
        bytes: file.bytes.to_vec().into(),
        ..file.clone()
    }
}

type Step = (Vec<FileKey>, Option<FileValue>, Vec<FileValue>, u64);
#[derive(Clone, Debug)]
struct Input {
    snapshot: Vec<FileValue>,
    steps: Vec<Step>,
    failure: bool,
    maximum: usize,
    bytes: usize,
    recent: Option<u32>,
}
type View = (Option<FileValue>, Vec<FileValue>, Vec<FileKey>, u64);
type Output = (
    std::result::Result<Vec<FileValue>, String>,
    Vec<std::result::Result<View, String>>,
);

fn observation(input: &Input) -> Observation {
    let mut changes: Vec<Result<Change>> = input
        .steps
        .iter()
        .map(|(removed, file, files, position)| {
            let files = files.iter().map(copied).collect();
            Ok(change(
                removed.clone(),
                file.as_ref().map(copied),
                files,
                *position,
            ))
        })
        .collect();
    if input.failure {
        changes.push(Err(Error::InvalidSegment));
    }
    let snapshot = Snapshot {
        files: input.snapshot.iter().map(copied).collect(),
        cursor: cursor(0),
        generation: Generation {
            volume: [0; 32],
            epoch: uuid::Uuid::nil(),
            position: 0,
            commit: [0; 32],
        },
    };
    Observation {
        snapshot: Some(snapshot),
        reset: false,
        changes: Box::pin(futures::stream::iter(changes)),
    }
}
fn run(input: &Input, old: bool) -> Output {
    let query = match input.recent {
        Some(recent) => Query::TaskView {
            task: component("a"),
            recent,
            reasoning: true,
            details: true,
            terminal: true,
        },
        None => Query::TaskOverviews,
    };
    let observe = if old {
        old::observe
    } else {
        super::super::observe
    };
    let mut observation = match observe(observation(input), &query, input.maximum, input.bytes) {
        Ok(observation) => observation,
        Err(error) => return (Err(error.to_string()), Vec::new()),
    };
    let files = observation.snapshot.take().unwrap().files;
    let changes = futures::executor::block_on(
        observation
            .changes
            .map(|change| {
                change
                    .map(|change| {
                        (
                            change.file,
                            change.files,
                            change.removed,
                            change.cursor.position,
                        )
                    })
                    .map_err(|error| error.to_string())
            })
            .collect(),
    );
    (Ok(files), changes)
}

fn sized() -> impl Strategy<Value = FileValue> {
    (0u8..30, 0usize..9).prop_map(pooled)
}
fn input() -> impl Strategy<Value = Input> {
    let removed = (0u8..30).prop_map(|index| pooled((index, 0)).key);
    let step = (
        prop::collection::vec(removed, 0..3),
        prop::option::of(sized()),
        prop::collection::vec(sized(), 0..4),
        0u64..6,
    );
    (
        prop::collection::vec(sized(), 0..24),
        prop::collection::vec(step, 0..8),
        any::<bool>(),
        1usize..12,
        1usize..32,
        prop::option::of(0u32..4),
    )
        .prop_map(|(snapshot, steps, failure, maximum, bytes, recent)| Input {
            snapshot,
            steps,
            failure,
            maximum,
            bytes,
            recent,
        })
}

proptest! {
    /// Output bytes, exact errors and the ordered insert/remove trace match the
    /// independently initialized old window for dense, sparse, tied and unknown kinds.
    #[test]
    fn window_matches_independent_old_window(input in input()) {
        let [expected, actual] =
            differential::pair::<_, _, Trace>(|| input.clone(), |input, old| run(&input, old));
        prop_assert_eq!(actual, expected);
    }
}

#[test]
fn window_reports_only_keys_visible_before_each_change() {
    let status = task(TaskFile::Status, 2);
    let completion = task(TaskFile::Completion(0), 2);
    let turns = [0, 1].map(|turn| task(TaskFile::TurnRequest(turn), 1));
    let requests = [0, 1].map(|index| unclassified(index, 1));
    let rewritten = task(TaskFile::Status, 1);
    let input = Input {
        snapshot: vec![status.clone(), completion.clone(), unclassified(9, 9)],
        steps: vec![
            // Explicit removal then reinsertion; a same-batch new turn is evicted by kind.
            (
                vec![status.key.clone(), FileKey::Goal],
                Some(rewritten.clone()),
                turns.to_vec(),
                3,
            ),
            // Oversized replacement removes; an equal-order tie evicts the lower key.
            (
                Vec::new(),
                Some(task(TaskFile::Completion(0), 40)),
                requests.to_vec(),
                3,
            ),
        ],
        failure: true,
        maximum: 3,
        bytes: 8,
        recent: Some(1),
    };
    let expected = (
        Ok(vec![status, completion.clone()]),
        vec![
            Ok((Some(rewritten), vec![turns[1].clone()], Vec::new(), 3)),
            Ok((None, vec![requests[1].clone()], vec![completion.key], 3)),
            Err(Error::InvalidSegment.to_string()),
        ],
    );
    let [old, new] =
        differential::pair::<_, _, Trace>(|| input.clone(), |input, old| run(&input, old));
    assert_eq!(new, old);
    assert_eq!(new.0, expected);
}

/// N retained one-byte files and three changes: an unrelated checkpoint, an
/// explicit removal and an oversized replacement.
fn fixture(n: usize) -> (Window, [Change; 3]) {
    let mut files: Vec<_> = (0..n).map(|index| unclassified(index, 1)).collect();
    let window = Window::new(&mut files, n, n, 0);
    let changes = [
        change(Vec::new(), None, Vec::new(), 7),
        change(vec![files[0].key.clone()], None, Vec::new(), 8),
        change(Vec::new(), Some(unclassified(1, n + 1)), Vec::new(), 9),
    ];
    (window, changes)
}
/// Allocations per change plus total allocated bytes, measured untraced.
fn allocations(n: usize, apply: fn(&mut Window, &mut Change)) -> [u64; 4] {
    let (mut window, mut changes) = fixture(n);
    let mut counts = [0; 4];
    for (index, change) in changes.iter_mut().enumerate() {
        let measured = measure(|| apply(&mut window, change));
        counts[index] = measured.count_total;
        counts[3] += measured.bytes_total;
    }
    counts
}
fn trace(n: usize) -> Vec<Trace> {
    differential::observe(
        || fixture(n),
        |(mut window, mut changes)| changes.iter_mut().for_each(|change| window.apply(change)),
    )
    .1
}
/// Restores the old path's per-change rebuild of every retained key.
fn rebuilding(window: &mut Window, change: &mut Change) {
    let before: BTreeSet<_> = window.entries.keys().cloned().collect();
    window.apply(change);
    drop(before);
}
fn old_allocations(n: usize) -> u64 {
    let mut files: Vec<_> = (0..n).map(|index| unclassified(index, 1)).collect();
    let mut window = old::window(&mut files, n, n, 0);
    let mut checkpoint = change(Vec::new(), None, Vec::new(), 7);
    measure(|| old::apply(&mut window, &mut checkpoint)).count_total
}

#[test]
fn window_change_work_is_independent_of_retained_files() {
    let exact = [0, 0, 1, 4 * std::mem::size_of::<FileKey>() as u64];
    for n in [64, 128] {
        assert_eq!(allocations(n, Window::apply), exact, "{n} files");
        assert_eq!(trace(n), [("remove", 0), ("remove", 1)], "{n} files");
    }
    assert_ne!(
        allocations(64, rebuilding),
        exact,
        "rebuilt keys were not detected"
    );
    let (old, older) = (old_allocations(64), old_allocations(128));
    assert!(
        0 < old && old < older,
        "independent baseline {old} -> {older}"
    );
}

/// Payloads still shared with another owner once the consumer takes them.
fn shared(files: impl IntoIterator<Item = FileValue>) -> usize {
    files
        .into_iter()
        .filter_map(|file| file.bytes.try_into_ordinary_vec().err())
        .count()
}
#[test]
fn window_retains_no_payload_custody() {
    // (snapshot files still shared, replacement still shared)
    let custody = |old: bool, retain: bool| {
        let mut files: Vec<_> = (0..16).map(|index| unclassified(index, 4)).collect();
        let mut replacement = change(Vec::new(), Some(unclassified(3, 4)), Vec::new(), 1);
        if old {
            let mut window = old::window(&mut files, 16, 64, 0);
            old::apply(&mut window, &mut replacement);
            return (shared(files), shared(replacement.file));
        }
        let mut window = Window::new(&mut files, 16, 64, 0);
        // Restores the old window's payload clones beside the metadata.
        let kept = if retain { files.clone() } else { Vec::new() };
        window.apply(&mut replacement);
        let kept_replacement = if retain {
            replacement.file.clone()
        } else {
            None
        };
        let result = (shared(files), shared(replacement.file));
        drop((kept, kept_replacement));
        result
    };
    assert_eq!(custody(false, false), (0, 0));
    assert_eq!(
        custody(false, true),
        (16, 1),
        "retained payloads were not detected"
    );
    assert_eq!(custody(true, false), (15, 1), "independent baseline");
}

/// Paired measurement: N retained files, then N/4 one-file changes, old and new.
#[test]
fn window_paired_measurement() {
    let n = 512;
    let [old, new] = [true, false].map(|old| {
        let mut files: Vec<_> = (0..n).map(|index| unclassified(index, 1)).collect();
        let mut changes: Vec<_> = (n..n + n / 4)
            .map(|index| {
                change(
                    Vec::new(),
                    Some(unclassified(index, 1)),
                    Vec::new(),
                    index as u64,
                )
            })
            .collect();
        let started = std::time::Instant::now();
        let allocation = measure(|| {
            if old {
                let mut window = old::window(&mut files, n, n, 0);
                changes
                    .iter_mut()
                    .for_each(|change| old::apply(&mut window, change));
            } else {
                let mut window = Window::new(&mut files, n, n, 0);
                changes.iter_mut().for_each(|change| window.apply(change));
            }
        });
        let elapsed = started.elapsed();
        let removed: Vec<_> = changes.into_iter().map(|change| change.removed).collect();
        println!(
            "old={old} n={n} allocations={} bytes={} elapsed={elapsed:?}",
            allocation.count_total, allocation.bytes_total
        );
        (files, removed, allocation.count_total)
    });
    assert_eq!((&new.0, &new.1), (&old.0, &old.1));
    assert!(new.2 < old.2);
}
