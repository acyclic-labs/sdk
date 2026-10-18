//! Independent old-index differential, exact work/allocation bounds and controls
//! for the derived command and display indexes.
use super::{
    Channel, Component, Error, FileKey, FileValue, Index, Query, Result, TaskFile, ToolFile, apply,
    command_query, recent,
};
use acyclic_test_support::measure;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

/// The pre-change index and its readers, kept verbatim apart from field paths.
mod old {
    use super::*;
    use crate::task_volume::command_query::{identity, related};

    pub(super) struct Stored {
        pub(super) file: FileValue,
        pub(super) position: u64,
    }
    #[derive(Default)]
    pub(super) struct Index {
        pub(super) files: BTreeMap<FileKey, Stored>,
        pub(super) executions: BTreeMap<(Option<Component>, u64), FileKey>,
        pub(super) pending_commands: BTreeMap<Option<Component>, BTreeSet<FileKey>>,
        pub(super) view_order: BTreeMap<(Component, u8, u64), FileKey>,
        pub(super) view_revisions: BTreeMap<FileKey, (u8, u64)>,
        pub(super) overviews: BTreeMap<Component, FileKey>,
    }
    pub(super) fn owner(key: &FileKey) -> Option<Component> {
        match key {
            FileKey::Session { session, .. } => Some(session.clone()),
            FileKey::Task {
                file:
                    TaskFile::Request(id) | TaskFile::ExecutionClaim(id) | TaskFile::ExecutionResult(id),
                ..
            } => identity(id)
                .and_then(|(session, _)| session)
                .map(|session| Component::new(session.to_string()).unwrap()),
            _ => None,
        }
    }
    pub(super) fn matches_command(session: &Component, id: &Component) -> bool {
        identity(id).is_some_and(|(owner, _)| {
            owner.is_none_or(|owner| owner.to_string() == session.as_str())
        })
    }
    fn is_result(key: &FileKey) -> bool {
        matches!(
            key,
            FileKey::Session {
                file: TaskFile::ExecutionResult(_),
                ..
            } | FileKey::Task {
                file: TaskFile::ExecutionResult(_),
                ..
            }
        ) && related(key, TaskFile::Request).is_some()
    }
    fn file_kind(file: &FileValue) -> Option<u8> {
        if matches!(
            file.key,
            FileKey::Task {
                file: TaskFile::Chunk { .. },
                ..
            }
        ) && file.channel == Some(Channel::Reasoning)
        {
            Some(3)
        } else {
            recent::kind(&file.key)
        }
    }
    fn record(index: &mut Index, file: &FileValue, position: u64) {
        let (FileKey::Task { task, file: leaf }, Some(kind)) = (&file.key, file_kind(file)) else {
            return;
        };
        if let Some((old_kind, sequence)) = index
            .view_revisions
            .insert(file.key.clone(), (kind, position))
        {
            index.view_order.remove(&(task.clone(), old_kind, sequence));
        }
        index
            .view_order
            .insert((task.clone(), kind, position), file.key.clone());
        if *leaf == TaskFile::Status {
            index.overviews.insert(task.clone(), file.key.clone());
        }
    }
    pub(super) fn apply(index: &mut Index, file: FileValue, position: u64) {
        record(index, &file, position);
        let owner = owner(&file.key);
        if is_result(&file.key) && !index.files.contains_key(&file.key) {
            index
                .executions
                .insert((owner.clone(), position), file.key.clone());
            if let Some(pending) = index.pending_commands.get_mut(&owner) {
                for make in [TaskFile::Request, TaskFile::ExecutionClaim] {
                    if let Some(key) = related(&file.key, make) {
                        pending.remove(&key);
                    }
                }
                if pending.is_empty() {
                    index.pending_commands.remove(&owner);
                }
            }
        } else if let Some(result) = related(&file.key, TaskFile::ExecutionResult) {
            if !index.files.contains_key(&result) {
                index
                    .pending_commands
                    .entry(owner)
                    .or_default()
                    .insert(file.key.clone());
            }
        }
        index
            .files
            .insert(file.key.clone(), Stored { file, position });
    }
    pub(super) fn commands(
        index: &Index,
        session: &Component,
        recent: u32,
        maximum: usize,
    ) -> Result<Vec<FileValue>> {
        let mut selected = BTreeMap::new();
        let mut insert = |key: &FileKey| -> Result<()> {
            if !selected.contains_key(key) && selected.len() == maximum {
                return Err(Error::LimitExceeded);
            }
            if let Some(file) = index.files.get(key) {
                selected.insert(key.clone(), file.file.clone());
            }
            Ok(())
        };
        for directory in ["config".to_owned(), format!("session-{}", session.as_str())] {
            for range in Query::UiDirectory(Component::new(directory)?).ranges() {
                for key in index.files.range(range).map(|(key, _)| key) {
                    insert(key)?;
                }
            }
        }
        for path in ["commands/catalog.json", "commands/themes.json"] {
            insert(&FileKey::parse(path)?)?;
        }
        for key in index
            .pending_commands
            .get(&None)
            .into_iter()
            .chain(index.pending_commands.get(&Some(session.clone())))
            .flat_map(|keys| keys.iter())
        {
            insert(key)?;
        }
        let mut tasks = index
            .executions
            .range((None, 0)..=(None, u64::MAX))
            .rev()
            .peekable();
        let mut own = index
            .executions
            .range((Some(session.clone()), 0)..=(Some(session.clone()), u64::MAX))
            .rev()
            .peekable();
        for _ in 0..recent {
            let next = match (tasks.peek(), own.peek()) {
                (Some((left, _)), Some((right, _))) if left.1 >= right.1 => tasks.next(),
                (_, Some(_)) => own.next(),
                (Some(_), None) => tasks.next(),
                _ => break,
            };
            let (_, key) = next.ok_or(Error::InvalidSegment)?;
            insert(key)?;
            if let Some(request) = related(key, TaskFile::Request) {
                insert(&request)?;
            }
        }
        Ok(selected.into_values().collect())
    }
    pub(super) fn views(index: &Index, query: &Query, maximum: usize) -> Result<Vec<FileValue>> {
        if let Query::TaskSession { task, session } = query {
            let display = Query::TaskView {
                task: task.clone(),
                recent: 128,
                reasoning: true,
                details: true,
                terminal: true,
            };
            let mut files = views(index, &display, maximum.saturating_sub(3))?;
            for (_, stored) in index
                .files
                .range(Query::Session(session.clone()).ranges().remove(0))
            {
                if files.len() == maximum {
                    return Err(Error::LimitExceeded);
                }
                files.push(stored.file.clone());
            }
            return Ok(files);
        }
        if let Query::TaskView { task, recent, .. } = query {
            if *recent == 0 {
                return Err(Error::LimitExceeded);
            }
            let mut files = Vec::new();
            for kind in 0..=7 {
                for ((_, _, sequence), key) in index
                    .view_order
                    .range((task.clone(), kind, 0)..=(task.clone(), kind, u64::MAX))
                    .rev()
                    .take((*recent as usize).min(maximum))
                {
                    let file = index.files.get(key).ok_or(Error::InvalidSegment)?;
                    if query.matches_file(&file.file) {
                        files.push((*sequence, file.file.clone()));
                    }
                }
            }
            files.sort_by_key(|(sequence, _)| *sequence);
            let excess = files.len().saturating_sub(maximum);
            Ok(files
                .into_iter()
                .skip(excess)
                .map(|(_, file)| file)
                .collect())
        } else {
            Ok(index
                .overviews
                .values()
                .take(maximum)
                .filter_map(|key| index.files.get(key).map(|stored| stored.file.clone()))
                .collect())
        }
    }
}

fn component(value: impl AsRef<str>) -> Component {
    Component::new(value).unwrap()
}
const SESSIONS: [&str; 2] = [
    "6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f",
    "00000000-0000-4000-8000-000000000001",
];
const OPERATIONS: [&str; 3] = [
    "00000000000000000000000000000001",
    "00000000000000000000000000000002",
    "00000000000000000000000000000003",
];
/// Session-owned, unowned and foreign command ids; malformed ids are not commands.
fn command(index: u8) -> Component {
    let operation = OPERATIONS[usize::from(index % 3)];
    match index % 4 {
        0 => component(operation),
        1 | 2 => component(format!(
            "command-{}.{operation}",
            SESSIONS[usize::from(index % 2)]
        )),
        _ => component(format!("bogus.{operation}")),
    }
}
/// Pool covering display kinds, reasoning chunks, task and session command
/// lifecycles, UI files and replacements of every key.
fn pooled((index, size): (u8, u8)) -> FileValue {
    let task = component(if index < 32 { "a" } else { "b" });
    let call = component(format!("c{}", index % 2));
    let turn = u64::from(index % 3);
    let id = command(index / 3);
    let bytes = vec![index; usize::from(size)];
    let (file, channel) = match index % 16 {
        0 => (TaskFile::Status, None),
        1 => (TaskFile::TurnRequest(turn), None),
        2 => (
            TaskFile::Chunk {
                turn,
                sequence: u64::from(index),
            },
            Some(Channel::Text),
        ),
        3 => (
            TaskFile::Chunk {
                turn,
                sequence: u64::from(index),
            },
            Some(Channel::Reasoning),
        ),
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
                file: ToolFile::Stdout(turn),
            },
            None,
        ),
        6 => (
            TaskFile::Tool {
                turn,
                call,
                file: ToolFile::Request,
            },
            None,
        ),
        7 => (TaskFile::Completion(turn), None),
        8 | 9 => (TaskFile::Request(id), None),
        10 => (TaskFile::ExecutionClaim(id), None),
        11 | 12 => (TaskFile::ExecutionResult(id), None),
        13 => {
            let session = component(SESSIONS[usize::from(index % 2)]);
            let file = [
                TaskFile::Request,
                TaskFile::ExecutionClaim,
                TaskFile::ExecutionResult,
            ][usize::from(index / 16 % 3)](id);
            return FileValue::new(FileKey::Session { session, file }, bytes);
        }
        14 => {
            let directory = ["config".to_owned(), format!("session-{}", SESSIONS[0])];
            let directory = component(&directory[usize::from(index / 16 % 2)]);
            let name = component("layout.json");
            return FileValue::new(FileKey::Ui { directory, name }, bytes);
        }
        _ => return FileValue::new(FileKey::parse("commands/catalog.json").unwrap(), bytes),
    };
    FileValue {
        key: FileKey::Task { task, file },
        channel,
        requester: None,
        publication: None,
        bytes: bytes.into(),
    }
}
fn queries(recent: u32) -> Vec<Query> {
    let view = |task: &str| Query::TaskView {
        task: component(task),
        recent,
        reasoning: recent % 2 == 0,
        details: true,
        terminal: recent != 1,
    };
    vec![
        view("a"),
        view("b"),
        Query::TaskSession {
            task: component("a"),
            session: component(SESSIONS[1]),
        },
        Query::TaskOverviews,
    ]
}
type Output = Vec<std::result::Result<Vec<FileValue>, String>>;
fn outputs(
    commands: impl Fn(&Component, u32, usize) -> Result<Vec<FileValue>>,
    views: impl Fn(&Query, usize) -> Result<Vec<FileValue>>,
    recent: u32,
    maximum: usize,
) -> Output {
    let mut output: Output = SESSIONS
        .iter()
        .map(|session| commands(&component(session), recent, maximum).map_err(|e| e.to_string()))
        .collect();
    output.extend(
        queries(recent)
            .iter()
            .map(|query| views(query, maximum).map_err(|e| e.to_string())),
    );
    output
}
/// Old and new derived state in one canonical shape, without the deleted revisions.
type Derived = (
    Vec<((Option<Component>, u64), FileKey)>,
    Vec<(Option<Component>, Vec<FileKey>)>,
    Vec<((Component, u8, u64), FileKey)>,
    Vec<(Component, FileKey)>,
);
fn candidate(files: &[FileValue], recent: u32, maximum: usize) -> (Output, Derived) {
    let mut index = Index::default();
    for (position, file) in files.iter().enumerate() {
        apply(&mut index, copied(file), position as u64 + 1);
    }
    let output = outputs(
        |session, recent, maximum| command_query::snapshot(&index, session, recent, maximum),
        |query, maximum| recent::snapshot(&index, query, maximum),
        recent,
        maximum,
    );
    let ((executions, pending), (order, overviews)) = (index.commands.parts(), index.views.parts());
    (output, (executions, pending, order, overviews))
}
fn baseline(files: &[FileValue], recent: u32, maximum: usize) -> (Output, Derived) {
    let mut index = old::Index::default();
    for (position, file) in files.iter().enumerate() {
        old::apply(&mut index, copied(file), position as u64 + 1);
    }
    let output = outputs(
        |session, recent, maximum| old::commands(&index, session, recent, maximum),
        |query, maximum| old::views(&index, query, maximum),
        recent,
        maximum,
    );
    let derived = (
        index.executions.clone().into_iter().collect(),
        index
            .pending_commands
            .iter()
            .map(|(owner, keys)| (owner.clone(), keys.iter().cloned().collect()))
            .collect(),
        index.view_order.clone().into_iter().collect(),
        index.overviews.clone().into_iter().collect(),
    );
    (output, derived)
}
/// Fresh payload storage per side.
fn copied(file: &FileValue) -> FileValue {
    FileValue {
        bytes: file.bytes.to_vec().into(),
        ..file.clone()
    }
}

proptest! {
    /// Snapshot bytes, exact errors and every derived index entry match the
    /// independently built old index over dense/sparse lifecycles and replacements.
    #[test]
    fn derived_indexes_match_independent_old_index(
        files in prop::collection::vec((0u8..64, 0u8..4).prop_map(pooled), 0..48),
        recent in 0u32..4,
        maximum in 1usize..12,
    ) {
        prop_assert_eq!(candidate(&files, recent, maximum), baseline(&files, recent, maximum));
    }
}

/// Index accesses for one apply, measured on the production index after `n` chunks.
fn examined(n: u64, file: FileValue, before: fn(&Index, &FileKey)) -> u64 {
    let mut index = Index::default();
    let task = component("a");
    for sequence in 0..n {
        let chunk = TaskFile::Chunk { turn: 0, sequence };
        apply(
            &mut index,
            FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: chunk,
                },
                b"x".to_vec(),
            ),
            sequence + 1,
        );
    }
    let request = TaskFile::Request(command(1));
    apply(
        &mut index,
        FileValue::new(
            FileKey::Task {
                task,
                file: request,
            },
            b"r".to_vec(),
        ),
        n + 1,
    );
    crate::test_support::measure_task_volume_work(|| {
        before(&index, &file.key);
        apply(&mut index, file, n + 2);
    })
    .1
}
fn nothing(_: &Index, _: &FileKey) {}
/// Restores the old duplicate presence probe ahead of the insert.
fn probing(index: &Index, key: &FileKey) {
    index.files.contains_key(key);
}

#[test]
fn apply_examines_a_constant_number_of_index_entries() {
    let task = || component("a");
    let result = || {
        FileValue::new(
            FileKey::Task {
                task: task(),
                file: TaskFile::ExecutionResult(command(1)),
            },
            b"done".to_vec(),
        )
    };
    let claim = || {
        FileValue::new(
            FileKey::Task {
                task: task(),
                file: TaskFile::ExecutionClaim(command(5)),
            },
            b"claim".to_vec(),
        )
    };
    let chunk = || {
        FileValue::new(
            FileKey::Task {
                task: task(),
                file: TaskFile::Chunk {
                    turn: 1,
                    sequence: 0,
                },
            },
            b"x".to_vec(),
        )
    };
    for n in [64, 128] {
        // One insert; a pending command also checks its result once.
        assert_eq!(examined(n, result(), nothing), 1, "{n} chunks");
        assert_eq!(examined(n, claim(), nothing), 2, "{n} chunks");
        assert_eq!(examined(n, chunk(), nothing), 1, "{n} chunks");
    }
    assert_ne!(
        examined(64, result(), probing),
        1,
        "the duplicate probe was not detected"
    );
}

#[test]
fn command_identity_checks_allocate_only_the_owner() {
    let id = command(1);
    let key = FileKey::Task {
        task: component("a"),
        file: TaskFile::Request(id.clone()),
    };
    let session = component(SESSIONS[1]);
    let count = |run: &dyn Fn()| measure(run).count_total;
    let new = [
        count(&|| {
            let _ = std::hint::black_box(command_query::matches(&session, &key));
        }),
        count(&|| drop(std::hint::black_box(command_query::owner(&key)))),
    ];
    assert_eq!(new, [0, 1]);
    let old = [
        count(&|| {
            let _ = std::hint::black_box(old::matches_command(&session, &id));
        }),
        count(&|| drop(std::hint::black_box(old::owner(&key)))),
    ];
    assert_eq!(old, [1, 2], "independent baseline");
    assert_eq!(command_query::owner(&key), old::owner(&key));
    assert_eq!(
        command_query::matches(&session, &key),
        old::matches_command(&session, &id)
    );
}

/// Display entries retained per display file: order plus Status overviews only.
#[test]
fn display_index_retains_one_entry_per_display_file() {
    let n = 64u64;
    let task = component("a");
    let files: Vec<_> = (0..n)
        .map(|sequence| {
            let chunk = TaskFile::Chunk { turn: 0, sequence };
            FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: chunk,
                },
                b"x".to_vec(),
            )
        })
        .chain([FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::Status,
            },
            b"s".to_vec(),
        )])
        .collect();
    let mut index = Index::default();
    let mut previous = old::Index::default();
    for (position, file) in files.iter().enumerate() {
        apply(&mut index, file.clone(), position as u64 + 1);
        old::apply(&mut previous, file.clone(), position as u64 + 1);
    }
    let (order, overviews) = index.views.parts();
    assert_eq!((order.len(), overviews.len()), (n as usize + 1, 1));
    assert_eq!(
        previous.view_order.len() + previous.overviews.len() + previous.view_revisions.len(),
        2 * (n as usize + 1) + 1,
        "independent baseline"
    );
}

/// Paired measurement: N chunk publications and N/16 command lifecycles.
#[test]
fn derived_index_paired_measurement() {
    let n = 4096u64;
    let task = component("a");
    let mut files = Vec::new();
    for sequence in 0..n {
        let chunk = TaskFile::Chunk { turn: 0, sequence };
        files.push(FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: chunk,
            },
            b"x".to_vec(),
        ));
        if sequence % 16 == 0 {
            let id = component(format!("command-{}.{:032x}", SESSIONS[0], sequence + 1));
            for file in [
                TaskFile::Request,
                TaskFile::ExecutionClaim,
                TaskFile::ExecutionResult,
            ] {
                files.push(FileValue::new(
                    FileKey::Task {
                        task: task.clone(),
                        file: file(id.clone()),
                    },
                    b"c".to_vec(),
                ));
            }
        }
    }
    let mut new_index = Index::default();
    let mut old_index = old::Index::default();
    let [old, new] = [true, false].map(|old| {
        // The input buffer outlives the measurement, so retained bytes only
        // net allocations made inside it.
        let mut inputs: Vec<_> = files.iter().map(copied).collect();
        let started = std::time::Instant::now();
        let allocation = measure(|| {
            for (position, file) in inputs.drain(..).enumerate() {
                if old {
                    old::apply(&mut old_index, file, position as u64 + 1);
                } else {
                    apply(&mut new_index, file, position as u64 + 1);
                }
            }
        });
        let elapsed = started.elapsed();
        println!(
            "old={old} files={} allocations={} bytes={} retained={} elapsed={elapsed:?}",
            files.len(),
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_current
        );
        allocation
    });
    assert!(new.count_total < old.count_total && new.bytes_current < old.bytes_current);
}

/// A status rewrite after N display files allocates nothing at N or 2N: the
/// replaced stored file locates the old order entry without a revision map.
#[test]
fn status_replacement_allocations_are_independent_of_retained_files() {
    let status = |body: &[u8]| {
        FileValue::new(
            FileKey::Task {
                task: component("a"),
                file: TaskFile::Status,
            },
            body.to_vec(),
        )
    };
    let counts = |n: u64, old: bool| {
        let mut index = Index::default();
        let mut previous = old::Index::default();
        let task = component("a");
        for sequence in 0..n {
            let chunk = FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Chunk { turn: 0, sequence },
                },
                b"x".to_vec(),
            );
            apply(&mut index, copied(&chunk), sequence + 1);
            old::apply(&mut previous, chunk, sequence + 1);
        }
        apply(&mut index, status(b"a"), n + 1);
        old::apply(&mut previous, status(b"a"), n + 1);
        let file = status(b"b");
        let measured = measure(|| {
            if old {
                old::apply(&mut previous, file, n + 2);
            } else {
                apply(&mut index, file, n + 2);
            }
        });
        [measured.count_total, measured.bytes_total]
    };
    let new = [counts(64, false), counts(128, false)];
    let old = [counts(64, true), counts(128, true)];
    assert_eq!(new, [[0, 0]; 2]);
    assert_eq!(old, [[0, 0]; 2], "independent baseline");
}

/// The baseline stores exactly main's historical value layout, so its
/// retained bytes cannot drift below the production index it is paired with.
#[test]
fn baseline_index_values_keep_the_historical_layout() {
    let file = FileValue::new(FileKey::parse("commands/catalog.json").unwrap(), Vec::new());
    let stored = old::Stored {
        file: file.clone(),
        position: 7,
    };
    let old::Stored { file: _, position } = &stored;
    assert_eq!(*position, 7);
    assert_eq!(
        std::mem::size_of::<old::Stored>(),
        std::mem::size_of::<super::StoredFile>()
    );
    assert_eq!(
        std::mem::align_of::<old::Stored>(),
        std::mem::align_of::<super::StoredFile>()
    );
}
