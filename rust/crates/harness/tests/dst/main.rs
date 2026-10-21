//! Seeded production replays: compare bytes, never rewrite identities or errors.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness::{
    Error, LocalHarness, Result, client,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::FilesystemHost,
    model::Model,
    resources::ProviderRef,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use acyclic_native_runtime::testing::DurableWrite;
use std::sync::Arc;
use std::{collections::BTreeMap, future::Future, path::Path, pin::Pin};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod clocks;
mod digest;
mod driver;
mod features;
mod recovery;
mod sources;

// Keep each simulator layer independent of the production future's layout.
type EntryFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

fn entry<'a, T>(future: impl Future<Output = T> + 'a) -> EntryFuture<'a, T> {
    Box::pin(future)
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Trace {
    polls: Vec<(u64, &'static str)>,
    boundaries: Vec<(String, String)>,
    writes: Vec<DurableWrite>,
    requests: Vec<u8>,
    dispatches: Vec<u8>,
    files: Vec<u8>,
    jsonl: Vec<u8>,
    durable: BTreeMap<String, (bool, Vec<u8>)>,
}

fn same_trace(left: &Trace, right: &Trace) -> std::result::Result<(), String> {
    if left.polls != right.polls {
        let index = left
            .polls
            .iter()
            .zip(&right.polls)
            .position(|(left, right)| left != right)
            .unwrap_or(left.polls.len().min(right.polls.len()));
        let start = index.saturating_sub(8);
        return Err(format!(
            "seeded task polling at {index} (lengths {}, {}): {:?} != {:?}; equal boundaries {}, writes {}, requests {}, dispatches {}, files {}, JSONL {}, durable {}",
            left.polls.len(),
            right.polls.len(),
            &left.polls[start..left.polls.len().min(index + 9)],
            &right.polls[start..right.polls.len().min(index + 9)],
            left.boundaries == right.boundaries,
            left.writes == right.writes,
            left.requests == right.requests,
            left.dispatches == right.dispatches,
            left.files == right.files,
            left.jsonl == right.jsonl,
            left.durable == right.durable,
        ));
    }
    if left.boundaries != right.boundaries {
        let index = left
            .boundaries
            .iter()
            .zip(&right.boundaries)
            .position(|(left, right)| left != right)
            .unwrap_or(left.boundaries.len().min(right.boundaries.len()));
        return Err(format!(
            "boundary {index}: {:?} != {:?}",
            left.boundaries.get(index),
            right.boundaries.get(index)
        ));
    }
    let write_difference = if left.writes != right.writes {
        let index = left
            .writes
            .iter()
            .zip(&right.writes)
            .position(|(left, right)| left != right)
            .unwrap_or(left.writes.len().min(right.writes.len()));
        Some(format!(
            "durable operation {index}: {:?} != {:?}",
            left.writes.get(index),
            right.writes.get(index)
        ))
    } else {
        None
    };
    for (name, left, right) in [
        ("requests", &left.requests, &right.requests),
        ("dispatches", &left.dispatches, &right.dispatches),
        ("files", &left.files, &right.files),
        ("jsonl", &left.jsonl, &right.jsonl),
    ] {
        if left != right {
            if name == "files" {
                let left: Vec<acyclic_harness::task_volume::FileValue> =
                    serde_json::from_slice(left).expect("serialized files");
                let right: Vec<acyclic_harness::task_volume::FileValue> =
                    serde_json::from_slice(right).expect("serialized files");
                if let Some((left, right)) = left.iter().zip(&right).find(|(a, b)| a != b) {
                    return Err(format!(
                        "{write_difference:?}; file {:?} != {:?}: {}",
                        left.key,
                        right.key,
                        byte_difference(&left.bytes, &right.bytes)
                    ));
                }
            }
            return Err(format!(
                "{write_difference:?}; {name}: {}",
                byte_difference(left, right)
            ));
        }
    }
    if left.durable != right.durable {
        for name in left.durable.keys().chain(right.durable.keys()) {
            match (left.durable.get(name), right.durable.get(name)) {
                (Some((left_kind, left)), Some((right_kind, right))) => {
                    if left_kind != right_kind || left != right {
                        return Err(format!(
                            "{write_difference:?}; durable {name} (symlink {left_kind}, {right_kind}): {}",
                            byte_difference(left, right)
                        ));
                    }
                }
                _ => return Err(format!("{write_difference:?}; durable path {name} missing")),
            }
        }
    }
    write_difference.map_or(Ok(()), Err)
}

fn byte_difference(left: &[u8], right: &[u8]) -> String {
    let offset = left
        .iter()
        .zip(right)
        .position(|(left, right)| left != right)
        .unwrap_or(left.len().min(right.len()));
    let context = |bytes: &[u8]| {
        String::from_utf8_lossy(&bytes[offset.saturating_sub(64)..bytes.len().min(offset + 256)])
            .into_owned()
    };
    format!("byte {offset}: {:?} != {:?}", context(left), context(right))
}

fn durable_bytes(directory: &Path) -> Result<BTreeMap<String, (bool, Vec<u8>)>> {
    let mut pending = vec![directory.to_path_buf()];
    let mut files = BTreeMap::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).map_err(storage)? {
            let entry = entry.map_err(storage)?;
            let path = entry.path();
            let kind = entry.file_type().map_err(storage)?;
            if kind.is_dir() {
                pending.push(path);
            } else if kind.is_file() || kind.is_symlink() {
                let name = path.strip_prefix(directory).map_err(storage)?;
                let name = name
                    .to_str()
                    .ok_or_else(|| Error::Invalid("non-UTF-8 durable path".into()))?;
                let bytes = if kind.is_symlink() {
                    let target = std::fs::read_link(&path).map_err(storage)?;
                    target
                        .to_str()
                        .ok_or_else(|| Error::Invalid("non-UTF-8 durable link".into()))?
                        .as_bytes()
                        .to_vec()
                } else {
                    std::fs::read(&path).map_err(storage)?
                };
                files.insert(name.to_owned(), (kind.is_symlink(), bytes));
            } else {
                return Err(Error::Invalid("unexpected durable entry".into()));
            }
        }
    }
    Ok(files)
}

fn integer_array(root: &serde_json::Value) -> bool {
    let mut pending = vec![root];
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Array(items) => {
                if !items.is_empty() && items.iter().all(serde_json::Value::is_number) {
                    return true;
                }
                pending.extend(items);
            }
            serde_json::Value::Object(fields) => pending.extend(fields.values()),
            _ => (),
        }
    }
    false
}

fn prompt<'a>(
    harness: &'a Arc<LocalHarness>,
    prompt: &'a str,
    expected: &'a str,
    headless: bool,
) -> EntryFuture<'a, Result<Vec<u8>>> {
    entry(async move {
        if !headless {
            assert_eq!(entry(harness.run(prompt)).await?.text, expected);
            return Ok(Vec::new());
        }
        // The same identity, request-file, client and finish path used by graphcoder -p.
        let operation = entry(client::local_identity(harness, Some(prompt), None)).await?;
        let command = client::prepare_local_prompt(operation, prompt)?;
        let (mut connection, target) =
            entry(client::connect_local(harness.clone(), operation)).await?;
        let mut jsonl = Vec::new();
        let outcome = entry(client::run_headless(
            &mut connection,
            command,
            target,
            &mut jsonl,
        ))
        .await;
        assert_eq!(
            entry(connection.finish(outcome)).await?.exit_code(),
            0,
            "headless outcome: {}",
            String::from_utf8_lossy(&jsonl)
        );
        assert_eq!(jsonl.last(), Some(&b'\n'), "incomplete JSONL frame");
        let text = std::str::from_utf8(&jsonl).map_err(storage)?;
        for line in text.lines() {
            let frame: serde_json::Value = serde_json::from_str(line).map_err(storage)?;
            assert!(
                !integer_array(&frame),
                "F17: integer-array bytes on the wire"
            );
        }
        let mut lines = text.lines();
        client::jsonl::parse_snapshot(lines.next().expect("missing initial frame"))?;
        let mut completed = false;
        for line in lines {
            let change = client::jsonl::parse_change(line)?;
            for file in change.file.iter().chain(&change.files) {
                if matches!(
                    file.key,
                    acyclic_harness::task_volume::FileKey::Session {
                        file: acyclic_harness::task_volume::TaskFile::ExecutionResult(_),
                        ..
                    }
                ) {
                    let outcome: serde_json::Value =
                        serde_json::from_slice(&file.bytes).map_err(storage)?;
                    assert_eq!(
                        outcome["ok"]["text"], expected,
                        "final text differs from provider output"
                    );
                    completed = true;
                }
            }
        }
        assert!(completed, "JSONL omitted the terminal outcome");
        Ok(jsonl)
    })
}

fn scenario(directory: &Path) -> EntryFuture<'_, Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)>> {
    entry(async move {
        // The independently initialized stores and settings never discover HOME.
        let reference =
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
        let volume = VolumeRef::new(
            reference.clone(),
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("dst".into()),
            &mut None,
        )?;
        let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
            .await
            .map_err(storage)?;
        FilesystemHost::new(fs, &reference, None)?
            .create_volume(&volume)
            .await?;
        let model = Model::new(
            "scripted".into(),
            "dst".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let provider = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Text("done".into()),
            ScriptedTurn::Text("reopened".into()),
        ]));
        let config = || -> Result<_> {
            Ok(local_harness_config(
                directory.join("filesystem"),
                (model.clone(), provider.clone()),
            )?
            .volume(volume.clone())
            .state_directory(directory.join("state")))
        };
        let harness = Arc::new(entry(LocalHarness::open(config()?)).await?);
        prompt(&harness, "finish this turn", "done", false).await?;
        let files = harness
            .task_files()
            .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
            .await
            .map_err(storage)?
            .snapshot
            .ok_or_else(|| Error::Invalid("missing snapshot".into()))?
            .files;
        drop(harness);
        let reopened = Arc::new(entry(LocalHarness::open(config()?)).await?);
        let restored = reopened
            .task_files()
            .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
            .await
            .map_err(storage)?
            .snapshot
            .ok_or_else(|| Error::Invalid("missing reopened snapshot".into()))?
            .files;
        assert_eq!(files, restored, "cold reopen lost acknowledged files");
        let jsonl = prompt(&reopened, "submit after reopening", "reopened", true).await?;
        let files = reopened
            .task_files()
            .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
            .await
            .map_err(storage)?
            .snapshot
            .ok_or_else(|| Error::Invalid("missing final snapshot".into()))?
            .files;
        Ok((
            serde_json::to_vec(&provider.requests()).map_err(storage)?,
            serde_json::to_vec(&provider.dispatches()).map_err(storage)?,
            serde_json::to_vec(&files).map_err(storage)?,
            jsonl,
        ))
    })
}

fn replay(seed: u64, erase_source: bool) -> EntryFuture<'static, Result<Trace>> {
    replay_with_directory_times(seed, erase_source, 1_700_000_000_000_000_000)
}

fn replay_with_directory_times(
    seed: u64,
    erase_source: bool,
    directory_time: i64,
) -> EntryFuture<'static, Result<Trace>> {
    entry(async move {
        // Negative control erases entropy without changing polling or time.
        let mut driver = if erase_source {
            driver::Driver::without_entropy(seed)
        } else {
            driver::Driver::new(seed, None)
        };
        // Native directory times are fixture inputs, just as scripted provider
        // output is. Physical identities and snapshot validation remain real;
        // every supplied timestamp is encoded in the unchanged durable trace.
        driver.schedule.set_native_times_provider(move |input| {
            if input.directory {
                acyclic_native_runtime::testing::NativeTimes {
                    created_ns: input.times.created_ns.map(|_| directory_time),
                    modified_ns: input.times.modified_ns.map(|_| directory_time),
                    accessed_ns: input.times.accessed_ns.map(|_| directory_time),
                    changed_ns: input.times.changed_ns.map(|_| directory_time),
                }
            } else {
                input.times
            }
        });
        let directory = tempfile::tempdir().map_err(storage)?;
        let result = driver.run(scenario(directory.path())).await;
        let driver::Outcome::Completed(result) = result else {
            panic!("uncut replay crashed");
        };
        let (requests, dispatches, files, jsonl) = result?;
        Ok(Trace {
            polls: driver.schedule.polls(),
            boundaries: driver.boundaries,
            writes: driver.schedule.durable_writes(),
            requests,
            dispatches,
            files,
            jsonl,
            durable: durable_bytes(directory.path())?,
        })
    })
}

#[test]
fn scripted_native_directory_times_reach_durable_replay_bytes() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let first_time = 1_700_000_000_000_000_017_i64;
        let second_time = 1_700_000_000_000_000_019_i64;
        let first = replay_with_directory_times(71, false, first_time).await?;
        let second = replay_with_directory_times(71, false, second_time).await?;
        for (trace, timestamp) in [(&first, first_time), (&second, second_time)] {
            let input = timestamp.to_le_bytes();
            assert!(
                trace
                    .durable
                    .values()
                    .any(|(_, bytes)| bytes.windows(input.len()).any(|window| window == input)),
                "native timestamp input never reached the durable replay bytes"
            );
        }
        assert_ne!(
            first.durable, second.durable,
            "changing native timestamp input did not change durable replay bytes"
        );
        assert!(same_trace(&first, &second).is_err());
        assert_ne!(digest::pair(&first), digest::pair(&second));
        Ok(())
    })
}

#[test]
fn seeded_production_turn_and_live_f17_trace_controls() -> Result<()> {
    digest::startup_value();
    let started = std::time::Instant::now();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let result = runtime.block_on(async {
        tokio::time::pause();
        // The same-seed peer and entropy-erasure world run in the other process
        // (digest::cross_process_seeded_replay_digest). All four worlds still
        // cover direct submit, cold reopen and the headless entry.
        let first = replay(71, false).await?;
        let first_digest = digest::emit(&first);
        live_f17_jsonl_never_emits_byte_arrays(&first)?;
        // Comparing held bytes must still detect even a one-bit disk mutation.
        let mut corrupted = first.clone();
        let (_, bytes) = corrupted
            .durable
            .values_mut()
            .find(|(_, bytes)| !bytes.is_empty())
            .expect("production durable bytes");
        bytes[0] ^= 1;
        assert!(
            same_trace(&first, &corrupted).is_err(),
            "one-bit durable mutation escaped the replay oracle"
        );
        assert_ne!(first_digest, digest::pair(&corrupted));
        drop(corrupted);
        assert!(
            !first.boundaries.is_empty(),
            "production boundaries were bypassed"
        );
        assert!(
            first.writes.iter().any(|write| write.requested > 0),
            "original durable writes were bypassed"
        );
        let different = replay(72, false).await?;
        assert_ne!(
            first.dispatches, different.dispatches,
            "seed did not reach production identities"
        );
        Ok(())
    });
    eprintln!(
        "direct/headless/F17 replay elapsed: {:?}",
        started.elapsed()
    );
    result
}

fn live_f17_jsonl_never_emits_byte_arrays(trace: &Trace) -> Result<()> {
    assert!(!trace.jsonl.is_empty(), "headless output bypassed");
    // F17's old production route derived serde directly from the stored
    // snapshot. The independent wire oracle must reject that same route.
    let text = std::str::from_utf8(&trace.jsonl).map_err(storage)?;
    let snapshot = client::jsonl::parse_snapshot(text.lines().next().expect("initial frame"))?;
    let mutant = serde_json::to_value(snapshot).map_err(storage)?;
    assert!(
        integer_array(&mutant),
        "F17: bypassing the production wire codec escaped the oracle"
    );
    Ok(())
}
