use super::*;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::NativeMaterializationWork;
use acyclic_fs::{CancellationToken, performance::work_budget};

fn schedule() -> Schedule {
    Schedule {
        version: 1,
        id: crate::agent::IntentId::try_from("retained-file".to_owned()).unwrap(),
        owner: AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16])),
        body: serde_json::json!({
            "prompt": "continue", "paths": ["one", "two"],
            "$serde_json::private::RawValue": {"literal": true}
        }),
        timezone: "UTC".into(),
        next_due_unix_ms: 100,
        recurrence: Some(Recurrence::Every { milliseconds: 10 }),
        missed: Missed::Coalesce,
        lifetime: Lifetime::Daemon,
        expires_unix_ms: None,
        remaining: None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn actor_record_encoding_retains_original_native_credit_and_refuses_before_construction()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_actors::{
        addressing::ActorReference,
        guest::{Checkpoint, CodecIdentity, Subscription},
        local::Store,
    };
    use acyclic_stream::StreamProviderExt as _;
    use acyclic_stream::{CommitOutcome, CommitRequest, IdempotencyKey, MemoryStream, StreamPath};
    use std::{collections::BTreeMap, sync::Arc};

    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider.clone(), StreamPath::new("native-record/actors")?);
    let actor = ActorReference::new(b"account", b"agents", b"owner")?;
    let subscription = StreamPath::new("native-record/input")?;
    let effects = store.create(
        &actor,
        BTreeMap::from([(
            "input".into(),
            Subscription {
                path: subscription.clone(),
                cursor: 0,
            },
        )]),
        Some(Checkpoint {
            codec: CodecIdentity {
                name: "native-record".into(),
                version: "1".into(),
            },
            bytes: bytes::Bytes::from_static(b"original checkpoint"),
        }),
    )?;
    let mut request = CommitRequest {
        deadline_unix_millis: None,
        conditions: Vec::new(),
        mutations: Vec::new(),
        idempotency_key: IdempotencyKey::new(b"native-record".to_vec())?,
    };
    for effect in effects {
        request.conditions.extend(effect.conditions);
        request.mutations.push(effect.mutation);
    }
    assert!(matches!(
        provider.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    let snapshot = store.load(&actor).await?;
    let state = snapshot.state().ok_or("missing original state")?;
    let expected = state.encode();
    let budget = work_budget(1_000_000, 1_000_000);
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let observer = PreparationIo::from_native_work(&work)?;
    let before_output = work.live_allocation_bytes();
    let before = work.receipt();
    let output = state
        .encode_native(&work.runtime_meter())
        .map_err(|error| crate::Error::NativeWork {
            cause: work.runtime_error(error),
            custody: None,
        })?;
    assert_eq!(output.value(), &expected);
    assert_eq!(
        work.receipt().allocation_operations - before.allocation_operations,
        1
    );
    assert_eq!(
        work.receipt().bytes_encoded - before.bytes_encoded,
        expected.len() as u64
    );
    assert_eq!(
        work.receipt().bytes_copied - before.bytes_copied,
        (b"\xffAST\x01".len()
            + b"native-record".len()
            + b"1".len()
            + b"original checkpoint".len()
            + b"input".len()
            + subscription.as_str().len()) as u64
    );
    assert_eq!(
        work.live_allocation_bytes() - before_output,
        output.value().capacity() as u64
    );
    drop(observer);
    cancellation.cancel();
    assert_eq!(output.value(), &expected);
    assert!(state.encode_native(&work.runtime_meter()).is_err());
    drop(output);
    assert_eq!(work.live_allocation_bytes(), baseline);

    for field in 0..4 {
        let cancellation = CancellationToken::new();
        let mut denied = budget;
        match field {
            0 => denied.peak_allocation_bytes = 0,
            1 => denied.allocation_operations = 0,
            2 => denied.bytes_encoded = 0,
            _ => denied.bytes_copied = 0,
        }
        let work = NativeMaterializationWork::new(denied, &cancellation)
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        // The ordinary encoder is the actual unobserved-allocation negative.
        assert_eq!(state.encode(), expected);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert!(state.encode_native(&work.runtime_meter()).is_err());
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(work.receipt().bytes_encoded, 0);
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn file_admission_keeps_original_credit_and_refuses_index_or_owner_substitution() -> Result<()> {
    let expected = schedule();
    let path = expected.path();
    let bytes = serde_json::to_vec(&expected).unwrap();
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(work_budget(1_000_000, 1_000_000), &cancellation)
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
    let baseline = native.live_allocation_bytes();
    let io = PreparationIo::from_native_work(&native)?;
    let foreign = AgentPath::root(&crate::agent::AgentId::from_bytes([2; 16]));
    let empty_body = Schedule {
        body: serde_json::json!({}),
        ..expected.clone()
    };
    let empty_bytes = serde_json::to_vec(&empty_body).unwrap();
    let admitted_empty = WakeFile::decode(&path, &expected.owner, &empty_bytes, &io)?;
    assert!(
        matches!(admitted_empty.definition(), WakeDefinition::Schedule(value) if value == &empty_body)
    );
    drop(admitted_empty);
    assert_eq!(native.live_allocation_bytes(), baseline);
    for (selected, owner) in [
        (
            path.replace("00000000000000000100", "00000000000000000900"),
            &expected.owner,
        ),
        (
            path.replacen("schedules/", "schedules/extra/", 1),
            &expected.owner,
        ),
        (path.clone(), &foreign),
    ] {
        assert!(WakeFile::decode(&selected, owner, &bytes, &io).is_err());
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    let actual = WakeFile::decode(&path, &expected.owner, &bytes, &io)?;
    drop(io);
    cancellation.cancel();
    let WakeDefinition::Schedule(actual_schedule) = actual.definition() else {
        panic!("schedule was decoded as another document");
    };
    assert_eq!(actual_schedule, &expected);
    assert!(native.live_allocation_bytes() > baseline);
    drop(actual);
    assert_eq!(native.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn wake_file_credit_refuses_before_an_unobserved_decoder_can_allocate() -> Result<()> {
    let expected = schedule();
    let bytes = serde_json::to_vec(&expected).unwrap();
    let cancellation = CancellationToken::new();
    let mut budget = work_budget(1_000_000, 1_000_000);
    budget.peak_allocation_bytes = 0;
    let native = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&native)?;
    // The former raw decoder can construct this document under the same zero
    // credit because it never consults the original native ledger.
    let raw: Schedule = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(raw, expected);
    assert_eq!(native.live_allocation_bytes(), 0);
    assert!(WakeFile::decode(&expected.path(), &expected.owner, &bytes, &io).is_err());
    assert_eq!(native.live_allocation_bytes(), 0);
    assert_eq!(native.receipt().backend_read_operations, 0);
    Ok(())
}

#[test]
fn monitor_admission_binds_the_deadline_and_resume_fence() -> Result<()> {
    let owner = schedule().owner;
    let mut expected = Monitor {
        version: 1,
        id: crate::agent::IntentId::try_from("file-monitor".to_owned()).unwrap(),
        source: "source/events".into(),
        cursor: MonitorCursor {
            next: 0,
            preceding: None,
        },
        filter: MonitorFilter::default(),
        wake: false,
        deadline_unix_ms: Some(100),
        coalesce_key: Some("files".into()),
    };
    let io =
        PreparationIo::from_budget(work_budget(1_000_000, 1_000_000), &CancellationToken::new())?;
    let path = expected.path();
    let bytes = serde_json::to_vec(&expected).unwrap();
    let file = WakeFile::decode(&path, &owner, &bytes, &io)?;
    assert!(matches!(file.definition(), WakeDefinition::Monitor(monitor) if monitor == &expected));
    assert!(
        WakeFile::decode(&path.replace("bounded/", "unbounded/"), &owner, &bytes, &io).is_err()
    );
    expected.cursor.next = 1;
    assert!(WakeFile::decode(&path, &owner, &serde_json::to_vec(&expected).unwrap(), &io).is_err());
    expected.cursor.preceding = Some([3; 32]);
    expected.deadline_unix_ms = Some(0);
    assert!(
        WakeFile::decode(
            &expected.path(),
            &owner,
            &serde_json::to_vec(&expected).unwrap(),
            &io
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn index_selection_reads_only_the_first_authored_document_and_refuses_a_false_due_name()
-> Result<()> {
    let cancellation = CancellationToken::new();
    let io = PreparationIo::from_budget(work_budget(20_000_000, 20_000_000), &cancellation)?;
    let fs = acyclic_fs::Fs::memory();
    let workspace = io.receive(
        fs.create_workspace("wake-index", io.remaining()?, &cancellation)
            .await,
    )?;
    let mut transaction = io.sdk(
        workspace
            .begin_transaction(acyclic_fs::IdempotencyKey::new(), &io, &cancellation)
            .await,
    )?;
    let expected = schedule();
    assert!(
        WakeFile::first(
            &mut transaction,
            &expected.owner,
            WakeIndex::Schedules,
            u64::MAX,
            &io,
        )?
        .await?
        .is_none()
    );
    io.sdk(
        transaction
            .create_dir_all("schedules", &io, &cancellation)
            .await,
    )?;
    let first_path = expected.path();
    io.sdk(
        transaction
            .write(
                &first_path,
                serde_json::to_vec(&expected).unwrap(),
                &io,
                &cancellation,
            )
            .await,
    )?;
    let mut later = expected.clone();
    later.next_due_unix_ms = 200;
    let later_path = later.path();
    // A malformed unselected suffix must not be decoded, copied or scanned.
    io.sdk(
        transaction
            .write(&later_path, b"not JSON".to_vec(), &io, &cancellation)
            .await,
    )?;
    let selected = WakeFile::first(
        &mut transaction,
        &expected.owner,
        WakeIndex::Schedules,
        u64::MAX,
        &io,
    )?
    .await?
    .expect("staged schedule is visible");
    assert!(matches!(selected.definition(), WakeDefinition::Schedule(value) if value == &expected));
    drop(selected);
    io.sdk(transaction.remove(&first_path, &io, &cancellation).await)?;
    assert!(
        WakeFile::first(
            &mut transaction,
            &expected.owner,
            WakeIndex::Schedules,
            u64::MAX,
            &io,
        )?
        .await
        .is_err()
    );
    io.sdk(
        transaction
            .write(
                &later_path,
                serde_json::to_vec(&later).unwrap(),
                &io,
                &cancellation,
            )
            .await,
    )?;
    let false_path = later_path.replace("00000000000000000200", "00000000000000000050");
    io.sdk(
        transaction
            .rename(&later_path, &false_path, false, &io, &cancellation)
            .await,
    )?;
    assert!(
        WakeFile::first(
            &mut transaction,
            &expected.owner,
            WakeIndex::Schedules,
            u64::MAX,
            &io,
        )?
        .await
        .is_err()
    );
    cancellation.cancel();
    assert!(
        WakeFile::first(
            &mut transaction,
            &expected.owner,
            WakeIndex::Schedules,
            u64::MAX,
            &io,
        )
        .is_err()
    );
    Ok(())
}
