use super::*;
use crate::executor::PreparationIo;
use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, OwnedBytes, StreamPath};

#[tokio::test]
async fn original_native_publication_survives_preparation_and_refuses_foreign_completion()
-> Result<()> {
    for mode in 0..3 {
        check(mode).await?;
    }
    Ok(())
}

async fn check(mode: u8) -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024);
    let original = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = original.live_allocation_bytes();
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let path = StreamPath::new("retained/publication")?;
    let (pending, producer) = {
        let io = PreparationIo::from_native_work(&original)?;
        let mut history = Some(io.history_io());
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        let (key, held) = crate::distributed::digest_key(&[mode; 32], &mut work)?;
        let mut request = commit_request(key, held, &mut work);
        add_condition(
            &mut request.0,
            &mut request.1,
            CommitCondition::Absent { path: path.clone() },
        )?;
        let mut records = Vec::new();
        request.1.reserve(1, &mut records)?;
        records.push(OwnedBytes::from_static(b"original immutable record").into());
        request.1.reserve(1, &mut request.0.mutations)?;
        request
            .0
            .mutations
            .push(CommitMutation::Append { path, records });
        request.0.deadline_unix_millis = Some(u64::MAX - 1);
        let (body, mut quota) = request;
        let (pending, producer) =
            NativeCommit::prepare(body, &mut quota, Arc::clone(&provider), Some(u64::MAX))?;
        assert_eq!(
            pending.pending.request().deadline_unix_millis,
            Some(u64::MAX - 1)
        );
        assert_eq!(quota.retained(), 0);
        drop(quota);
        (pending, producer)
    };
    assert!(original.live_allocation_bytes() > baseline);
    if mode == 1 {
        drop(producer);
    } else {
        native_call_result(&original, producer.await)?.map_err(Error::from)?;
    }
    let foreign = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let foreign_baseline = foreign.live_allocation_bytes();
    let io = PreparationIo::from_native_work(if mode == 2 { &foreign } else { &original })?;
    let mut history = Some(io.history_io());
    let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
    let mut quota = CodecQuota::from_retained(&mut work, 0);
    let before = original.receipt().authority_records_appended;
    let mut phase = CommitPhase::Unknown;
    let result = pending
        .finish(
            Some(OperationId::from_bytes([mode; 16])),
            Some(&mut phase),
            &mut quota,
        )
        .await;
    match mode {
        0 => {
            let (outcome, held) = result?;
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
            assert_eq!(phase, CommitPhase::Committed);
            assert_eq!(original.receipt().authority_records_appended, before + 1);
            drop(outcome);
            quota.adopt(held)?;
        }
        1 => {
            assert!(matches!(result, Err(Error::NativeWork { .. })));
            assert_eq!(phase, CommitPhase::Unknown);
            drop(result);
        }
        _ => {
            assert!(matches!(
                result,
                Err(Error::NativeBoundary {
                    code: wire::ErrorCode::Invalid,
                    ..
                })
            ));
            assert_eq!(phase, CommitPhase::Unknown);
            drop(result);
        }
    }
    drop(quota);
    drop(io);
    drop(provider);
    assert_eq!(original.live_allocation_bytes(), baseline, "mode {mode}");
    assert_eq!(foreign.live_allocation_bytes(), foreign_baseline);
    Ok(())
}
