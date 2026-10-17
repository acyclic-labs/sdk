use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

fn authorization() -> std::future::Ready<Result<(Vec<CommitCondition>, Option<u64>)>> {
    std::future::ready(Ok((Vec::new(), None)))
}

proptest! {
    // Property: replay uses the exact retained output without executing again;
    // a failed/cancelled dispatch leaves a claim that prevents every retry.
    #[test]
    fn retained_outcomes_replay_and_uncertain_effects_are_never_resent(
        text in ".{0,96}",
        operation in any::<[u8;16]>(),
        interrupted in any::<bool>(),
    ) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(
                Arc::new(MemoryStream::default()), StreamPath::new("hook-effects")?,
            ).await?;
            let task = TaskId::from_bytes([1;16]);
            let operation = OperationId::from_bytes(operation);
            let first = run_once::<_,_,_,_,_,true>(
                &volume, (task, operation, "session-start"), 4096, true, authorization,
                || async {
                    if interrupted { return Err(Error::Indeterminate(operation)); }
                    Ok(HookOutput { additional_context: vec![text.clone()], ..Default::default() })
                },
            ).await;
            if interrupted {
                prop_assert!(matches!(first, Err(Error::Indeterminate(id)) if id == operation));
            } else {
                prop_assert_eq!(first?.additional_context, vec![text.clone()]);
            }
            let replay = run_once::<_,_,_,_,_,true>(
                &volume, (task, operation, "session-start"), 4096, false, authorization,
                || async { panic!("recovery resent a hook") },
            ).await;
            if interrupted {
                prop_assert!(matches!(replay, Err(Error::Indeterminate(id)) if id == operation));
            } else {
                prop_assert_eq!(replay?.additional_context, vec![text]);
            }
            Ok(())
        })?;
    }
}

#[test]
fn reconciliation_of_an_unstarted_phase_publishes_nothing() -> Result<()> {
    futures::executor::block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("unstarted-hooks")?,
        )
        .await?;
        let head = volume.head().await?;
        let output = run_once::<_, _, _, _, _, true>(
            &volume,
            (
                TaskId::from_bytes([1; 16]),
                OperationId::from_bytes([2; 16]),
                "SessionStart",
            ),
            4096,
            false,
            authorization,
            || async { panic!("reconciliation launched a hook") },
        )
        .await?;
        assert_eq!(output, HookOutput::default());
        assert_eq!(volume.head().await?, head);
        Ok(())
    })
}

proptest! {
    // Property: overflow has a bounded, typed, replayable outcome. It never
    // turns a completed process into an indefinitely uncertain dispatch claim.
    #[test]
    fn oversized_outcomes_retain_a_failure_and_are_not_resent(length in 513usize..4096, blocking in any::<bool>()) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("oversized-hooks")?).await?;
            let task = TaskId::from_bytes([1;16]);
            let operation = OperationId::from_bytes([2;16]);
            let output = run_once::<_,_,_,_,_,true>(&volume, (task, operation, "SessionStart"), 512, true, authorization,
                || async { Ok(HookOutput { blocked: blocking.then(|| "denied".into()),
                    additional_context: vec!["x".repeat(length)], ..Default::default() }) }).await?;
            prop_assert!(matches!(output.diagnostics[0].cause, crate::ProcessError::OutputLimit { .. }), "expected output limit cause");
            prop_assert_eq!(output.blocks(), blocking);
            prop_assert!(serde_json::to_vec(&output)?.len() <= 512);
            let replay = run_once::<_,_,_,_,_,true>(&volume, (task, operation, "SessionStart"), 512, false, authorization,
                || async { panic!("overflow caused a hook resend") }).await?;
            prop_assert_eq!(replay, output);
            Ok(())
        })?;
    }
}
