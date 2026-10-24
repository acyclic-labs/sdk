use super::*;
use proptest::prelude::*;

// Diagnostic text does not determine whether dispatch ran. Check every
// capability state for each generated diagnostic.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn dispatch_consumes_failure_capability(message in "[a-z ]{1,48}") {
        for started in [false, true] {
            for delegated in [false, true] {
                futures::executor::block_on(async {
                    let journal = Journal::default();
                    let mut phase = DispatchPhase::new();
                    if started { phase.start(); }
                    if delegated { phase.delegate(); }
                    let progress = phase.finish::<()>(Err(Error::Invalid(message.clone())), &journal, OperationId::new(), None).await;
                    match progress {
                        Ok(ExecutionProgress::PreDispatchFailed(failure)) => {
                            prop_assert!(!started && !delegated);
                            prop_assert!(failure.message().contains(&message));
                        }
                        Err(Error::Invalid(original)) => {
                            prop_assert!(started || delegated);
                            prop_assert_eq!(original, message.clone());
                        }
                        _ => prop_assert!(false, "unexpected progress"),
                    }
                    Ok(())
                })?;
            }
        }
    }
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn unreadable_dispatch_history_preserves_its_failure() -> Result<()> {
    let journal = Journal::default();
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = PreparationIo::from_budget(acyclic_fs::WorkBudget::default(), &cancellation)?;
    let progress = DispatchPhase::new()
        .finish::<()>(
            Err(Error::Invalid("preparation failed".into())),
            &journal,
            OperationId::new(),
            Some(&io),
        )
        .await;
    assert!(
        matches!(progress, Err(Error::Unsupported(message)) if message.contains("finite test journal IO is unavailable"))
    );
    assert_eq!(journal.3.load(Ordering::SeqCst), 0);
    Ok(())
}
