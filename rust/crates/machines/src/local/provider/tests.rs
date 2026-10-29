use super::*;
use crate::local::provider_tests::{enrollment, options};
use futures::FutureExt;

#[tokio::test]
async fn pending_watch_follows_original_publication_and_ends_on_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    for complete in [true, false] {
        let machine = MachineId::new();
        let backend = LocalBackend {
            machine: LocalMachine::new(machine, options())?,
            generation: std::sync::Mutex::new(Generation::new()?),
        };
        // Retain the actual native execution lease that stop must join. No
        // fake completion source, timer or provider implementation participates.
        let mut physical = backend.execution()?;
        let local = AdmittedMachine::open(
            backend,
            enrollment(machine),
            Arc::new(acyclic_stream::MemoryStream::default()),
            StreamPath::new(format!("machine-tests/{machine}"))?,
        )
        .await?;
        let key = IdempotencyKey::new();
        let mut stopping = Box::pin(local.suspend(machine, key));
        // One admission turn reaches the real held lease. Subsequent progress
        // comes only from its release or the original producer's destruction.
        assert!(futures::poll!(&mut stopping).is_pending());
        let operation = local.recover_operation(key).await?;
        let mut watched = local.watch_operation(operation).await?;
        assert_eq!(
            watched.next().await.unwrap()?.phase,
            OperationPhase::Pending
        );
        assert!(watched.next().now_or_never().is_none());
        if complete {
            physical.retired(true);
            let (settled, observed) = futures::join!(stopping, watched.next());
            assert_eq!(settled?, MutationOutcome::Suspended(machine));
            assert_eq!(observed.unwrap()?.phase, OperationPhase::Succeeded);
            assert!(watched.next().await.is_none());
        } else {
            // An abandoned accepted mutation does not supply a terminal event.
            // Its original loss signal must end this existing watch promptly.
            drop(stopping);
            assert!(matches!(watched.next().await,
                Some(Err(ProviderError::Indeterminate(original))) if original == key));
            assert!(watched.next().await.is_none());
            assert!(matches!(local.watch_operation(operation).await,
                Err(ProviderError::Indeterminate(original)) if original == key));
            assert!(matches!(local.recover(key).await,
                Err(ProviderError::Indeterminate(original)) if original == key));
            physical.retired(true);
        }
    }
    Ok(())
}
