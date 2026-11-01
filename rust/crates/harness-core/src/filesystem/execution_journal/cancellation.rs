//! Runtime-owned delivery cancellation used by execution waits.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn follow_cancellation(&self) -> Result<()> {
        let Some(stop) = &self.stop else {
            return std::future::pending().await;
        };
        stop.cancelled().await;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tool_preparation_follows_only_the_original_journal_stop_and_read_result() -> Result<()>
    {
        let task = crate::runtime::test_context::agent_context(
            crate::runtime::RuntimeScope::default(),
            None,
        )?;
        assert!(matches!(
            task.model_cancellation(None),
            Err(Error::Unsupported(_))
        ));
        let (journal, _issuer) = super::super::test_support::journal().await?;
        let task = task.with_execution_journal(Arc::new(journal));
        // A bound journal without a stop binding stays pending; a completed
        // read still supplies its exact value rather than inventing a stop.
        let mut unbound = task.model_cancellation(None)?;
        assert!(futures::poll!(unbound.as_mut()).is_pending());
        assert_eq!(
            crate::filesystem::fs_tools::read_before_effect(&mut unbound, async { Ok(17) }).await?,
            17
        );
        drop(unbound);

        let original = acyclic_actors::guest::CancellationToken::new();
        let unrelated = acyclic_actors::guest::CancellationToken::new();
        let (journal, _issuer) = super::super::test_support::journal().await?;
        let task = task.with_execution_journal(Arc::new(journal.with_stop(original.clone())));
        {
            let mut cancellation = task.model_cancellation(None)?;
            let (producer, receiver) = futures::channel::oneshot::channel::<u8>();
            let lookup =
                crate::filesystem::fs_tools::read_before_effect(&mut cancellation, async {
                    receiver
                        .await
                        .map_err(|_| Error::Conflict("read producer lost".into()))
                });
            futures::pin_mut!(lookup);
            assert!(futures::poll!(lookup.as_mut()).is_pending());
            unrelated.cancel();
            assert!(futures::poll!(lookup.as_mut()).is_pending());
            original.cancel();
            assert!(matches!(
                lookup.await,
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                ))
            ));
            assert!(
                producer.is_canceled(),
                "cancelled preparation retained its read"
            );
        }

        let (journal, _issuer) = super::super::test_support::journal().await?;
        let task = task.with_execution_journal(Arc::new(journal));
        let mut cancellation = task.model_cancellation(None)?;
        let (producer, receiver) = futures::channel::oneshot::channel::<u8>();
        let lookup = crate::filesystem::fs_tools::read_before_effect(&mut cancellation, async {
            receiver
                .await
                .map_err(|_| Error::Conflict("read producer lost".into()))
        });
        futures::pin_mut!(lookup);
        assert!(futures::poll!(lookup.as_mut()).is_pending());
        drop(producer);
        assert!(
            matches!(lookup.await, Err(Error::Conflict(reason)) if reason == "read producer lost")
        );

        // Without the original cancellation select the same pending read
        // cannot terminate merely because the original stop has fired.
        let (_producer, receiver) = futures::channel::oneshot::channel::<u8>();
        futures::pin_mut!(receiver);
        assert!(futures::poll!(receiver.as_mut()).is_pending());
        Ok(())
    }
}
