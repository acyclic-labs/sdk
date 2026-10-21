use super::*;

/// Addressable handle for an admitted live task.
pub struct TaskHandle<T> {
    pub(super) id: OperationId,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) join: tokio::task::JoinHandle<Outcome<T>>,
    #[cfg(target_arch = "wasm32")]
    pub(super) outcome: oneshot::Receiver<Outcome<T>>,
    #[cfg(target_arch = "wasm32")]
    pub(super) abort: AbortHandle,
}

impl<T> TaskHandle<T> {
    /// Returns the operation identity.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        self.abort_handle().abort();
    }
    pub(super) fn abort_handle(&self) -> AbortHandle {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.join.abort_handle()
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.abort.clone()
        }
    }

    /// Waits for an outcome, cancelling this task if the owned wait is dropped.
    pub fn result(self) -> impl Future<Output = Outcome<T>> {
        struct CancelOnDrop(Option<AbortHandle>);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                if let Some(handle) = &self.0 {
                    handle.abort();
                }
            }
        }
        let guard = CancelOnDrop(Some(self.abort_handle()));
        async move {
            let mut guard = guard;
            let outcome = self.wait().await;
            guard.0 = None;
            outcome
        }
    }

    pub(super) async fn wait(self) -> Outcome<T> {
        #[cfg(not(target_arch = "wasm32"))]
        let outcome = match self.join.await {
            Ok(outcome) => outcome,
            Err(error) if error.is_cancelled() => Outcome::Cancelled,
            Err(error) => Outcome::Failed {
                message: error.to_string(),
            },
        };
        #[cfg(target_arch = "wasm32")]
        let outcome = match self.outcome.await {
            Ok(outcome) => outcome,
            Err(error) => Outcome::Failed {
                message: error.to_string(),
            },
        };
        outcome
    }
}
