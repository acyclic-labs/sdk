/// A lease's pending release, taken exactly once.
type Release = Option<BoxProviderFuture<'static, Result<(), StreamError>>>;

/// Custody of one ephemeral subtree; see [`StreamProvider::lease_ephemeral`].
/// Dropping it releases the subtree without blocking.
#[must_use = "dropping the lease releases its subtree"]
pub struct EphemeralLease {
    root: StreamPath,
    // Only ever reached through `&mut self`, so the lock is never contended:
    // it makes custody shareable across threads, as every owner needs.
    /// The release future, taken exactly once by `settle` or drop.
    pub release: std::sync::Mutex<Release>,
}

impl EphemeralLease {
    /// A lease whose `release` removes the subtree at `root` exactly once.
    pub fn new(
        root: StreamPath,
        release: BoxProviderFuture<'static, Result<(), StreamError>>,
    ) -> Self {
        Self {
            root,
            release: std::sync::Mutex::new(Some(release)),
        }
    }

    /// The leased subtree root.
    #[must_use]
    pub fn root(&self) -> &StreamPath {
        &self.root
    }

    /// Releases the subtree and waits until it is gone. Once started, cleanup
    /// remains owned even if this waiter is cancelled.
    pub async fn settle(mut self) -> Result<(), StreamError> {
        let Some(release) = take_release(&mut self.release) else {
            return Ok(());
        };
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        {
            let (send, receive) = futures::channel::oneshot::channel();
            crate::runtime::PlatformExecutor.spawn_owned(futures::FutureExt::map(
                release,
                move |result| {
                    let _ = send.send(result);
                },
            ));
            receive.await.map_err(|_| StreamError::Unavailable)?
        }
        #[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
        release.await
    }
}

/// Takes the release future, ignoring a poisoned lock.
pub fn take_release(release: &mut std::sync::Mutex<Release>) -> Release {
    release
        .get_mut()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

impl std::fmt::Debug for EphemeralLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EphemeralLease")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Drop for EphemeralLease {
    fn drop(&mut self) {
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        if let Some(release) = take_release(&mut self.release) {
            crate::runtime::PlatformExecutor
                .spawn_owned(futures::FutureExt::map(release, |_released| ()));
        }
    }
}
