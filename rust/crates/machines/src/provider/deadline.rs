use super::*;

/// One deadline through credential admission, dispatch and streamed completion.
/// WallAlarm also wakes a silent transport when authenticated wall time expires.
pub(crate) struct GuestDeadline {
    until: tokio::time::Instant,
    expiry: Option<u64>,
    wake: Arc<tokio::sync::Notify>,
    _alarm: Option<acyclic_native_runtime::WallAlarm>,
}

impl GuestDeadline {
    pub(crate) fn new(timeout: Duration, expiry: Option<u64>) -> Result<Self, ProviderError> {
        let until = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                ProviderError::Invalid(
                    "guest deadline overflow; recovery: supply a representable original deadline"
                        .into(),
                )
            })?;
        let wake = Arc::new(tokio::sync::Notify::new());
        let alarm = expiry.map(|expiry| {
            let notify = wake.clone();
            acyclic_native_runtime::WallAlarm::register(expiry, Arc::new(move || notify.notify_one()))
                .map_err(|error| ProviderError::Rejected(format!("guest expiry alarm failed: {error}; recovery: restore the wall-clock alarm before dispatch")))
        }).transpose()?;
        Ok(Self {
            until,
            expiry,
            wake,
            _alarm: alarm,
        })
    }

    pub(crate) fn absolute(expiry: u64) -> Result<Self, ProviderError> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| ProviderError::Rejected(format!("guest clock admission failed: {error}; recovery: restore the system clock before dispatch")))?;
        let timeout = Duration::from_millis(expiry).saturating_sub(now);
        Self::new(timeout, Some(expiry))
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.until
            .saturating_duration_since(tokio::time::Instant::now())
    }

    pub(crate) async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> Option<T> {
        if self.expiry.is_some_and(|expiry| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(true, |now| now.as_millis() >= u128::from(expiry))
        }) {
            return None;
        }
        tokio::select! {
            value = tokio::time::timeout_at(self.until, future) => value.ok(),
            () = self.wake.notified() => None,
        }
    }
}
