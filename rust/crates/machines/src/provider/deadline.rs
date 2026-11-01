use super::*;

/// One deadline through credential admission, dispatch and streamed completion.
/// WallAlarm also wakes a silent transport when authenticated wall time expires.
pub struct GuestDeadline {
    until: Option<tokio::time::Instant>,
    expiry: Option<u64>,
    wake: Option<Arc<tokio::sync::Notify>>,
    _alarm: Option<acyclic_native_runtime::WallAlarm>,
}

impl GuestDeadline {
    pub(crate) fn new(
        timeout: Option<Duration>,
        expiry: Option<u64>,
    ) -> Result<Self, ProviderError> {
        let until = timeout
            .map(|timeout| {
                tokio::time::Instant::now()
                    .checked_add(timeout)
                    .ok_or_else(|| {
                        ProviderError::Invalid(
                    "guest deadline overflow; recovery: supply a representable original deadline"
                        .into(),
                )
                    })
            })
            .transpose()?;
        let wake = expiry.map(|_| Arc::new(tokio::sync::Notify::new()));
        let alarm = wake.as_ref().zip(expiry).map(|(wake, expiry)| {
            let notify = wake.clone();
            acyclic_native_runtime::WallAlarm::register(expiry, Arc::new(move |_outcome| notify.notify_one()))
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
        Self::new(Some(timeout), Some(expiry))
    }

    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.until
            .map(|until| until.saturating_duration_since(tokio::time::Instant::now()))
    }

    pub(crate) fn until(&self) -> Option<std::time::Instant> {
        self.until.map(tokio::time::Instant::into_std)
    }

    pub(crate) async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> Option<T> {
        if self.expiry.is_some_and(|expiry| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(true, |now| now.as_millis() >= u128::from(expiry))
        }) {
            return None;
        }
        let timed = async {
            match self.until {
                Some(until) => tokio::time::timeout_at(until, future).await.ok(),
                None => Some(future.await),
            }
        };
        match &self.wake {
            Some(wake) => tokio::select! {
                value = timed => value,
                () = wake.notified() => None,
            },
            None => timed.await,
        }
    }
}
