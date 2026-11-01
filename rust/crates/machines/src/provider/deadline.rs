use super::*;

/// One deadline through credential admission, dispatch and streamed completion.
/// WallAlarm also wakes a silent transport when authenticated wall time expires.
pub struct GuestDeadline {
    until: Option<tokio::time::Instant>,
    expiry: Option<u64>,
    wake: Option<tokio::sync::watch::Receiver<Option<DeadlineStop>>>,
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
        let (wake, alarm) = match expiry {
            Some(expiry) => {
                let (terminal, wake) = terminal();
                let alarm = acyclic_native_runtime::WallAlarm::register(
                    expiry,
                    Arc::new(move |outcome| terminal.finish(outcome)),
                )
                .map_err(|error| {
                    DeadlineStop::SourceLost(DeadlineSourceLost(Arc::new(error))).before_dispatch()
                })?;
                (Some(wake), Some(alarm))
            }
            None => (None, None),
        };
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

    pub(crate) async fn run<T>(
        &self,
        future: impl std::future::Future<Output = T>,
    ) -> Result<T, DeadlineStop> {
        // Subscribe to the retained decision before checking time or polling work.
        // A callback that failed already must not be relabelled as expiry.
        let mut wake = self.wake.clone();
        if let Some(stop) = wake.as_ref().and_then(|wake| wake.borrow().clone()) {
            return Err(stop);
        }
        if self.expiry.is_some_and(|expiry| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(true, |now| now.as_millis() >= u128::from(expiry))
        }) {
            return Err(DeadlineStop::Expired);
        }
        let timed = async {
            match self.until {
                Some(until) => tokio::time::timeout_at(until, future)
                    .await
                    .map_err(|_| DeadlineStop::Expired),
                None => Ok(future.await),
            }
        };
        match &mut wake {
            Some(wake) => tokio::select! {
                biased;
                decision = wake.wait_for(Option::is_some) => {
                    match decision {
                        Ok(decision) => Err(decision.clone().unwrap_or_else(source_lost)),
                        Err(_) => Err(source_lost()),
                    }
                }
                value = timed => value,
            },
            None => timed.await,
        }
    }
}

/// The callback and its retirement are the sole producers of this decision.
struct Terminal(tokio::sync::watch::Sender<Option<DeadlineStop>>);

fn terminal() -> (Terminal, tokio::sync::watch::Receiver<Option<DeadlineStop>>) {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    (Terminal(sender), receiver)
}

impl Terminal {
    fn finish(&self, outcome: Result<(), Arc<std::io::Error>>) {
        let stop = match outcome {
            Ok(()) => DeadlineStop::Expired,
            Err(error) => DeadlineStop::SourceLost(DeadlineSourceLost(error)),
        };
        self.0.send_if_modified(|decision| {
            if decision.is_some() {
                return false;
            }
            *decision = Some(stop);
            true
        });
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        // Dropping an uncalled callback is source loss, never an expiry or a
        // permanently pending observation. The same decision serves late runs.
        if self.0.borrow().is_none() {
            let _ = self.0.send_replace(Some(source_lost()));
        }
    }
}

fn source_lost() -> DeadlineStop {
    DeadlineStop::SourceLost(DeadlineSourceLost(Arc::new(std::io::Error::other(
        "step=guest-alarm-terminal; recovery=restore the original alarm producer before dispatch",
    ))))
}

#[cfg(test)]
mod tests;
