//! The platform clock and cancellable timer boundary for portable Harness tasks.

use crate::{Error, Result};
use acyclic_stream::{SystemUnixMillisClock, UnixMillisClock as _};
use std::{future::Future, time::Duration};

pub(crate) fn unix_millis() -> Result<u64> {
    let now = SystemUnixMillisClock.now_unix_millis();
    if now == 0 || now == u64::MAX {
        return Err(Error::Invalid("platform wall clock is unavailable".into()));
    }
    #[cfg(target_arch = "wasm32")]
    if now > crate::conversation::MAX_EXACT_JS_INTEGER {
        return Err(Error::Invalid(
            "platform wall clock exceeds supported range".into(),
        ));
    }
    Ok(now)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep(duration: Duration) -> Result<()> {
    tokio::time::sleep(duration).await;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep(duration: Duration) -> Result<()> {
    // Browser timers coerce delays to signed 32-bit integers. Never let a large
    // deadline wrap into an immediate wakeup; retain each chunk until it fires.
    let mut remaining = duration.as_nanos().div_ceil(1_000_000);
    while remaining > 0 {
        let chunk = remaining.min(u128::from(i32::MAX.unsigned_abs()));
        let delay = i32::try_from(chunk)
            .map_err(|_| Error::Invalid("timer delay exceeds supported range".into()))?;
        browser::Timer::new(delay)?.wait().await?;
        remaining -= chunk;
    }
    Ok(())
}

pub(crate) async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| Error::Invalid("task deadline has expired".into()))
    }
    #[cfg(target_arch = "wasm32")]
    {
        let delay = sleep(duration);
        futures::pin_mut!(future, delay);
        match futures::future::select(future, delay).await {
            futures::future::Either::Left((output, _)) => Ok(output),
            futures::future::Either::Right((elapsed, _)) => {
                elapsed?;
                Err(Error::Invalid("task deadline has expired".into()))
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
pub(crate) struct Deadline(tokio::time::Instant);

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy)]
pub(crate) struct Deadline(f64);

impl Deadline {
    pub(crate) fn after(duration: Duration) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            tokio::time::Instant::now()
                .checked_add(duration)
                .map(Self)
                .ok_or_else(|| Error::Invalid("task deadline exceeds supported range".into()))
        }
        #[cfg(target_arch = "wasm32")]
        {
            let deadline = browser::monotonic_millis()? + duration.as_secs_f64() * 1_000.0;
            if !deadline.is_finite() {
                return Err(Error::Invalid(
                    "task deadline exceeds supported range".into(),
                ));
            }
            Ok(Self(deadline))
        }
    }

    pub(crate) async fn wait<F: Future>(self, future: F) -> Result<F::Output> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            tokio::time::timeout_at(self.0, future)
                .await
                .map_err(|_| Error::Invalid("task deadline has expired".into()))
        }
        #[cfg(target_arch = "wasm32")]
        {
            let remaining = self.0 - browser::monotonic_millis()?;
            if remaining <= 0.0 {
                return Err(Error::Invalid("task deadline has expired".into()));
            }
            let duration = Duration::try_from_secs_f64(remaining / 1_000.0)
                .map_err(|_| Error::Invalid("task deadline exceeds supported range".into()))?;
            timeout(duration, future).await
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod browser {
    use super::{Error, Result};
    use js_sys::{Function, Reflect};
    use tokio::sync::oneshot;
    use wasm_bindgen::{JsCast as _, JsValue, closure::Closure};

    fn function(owner: &JsValue, name: &str) -> Result<Function> {
        Reflect::get(owner, &JsValue::from_str(name))
            .and_then(|value| value.dyn_into())
            .map_err(|_| Error::Unsupported(format!("browser {name} is unavailable")))
    }

    pub(super) fn monotonic_millis() -> Result<f64> {
        let performance = Reflect::get(&js_sys::global(), &JsValue::from_str("performance"))
            .map_err(|_| Error::Unsupported("browser monotonic clock is unavailable".into()))?;
        let now = function(&performance, "now")?
            .call0(&performance)
            .ok()
            .and_then(|value| value.as_f64())
            .filter(|now| now.is_finite() && *now >= 0.0)
            .ok_or_else(|| Error::Invalid("browser monotonic clock is unavailable".into()))?;
        Ok(now)
    }

    pub(super) struct Timer {
        owner: JsValue,
        clear: Function,
        id: JsValue,
        // Retain the callback until the timer is cleared, including cancellation.
        _callback: Closure<dyn FnMut()>,
        completed: oneshot::Receiver<()>,
    }

    impl Timer {
        pub(super) fn new(delay: i32) -> Result<Self> {
            let owner: JsValue = js_sys::global().into();
            let set = function(&owner, "setTimeout")?;
            let clear = function(&owner, "clearTimeout")?;
            let (send, completed) = oneshot::channel();
            let mut send = Some(send);
            let callback = Closure::new(move || {
                if let Some(send) = send.take() {
                    let _ = send.send(());
                }
            });
            let id = set
                .call2(&owner, callback.as_ref(), &JsValue::from(delay))
                .map_err(|_| Error::Storage("browser timer could not be scheduled".into()))?;
            Ok(Self {
                owner,
                clear,
                id,
                _callback: callback,
                completed,
            })
        }

        pub(super) async fn wait(mut self) -> Result<()> {
            (&mut self.completed)
                .await
                .map_err(|_| Error::Storage("browser timer ended without notification".into()))
        }
    }

    impl Drop for Timer {
        fn drop(&mut self) {
            let _ = self.clear.call1(&self.owner, &self.id);
        }
    }
}
