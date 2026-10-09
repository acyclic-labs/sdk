//! Executor and timer operations only; task ownership remains in `live`.

use crate::{Error, Outcome, Result};
use acyclic_stream::{SystemUnixMillisClock, UnixMillisClock};
use std::{future::Future, time::Duration};

pub(crate) fn now_unix_millis() -> u64 {
    SystemUnixMillisClock.now_unix_millis()
}

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
pub(crate) use futures::future::AbortHandle;
#[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
pub(crate) use tokio::task::AbortHandle;

pub(crate) struct TaskJoin<T> {
    #[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
    join: tokio::task::JoinHandle<Outcome<T>>,
    #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
    receive: futures::channel::oneshot::Receiver<Outcome<T>>,
    #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
    abort: AbortHandle,
}

impl<T: acyclic_stream::ProviderTask + 'static> TaskJoin<T> {
    pub(crate) fn spawn(
        future: impl Future<Output = Outcome<T>> + acyclic_stream::ProviderTask + 'static,
    ) -> Self {
        #[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
        {
            Self {
                join: tokio::spawn(future),
            }
        }
        #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
        {
            let (abort, registration) = AbortHandle::new_pair();
            let (send, receive) = futures::channel::oneshot::channel();
            wasm_bindgen_futures::spawn_local(async move {
                let outcome = futures::future::Abortable::new(future, registration)
                    .await
                    .unwrap_or(Outcome::Cancelled);
                let _ = send.send(outcome);
            });
            Self { receive, abort }
        }
    }
}

impl<T> TaskJoin<T> {
    pub(crate) fn abort_handle(&self) -> AbortHandle {
        #[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
        {
            self.join.abort_handle()
        }
        #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
        {
            self.abort.clone()
        }
    }

    pub(crate) fn abort(&self) {
        self.abort_handle().abort();
    }

    pub(crate) async fn result(self) -> Outcome<T> {
        #[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
        {
            match self.join.await {
                Ok(outcome) => outcome,
                Err(error) if error.is_cancelled() => Outcome::Cancelled,
                Err(error) => Outcome::Failed {
                    message: error.to_string(),
                },
            }
        }
        #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
        {
            self.receive.await.unwrap_or_else(|error| Outcome::Failed {
                message: error.to_string(),
            })
        }
    }
}

pub(crate) async fn sleep(duration: Duration) -> Result<()> {
    #[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
    {
        tokio::time::sleep(duration).await;
        Ok(())
    }
    #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
    {
        browser::sleep(duration).await
    }
}

pub(crate) async fn timeout<T>(duration: Duration, future: impl Future<Output = T>) -> Result<T> {
    let timer = sleep(duration);
    futures::pin_mut!(future, timer);
    match futures::future::select(timer, future).await {
        futures::future::Either::Left((result, _)) => {
            result?;
            Err(Error::Invalid("task deadline has expired".into()))
        }
        futures::future::Either::Right((result, _)) => Ok(result),
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
mod browser {
    use super::*;
    use wasm_bindgen::{JsCast as _, JsValue, closure::Closure};

    // Keep the callback alive until completion or cancellation. Dropping a
    // deadline race clears its timer rather than retaining an abandoned wake.
    struct Timer {
        global: JsValue,
        clear: js_sys::Function,
        id: JsValue,
        _callback: Closure<dyn FnMut()>,
    }
    impl Drop for Timer {
        fn drop(&mut self) {
            let _ = self.clear.call1(&self.global, &self.id);
        }
    }

    pub(super) async fn sleep(duration: Duration) -> Result<()> {
        let global = js_sys::global();
        let function = |name| {
            js_sys::Reflect::get(&global, &JsValue::from_str(name))
                .ok()
                .and_then(|value| value.dyn_into::<js_sys::Function>().ok())
                .ok_or_else(|| Error::Unsupported("browser timer provider is unavailable".into()))
        };
        let set = function("setTimeout")?;
        let clear = function("clearTimeout")?;
        let mut remaining = duration;
        while !remaining.is_zero() {
            // setTimeout accepts a signed 32-bit millisecond delay. Batches
            // preserve the complete caller duration, including sub-ms tails.
            let chunk = remaining.min(Duration::from_millis(i32::MAX as u64));
            let millis =
                chunk.as_millis() + u128::from(!chunk.subsec_nanos().is_multiple_of(1_000_000));
            let millis = u32::try_from(millis)
                .map_err(|_| Error::Invalid("browser timer chunk is out of range".into()))?;
            let (send, receive) = futures::channel::oneshot::channel();
            let mut send = Some(send);
            let callback = Closure::wrap(Box::new(move || {
                if let Some(send) = send.take() {
                    let _ = send.send(());
                }
            }) as Box<dyn FnMut()>);
            let id = set
                .call2(
                    &global,
                    callback.as_ref(),
                    &JsValue::from_f64(f64::from(millis)),
                )
                .map_err(|_| Error::Unsupported("browser timer provider failed".into()))?;
            let timer = Timer {
                global: global.clone().into(),
                clear: clear.clone(),
                id,
                _callback: callback,
            };
            receive
                .await
                .map_err(|_| Error::Storage("browser timer wake was lost".into()))?;
            drop(timer);
            remaining = remaining.saturating_sub(chunk);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct DropSignal(Arc<AtomicBool>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn deadline_drops_incomplete_work_and_preserves_ready_result() {
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = DropSignal(dropped.clone());
        let work = async move {
            let _signal = signal;
            std::future::pending::<()>().await;
        };
        assert!(matches!(
            timeout(Duration::from_millis(1), work).await,
            Err(Error::Invalid(_))
        ));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            timeout(Duration::from_secs(1), async { 7 }).await.unwrap(),
            7
        );
    }
}
