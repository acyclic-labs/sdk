//! Actual event-loop or Tokio waits; dropping a browser wait clears its timer.
use crate::{Error, Result};
use acyclic_stream::UnixMillisClock as _;
use futures::future::{Either, select};
use std::{future::Future, pin::pin};

pub(crate) async fn timeout_until<T>(deadline: u64, future: impl Future<Output = T>) -> Result<T> {
    let wait = pin!(wait_until(deadline));
    let future = pin!(future);
    match select(wait, future).await {
        Either::Left((elapsed, _)) => {
            elapsed?;
            Err(Error::Invalid("task deadline has expired".into()))
        }
        Either::Right((result, _)) => {
            if acyclic_stream::SystemUnixMillisClock.now_unix_millis() >= deadline {
                Err(Error::Invalid("task deadline has expired".into()))
            } else { Ok(result) }
        }
    }
}

async fn wait_until(deadline: u64) -> Result<()> {
    loop {
        let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
        if now >= deadline { return Ok(()); }
        sleep_millis(deadline.saturating_sub(now).min(1_000)).await?;
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep_millis(milliseconds: u64) -> Result<()> {
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep_millis(milliseconds: u64) -> Result<()> {
    let mut remaining = milliseconds;
    while remaining != 0 {
        let interval = remaining.min(2_147_483_647);
        let bounded = u32::try_from(interval)
            .map_err(|_| Error::Invalid("task timer interval is not representable".into()))?;
        sleep_timeout(bounded).await?;
        remaining -= interval;
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn sleep_timeout(milliseconds: u32) -> Result<()> {
    use wasm_bindgen::{JsCast as _, JsValue};
    struct Timeout { global: JsValue, clear: js_sys::Function, handle: JsValue }
    impl Drop for Timeout {
        fn drop(&mut self) { let _ = self.clear.call1(&self.global, &self.handle); }
    }
    let timer_error = |error: JsValue| Error::Storage(format!("task deadline timer failed: {error:?}"));
    let global = js_sys::global();
    let set = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .map_err(timer_error)?.dyn_into::<js_sys::Function>().map_err(timer_error)?;
    let clear = js_sys::Reflect::get(&global, &JsValue::from_str("clearTimeout"))
        .map_err(timer_error)?.dyn_into::<js_sys::Function>().map_err(timer_error)?;
    let mut handle = JsValue::UNDEFINED;
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        match set.call2(&global, &resolve, &JsValue::from_f64(f64::from(milliseconds))) {
            Ok(value) => handle = value,
            Err(error) => { let _ = reject.call1(&JsValue::UNDEFINED, &error); }
        }
    });
    let _timeout = Timeout { global: global.into(), clear, handle };
    wasm_bindgen_futures::JsFuture::from(promise).await.map_err(timer_error)?;
    Ok(())
}
