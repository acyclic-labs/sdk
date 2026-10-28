use super::publication::Key;
use super::*;
use std::{cell::RefCell, rc::Weak};
use wasm_bindgen::closure::Closure;

pub(super) struct Publications {
    channel: web_sys::BroadcastChannel,
    pub(super) interests: crate::interest::Interests<Key>,
    pub(super) follows: crate::interest::Interests<
        StreamPath,
        Arc<crate::query_sync::SyncMutex<crate::follow_index::FollowIndex>>,
    >,
    target: Rc<RefCell<Weak<Inner>>>,
    refresh: Rc<crate::interest::Refresh>,
    _receive: Closure<dyn FnMut(web_sys::MessageEvent)>,
}
impl Publications {
    pub(super) fn new(name: &str) -> Result<Self, StreamError> {
        let channel = web_sys::BroadcastChannel::new(&format!("acyclic.stream.publication:{name}"))
            .map_err(backend)?;
        let target: Rc<RefCell<Weak<Inner>>> = Rc::new(RefCell::new(Weak::new()));
        let refresh = Rc::new(crate::interest::Refresh::new());
        let receiver = (Rc::clone(&target), Rc::clone(&refresh));
        let receive = Closure::new(move |_: web_sys::MessageEvent| {
            #[cfg(test)]
            if let Some(inner) = receiver.0.borrow().upgrade() {
                inner.messages.set(inner.messages.get() + 1);
            }
            let _ = wake(&receiver.0, &receiver.1, true);
        });
        channel.set_onmessage(Some(receive.as_ref().unchecked_ref()));
        Ok(Self {
            channel,
            interests: crate::interest::Interests::new(),
            follows: crate::interest::Interests::new(),
            target,
            refresh,
            _receive: receive,
        })
    }
    pub(super) fn follow_index(
        &self,
        path: &StreamPath,
    ) -> Result<
        crate::interest::Interest<
            StreamPath,
            Arc<crate::query_sync::SyncMutex<crate::follow_index::FollowIndex>>,
        >,
        StreamError,
    > {
        self.follows.subscribe_with(path.clone(), Arc::default)
    }
    pub(super) fn invalidate_follows(&self) {
        let Ok(paths) = self.follows.keys_in::<StreamPath, _>(..) else {
            return;
        };
        let indices = paths
            .into_iter()
            .filter_map(|path| self.follows.get(&path).ok().flatten())
            .collect::<Vec<_>>();
        for index in indices {
            index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .invalidate();
        }
    }
    pub(super) fn publish(&self) -> Result<(), StreamError> {
        self.wake(false)?;
        self.channel
            .post_message(&JsValue::UNDEFINED)
            .map_err(backend)
    }
    pub(super) fn bind(&self, provider: &Rc<Inner>) {
        *self.target.borrow_mut() = Rc::downgrade(provider);
    }
    pub(super) fn wake(&self, force: bool) -> Result<(), StreamError> {
        wake(&self.target, &self.refresh, force)
    }
}
fn wake(
    target: &Rc<RefCell<Weak<Inner>>>,
    refresh: &Rc<crate::interest::Refresh>,
    force: bool,
) -> Result<(), StreamError> {
    if !refresh.request(force)? {
        return Ok(());
    }
    let target = target.borrow().clone();
    let refresh = Rc::clone(refresh);
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            let Some(inner) = target.upgrade() else {
                break;
            };
            let force = match refresh.take() {
                Ok(Some(force)) => force,
                Ok(None) => break,
                Err(_) => {
                    let _ = inner.publications.interests.notify_all();
                    inner.publications.invalidate_follows();
                    break;
                }
            };
            let provider = BrowserStream { inner };
            if provider.refresh_publication(force).await.is_err() {
                let _ = provider.inner.publications.interests.notify_all();
                provider.inner.publications.invalidate_follows();
            }
        }
    });
    Ok(())
}
impl Drop for Publications {
    fn drop(&mut self) {
        self.channel.set_onmessage(None);
        self.channel.close();
    }
}
