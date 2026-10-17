use super::publication::Key;
use super::*;
use std::{cell::RefCell, rc::Weak};
use wasm_bindgen::closure::Closure;

pub(super) struct Publications {
    channel: web_sys::BroadcastChannel,
    pub(super) interests: crate::interest::Interests<Key>,
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
            target,
            refresh,
            _receive: receive,
        })
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
                    break;
                }
            };
            let provider = BrowserStream { inner };
            if provider.refresh_publication(force).await.is_err() {
                let _ = provider.inner.publications.interests.notify_all();
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
impl BrowserStream {
    pub(super) async fn discover(
        &self,
        request: ChildrenRequest,
    ) -> Result<ChildStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        // Subscribe before refreshing IndexedDB, closing the snapshot/follow race.
        let changed = self
            .inner
            .publications
            .interests
            .subscribe(Key::Children(request.parent.clone()))?;
        self.with_provider(async |_| Ok(())).await?;
        Ok(stream::try_unfold(
            (self.clone(), request.parent, None, changed),
            |(provider, parent, mut previous, mut changed)| async move {
                let path = changed
                    .next(async || {
                        provider
                            .with_cached_provider(async |memory| {
                                memory.next_child(parent.as_ref(), previous.as_ref()).await
                            })
                            .await
                    })
                    .await?;
                previous = Some(path.clone());
                Ok(Some((
                    Child { path },
                    (provider, parent, previous, changed),
                )))
            },
        )
        .boxed_local())
    }
}
