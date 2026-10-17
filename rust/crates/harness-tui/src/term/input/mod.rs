//! Native async terminal input. Each platform has exactly one byte source;
//! every platform shares this decoder loop, so there is no blocking read path.
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_core::Stream;
use tokio::time::{Instant, Sleep, sleep_until};

use super::Decoder;
use super::decoder::ESCAPE_TIMEOUT;
use crate::key_event::InputEvent;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix::Source;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::Source;

pub(super) enum Chunk<'a> {
    Bytes(&'a [u8]),
    Resized,
}

/// Classified terminal input. Dropping the stream releases the terminal
/// immediately: no thread is left waiting for another key.
pub struct Events {
    source: Source,
    size: (u16, u16),
    decoder: Decoder,
    ready: VecDeque<InputEvent>,
    escape: Option<Pin<Box<Sleep>>>,
    escape_armed: bool,
    ended: bool,
    /// Keys typed during negotiation are already classified; they come first.
    negotiated: bool,
}

impl Events {
    fn new(source: Source, decoder: Decoder) -> io::Result<Self> {
        let size = source.size()?;
        Ok(Self {
            size,
            source,
            decoder,
            ready: VecDeque::new(),
            negotiated: true,
            escape: None,
            escape_armed: false,
            ended: false,
        })
    }

    /// Last observed terminal dimensions; resize notifications update this cache.
    pub fn size(&self) -> (u16, u16) {
        self.size
    }

    /// Wait out a pending lone Escape. The timer is allocated once per stream
    /// and re-armed only by a quiet source, matching the decoder's framing.
    fn poll_escape(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let deadline = Instant::now() + ESCAPE_TIMEOUT;
        let timer = self
            .escape
            .get_or_insert_with(|| Box::pin(sleep_until(deadline)));
        if !self.escape_armed {
            timer.as_mut().reset(deadline);
            self.escape_armed = true;
        }
        ready!(timer.as_mut().poll(cx));
        self.escape_armed = false;
        Poll::Ready(())
    }

    /// After a chunk: an Escape still pending waits a full timeout from this
    /// input. Otherwise the registered deadline moves out of reach instead of
    /// being dropped: the storage is reused and an idle stream takes no timer
    /// wakeup.
    fn rearm_escape(&mut self) {
        if !self.escape_armed {
            return;
        }
        let Some(timer) = &mut self.escape else {
            return;
        };
        if self.decoder.escape_pending() {
            timer.as_mut().reset(Instant::now() + ESCAPE_TIMEOUT);
        } else {
            timer.as_mut().reset(Instant::now() + DISARMED);
            self.escape_armed = false;
        }
    }
}

/// Far enough that a disarmed escape timer never fires in a session.
const DISARMED: std::time::Duration = std::time::Duration::from_secs(86_400 * 365 * 30);

impl Stream for Events {
    type Item = io::Result<InputEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(event)));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            let decoded = if std::mem::take(&mut this.negotiated) {
                Ok(())
            } else {
                match this.source.poll_read(cx) {
                    Poll::Ready(Ok(Chunk::Resized)) => {
                        let size = match this.source.size() {
                            Ok(size) => size,
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        };
                        if let Some(event) = resize_event(&mut this.size, size) {
                            return Poll::Ready(Some(Ok(event)));
                        }
                        continue;
                    }
                    Poll::Ready(Ok(Chunk::Bytes([]))) => {
                        this.ended = true;
                        this.decoder.finish_input()
                    }
                    // The source's own storage, decoded in place.
                    Poll::Ready(Ok(Chunk::Bytes(bytes))) => this.decoder.feed(bytes),
                    Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                    Poll::Pending if this.decoder.escape_pending() => {
                        ready!(this.poll_escape(cx));
                        this.decoder.finish_input()
                    }
                    Poll::Pending => return Poll::Pending,
                }
            };
            this.rearm_escape();
            // A rejected unit is reported first; the rest of its chunk is
            // already classified and follows in order.
            let mut intercepted = Ok(());
            for event in this.decoder.drain_events() {
                match this.source.intercept(&event) {
                    Ok(false) => this.ready.push_back(event),
                    Ok(true) => {}
                    Err(error) => intercepted = intercepted.and(Err(error)),
                }
            }
            if let Err(error) = decoded.and(intercepted) {
                return Poll::Ready(Some(Err(error)));
            }
        }
    }
}

fn resize_event(current: &mut (u16, u16), next: (u16, u16)) -> Option<InputEvent> {
    if std::mem::replace(current, next) == next {
        None
    } else {
        Some(InputEvent::Resize(next.0, next.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn resize_cache_emits_exactly_dimension_changes(
            initial in any::<(u16, u16)>(),
            sizes in prop::collection::vec(any::<(u16, u16)>(), 0..64),
        ) {
            let mut cache = initial;
            let mut expected = initial;
            for size in sizes {
                let event = resize_event(&mut cache, size);
                prop_assert_eq!(event, (size != expected).then_some(InputEvent::Resize(size.0, size.1)));
                prop_assert_eq!(cache, size);
                expected = size;
            }
        }
    }
}
