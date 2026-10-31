//! Session admission modes over the shared persistent source scheduler.
#[cfg(loom)]
use super::sync::{AtomicU64, Ordering};
use super::*;
use crate::runtime::sources::{ALL, EITHER, LATEST};
#[cfg(any(test, loom))]
use crate::runtime::sources::{OPENING, Wake};
#[cfg(any(test, loom))]
use futures::task::ArcWake;
#[cfg(any(test, loom))]
use std::task::{Context, Poll};

#[cfg(loom)]
pub(super) mod model;
#[cfg(all(test, not(loom)))]
mod tests;

pub(super) type Sources = crate::runtime::ReadySources<Source>;
impl Channel {
    pub(super) fn source_mode(&self) -> Option<usize> {
        if self.ending.is_some() || self.progress.is_some() {
            None
        } else {
            let all = self.pending.is_empty() && self.credit.frames > 0 && self.credit.bytes > 0;
            // One bounded lookahead permits metadata progress without data
            // credit. A matching record fills the queue and stops this path.
            let latest = (self.latest && self.pending.len() < 3)
                || (self.pending.is_empty()
                    && self
                        .binding
                        .as_ref()
                        .is_some_and(|binding| !binding.is_reply()));
            match (all, latest) {
                (true, true) => Some(EITHER),
                (true, false) => Some(ALL),
                (false, true) => Some(LATEST),
                (false, false) => None,
            }
        }
    }
}
impl<P: StreamProvider + ?Sized> Session<P> {
    pub(super) fn refresh_source(&mut self, id: u64) {
        let mode = self.channels.get(&id).and_then(Channel::source_mode);
        if let Some(channel) = self.channels.get_mut(&id) {
            if let Some(length) = channel.indexed.take() {
                self.runnable.remove(&(length, id));
            }
            if channel.credit.frames > 0
                && let Some(head) = channel.pending.front()
                && head.frame.wire().encoded_len() as u64 <= channel.credit.bytes
            {
                let length = head.frame.wire().encoded_len();
                channel.indexed = Some(length);
                self.runnable.insert((length, id));
            }
        }
        self.sources.mode(id, mode);
    }
}
