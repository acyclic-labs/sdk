//! Lazy cursors over a pinned immutable history. Storage ports supply pages;
//! the cursor retains at most one caller-budgeted page.

use crate::{ProviderPlatform, ProviderTask, Record, StreamError};
use std::{
    future::Future,
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    /// A required exclusive range over an original record source. Unlike an
    /// at-most read, early EOF is a typed failure, never a shorter history.
    /// The caller supplies the original authorized source and pinned range;
    /// this adapter creates neither a source, a revision nor a read.
    /// Each poll delegates at most once and keeps only two cursors and one
    /// terminal bit beside the source. It allocates and copies no records.
    pub struct ExactRange<S> {
        #[pin]
        source: S,
        next: u64,
        end: u64,
        done: bool,
    }
}

impl<S> ExactRange<S> {
    /// Retains the source without polling it. Empty ranges require no source
    /// event. Reversed ranges refuse before any source work.
    pub fn new(source: S, range: Range<u64>) -> Result<Self, StreamError> {
        if range.start > range.end {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            source,
            next: range.start,
            end: range.end,
            done: false,
        })
    }
}

impl<S: futures::Stream<Item = Result<Record, StreamError>>> futures::Stream for ExactRange<S> {
    type Item = Result<Record, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done || *this.next == *this.end {
            return Poll::Ready(None);
        }
        let result = match this.source.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(record))) if record.sequence == *this.next => {
                // next < end, so increment cannot overflow even at u64::MAX.
                *this.next += 1;
                return Poll::Ready(Some(Ok(record)));
            }
            Poll::Ready(Some(Err(error))) => Err(error),
            Poll::Ready(_) => Err(StreamError::Unavailable),
        };
        *this.done = true;
        Poll::Ready(Some(result))
    }
}

/// A pinned history cut, including the inherited cuts of every ancestor.
/// Pages are nonempty and contiguous, begin at `after` (the next unread sequence), and
/// contain at most `budget` records. `None` means the pinned cut is exhausted.
/// A port may return a shorter page at a storage or fork boundary.
pub trait PageSource: ProviderPlatform {
    /// Owned page; memory ports can retain immutable batches without copying.
    type Page: Iterator<Item = Record> + ProviderTask;

    /// Reads one page without changing the pinned history or caller position.
    /// The source may keep where the page ended, so the next one resumes
    /// there. Cancelling this future must leave the source reusable at `after`.
    fn next_page(
        &mut self,
        after: u64,
        budget: u32,
    ) -> impl Future<Output = Result<Option<Self::Page>, StreamError>> + ProviderTask;
}

/// A single page of retained state, independent of the total history size.
pub struct PinnedCursor<S: PageSource> {
    source: S,
    page: Option<std::iter::Peekable<S::Page>>,
    next: u64,
    budget: u32,
    exhausted: bool,
}

impl<S: PageSource> PinnedCursor<S> {
    /// Pins a source without reading it. A zero page budget is invalid.
    pub fn new(source: S, after: u64, budget: u32) -> Result<Self, StreamError> {
        if budget == 0 {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            source,
            page: None,
            next: after,
            budget,
            exhausted: false,
        })
    }

    /// The immutable source currently owned by this cursor.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// The next unread sequence, advanced only when a record is consumed.
    pub fn position(&self) -> u64 {
        self.next
    }

    /// Replaces a pin after the caller has checked the retained prefix.
    /// A proven append may keep the current page; a replaced prefix cannot.
    pub fn repin(&mut self, source: S, keep_page: bool) {
        if !keep_page {
            self.page = None;
        }
        self.source = source;
        self.exhausted = false;
    }

    pub(crate) fn seed_page(&mut self, page: S::Page) {
        self.page = Some(page.peekable());
    }

    /// Observes the next record without advancing the application cursor.
    pub async fn peek(&mut self) -> Result<Option<&Record>, StreamError> {
        if self.page.as_mut().is_some_and(|page| page.peek().is_none()) {
            self.page = None;
        }
        if self.page.is_none() && !self.exhausted {
            self.page = self
                .source
                .next_page(self.next, self.budget)
                .await?
                .map(Iterator::peekable);
            self.exhausted = self.page.is_none();
            if self.page.as_mut().is_some_and(|page| page.peek().is_none()) {
                return Err(StreamError::Unavailable);
            }
        }
        let record = self.page.as_mut().and_then(|page| page.peek());
        if record.is_some_and(|record| record.sequence != self.next) {
            return Err(StreamError::Unavailable);
        }
        Ok(record)
    }

    /// Consumes one record. Cancellation while loading a page does not advance.
    pub async fn next(&mut self) -> Result<Option<Record>, StreamError> {
        if self.peek().await?.is_none() {
            return Ok(None);
        }
        let record = self
            .page
            .as_mut()
            .and_then(Iterator::next)
            .ok_or(StreamError::Unavailable)?;
        self.next = record
            .sequence
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        Ok(Some(record))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "pages_tests.rs"]
mod tests;
