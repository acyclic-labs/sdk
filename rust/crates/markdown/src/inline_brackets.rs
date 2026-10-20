use super::State;
use std::ops::Range;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Closure {
    Reference,
    Image,
    Link(bool),
}

pub(super) struct Closed {
    pub(super) previous: usize,
    pub(super) range: Range<usize>,
    pub(super) kind: Closure,
}

impl State {
    pub(super) fn preview_code(
        &self,
        token: &crate::inline_scan::tokens::Token,
    ) -> Option<&super::codes::Code> {
        use crate::inline_scan::tokens::Kind;
        let escaped = match token.kind {
            Kind::Run { byte: b'`', .. } => false,
            Kind::Escape('`') => true,
            _ => return None,
        };
        self.codes
            .get(token.range.len().checked_sub(usize::from(escaped))?)
    }

    #[cfg(test)]
    fn closed_at(&self, pending: Option<&crate::inline_scan::tokens::Token>) -> usize {
        pending
            .and_then(|token| self.preview_code(token))
            .map_or(self.closed, |code| code.before.closed())
    }

    pub(super) fn label(&self, cursor: usize) -> Option<&Closed> {
        let index = cursor.checked_sub(1)?;
        let (page, offset) = super::forest::slot(index);
        let label = self.closed_labels.get(page)?.get(offset)?;
        debug_assert!(label.previous < cursor);
        Some(label)
    }

    pub(super) fn remember_closed(&mut self, range: Range<usize>, opening: usize) -> usize {
        // Eligibility belongs to the label at its closing bracket. A link
        // recognized later may be literal syntax inside this label's URL.
        let kind = if opening == 0 {
            Closure::Reference
        } else {
            let Some(bracket) = self.opening(opening) else {
                self.invalid = true;
                return self.closed;
            };
            if bracket.image {
                Closure::Image
            } else {
                Closure::Link(bracket.start >= self.link_floor)
            }
        };
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Closed>());
        if super::forest::slot(self.closed_count).1 == 0 {
            #[cfg(test)]
            if self.closed_labels.len() == self.closed_labels.capacity() {
                crate::work::copy(self.closed_labels.len() * std::mem::size_of::<Vec<Closed>>());
            }
            self.closed_labels
                .push(super::forest::new_page(self.closed_labels.len()));
        }
        let Some(page) = self.closed_labels.last_mut() else {
            self.invalid = true;
            return self.closed;
        };
        super::forest::push_page(
            page,
            Closed {
                previous: self.closed,
                range,
                kind,
            },
        );
        self.closed_count += 1;
        self.closed = self.closed_count;
        self.closed
    }
}

#[cfg(test)]
thread_local! { pub(super) static FORGET_RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_brackets.rs"]
mod tests;

#[cfg(test)]
thread_local! { pub(in crate::inline_scan) static CURRENT_LINK_FLOOR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
