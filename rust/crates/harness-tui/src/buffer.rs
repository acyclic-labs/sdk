use std::ops::Range;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Buffer {
    pub text: String,
    pub cursor: usize,
    pub anchor: usize,
    pub folds: Vec<Range<usize>>,
}

impl Buffer {
    pub fn selection(&self) -> Range<usize> {
        self.anchor.min(self.cursor)..self.anchor.max(self.cursor)
    }

    pub fn boundaries(&self) -> Vec<usize> {
        self.text
            .grapheme_indices(true)
            .map(|(i, _)| i)
            .filter(|i| !self.folds.iter().any(|r| r.start < *i && *i < r.end))
            .chain(std::iter::once(self.text.len()))
            .collect()
    }

    pub fn floor(&self, offset: usize) -> usize {
        self.boundaries()
            .into_iter()
            .take_while(|i| *i <= offset)
            .last()
            .unwrap_or(0)
    }

    pub fn ceil(&self, offset: usize) -> usize {
        self.boundaries()
            .into_iter()
            .find(|i| *i >= offset)
            .unwrap_or(self.text.len())
    }

    pub fn previous(&self, offset: usize) -> usize {
        self.boundaries()
            .into_iter()
            .take_while(|i| *i < offset)
            .last()
            .unwrap_or(0)
    }

    pub fn next(&self, offset: usize) -> usize {
        self.boundaries()
            .into_iter()
            .find(|i| *i > offset)
            .unwrap_or(self.text.len())
    }

    pub fn replace(&mut self, range: Range<usize>, text: &str, fold: bool) {
        self.folds.retain_mut(|r| {
            if r.end <= range.start {
                return true;
            }
            if r.start >= range.end {
                r.start = r.start - range.len() + text.len();
                r.end = r.end - range.len() + text.len();
                return true;
            }
            false
        });
        self.text.replace_range(range.clone(), text);
        let end = range.start + text.len();
        // Inserting a combining mark, ZWJ, or regional indicator may join its
        // neighbours. Keep both cursor and fold ends on the new segmentation.
        if fold && !text.is_empty() {
            let start = self.floor(range.start);
            let end = self.ceil(end);
            self.folds.retain(|r| r.end <= start || r.start >= end);
            self.folds.push(start..end);
            self.folds.sort_by_key(|r| r.start);
        }
        // Existing fold boundaries can also be joined by an adjacent insertion.
        let boundaries: Vec<_> = self
            .text
            .grapheme_indices(true)
            .map(|(i, _)| i)
            .chain(std::iter::once(self.text.len()))
            .collect();
        self.folds
            .retain(|r| boundaries.contains(&r.start) && boundaries.contains(&r.end));
        self.cursor = self.ceil(end);
        self.anchor = self.cursor;
    }

    pub fn word_left(&self, offset: usize) -> usize {
        let mut at = offset;
        while at > 0
            && self.text[self.previous(at)..at]
                .chars()
                .all(char::is_whitespace)
        {
            at = self.previous(at);
        }
        while at > 0
            && !self.text[self.previous(at)..at]
                .chars()
                .all(char::is_whitespace)
        {
            at = self.previous(at);
        }
        at
    }

    pub fn word_right(&self, offset: usize) -> usize {
        let mut at = offset;
        while at < self.text.len()
            && self.text[at..self.next(at)]
                .chars()
                .all(char::is_whitespace)
        {
            at = self.next(at);
        }
        while at < self.text.len()
            && !self.text[at..self.next(at)]
                .chars()
                .all(char::is_whitespace)
        {
            at = self.next(at);
        }
        at
    }
}

pub(crate) fn normalize(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
