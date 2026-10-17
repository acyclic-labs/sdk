use caseless::Caseless;
use std::ops::Range;

/// Flat folded text and source origins, maintained only as transcript data changes.
#[derive(Default)]
pub(crate) struct Index {
    pub text: String,
    pub revision: u64,
    origins: Vec<Range<usize>>,
    source_len: usize,
}

impl Index {
    pub fn replace_suffix(&mut self, source: &str, at: usize) {
        assert!(self.revision < u64::MAX, "search index revision overflow");
        self.revision += 1;
        let at = at.min(self.source_len);
        self.source_len = source.len();
        let cut = self.origins.partition_point(|origin| origin.end <= at);
        self.text.truncate(cut);
        self.origins.truncate(cut);
        for (start, character) in crate::buffer::slice(source, at..).char_indices() {
            let origin = at + start..at + start + character.len_utf8();
            for folded in std::iter::once(character).default_case_fold() {
                self.text.push(folded);
                self.origins
                    .extend(std::iter::repeat_n(origin.clone(), folded.len_utf8()));
            }
        }
    }

    pub fn folded_range(&self, source: &Range<usize>) -> Range<usize> {
        self.origins
            .partition_point(|origin| origin.end <= source.start)
            ..self
                .origins
                .partition_point(|origin| origin.start < source.end)
    }

    pub fn source(&self, at: usize, bytes: usize) -> Range<usize> {
        let origins = self.origins.get(at..at + bytes).unwrap_or_default();
        match (origins.first(), origins.last()) {
            (Some(first), Some(last)) => first.start..last.end,
            _ => 0..0,
        }
    }
}
