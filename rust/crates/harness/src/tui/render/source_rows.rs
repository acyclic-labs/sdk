use crate::{task_volume::FileKey, tui::TranscriptOrigin};
use std::{
    collections::{BTreeMap, VecDeque},
    ops::Range,
};

pub(super) fn identity(key: &FileKey) -> [u8; 32] {
    *blake3::hash(key.path().as_bytes()).as_bytes()
}

#[derive(Clone)]
struct SourceSpan {
    rows: Range<usize>,
    ordinal: usize,
}

#[derive(Default)]
pub(super) struct SourceRows {
    // Absolute cache coordinates retain a surviving file's source ordinal when
    // the prefix advances. Only source boundaries, never styled rows, are stored.
    spans: VecDeque<([u8; 32], SourceSpan)>,
    files: BTreeMap<([u8; 32], usize), SourceSpan>,
    base: usize,
    end: usize,
}

impl SourceRows {
    pub(super) fn extend_current(&mut self, total: usize) {
        if let Some((file, _)) = self.spans.back() {
            self.extend(*file, total);
        }
    }
    pub(super) fn origin(&self, line: usize) -> Option<TranscriptOrigin> {
        let at = self.base.checked_add(line)?;
        let index = self.spans.partition_point(|(_, span)| span.rows.end <= at);
        let (file, span) = self.spans.get(index)?;
        span.rows.contains(&at).then(|| TranscriptOrigin {
            file: *file,
            line: span.ordinal + at - span.rows.start,
        })
    }
    pub(super) fn resolve(&self, origin: TranscriptOrigin) -> Option<usize> {
        let (_, span) = self
            .files
            .range((origin.file, 0)..=(origin.file, origin.line))
            .next_back()?;
        let at = span
            .rows
            .start
            .checked_add(origin.line.checked_sub(span.ordinal)?)?;
        (span.rows.contains(&at) && at >= self.base).then(|| at - self.base)
    }
    fn append(&mut self, origin: TranscriptOrigin, count: usize) {
        if count == 0 {
            return;
        }
        let end = self.end + count;
        if let Some((file, span)) = self.spans.back_mut()
            && *file == origin.file
            && span.rows.end == self.end
            && span.ordinal + span.rows.len() == origin.line
        {
            span.rows.end = end;
            if let Some(indexed) = self.files.get_mut(&(*file, span.ordinal)) {
                indexed.rows.end = end;
            }
        } else {
            let span = SourceSpan {
                rows: self.end..end,
                ordinal: origin.line,
            };
            self.files.insert((origin.file, span.ordinal), span.clone());
            self.spans.push_back((origin.file, span));
        }
        self.end = end;
    }
    pub(super) fn push(&mut self, origin: Option<TranscriptOrigin>) {
        if let Some(origin) = origin {
            self.append(origin, 1);
        } else {
            self.end += 1;
        }
    }
    pub(super) fn extend(&mut self, file: [u8; 32], total: usize) {
        let end = self.base + total;
        if end <= self.end {
            return;
        }
        let line = self
            .spans
            .back()
            .filter(|(last, _)| *last == file)
            .map_or(0, |(_, span)| span.ordinal + span.rows.len());
        self.append(TranscriptOrigin { file, line }, end - self.end);
    }
    pub(super) fn truncate(&mut self, total: usize) {
        let end = self.base + total;
        if end >= self.end {
            return;
        }
        while self
            .spans
            .back()
            .is_some_and(|(_, span)| span.rows.start >= end)
        {
            if let Some((file, span)) = self.spans.pop_back() {
                self.files.remove(&(file, span.ordinal));
            }
        }
        if let Some((file, span)) = self.spans.back_mut() {
            span.rows.end = span.rows.end.min(end);
            if let Some(indexed) = self.files.get_mut(&(*file, span.ordinal)) {
                indexed.rows.end = span.rows.end;
            }
        }
        self.end = end;
    }
    pub(super) fn discard(&mut self, count: usize) {
        self.base = self.base.saturating_add(count).min(self.end);
        while self
            .spans
            .front()
            .is_some_and(|(_, span)| span.rows.end <= self.base)
        {
            if let Some((file, span)) = self.spans.pop_front() {
                self.files.remove(&(file, span.ordinal));
            }
        }
    }
    // Replace one whole tool block using its original header/output identities.
    // The bounded tool details need no temporary row or provenance allocation.
    pub(super) fn replace(
        &mut self,
        old: Range<usize>,
        origins: impl ExactSizeIterator<Item = Option<TranscriptOrigin>>,
    ) {
        let start = self.base + old.start;
        let end = self.base + old.end;
        let count = origins.len();
        let index = self
            .spans
            .partition_point(|(_, span)| span.rows.start < start);
        while self
            .spans
            .get(index)
            .is_some_and(|(_, span)| span.rows.start < end)
        {
            if let Some((file, span)) = self.spans.remove(index) {
                self.files.remove(&(file, span.ordinal));
            }
        }
        for (file, span) in self.spans.iter_mut().skip(index) {
            span.rows.start = span.rows.start - old.len() + count;
            span.rows.end = span.rows.end - old.len() + count;
            if let Some(indexed) = self.files.get_mut(&(*file, span.ordinal)) {
                *indexed = span.clone();
            }
        }
        let mut next = index;
        for (row, origin) in origins.enumerate() {
            let Some(origin) = origin else {
                continue;
            };
            let at = start + row;
            if next > index
                && let Some((file, span)) = self.spans.get_mut(next - 1)
                && *file == origin.file
                && span.rows.end == at
                && span.ordinal + span.rows.len() == origin.line
            {
                span.rows.end += 1;
                if let Some(indexed) = self.files.get_mut(&(*file, span.ordinal)) {
                    indexed.rows.end += 1;
                }
                continue;
            }
            let span = SourceSpan {
                rows: at..at + 1,
                ordinal: origin.line,
            };
            self.files.insert((origin.file, span.ordinal), span.clone());
            self.spans.insert(next, (origin.file, span));
            next += 1;
        }
        self.end = self.end - old.len() + count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn missing_provenance_cannot_borrow_a_neighbor_identity() {
        let mut sources = SourceRows::default();
        sources.push(Some(TranscriptOrigin {
            file: [1; 32],
            line: 0,
        }));
        sources.push(None);
        sources.push(Some(TranscriptOrigin {
            file: [2; 32],
            line: 0,
        }));
        assert_eq!(sources.origin(1), None);
        assert_eq!(
            sources.resolve(TranscriptOrigin {
                file: [2; 32],
                line: 0
            }),
            Some(2)
        );
    }

    #[test]
    fn source_identity_can_resume_after_an_unattributed_row() {
        let mut sources = SourceRows::default();
        let first = TranscriptOrigin {
            file: [1; 32],
            line: 0,
        };
        let next = TranscriptOrigin {
            file: [1; 32],
            line: 1,
        };
        sources.push(Some(first));
        sources.push(None);
        sources.push(Some(next));
        assert_eq!(sources.origin(1), None);
        assert_eq!(sources.resolve(first), Some(0));
        assert_eq!(sources.resolve(next), Some(2));
        sources.truncate(2);
        assert_eq!(sources.origin(1), None);
        assert_eq!(sources.resolve(first), Some(0));
        assert_eq!(sources.resolve(next), None);
    }

    #[test]
    #[should_panic(expected = "missing provenance must remain absent")]
    fn negative_control_merging_across_a_gap_borrows_a_neighbor_identity() {
        let mut sources = SourceRows::default();
        sources.push(Some(TranscriptOrigin {
            file: [1; 32],
            line: 0,
        }));
        sources.push(None);
        // The old merge shape extended the prior source through the gap.
        sources.spans.front_mut().unwrap().1.rows.end = 2;
        assert_eq!(
            sources.origin(1),
            None,
            "missing provenance must remain absent"
        );
    }

    proptest! {
        #[test]
        fn arbitrary_source_gaps_interleavings_and_retained_cuts_preserve_identity(
            entries in prop::collection::vec(prop::option::of(any::<bool>()), 1..80),
            keep in any::<prop::sample::Index>(),
            drop in any::<prop::sample::Index>(),
        ) {
            let mut sources = SourceRows::default();
            let mut ordinal = [0;2];
            let oracle: Vec<_> = entries.into_iter().map(|entry| entry.map(|file| {
                let index = usize::from(file);
                let origin = TranscriptOrigin {file: [u8::from(file);32], line: ordinal[index]};
                ordinal[index] += 1;
                origin
            })).collect();
            for origin in &oracle { sources.push(*origin); }
            let mut batch = SourceRows::default();
            batch.replace(0..0, oracle.iter().copied());
            let end = keep.index(oracle.len()+1);
            let start = drop.index(end+1);
            for mut sources in [sources, batch] {
                sources.truncate(end);
                sources.discard(start);
                for (line, origin) in oracle[start..end].iter().enumerate() {
                    prop_assert_eq!(sources.origin(line), *origin);
                    if let Some(origin) = origin { prop_assert_eq!(sources.resolve(*origin), Some(line)); }
                }
                prop_assert_eq!(sources.origin(end-start), None);
            }
        }
        #[test]
        fn replacing_a_tool_block_preserves_every_other_source(
            lengths in prop::collection::vec(1usize..9, 1..24),
            changed in any::<prop::sample::Index>(),
            replacement in 0usize..12,
        ) {
            let selected = changed.index(lengths.len());
            let mut sources = SourceRows::default();
            let mut oracle = Vec::new();
            let mut old = 0..0;
            for (index, count) in lengths.into_iter().enumerate() {
                let start = oracle.len();
                for line in 0..count {
                    let mut file = [0;32];
                    file[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
                    file[31] = u8::from(line != 0);
                    let origin = TranscriptOrigin {file, line: line.saturating_sub(1)};
                    sources.push(Some(origin));
                    oracle.push(origin);
                }
                if index == selected { old = start..oracle.len(); }
            }
            let new = (0..replacement).map(|line| TranscriptOrigin {file: [255;32], line});
            sources.replace(old.clone(), new.clone().map(Some));
            oracle.splice(old, new);
            for (line, origin) in oracle.into_iter().enumerate() {
                prop_assert_eq!(sources.origin(line), Some(origin));
                prop_assert_eq!(sources.resolve(origin), Some(line));
            }
        }

        #[test]
        fn source_ordinals_survive_every_retained_prefix(
            lengths in prop::collection::vec(1usize..9, 1..24),
            removed in any::<prop::sample::Index>(),
        ) {
            let mut sources = SourceRows::default();
            let mut oracle = Vec::new();
            for (index, count) in lengths.into_iter().enumerate() {
                let mut file = [0;32];
                file[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
                oracle.extend((0..count).map(|line| TranscriptOrigin {file, line}));
                sources.extend(file, oracle.len());
            }
            let prefix = removed.index(oracle.len() + 1);
            sources.discard(prefix);
            for (line, origin) in oracle.into_iter().skip(prefix).enumerate() {
                prop_assert_eq!(sources.origin(line), Some(origin));
                prop_assert_eq!(sources.resolve(origin), Some(line));
            }
        }
    }
}
